package io.avery.conveyor;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.stream.Collector;
import java.util.stream.Stream;

/**
 * Interrelated interfaces for expressing flow-controlled processing pipelines, where {@link Sink Sinks} accept elements
 * and {@link Source Sources} yield elements.
 *
 * <p>TODO: Rephrase the README, except more technical-sounding
 */
public class Belt {
    private Belt() {}
    
    // --- Stages ---
    
    /**
     * Sealed interface over {@link Source Source}, {@link Sink Sink}, and {@link Station Station}. Stages have
     * different input/output configurations, or "shapes", which can be connected to form a processing pipeline.
     *
     * <ul>
     *     <li>{@code Source} - yields output elements
     *     <li>{@code Sink} - accepts input elements
     *     <li>{@code Station} - connects {@code Sources} to {@code Sinks}
     * </ul>
     *
     * (For a "stage" that accepts input elements and yields output elements, see {@link Segue Segue}.)
     *
     * <p>Stages contain zero or more "station {@link #tasks tasks}", which drain from a station's source to its sink.
     */
    public sealed interface Stage {
        /**
         * Returns a stream of station tasks enclosed by this stage. Each station task proceeds by:
         *
         * <ol>
         *     <li>draining the source to the sink
         *     <li>completing the sink normally if no exception was thrown
         *     <li>closing the source
         *     <li>completing the sink abruptly if an exception was thrown, suppressing further exceptions
         * </ol>
         *
         * <p>If any of steps 1-3 throw an exception, the initial exception is caught and re-thrown at the end of the
         * task.
         *
         * <p>If the initial exception is an {@link InterruptedException}, or if any completions throw
         * {@code InterruptedException}, the thread interrupt status will be set when the exception is caught, and will
         * remain set until any subsequent throw of {@code InterruptedException}. (This ensures that recovery operators
         * see the interrupt, and do not unintentionally interfere with interrupt responsiveness.)
         *
         * @implSpec A stage that delegates to other stages should include their {@code tasks} in its own.
         *
         * <p>The default implementation returns an empty stream.
         */
        default Stream<Callable<Void>> tasks() { return Stream.empty(); }
    }
    
    /**
     * A {@link Stage Stage} that represents connected {@link Source Source} and {@link Sink Sink} stages. A station may
     * enclose:
     *
     * <ul>
     *     <li>A {@code StepSource} connected to a {@code Sink}
     *     <li>A {@code Source} connected to a {@code StepSink}
     * </ul>
     *
     * <p>A station itself accepts no input and yields no output. Instead, the station encloses a {@code Callable} task
     * that will drain elements from the station's source to its sink, then complete the sink and close the source. The
     * {@link #tasks tasks} method exposes this task, along with tasks from any upstream or downstream stations that
     * were connected to form this station. These tasks should generally be concurrently run, in case sources or sinks
     * connect across a boundary, to avoid the effects of unmitigated buffer depletion or saturation (including
     * potential deadlock).
     *
     * @see #tasks()
     */
    public sealed interface Station extends Stage permits Belts.ClosedStation, Belts.ChainStation {
        /**
         * Connects the {@code upstream} station before this station. Returns a new station that includes the
         * {@code upstream} station's {@link #tasks tasks} before this station's tasks.
         *
         * @param upstream the upstream station
         * @return a new station that includes the upstream station's tasks before this station's tasks
         * @throws NullPointerException if upstream is null
         */
        default Station compose(Station upstream) {
            return new Belts.ChainStation(upstream, this);
        }
        
        /**
         * Connects the {@code upstream} sink before this station. Returns a new sink that behaves like the
         * {@code upstream} sink, except that its {@link #tasks tasks} will also include this station's tasks.
         *
         * @param upstream the upstream sink
         * @return a new sink that also includes this station's tasks
         * @param <T> the upstream sink element type
         * @throws NullPointerException if upstream is null
         */
        default <T> Sink<T> compose(Sink<? super T> upstream) {
            return new Belts.ChainSink<>(upstream, this);
        }
        
        /**
         * Connects the {@code upstream} sink before this station. Returns a new sink that behaves like the
         * {@code upstream} sink, except that its {@link #tasks tasks} will also include this station's tasks.
         *
         * @param upstream the upstream sink
         * @return a new sink that also includes this station's tasks
         * @param <T> the upstream sink element type
         * @throws NullPointerException if upstream is null
         */
        default <T> StepSink<T> compose(StepSink<? super T> upstream) {
            return new Belts.ChainStepSink<>(upstream, this);
        }
        
        /**
         * Connects the {@code downstream} station after this station. Returns a new station that includes the
         * {@code downstream} station's {@link #tasks tasks} after this station's tasks.
         *
         * @param downstream the downstream station
         * @return a new station that includes the downstream station's tasks after this station's tasks
         * @throws NullPointerException if downstream is null
         */
        default Station andThen(Station downstream) {
            return new Belts.ChainStation(this, downstream);
        }
        
        /**
         * Connects the {@code downstream} source after this station. Returns a new source that behaves like the
         * {@code downstream} source, except that its {@link #tasks tasks} will also include this station's tasks.
         *
         * @param downstream the downstream source
         * @return a new source that also includes this station's tasks
         * @param <T> the downstream source element type
         * @throws NullPointerException if downstream is null
         */
        default <T> Source<T> andThen(Source<? extends T> downstream) {
            return new Belts.ChainSource<>(this, downstream);
        }
        
        /**
         * Connects the {@code downstream} source after this station. Returns a new source that behaves like the
         * {@code downstream} source, except that its {@link #tasks tasks} will also include this station's tasks.
         *
         * @param downstream the downstream source
         * @return a new source that also includes this station's tasks
         * @param <T> the downstream source element type
         * @throws NullPointerException if downstream is null
         */
        default <T> StepSource<T> andThen(StepSource<? extends T> downstream) {
            return new Belts.ChainStepSource<>(this, downstream);
        }
    }
    
    /**
     * A {@link Stage Stage} that accepts input elements.
     *
     * <p>A sink may include {@link #tasks tasks} from downstream stations. Such tasks should generally be concurrently
     * run while accepting elements, in case the sink connects across a downstream boundary, to avoid the effects of
     * unmitigated buffer saturation (including potential deadlock).
     *
     * <p>This is a functional interface whose functional method is {@link #drainFromSource(StepSource)}.
     *
     * @param <In> the input element type
     */
    @FunctionalInterface
    public non-sealed interface Sink<In> extends Stage {
        /**
         * Pulls as many elements as possible from the source to this sink. This proceeds until either an exception is
         * thrown, this sink cancels, or this sink is unable to accept more elements from the source (which does not
         * necessarily mean the source drained). Returns {@code true} if the source definitely drained, meaning a call
         * to {@link StepSource#pull pull} returned {@code null}; else returns {@code false}.
         *
         * @implSpec Implementors should restrict to {@link StepSource#pull pulling} from the source. Closing the source
         * is the caller's responsibility, as the source may be reused after this method is called.
         *
         * <p>Implementors should document if they concurrently pull from the source, as this is not safe for all
         * sources.
         *
         * @param source the source to drain from
         * @return {@code true} if the source drained
         * @throws Exception if unable to drain
         */
        boolean drainFromSource(StepSource<? extends In> source) throws Exception;
        
        /**
         * Signals any nearest downstream boundary sources to stop yielding elements that arrive after this signal.
         *
         * @implSpec A linked boundary sink should implement its {@link StepSink#push push} and
         * {@link #drainFromSource drainFromSource} methods to discard elements and return {@code false} after this
         * method is called, to prevent unbounded buffering or deadlock. The linked boundary source should return
         * {@code null} from {@link StepSource#pull pull} and {@code false} from {@link Source#drainToSink drainToSink}
         * after yielding all values that arrived before it received this signal.
         *
         * <p>A sink that delegates to downstream sinks should call {@code complete} on each downstream sink before
         * returning from this method, unless this method throws before completing any sinks. If completing any sink
         * throws an exception, subsequent exceptions should be suppressed onto the first exception. If completing any
         * sink throws {@link InterruptedException}, the thread interrupt status should be set when the exception is
         * caught, and remain set until any subsequent throw of {@code InterruptedException} (in accordance with
         * {@link #tasks Stage.tasks}). The utility method {@link Belts#composedComplete(Stream)} is provided for common
         * use cases.
         *
         * <p>The default implementation does nothing.
         *
         * @throws Exception if unable to complete
         */
        default void complete() throws Exception { }
        
        /**
         * Signals any nearest downstream boundary sources to stop yielding elements and throw
         * {@link UpstreamException}.
         *
         * @implSpec A linked boundary sink should implement its {@link StepSink#push push} and
         * {@link #drainFromSource drainFromSource} methods to discard elements and return {@code false} after this
         * method is called, to prevent unbounded buffering or deadlock. The linked boundary source should throw an
         * {@link UpstreamException}, wrapping the cause passed to this method, upon initiating any subsequent calls to
         * {@link StepSource#pull pull} or subsequent pushes in {@link Source#drainToSink drainToSink}.
         *
         * <p>A sink that delegates to downstream sinks should call {@code completeAbruptly} on each downstream sink
         * before returning from this method, <strong>even if this method throws</strong>. If completing any sink throws
         * an exception, subsequent exceptions should be suppressed onto the first exception. If completing any sink
         * throws {@link InterruptedException}, the thread interrupt status should be set when the exception is caught,
         * and remain set until any subsequent throw of {@code InterruptedException} (in accordance with
         * {@link #tasks Stage.tasks}). The utility method {@link Belts#composedCompleteAbruptly(Stream, Throwable)} is
         * provided for common use cases.
         *
         * <p>The default implementation does nothing.
         *
         * @param cause the causal exception
         * @throws Exception if unable to complete
         */
        default void completeAbruptly(Throwable cause) throws Exception { }
        
        /**
         * Connects the {@code upstream} source before this sink. Returns a new station that includes a
         * {@link #tasks task} to drain from the {@code upstream} source to this sink.
         *
         * @param upstream the upstream source
         * @return a new station that will drain from the source to this sink
         * @throws NullPointerException if upstream is null
         */
        default Station compose(StepSource<? extends In> upstream) {
            return new Belts.ClosedStation<>(upstream, this);
        }
        
        /**
         * Connects the {@code upstream} segue before this sink. Returns a new sink that behaves like the segue's sink,
         * except that it includes a {@link #tasks task} to drain from the segue's source to this sink.
         *
         * @param upstream the upstream segue
         * @return a new sink that also includes a downstream task
         * @param <T> the upstream sink element type
         * @throws NullPointerException if upstream is null
         */
        default <T> Sink<T> compose(SinkStepSource<? super T, ? extends In> upstream) {
            return new Belts.ChainSink<>(upstream.sink(), new Belts.ClosedStation<>(upstream.source(), this));
        }
        
        /**
         * Connects the {@code upstream} segue before this sink. Returns a new sink that behaves like the segue's sink,
         * except that it includes a {@link #tasks task} to drain from the segue's source to this sink.
         *
         * @param upstream the upstream segue
         * @return a new sink that also includes a downstream task
         * @param <T> the upstream sink element type
         * @throws NullPointerException if upstream is null
         */
        default <T> StepSink<T> compose(StepSegue<? super T, ? extends In> upstream) {
            return new Belts.ChainStepSink<>(upstream.sink(), new Belts.ClosedStation<>(upstream.source(), this));
        }
        
        /**
         * Connects the {@code downstream} station after this sink. Returns a new sink that behaves like this sink,
         * except that it includes {@link #tasks tasks} from the {@code downstream} station.
         *
         * @param downstream the downstream station
         * @return a new sink that also includes tasks from a downstream station
         * @throws NullPointerException if downstream is null
         */
        default Sink<In> andThen(Station downstream) {
            return new Belts.ChainSink<>(this, downstream);
        }
        
        /**
         * Connects the {@code downstream} source after this sink. Returns a new segue that pairs this sink with the
         * {@code downstream} source.
         *
         * @param downstream the downstream source
         * @return a new segue that pairs this sink with the {@code downstream} source
         * @param <T> the downstream source element type
         * @throws NullPointerException if downstream is null
         */
        default <T> Segue<In, T> andThen(Source<? extends T> downstream) {
            return new Belts.ChainSegue<>(this, downstream);
        }
        
        /**
         * Connects the {@code downstream} source after this sink. Returns a new segue that pairs this sink with the
         * {@code downstream} source.
         *
         * @param downstream the downstream source
         * @return a new segue that pairs this sink with the {@code downstream} source
         * @param <T> the downstream source element type
         * @throws NullPointerException if downstream is null
         */
        default <T> SinkStepSource<In, T> andThen(StepSource<? extends T> downstream) {
            return new Belts.ChainSinkStepSource<>(this, downstream);
        }
        
        /**
         * Connects the {@code upstream} operator before this sink. Returns an upstream sink obtained by applying the
         * {@code upstream} operator to this sink.
         *
         * @param upstream a function that creates an upstream sink from a downstream sink
         * @return an upstream sink obtained by applying the {@code upstream} operator to this sink
         * @param <T> the upstream sink element type
         * @throws NullPointerException if upstream is null
         */
        @SuppressWarnings("unchecked")
        default <T> Sink<T> compose(SinkOperator<? super T, ? extends In> upstream) {
            return (Sink<T>) upstream.andThen(this);
        }
        
        /**
         * Connects the {@code upstream} operator before this sink. Returns an upstream sink obtained by applying the
         * {@code upstream} operator to this sink.
         *
         * @param upstream a function that creates an upstream sink from a downstream sink
         * @return an upstream sink obtained by applying the {@code upstream} operator to this sink
         * @param <T> the upstream sink element type
         * @throws NullPointerException if upstream is null
         */
        @SuppressWarnings("unchecked")
        default <T> StepSink<T> compose(StepToSinkOperator<? super T, ? extends In> upstream) {
            return (StepSink<T>) upstream.andThen(this);
        }
    }
    
    /**
     * A {@link Stage Stage} that yields output elements.
     *
     * <p>A source may include {@link #tasks tasks} from upstream stations. Such tasks should generally be concurrently
     * run while yielding elements, in case the source connects across an upstream boundary, to avoid the effects of
     * unmitigated buffer depletion (including potential deadlock).
     *
     * <p>This is a functional interface whose functional method is {@link #drainToSink(StepSink)}.
     *
     * @param <Out> the output element type
     */
    @FunctionalInterface
    public non-sealed interface Source<Out> extends Stage, AutoCloseable {
        /**
         * Pushes as many elements as possible from this source to the sink. This proceeds until either an exception is
         * thrown, this source is drained, or this source is unable to push more elements to the sink (which does not
         * necessarily mean the sink cancelled). Returns {@code false} if the sink definitely cancelled, meaning a call
         * to {@link StepSink#push push} returned {@code false}; else returns {@code true}.
         *
         * @implSpec Implementors should restrict to {@link StepSink#push pushing} to the sink. Completing the sink is
         * the caller's responsibility, as the sink may be reused after this method is called.
         *
         * <p>Implementors should document if they concurrently push to the sink, as this is not safe for all sinks.
         *
         * @param sink the sink to drain to
         * @return {@code false} if the sink cancelled
         * @throws Exception if unable to drain
         */
        boolean drainToSink(StepSink<? super Out> sink) throws Exception;
        
        /**
         * Relinquishes any underlying resources held by this source.
         *
         * @implSpec Calling this method may cause this source to stop yielding elements from
         * {@link StepSource#pull pull} and {@link #drainToSink drainToSink}. In that case, if this is a linked boundary
         * source, the linked boundary sink should implement its {@link StepSink#push push} and
         * {@link Sink#drainFromSource drainFromSource} methods to discard elements and return {@code false} after this
         * method is called, to prevent unbounded buffering or deadlock.
         *
         * <p>A source that delegates to upstream sources should call {@code close} on each upstream source before
         * returning from this method, <strong>even if this method throws</strong>. If closing any source throws an
         * exception, subsequent exceptions should be suppressed onto the first exception. The utility method
         * {@link Belts#composedClose(Stream)} is provided for common use cases.
         *
         * <p>The default implementation does nothing.
         *
         * @throws Exception if unable to close
         */
        default void close() throws Exception { }
        
        /**
         * Performs the given action for each remaining element of the source. This proceeds until either an exception
         * is thrown, this source is drained, or this source is unable to push more elements to the {@code Consumer}.
         *
         * @param action the action to be performed for each element
         * @throws Exception if unable to drain
         */
        default void forEach(Consumer<? super Out> action) throws Exception {
            Objects.requireNonNull(action);
            
            class ConsumerSink implements StepSink<Out> {
                @Override
                public boolean push(Out input) {
                    action.accept(input);
                    return true;
                }
            }
            
            drainToSink(new ConsumerSink());
        }
        
        /**
         * Performs a mutable reduction operation on the remaining elements of this source using a {@code Collector}.
         * This proceeds until either an exception is thrown, this source is drained, or this source is unable to push
         * more elements to the {@code Collector}.
         *
         * @see Stream#collect(Collector)
         *
         * @param collector the {@code Collector} describing the reduction
         * @return the result of the reduction
         * @param <A> the intermediate accumulation type of the {@code Collector}
         * @param <R> the type of the result
         * @throws Exception if unable to drain
         */
        default <A, R> R collect(Collector<? super Out, A, R> collector) throws Exception {
            var accumulator = collector.accumulator();
            var finisher = collector.finisher();
            A acc = collector.supplier().get();
            
            class CollectorSink implements StepSink<Out> {
                @Override
                public boolean push(Out input) {
                    accumulator.accept(acc, input);
                    return true;
                }
            }
            
            drainToSink(new CollectorSink());
            return finisher.apply(acc);
        }
        
        /**
         * Connects the {@code upstream} station before this source. Returns a new source that behaves like this source,
         * except that it includes {@link #tasks tasks} from the {@code upstream} station.
         *
         * @param upstream the upstream station
         * @return a new source that also includes tasks from an upstream station
         * @throws NullPointerException if upstream is null
         */
        default Source<Out> compose(Station upstream) {
            return new Belts.ChainSource<>(upstream, this);
        }
        
        /**
         * Connects the {@code upstream} sink before this source. Returns a new segue that pairs the {@code upstream}
         * sink with this source.
         *
         * @param upstream the upstream sink
         * @return a new segue that pairs the {@code upstream} sink with this source
         * @param <T> the upstream sink element type
         * @throws NullPointerException if upstream is null
         */
        default <T> Segue<T, Out> compose(Sink<? super T> upstream) {
            return new Belts.ChainSegue<>(upstream, this);
        }
        
        /**
         * Connects the {@code upstream} sink before this source. Returns a new segue that pairs the {@code upstream}
         * sink with this source.
         *
         * @param upstream the upstream sink
         * @return a new segue that pairs the {@code upstream} sink with this source
         * @param <T> the upstream sink element type
         * @throws NullPointerException if upstream is null
         */
        default <T> StepSinkSource<T, Out> compose(StepSink<? super T> upstream) {
            return new Belts.ChainStepSinkSource<>(upstream, this);
        }
        
        /**
         * Connects the {@code downstream} sink after this source. Returns a new station that includes a
         * {@link #tasks task} to drain from this source to the {@code downstream} sink.
         *
         * @param downstream the downstream sink
         * @return a new station that will drain from this source to the sink
         * @throws NullPointerException if downstream is null
         */
        default Station andThen(StepSink<? super Out> downstream) {
            return new Belts.ClosedStation<>(this, downstream);
        }
        
        /**
         * Connects the {@code downstream} segue after this source. Returns a new source that behaves like the segue's
         * source, except that it includes a {@link #tasks task} to drain from this source to the segue's sink.
         *
         * @param downstream the downstream segue
         * @return a new source that also includes an upstream task
         * @param <T> the downstream source element type
         * @throws NullPointerException if downstream is null
         */
        default <T> Source<T> andThen(StepSinkSource<? super Out, ? extends T> downstream) {
            return new Belts.ChainSource<>(new Belts.ClosedStation<>(this, downstream.sink()), downstream.source());
        }
        
        /**
         * Connects the {@code downstream} segue after this source. Returns a new source that behaves like the segue's
         * source, except that it includes a {@link #tasks task} to drain from this source to the segue's sink.
         *
         * @param downstream the downstream segue
         * @return a new source that also includes an upstream task
         * @param <T> the downstream source element type
         * @throws NullPointerException if downstream is null
         */
        default <T> StepSource<T> andThen(StepSegue<? super Out, ? extends T> downstream) {
            return new Belts.ChainStepSource<>(new Belts.ClosedStation<>(this, downstream.sink()), downstream.source());
        }
        
        /**
         * Connects the {@code downstream} operator after this source. Returns a downstream source obtained by applying
         * the {@code downstream} operator to this source.
         *
         * @param downstream a function that creates a downstream source from an upstream source
         * @return a downstream source obtained by applying the {@code downstream} operator to this source
         * @param <T> the downstream source element type
         * @throws NullPointerException if downstream is null
         */
        @SuppressWarnings("unchecked")
        default <T> Source<T> andThen(SourceOperator<? super Out, ? extends T> downstream) {
            return (Source<T>) downstream.compose(this);
        }
        
        /**
         * Connects the {@code downstream} operator after this source. Returns a downstream source obtained by applying
         * the {@code downstream} operator to this source.
         *
         * @param downstream a function that creates a downstream source from an upstream source
         * @return a downstream source obtained by applying the {@code downstream} operator to this source
         * @param <T> the downstream source element type
         * @throws NullPointerException if downstream is null
         */
        @SuppressWarnings("unchecked")
        default <T> StepSource<T> andThen(SourceToStepOperator<? super Out, ? extends T> downstream) {
            return (StepSource<T>) downstream.compose(this);
        }
    }
    
    /**
     * A {@link Sink Sink} that can accept input elements one at a time.
     *
     * <p>This is a functional interface whose functional method is {@link #push(Object)}.
     *
     * @param <In> the input element type
     */
    @FunctionalInterface
    public interface StepSink<In> extends Sink<In> {
        /**
         * Pushes the input element to this sink for processing. Returns {@code false} if this sink cancelled during or
         * prior to this call, in which case the element may not have been fully processed.
         *
         * @implSpec Once this method returns {@code false}, subsequent calls should also discard the input element and
         * return {@code false}, to indicate the sink is permanently cancelled and no longer accepting elements. This
         * method should not permit {@code null} inputs.
         *
         * @param input the input element
         * @return {@code false} if this sink cancelled, else {@code true}
         * @throws NullPointerException if input is null
         * @throws Exception if unable to push
         */
        boolean push(In input) throws Exception;
        
        /**
         * {@inheritDoc}
         *
         * @implSpec The implementation loops, pulling from the source and pushing to this sink, until either the
         * source drains or this sink cancels. This is equivalent to the implementation of
         * {@link StepSource#drainToSink StepSource.drainToSink(StepSink)}.
         */
        @Override
        default boolean drainFromSource(StepSource<? extends In> source) throws Exception {
            for (In e; (e = source.pull()) != null; ) {
                if (!push(e)) {
                    return false;
                }
            }
            return true;
        }
        
        /**
         * Connects the {@code upstream} source before this sink. Returns a new station that includes a
         * {@link #tasks task} to drain from the {@code upstream} source to this sink.
         *
         * @param upstream the upstream source
         * @return a new station that will drain from the source to this sink
         * @throws NullPointerException if upstream is null
         */
        default Station compose(Source<? extends In> upstream) {
            return new Belts.ClosedStation<>(upstream, this);
        }
        
        /**
         * Connects the {@code upstream} segue before this sink. Returns a new sink that behaves like the segue's sink,
         * except that it includes a {@link #tasks task} to drain from the segue's source to this sink.
         *
         * @param upstream the upstream segue
         * @return a new sink that also includes a downstream task
         * @param <T> the upstream sink element type
         * @throws NullPointerException if upstream is null
         */
        default <T> Sink<T> compose(Segue<? super T, ? extends In> upstream) {
            return new Belts.ChainSink<>(upstream.sink(), new Belts.ClosedStation<>(upstream.source(), this));
        }
        
        /**
         * Connects the {@code upstream} segue before this sink. Returns a new sink that behaves like the segue's sink,
         * except that it includes a {@link #tasks task} to drain from the segue's source to this sink.
         *
         * @param upstream the upstream segue
         * @return a new sink that also includes a downstream task
         * @param <T> the upstream sink element type
         * @throws NullPointerException if upstream is null
         */
        default <T> StepSink<T> compose(StepSinkSource<? super T, ? extends In> upstream) {
            return new Belts.ChainStepSink<>(upstream.sink(), new Belts.ClosedStation<>(upstream.source(), this));
        }
        
        @Override
        default StepSink<In> andThen(Station downstream) {
            return new Belts.ChainStepSink<>(this, downstream);
        }
        
        @Override
        default <T> StepSinkSource<In, T> andThen(Source<? extends T> downstream) {
            return new Belts.ChainStepSinkSource<>(this, downstream);
        }
        
        @Override
        default <T> StepSegue<In, T> andThen(StepSource<? extends T> downstream) {
            return new Belts.ChainStepSegue<>(this, downstream);
        }
        
        /**
         * Connects the {@code upstream} operator before this sink. Returns an upstream sink obtained by applying the
         * {@code upstream} operator to this sink.
         *
         * @param upstream a function that creates an upstream sink from a downstream sink
         * @return an upstream sink obtained by applying the {@code upstream} operator to this sink
         * @param <T> the upstream sink element type
         * @throws NullPointerException if upstream is null
         */
        @SuppressWarnings("unchecked")
        default <T> Sink<T> compose(SinkToStepOperator<? super T, ? extends In> upstream) {
            return (Sink<T>) upstream.andThen(this);
        }
        
        /**
         * Connects the {@code upstream} operator before this sink. Returns an upstream sink obtained by applying the
         * {@code upstream} operator to this sink.
         *
         * @param upstream a function that creates an upstream sink from a downstream sink
         * @return an upstream sink obtained by applying the {@code upstream} operator to this sink
         * @param <T> the upstream sink element type
         * @throws NullPointerException if upstream is null
         */
        @SuppressWarnings("unchecked")
        default <T> StepSink<T> compose(StepSinkOperator<? super T, ? extends In> upstream) {
            return (StepSink<T>) upstream.andThen(this);
        }
    }
    
    /**
     * A {@link Source Source} that can yield output elements one at a time.
     *
     * <p>This is a functional interface whose functional method is {@link #pull()}.
     *
     * @param <Out> the output element type
     */
    @FunctionalInterface
    public interface StepSource<Out> extends Source<Out> {
        /**
         * Pulls this source for the next element. Returns {@code null} if this source is drained.
         *
         * @implSpec Once this method returns {@code null}, subsequent calls should also return {@code null}, to indicate
         * the source is permanently drained and no longer yielding elements.
         *
         * @return the next element from this source, or {@code null} if this source is drained
         * @throws Exception if unable to pull
         */
        Out pull() throws Exception;
        
        /**
         * {@inheritDoc}
         *
         * @implSpec The implementation loops, pulling from this source and pushing to the sink, until either this
         * source drains or the sink cancels. This is equivalent to the implementation of
         * {@link StepSink#drainFromSource StepSink.drainFromSource(StepSource)}.
         */
        @Override
        default boolean drainToSink(StepSink<? super Out> sink) throws Exception {
            for (Out e; (e = pull()) != null; ) {
                if (!sink.push(e)) {
                    return false;
                }
            }
            return true;
        }
        
        @Override
        default StepSource<Out> compose(Station upstream) {
            return new Belts.ChainStepSource<>(upstream, this);
        }
        
        @Override
        default <T> SinkStepSource<T, Out> compose(Sink<? super T> upstream) {
            return new Belts.ChainSinkStepSource<>(upstream, this);
        }
        
        @Override
        default <T> StepSegue<T, Out> compose(StepSink<? super T> upstream) {
            return new Belts.ChainStepSegue<>(upstream, this);
        }
        
        /**
         * Connects the {@code downstream} sink after this source. Returns a new station that includes a
         * {@link #tasks task} to drain from this source to the {@code downstream} sink.
         *
         * @param downstream the downstream sink
         * @return a new station that will drain from this source to the sink
         * @throws NullPointerException if downstream is null
         */
        default Station andThen(Sink<? super Out> downstream) {
            return new Belts.ClosedStation<>(this, downstream);
        }
        
        /**
         * Connects the {@code downstream} segue after this source. Returns a new source that behaves like the segue's
         * source, except that it includes a {@link #tasks task} to drain from this source to the segue's sink.
         *
         * @param downstream the downstream segue
         * @return a new source that also includes an upstream task
         * @param <T> the downstream source element type
         * @throws NullPointerException if downstream is null
         */
        default <T> Source<T> andThen(Segue<? super Out, ? extends T> downstream) {
            return new Belts.ChainSource<>(new Belts.ClosedStation<>(this, downstream.sink()), downstream.source());
        }
        
        /**
         * Connects the {@code downstream} segue after this source. Returns a new source that behaves like the segue's
         * source, except that it includes a {@link #tasks task} to drain from this source to the segue's sink.
         *
         * @param downstream the downstream segue
         * @return a new source that also includes an upstream task
         * @param <T> the downstream source element type
         * @throws NullPointerException if downstream is null
         */
        default <T> StepSource<T> andThen(SinkStepSource<? super Out, ? extends T> downstream) {
            return new Belts.ChainStepSource<>(new Belts.ClosedStation<>(this, downstream.sink()), downstream.source());
        }
        
        /**
         * Connects the {@code downstream} operator after this source. Returns a downstream source obtained by applying
         * the {@code downstream} operator to this source.
         *
         * @param downstream a function that creates a downstream source from an upstream source
         * @return a downstream source obtained by applying the {@code downstream} operator to this source
         * @param <T> the downstream source element type
         * @throws NullPointerException if downstream is null
         */
        @SuppressWarnings("unchecked")
        default <T> Source<T> andThen(StepToSourceOperator<? super Out, ? extends T> downstream) {
            return (Source<T>) downstream.compose(this);
        }
        
        /**
         * Connects the {@code downstream} operator after this source. Returns a downstream source obtained by applying
         * the {@code downstream} operator to this source.
         *
         * @param downstream a function that creates a downstream source from an upstream source
         * @return a downstream source obtained by applying the {@code downstream} operator to this source
         * @param <T> the downstream source element type
         * @throws NullPointerException if downstream is null
         */
        @SuppressWarnings("unchecked")
        default <T> StepSource<T> andThen(StepSourceOperator<? super Out, ? extends T> downstream) {
            return (StepSource<T>) downstream.compose(this);
        }
    }
    
    // --- Segues ---
    
    /**
     * A {@link Sink Sink} paired with a {@link Source Source}.
     *
     * <p>The sink and source need not be related. However, it is common for the sink and source to be internally
     * "linked", as by shared state, such that the elements input to the sink determine or influence the elements output
     * from the source. This allows data to transition across the "asynchronous boundary" between threads, if the
     * thread(s) draining to the sink differ from the thread(s) draining from the source.
     *
     * @param <In> the input element type
     * @param <Out> the output element type
     */
    public interface Segue<In, Out> {
        /**
         * Returns the {@link Sink sink} side of this segue.
         * @return the sink side of this segue
         */
        Sink<In> sink();
        
        /**
         * Returns the {@link Source source} side of this segue.
         * @return the source side of this segue
         */
        Source<Out> source();
        
        /**
         * Connects the {@code upstream} source before this segue. Returns a new source that behaves like this segue's
         * source, except that it includes a {@link Stage#tasks task} to drain from the {@code upstream} source to this
         * segue's sink.
         *
         * @param upstream the upstream source
         * @return a new source that also includes an upstream task
         * @throws NullPointerException if upstream is null
         */
        default Source<Out> compose(StepSource<? extends In> upstream) {
            return new Belts.ChainSource<>(new Belts.ClosedStation<>(upstream, sink()), source());
        }
        
        /**
         * Connects the {@code upstream} segue before this segue. Returns a new segue that pairs the {@code upstream}
         * segue's sink with a new source that behaves like this segue's source, except that it includes a
         * {@link Stage#tasks task} to drain the interior source to sink.
         *
         * <p>This method behaves equivalently to:
         * {@snippet :
         * this.compose(upstream.source()).compose(upstream.sink())
         * }
         *
         * @param upstream the upstream segue
         * @return a new segue whose source also includes an upstream task
         * @param <T> the upstream sink element type
         * @throws NullPointerException if upstream is null
         */
        default <T> Segue<T, Out> compose(SinkStepSource<? super T, ? extends In> upstream) {
            return new Belts.ChainSegue<>(upstream.sink(), new Belts.ChainSource<>(new Belts.ClosedStation<>(upstream.source(), sink()), source()));
        }
        
        /**
         * Connects the {@code upstream} segue before this segue. Returns a new segue that pairs the {@code upstream}
         * segue's sink with a new source that behaves like this segue's source, except that it includes a
         * {@link Stage#tasks task} to drain the interior source to sink.
         *
         * <p>This method behaves equivalently to:
         * {@snippet :
         * this.compose(upstream.source()).compose(upstream.sink())
         * }
         *
         * @param upstream the upstream segue
         * @return a new segue whose source also includes an upstream task
         * @param <T> the upstream sink element type
         * @throws NullPointerException if upstream is null
         */
        default <T> StepSinkSource<T, Out> compose(StepSegue<? super T, ? extends In> upstream) {
            return new Belts.ChainStepSinkSource<>(upstream.sink(), new Belts.ChainSource<>(new Belts.ClosedStation<>(upstream.source(), sink()), source()));
        }
        
        /**
         * Connects the {@code downstream} sink after this segue. Returns a new sink that behaves like this segue's
         * sink, except that it includes a {@link Stage#tasks task} to drain from this segue's source to the
         * {@code downstream} sink.
         *
         * @param downstream the downstream sink
         * @return a new sink that also includes a downstream task
         * @throws NullPointerException if downstream is null
         */
        default Sink<In> andThen(StepSink<? super Out> downstream) {
            return new Belts.ChainSink<>(sink(), new Belts.ClosedStation<>(source(), downstream));
        }
        
        /**
         * Connects the {@code downstream} segue after this segue. Returns a new segue that pairs the {@code downstream}
         * segue's source with a new sink that behaves like this segue's sink, except that it includes a
         * {@link Stage#tasks task} to drain the interior source to sink.
         *
         * <p>This method behaves equivalently to:
         * {@snippet :
         * this.andThen(downstream.sink()).andThen(downstream.source())
         * }
         *
         * @param downstream the downstream segue
         * @return a new segue whose sink also includes a downstream task
         * @param <T> the downstream source element type
         * @throws NullPointerException if downstream is null
         */
        default <T> Segue<In, T> andThen(StepSinkSource<? super Out, ? extends T> downstream) {
            return new Belts.ChainSegue<>(new Belts.ChainSink<>(sink(), new Belts.ClosedStation<>(source(), downstream.sink())), downstream.source());
        }
        
        /**
         * Connects the {@code downstream} segue after this segue. Returns a new segue that pairs the {@code downstream}
         * segue's source with a new sink that behaves like this segue's sink, except that it includes a
         * {@link Stage#tasks task} to drain the interior source to sink
         *
         * <p>This method behaves equivalently to:
         * {@snippet :
         * this.andThen(downstream.sink()).andThen(downstream.source())
         * }
         *
         * @param downstream the downstream segue
         * @return a new segue whose sink also includes a downstream task
         * @param <T> the downstream source element type
         * @throws NullPointerException if downstream is null
         */
        default <T> SinkStepSource<In, T> andThen(StepSegue<? super Out, ? extends T> downstream) {
            return new Belts.ChainSinkStepSource<>(new Belts.ChainSink<>(sink(), new Belts.ClosedStation<>(source(), downstream.sink())), downstream.source());
        }
        
        /**
         * Connects the {@code upstream} operator before this segue. Returns a new segue that pairs this segue's source
         * with the upstream sink obtained by applying the {@code upstream} operator to this segue's sink.
         *
         * @param upstream a function that creates an upstream sink from a downstream sink
         * @return a new segue with sink transformed
         * @param <T> the upstream sink element type
         * @throws NullPointerException if upstream is null
         */
        @SuppressWarnings("unchecked")
        default <T> Segue<T, Out> compose(SinkOperator<? super T, ? extends In> upstream) {
            return (Segue<T, Out>) upstream.andThen(sink()).andThen(source());
        }
        
        /**
         * Connects the {@code upstream} operator before this segue. Returns a new segue that pairs this segue's source
         * with the upstream sink obtained by applying the {@code upstream} operator to this segue's sink.
         *
         * @param upstream a function that creates an upstream sink from a downstream sink
         * @return a new segue with sink transformed
         * @param <T> the upstream sink element type
         * @throws NullPointerException if upstream is null
         */
        @SuppressWarnings("unchecked")
        default <T> StepSinkSource<T, Out> compose(StepToSinkOperator<? super T, ? extends In> upstream) {
            return (StepSinkSource<T, Out>) upstream.andThen(sink()).andThen(source());
        }
        
        /**
         * Connects the {@code downstream} operator after this segue. Returns a new segue that pairs this segue's sink
         * with the downstream source obtained by applying the {@code downstream} operator to this segue's source.
         *
         * @param downstream a function that creates a downstream source from an upstream source
         * @return a new segue with source transformed
         * @param <T> the downstream source element type
         * @throws NullPointerException if downstream is null
         */
        default <T> Segue<In, T> andThen(SourceOperator<? super Out, ? extends T> downstream) {
            return sink().andThen(downstream.compose(source()));
        }
        
        /**
         * Connects the {@code downstream} operator after this segue. Returns a new segue that pairs this segue's sink
         * with the downstream source obtained by applying the {@code downstream} operator to this segue's source.
         *
         * @param downstream a function that creates a downstream source from an upstream source
         * @return a new segue with source transformed
         * @param <T> the downstream source element type
         * @throws NullPointerException if downstream is null
         */
        default <T> SinkStepSource<In, T> andThen(SourceToStepOperator<? super Out, ? extends T> downstream) {
            return sink().andThen(downstream.compose(source()));
        }
    }
    
    /**
     * A {@link Segue Segue} where the sink is a {@link StepSink StepSink}.
     *
     * @param <In> the input element type
     * @param <Out> the output element type
     */
    public interface StepSinkSource<In, Out> extends Segue<In, Out> {
        @Override
        StepSink<In> sink();
        
        /**
         * Connects the {@code upstream} source before this segue. Returns a new source that behaves like this segue's
         * source, except that it includes a {@link Stage#tasks task} to drain from the {@code upstream} source to this
         * segue's sink.
         *
         * @param upstream the upstream source
         * @return a new source that also includes an upstream task
         * @throws NullPointerException if upstream is null
         */
        default Source<Out> compose(Source<? extends In> upstream) {
            return new Belts.ChainSource<>(new Belts.ClosedStation<>(upstream, sink()), source());
        }
        
        /**
         * Connects the {@code upstream} segue before this segue. Returns a new segue that pairs the {@code upstream}
         * segue's sink with a new source that behaves like this segue's source, except that it includes a
         * {@link Stage#tasks task} to drain the interior source to sink.
         *
         * <p>This method behaves equivalently to:
         * {@snippet :
         * this.compose(upstream.source()).compose(upstream.sink())
         * }
         *
         * @param upstream the upstream segue
         * @return a new segue whose source also includes an upstream task
         * @param <T> the upstream sink element type
         * @throws NullPointerException if upstream is null
         */
        default <T> Segue<T, Out> compose(Segue<? super T, ? extends In> upstream) {
            return new Belts.ChainSegue<>(upstream.sink(), new Belts.ChainSource<>(new Belts.ClosedStation<>(upstream.source(), sink()), source()));
        }
        
        /**
         * Connects the {@code upstream} segue before this segue. Returns a new segue that pairs the {@code upstream}
         * segue's sink with a new source that behaves like this segue's source, except that it includes a
         * {@link Stage#tasks task} to drain the interior source to sink.
         *
         * <p>This method behaves equivalently to:
         * {@snippet :
         * this.compose(upstream.source()).compose(upstream.sink())
         * }
         *
         * @param upstream the upstream segue
         * @return a new segue whose source also includes an upstream task
         * @param <T> the upstream sink element type
         * @throws NullPointerException if upstream is null
         */
        default <T> StepSinkSource<T, Out> compose(StepSinkSource<? super T, ? extends In> upstream) {
            return new Belts.ChainStepSinkSource<>(upstream.sink(), new Belts.ChainSource<>(new Belts.ClosedStation<>(upstream.source(), sink()), source()));
        }
        
        @Override
        default StepSink<In> andThen(StepSink<? super Out> downstream) {
            return new Belts.ChainStepSink<>(sink(), new Belts.ClosedStation<>(source(), downstream));
        }
        
        @Override
        default <T> StepSinkSource<In, T> andThen(StepSinkSource<? super Out, ? extends T> downstream) {
            return new Belts.ChainStepSinkSource<>(new Belts.ChainStepSink<>(sink(), new Belts.ClosedStation<>(source(), downstream.sink())), downstream.source());
        }
        
        @Override
        default <T> StepSegue<In, T> andThen(StepSegue<? super Out, ? extends T> downstream) {
            return new Belts.ChainStepSegue<>(new Belts.ChainStepSink<>(sink(), new Belts.ClosedStation<>(source(), downstream.sink())), downstream.source());
        }
        
        /**
         * Connects the {@code upstream} operator before this segue. Returns a new segue that pairs this segue's source
         * with the upstream sink obtained by applying the {@code upstream} operator to this segue's sink.
         *
         * @param upstream a function that creates an upstream sink from a downstream sink
         * @return a new segue with sink transformed
         * @param <T> the upstream sink element type
         * @throws NullPointerException if upstream is null
         */
        @SuppressWarnings("unchecked")
        default <T> Segue<T, Out> compose(SinkToStepOperator<? super T, ? extends In> upstream) {
            return (Segue<T, Out>) upstream.andThen(sink()).andThen(source());
        }
        
        /**
         * Connects the {@code upstream} operator before this segue. Returns a new segue that pairs this segue's source
         * with the upstream sink obtained by applying the {@code upstream} operator to this segue's sink.
         *
         * @param upstream a function that creates an upstream sink from a downstream sink
         * @return a new segue with sink transformed
         * @param <T> the upstream sink element type
         * @throws NullPointerException if upstream is null
         */
        @SuppressWarnings("unchecked")
        default <T> StepSinkSource<T, Out> compose(StepSinkOperator<? super T, ? extends In> upstream) {
            return (StepSinkSource<T, Out>) upstream.andThen(sink()).andThen(source());
        }
        
        @Override
        default <T> StepSinkSource<In, T> andThen(SourceOperator<? super Out, ? extends T> downstream) {
            return sink().andThen(downstream.compose(source()));
        }
        
        @Override
        default <T> StepSegue<In, T> andThen(SourceToStepOperator<? super Out, ? extends T> downstream) {
            return sink().andThen(downstream.compose(source()));
        }
    }
    
    /**
     * A {@link Segue Segue} where the source is a {@link StepSource StepSource}.
     *
     * @param <In> the input element type
     * @param <Out> the output element type
     */
    public interface SinkStepSource<In, Out> extends Segue<In, Out> {
        @Override
        StepSource<Out> source();
        
        @Override
        default StepSource<Out> compose(StepSource<? extends In> upstream) {
            return new Belts.ChainStepSource<>(new Belts.ClosedStation<>(upstream, sink()), source());
        }
        
        @Override
        default <T> SinkStepSource<T, Out> compose(SinkStepSource<? super T, ? extends In> upstream) {
            return new Belts.ChainSinkStepSource<>(upstream.sink(), new Belts.ChainStepSource<>(new Belts.ClosedStation<>(upstream.source(), sink()), source()));
        }
        
        @Override
        default <T> StepSegue<T, Out> compose(StepSegue<? super T, ? extends In> upstream) {
            return new Belts.ChainStepSegue<>(upstream.sink(), new Belts.ChainStepSource<>(new Belts.ClosedStation<>(upstream.source(), sink()), source()));
        }
        
        /**
         * Connects the {@code downstream} sink after this segue. Returns a new sink that behaves like this segue's
         * sink, except that it includes a {@link Stage#tasks task} to drain from this segue's source to the
         * {@code downstream} sink.
         *
         * @param downstream the downstream sink
         * @return a new sink that also includes a downstream task
         * @throws NullPointerException if downstream is null
         */
        default Sink<In> andThen(Sink<? super Out> downstream) {
            return new Belts.ChainSink<>(sink(), new Belts.ClosedStation<>(source(), downstream));
        }
        
        /**
         * Connects the {@code downstream} segue after this segue. Returns a new segue that pairs the {@code downstream}
         * segue's source with a new sink that behaves like this segue's sink, except that it includes a
         * {@link Stage#tasks task} to drain the interior source to sink.
         *
         * <p>This method behaves equivalently to:
         * {@snippet :
         * this.andThen(downstream.sink()).andThen(downstream.source())
         * }
         *
         * @param downstream the downstream segue
         * @return a new segue whose sink also includes a downstream task
         * @param <T> the downstream source element type
         * @throws NullPointerException if downstream is null
         */
        default <T> Segue<In, T> andThen(Segue<? super Out, ? extends T> downstream) {
            return new Belts.ChainSegue<>(new Belts.ChainSink<>(sink(), new Belts.ClosedStation<>(source(), downstream.sink())), downstream.source());
        }
        
        /**
         * Connects the {@code downstream} segue after this segue. Returns a new segue that pairs the {@code downstream}
         * segue's source with a new sink that behaves like this segue's sink, except that it includes a
         * {@link Stage#tasks task} to drain the interior source to sink.
         *
         * <p>This method behaves equivalently to:
         * {@snippet :
         * this.andThen(downstream.sink()).andThen(downstream.source())
         * }
         *
         * @param downstream the downstream segue
         * @return a new segue whose sink also includes a downstream task
         * @param <T> the downstream source element type
         * @throws NullPointerException if downstream is null
         */
        default <T> SinkStepSource<In, T> andThen(SinkStepSource<? super Out, ? extends T> downstream) {
            return new Belts.ChainSinkStepSource<>(new Belts.ChainSink<>(sink(), new Belts.ClosedStation<>(source(), downstream.sink())), downstream.source());
        }
        
        @Override
        @SuppressWarnings("unchecked")
        default <T> SinkStepSource<T, Out> compose(SinkOperator<? super T, ? extends In> upstream) {
            return (SinkStepSource<T, Out>) upstream.andThen(sink()).andThen(source());
        }
        
        @Override
        @SuppressWarnings("unchecked")
        default <T> StepSegue<T, Out> compose(StepToSinkOperator<? super T, ? extends In> upstream) {
            return (StepSegue<T, Out>) upstream.andThen(sink()).andThen(source());
        }
        
        /**
         * Connects the {@code downstream} operator after this segue. Returns a new segue that pairs this segue's sink
         * with the downstream source obtained by applying the {@code downstream} operator to this segue's source.
         *
         * @param downstream a function that creates a downstream source from an upstream source
         * @return a new segue with source transformed
         * @param <T> the downstream source element type
         * @throws NullPointerException if downstream is null
         */
        default <T> Segue<In, T> andThen(StepToSourceOperator<? super Out, ? extends T> downstream) {
            return sink().andThen(downstream.compose(source()));
        }
        
        /**
         * Connects the {@code downstream} operator after this segue. Returns a new segue that pairs this segue's sink
         * with the downstream source obtained by applying the {@code downstream} operator to this segue's source.
         *
         * @param downstream a function that creates a downstream source from an upstream source
         * @return a new segue with source transformed
         * @param <T> the downstream source element type
         * @throws NullPointerException if downstream is null
         */
        default <T> SinkStepSource<In, T> andThen(StepSourceOperator<? super Out, ? extends T> downstream) {
            return sink().andThen(downstream.compose(source()));
        }
    }
    
    /**
     * A {@link Segue Segue} where the sink is a {@link StepSink StepSink} and the source is a
     * {@link StepSource StepSource}.
     *
     * @param <In> the input element type
     * @param <Out> the output element type
     */
    public interface StepSegue<In, Out> extends StepSinkSource<In, Out>, SinkStepSource<In, Out> {
        @Override
        default StepSource<Out> compose(Source<? extends In> upstream) {
            return new Belts.ChainStepSource<>(new Belts.ClosedStation<>(upstream, sink()), source());
        }
        
        @Override
        default <T> SinkStepSource<T, Out> compose(Segue<? super T, ? extends In> upstream) {
            return new Belts.ChainSinkStepSource<>(upstream.sink(), new Belts.ChainStepSource<>(new Belts.ClosedStation<>(upstream.source(), sink()), source()));
        }
        
        @Override
        default <T> StepSegue<T, Out> compose(StepSinkSource<? super T, ? extends In> upstream) {
            return new Belts.ChainStepSegue<>(upstream.sink(), new Belts.ChainStepSource<>(new Belts.ClosedStation<>(upstream.source(), sink()), source()));
        }
        
        @Override
        default StepSink<In> andThen(Sink<? super Out> downstream) {
            return new Belts.ChainStepSink<>(sink(), new Belts.ClosedStation<>(source(), downstream));
        }
        
        @Override
        default <T> StepSinkSource<In, T> andThen(Segue<? super Out, ? extends T> downstream) {
            return new Belts.ChainStepSinkSource<>(new Belts.ChainStepSink<>(sink(), new Belts.ClosedStation<>(source(), downstream.sink())), downstream.source());
        }
        
        @Override
        default <T> StepSegue<In, T> andThen(SinkStepSource<? super Out, ? extends T> downstream) {
            return new Belts.ChainStepSegue<>(new Belts.ChainStepSink<>(sink(), new Belts.ClosedStation<>(source(), downstream.sink())), downstream.source());
        }
        
        @Override
        @SuppressWarnings("unchecked")
        default <T> SinkStepSource<T, Out> compose(SinkToStepOperator<? super T, ? extends In> upstream) {
            return (SinkStepSource<T, Out>) upstream.andThen(sink()).andThen(source());
        }
        
        @Override
        @SuppressWarnings("unchecked")
        default <T> StepSegue<T, Out> compose(StepSinkOperator<? super T, ? extends In> upstream) {
            return (StepSegue<T, Out>) upstream.andThen(sink()).andThen(source());
        }
        
        @Override
        default <T> StepSinkSource<In, T> andThen(StepToSourceOperator<? super Out, ? extends T> downstream) {
            return sink().andThen(downstream.compose(source()));
        }
        
        @Override
        default <T> StepSegue<In, T> andThen(StepSourceOperator<? super Out, ? extends T> downstream) {
            return sink().andThen(downstream.compose(source()));
        }
    }
    
    // --- Sink Operators ---
    // Sink<In> = SinkOperator<In, Out> andThen Sink<Out>
    //          = Sink<Out> compose SinkOperator<In, Out>
    
    /**
     * Represents an operation on a downstream {@link Sink Sink} that produces an upstream {@code Sink}.
     *
     * <p>Note that methods on this interface are named according to the direction of data flow, not the direction of
     * function application. Methods named "andThen" take an argument that applies downstream of this operator. Methods
     * named "compose" take an argument that applies upstream of this operator.
     *
     * <p>This is a functional interface whose functional method is {@link #andThen(Sink)}.
     *
     * @param <In> the upstream element type
     * @param <Out> the downstream element type
     */
    @FunctionalInterface
    public interface SinkOperator<In, Out> {
        /**
         * Connects the {@code downstream} sink after this operator. Returns an upstream sink.
         *
         * @implSpec The upstream sink is generally expected to internally delegate to the downstream sink in some way.
         * This allows for transforming elements or signals as they travel from upstream to downstream.
         *
         * @param downstream the downstream sink
         * @return the upstream sink
         */
        Sink<In> andThen(Sink<? super Out> downstream);
        
        /**
         * Connects the {@code downstream} segue after this operator. Returns a new segue that pairs the
         * {@code downstream} segue's source with the upstream sink obtained by applying this operator to the
         * {@code downstream} segue's sink.
         *
         * @param downstream the downstream segue
         * @return a new segue with sink transformed
         * @param <T> the downstream source element type
         * @throws NullPointerException if downstream is null
         */
        default <T> Segue<In, T> andThen(Segue<? super Out, ? extends T> downstream) {
            return andThen(downstream.sink()).andThen(downstream.source());
        }
        
        /**
         * Connects the {@code downstream} segue after this operator. Returns a new segue that pairs the
         * {@code downstream} segue's source with the upstream sink obtained by applying this operator to the
         * {@code downstream} segue's sink.
         *
         * @param downstream the downstream segue
         * @return a new segue with sink transformed
         * @param <T> the downstream source element type
         * @throws NullPointerException if downstream is null
         */
        default <T> SinkStepSource<In, T> andThen(SinkStepSource<? super Out, ? extends T> downstream) {
            return andThen(downstream.sink()).andThen(downstream.source());
        }
        
        /**
         * Connects the {@code upstream} operator before this operator. Returns a composed operator that applies the
         * {@code upstream} operator to the result of this operator.
         *
         * @param upstream the upstream operator
         * @return a composed operator that applies the {@code upstream} operator to the result of this operator
         * @param <T> the upstream element type
         * @throws NullPointerException if upstream is null
         */
        @SuppressWarnings("unchecked")
        default <T> SinkOperator<T, Out> compose(SinkOperator<? super T, ? extends In> upstream) {
            Objects.requireNonNull(upstream);
            return sink -> (Sink<T>) upstream.andThen(andThen(sink));
        }
        
        /**
         * Connects the {@code upstream} operator before this operator. Returns a composed operator that applies the
         * {@code upstream} operator to the result of this operator.
         *
         * @param upstream the upstream operator
         * @return a composed operator that applies the {@code upstream} operator to the result of this operator
         * @param <T> the upstream element type
         * @throws NullPointerException if upstream is null
         */
        @SuppressWarnings("unchecked")
        default <T> StepToSinkOperator<T, Out> compose(StepToSinkOperator<? super T, ? extends In> upstream) {
            Objects.requireNonNull(upstream);
            return sink -> (StepSink<T>) upstream.andThen(andThen(sink));
        }
        
        /**
         * Connects the {@code downstream} operator after this operator. Returns a composed operator that applies this
         * operator to the result of the {@code downstream} operator.
         *
         * @param downstream the downstream operator
         * @return a composed operator that applies this operator to the result of the {@code downstream} operator
         * @param <T> the downstream element type
         * @throws NullPointerException if downstream is null
         */
        default <T> SinkOperator<In, T> andThen(SinkOperator<? super Out, ? extends T> downstream) {
            Objects.requireNonNull(downstream);
            return sink -> andThen(downstream.andThen(sink));
        }
        
        /**
         * Connects the {@code downstream} operator after this operator. Returns a composed operator that applies this
         * operator to the result of the {@code downstream} operator.
         *
         * @param downstream the downstream operator
         * @return a composed operator that applies this operator to the result of the {@code downstream} operator
         * @param <T> the downstream element type
         * @throws NullPointerException if downstream is null
         */
        default <T> SinkToStepOperator<In, T> andThen(SinkToStepOperator<? super Out, ? extends T> downstream) {
            Objects.requireNonNull(downstream);
            return sink -> andThen(downstream.andThen(sink));
        }
    }
    
    /**
     * Represents an operation on a downstream {@link StepSink StepSink} that produces an upstream {@link Sink Sink}.
     *
     * <p>Note that methods on this interface are named according to the direction of data flow, not the direction of
     * function application. Methods named "andThen" take an argument that applies downstream of this operator. Methods
     * named "compose" take an argument that applies upstream of this operator.
     *
     * <p>This is a functional interface whose functional method is {@link #andThen(StepSink)}.
     *
     * @param <In> the upstream element type
     * @param <Out> the downstream element type
     */
    @FunctionalInterface
    public interface SinkToStepOperator<In, Out> {
        /**
         * Connects the {@code downstream} sink after this operator. Returns an upstream sink.
         *
         * @implSpec The upstream sink is generally expected to internally delegate to the downstream sink in some way.
         * This allows for transforming elements or signals as they travel from upstream to downstream.
         *
         * @param downstream the downstream sink
         * @return the upstream sink
         */
        Sink<In> andThen(StepSink<? super Out> downstream);
        
        /**
         * Connects the {@code downstream} segue after this operator. Returns a new segue that pairs the
         * {@code downstream} segue's source with the upstream sink obtained by applying this operator to the
         * {@code downstream} segue's sink.
         *
         * @param downstream the downstream segue
         * @return a new segue with sink transformed
         * @param <T> the downstream source element type
         * @throws NullPointerException if downstream is null
         */
        default <T> Segue<In, T> andThen(StepSinkSource<? super Out, ? extends T> downstream) {
            return andThen(downstream.sink()).andThen(downstream.source());
        }
        
        /**
         * Connects the {@code downstream} segue after this operator. Returns a new segue that pairs the
         * {@code downstream} segue's source with the upstream sink obtained by applying this operator to the
         * {@code downstream} segue's sink.
         *
         * @param downstream the downstream segue
         * @return a new segue with sink transformed
         * @param <T> the downstream source element type
         * @throws NullPointerException if downstream is null
         */
        default <T> SinkStepSource<In, T> andThen(StepSegue<? super Out, ? extends T> downstream) {
            return andThen(downstream.sink()).andThen(downstream.source());
        }
        
        /**
         * Connects the {@code upstream} operator before this operator. Returns a composed operator that applies the
         * {@code upstream} operator to the result of this operator.
         *
         * @param upstream the upstream operator
         * @return a composed operator that applies the {@code upstream} operator to the result of this operator
         * @param <T> the upstream element type
         * @throws NullPointerException if upstream is null
         */
        @SuppressWarnings("unchecked")
        default <T> SinkToStepOperator<T, Out> compose(SinkOperator<? super T, ? extends In> upstream) {
            Objects.requireNonNull(upstream);
            return sink -> (Sink<T>) upstream.andThen(andThen(sink));
        }
        
        /**
         * Connects the {@code upstream} operator before this operator. Returns a composed operator that applies the
         * {@code upstream} operator to the result of this operator.
         *
         * @param upstream the upstream operator
         * @return a composed operator that applies the {@code upstream} operator to the result of this operator
         * @param <T> the upstream element type
         * @throws NullPointerException if upstream is null
         */
        @SuppressWarnings("unchecked")
        default <T> StepSinkOperator<T, Out> compose(StepToSinkOperator<? super T, ? extends In> upstream) {
            Objects.requireNonNull(upstream);
            return sink -> (StepSink<T>) upstream.andThen(andThen(sink));
        }
        
        /**
         * Connects the {@code downstream} operator after this operator. Returns a composed operator that applies this
         * operator to the result of the {@code downstream} operator.
         *
         * @param downstream the downstream operator
         * @return a composed operator that applies this operator to the result of the {@code downstream} operator
         * @param <T> the downstream element type
         * @throws NullPointerException if downstream is null
         */
        default <T> SinkOperator<In, T> andThen(StepToSinkOperator<? super Out, ? extends T> downstream) {
            Objects.requireNonNull(downstream);
            return sink -> andThen(downstream.andThen(sink));
        }
        
        /**
         * Connects the {@code downstream} operator after this operator. Returns a composed operator that applies this
         * operator to the result of the {@code downstream} operator.
         *
         * @param downstream the downstream operator
         * @return a composed operator that applies this operator to the result of the {@code downstream} operator
         * @param <T> the downstream element type
         * @throws NullPointerException if downstream is null
         */
        default <T> SinkToStepOperator<In, T> andThen(StepSinkOperator<? super Out, ? extends T> downstream) {
            Objects.requireNonNull(downstream);
            return sink -> andThen(downstream.andThen(sink));
        }
    }
    
    /**
     * Represents an operation on a downstream {@link Sink Sink} that produces an upstream {@link StepSink StepSink}.
     *
     * <p>Note that methods on this interface are named according to the direction of data flow, not the direction of
     * function application. Methods named "andThen" take an argument that applies downstream of this operator. Methods
     * named "compose" take an argument that applies upstream of this operator.
     *
     * <p>This is a functional interface whose functional method is {@link #andThen(Sink)}.
     *
     * @param <In> the upstream element type
     * @param <Out> the downstream element type
     */
    @FunctionalInterface
    public interface StepToSinkOperator<In, Out> extends SinkOperator<In, Out> {
        /**
         * Connects the {@code downstream} sink after this operator. Returns an upstream sink.
         *
         * @implSpec The upstream sink is generally expected to internally delegate to the downstream sink in some way.
         * This allows for transforming elements or signals as they travel from upstream to downstream.
         *
         * @param downstream the downstream sink
         * @return the upstream sink
         */
        @Override
        StepSink<In> andThen(Sink<? super Out> downstream);
        
        /**
         * Connects the {@code downstream} segue after this operator. Returns a new segue that pairs the
         * {@code downstream} segue's source with the upstream sink obtained by applying this operator to the
         * {@code downstream} segue's sink.
         *
         * @param downstream the downstream segue
         * @return a new segue with sink transformed
         * @param <T> the downstream source element type
         * @throws NullPointerException if after is null
         */
        @Override
        default <T> StepSinkSource<In, T> andThen(Segue<? super Out, ? extends T> downstream) {
            return andThen(downstream.sink()).andThen(downstream.source());
        }
        
        /**
         * Connects the {@code downstream} segue after this operator. Returns a new segue that pairs the
         * {@code downstream} segue's source with the upstream sink obtained by applying this operator to the
         * {@code downstream} segue's sink.
         *
         * @param downstream the downstream segue
         * @return a new segue with sink transformed
         * @param <T> the downstream source element type
         * @throws NullPointerException if after is null
         */
        @Override
        default <T> StepSegue<In, T> andThen(SinkStepSource<? super Out, ? extends T> downstream) {
            return andThen(downstream.sink()).andThen(downstream.source());
        }
        
        /**
         * Connects the {@code upstream} operator before this operator. Returns a composed operator that applies the
         * {@code upstream} operator to the result of this operator.
         *
         * @param upstream the upstream operator
         * @return a composed operator that applies the {@code upstream} operator to the result of this operator
         * @param <T> the upstream element type
         * @throws NullPointerException if upstream is null
         */
        @SuppressWarnings("unchecked")
        default <T> SinkOperator<T, Out> compose(SinkToStepOperator<? super T, ? extends In> upstream) {
            Objects.requireNonNull(upstream);
            return sink -> (Sink<T>) upstream.andThen(andThen(sink));
        }
        
        /**
         * Connects the {@code upstream} operator before this operator. Returns a composed operator that applies the
         * {@code upstream} operator to the result of this operator.
         *
         * @param upstream the upstream operator
         * @return a composed operator that applies the {@code upstream} operator to the result of this operator
         * @param <T> the upstream element type
         * @throws NullPointerException if upstream is null
         */
        @SuppressWarnings("unchecked")
        default <T> StepToSinkOperator<T, Out> compose(StepSinkOperator<? super T, ? extends In> upstream) {
            Objects.requireNonNull(upstream);
            return sink -> (StepSink<T>) upstream.andThen(andThen(sink));
        }
        
        /**
         * Connects the {@code downstream} operator after this operator. Returns a composed operator that applies this
         * operator to the result of the {@code downstream} operator.
         *
         * @param downstream the downstream operator
         * @return a composed operator that applies this operator to the result of the {@code downstream} operator
         * @param <T> the downstream element type
         * @throws NullPointerException if downstream is null
         */
        @Override
        default <T> StepToSinkOperator<In, T> andThen(SinkOperator<? super Out, ? extends T> downstream) {
            Objects.requireNonNull(downstream);
            return sink -> andThen(downstream.andThen(sink));
        }
        
        /**
         * Connects the {@code downstream} operator after this operator. Returns a composed operator that applies this
         * operator to the result of the {@code downstream} operator.
         *
         * @param downstream the downstream operator
         * @return a composed operator that applies this operator to the result of the {@code downstream} operator
         * @param <T> the downstream element type
         * @throws NullPointerException if downstream is null
         */
        @Override
        default <T> StepSinkOperator<In, T> andThen(SinkToStepOperator<? super Out, ? extends T> downstream) {
            Objects.requireNonNull(downstream);
            return sink -> andThen(downstream.andThen(sink));
        }
    }
    
    /**
     * Represents an operation on a downstream {@link StepSink StepSink} that produces an upstream {@code StepSink}.
     *
     * <p>Note that methods on this interface are named according to the direction of data flow, not the direction of
     * function application. Methods named "andThen" take an argument that applies downstream of this operator. Methods
     * named "compose" take an argument that applies upstream of this operator.
     *
     * <p>This is a functional interface whose functional method is {@link #andThen(StepSink)}.
     *
     * @param <In> the upstream element type
     * @param <Out> the downstream element type
     */
    @FunctionalInterface
    public interface StepSinkOperator<In, Out> extends SinkToStepOperator<In, Out> {
        /**
         * Connects the {@code downstream} sink after this operator. Returns an upstream sink.
         *
         * @implSpec The upstream sink is generally expected to internally delegate to the downstream sink in some way.
         * This allows for transforming elements or signals as they travel from upstream to downstream.
         *
         * @param downstream the downstream sink
         * @return the upstream sink
         */
        @Override
        StepSink<In> andThen(StepSink<? super Out> downstream);
        
        /**
         * Connects the {@code downstream} segue after this operator. Returns a new segue that pairs the
         * {@code downstream} segue's source with the upstream sink obtained by applying this operator to the
         * {@code downstream} segue's sink.
         *
         * @param downstream the downstream segue
         * @return a new segue with sink transformed
         * @param <T> the downstream source element type
         * @throws NullPointerException if downstream is null
         */
        @Override
        default <T> StepSinkSource<In, T> andThen(StepSinkSource<? super Out, ? extends T> downstream) {
            return andThen(downstream.sink()).andThen(downstream.source());
        }
        
        /**
         * Connects the {@code downstream} segue after this operator. Returns a new segue that pairs the
         * {@code downstream} segue's source with the upstream sink obtained by applying this operator to the
         * {@code downstream} segue's sink.
         *
         * @param downstream the downstream segue
         * @return a new segue with sink transformed
         * @param <T> the downstream source element type
         * @throws NullPointerException if downstream is null
         */
        @Override
        default <T> StepSegue<In, T> andThen(StepSegue<? super Out, ? extends T> downstream) {
            return andThen(downstream.sink()).andThen(downstream.source());
        }
        
        /**
         * Connects the {@code upstream} operator before this operator. Returns a composed operator that applies the
         * {@code upstream} operator to the result of this operator.
         *
         * @param upstream the upstream operator
         * @return a composed operator that applies the {@code upstream} operator to the result of this operator
         * @param <T> the upstream element type
         * @throws NullPointerException if upstream is null
         */
        @SuppressWarnings("unchecked")
        default <T> SinkToStepOperator<T, Out> compose(SinkToStepOperator<? super T, ? extends In> upstream) {
            Objects.requireNonNull(upstream);
            return sink -> (Sink<T>) upstream.andThen(andThen(sink));
        }
        
        /**
         * Connects the {@code upstream} operator before this operator. Returns a composed operator that applies the
         * {@code upstream} operator to the result of this operator.
         *
         * @param upstream the upstream operator
         * @return a composed operator that applies the {@code upstream} operator to the result of this operator
         * @param <T> the upstream element type
         * @throws NullPointerException if upstream is null
         */
        @SuppressWarnings("unchecked")
        default <T> StepSinkOperator<T, Out> compose(StepSinkOperator<? super T, ? extends In> upstream) {
            Objects.requireNonNull(upstream);
            return sink -> (StepSink<T>) upstream.andThen(andThen(sink));
        }
        
        /**
         * Connects the {@code downstream} operator after this operator. Returns a composed operator that applies this
         * operator to the result of the {@code downstream} operator.
         *
         * @param downstream the downstream operator
         * @return a composed operator that applies this operator to the result of the {@code downstream} operator
         * @param <T> the downstream element type
         * @throws NullPointerException if downstream is null
         */
        @Override
        default <T> StepToSinkOperator<In, T> andThen(StepToSinkOperator<? super Out, ? extends T> downstream) {
            Objects.requireNonNull(downstream);
            return sink -> andThen(downstream.andThen(sink));
        }
        
        /**
         * Connects the {@code downstream} operator after this operator. Returns a composed operator that applies this
         * operator to the result of the {@code downstream} operator.
         *
         * @param downstream the downstream operator
         * @return a composed operator that applies this operator to the result of the {@code downstream} operator
         * @param <T> the downstream element type
         * @throws NullPointerException if downstream is null
         */
        @Override
        default <T> StepSinkOperator<In, T> andThen(StepSinkOperator<? super Out, ? extends T> downstream) {
            Objects.requireNonNull(downstream);
            return sink -> andThen(downstream.andThen(sink));
        }
    }

    // --- Source Operators ---
    // Source<Out> = SourceOperator<In, Out> compose Source<In>
    //             = Source<In> andThen SourceOperator<In, Out>
    
    /**
     * Represents an operation on an upstream {@link Source Source} that produces a downstream {@code Source}.
     *
     * <p>This is a functional interface whose functional method is {@link #compose(Source)}.
     *
     * @param <In> the upstream element type
     * @param <Out> the downstream element type
     */
    @FunctionalInterface
    public interface SourceOperator<In, Out> {
        /**
         * Connects the {@code upstream} source before this operator. Returns a downstream source.
         *
         * @implSpec The downstream source is generally expected to internally delegate to the upstream source in some
         * way. This allows for transforming elements or signals as they travel from upstream to downstream.
         *
         * @param upstream the upstream source
         * @return the downstream source
         */
        Source<Out> compose(Source<? extends In> upstream);
        
        /**
         * Connects the {@code upstream} segue before this operator. Returns a new segue that pairs the {@code upstream}
         * segue's sink with the downstream source obtained by applying this operator to the {@code upstream} segue's
         * source.
         *
         * @param upstream the upstream segue
         * @return a new segue with source transformed
         * @param <T> the upstream sink element type
         * @throws NullPointerException if upstream is null
         */
        default <T> Segue<T, Out> compose(Segue<? super T, ? extends In> upstream) {
            return compose(upstream.source()).compose(upstream.sink());
        }
        
        /**
         * Connects the {@code upstream} segue before this operator. Returns a new segue that pairs the {@code upstream}
         * segue's sink with the downstream source obtained by applying this operator to the {@code upstream} segue's
         * source.
         *
         * @param upstream the upstream segue
         * @return a new segue with source transformed
         * @param <T> the upstream sink element type
         * @throws NullPointerException if upstream is null
         */
        default <T> StepSinkSource<T, Out> compose(StepSinkSource<? super T, ? extends In> upstream) {
            return compose(upstream.source()).compose(upstream.sink());
        }
        
        /**
         * Connects the {@code upstream} operator before this operator. Returns a composed operator that applies this
         * operator to the result of the {@code upstream} operator.
         *
         * @param upstream the upstream operator
         * @return a composed operator that applies this operator to the result of the {@code upstream} operator
         * @param <T> the upstream element type
         * @throws NullPointerException if upstream is null
         */
        default <T> StepToSourceOperator<T, Out> compose(StepToSourceOperator<? super T, ? extends In> upstream) {
            Objects.requireNonNull(upstream);
            return source -> compose(upstream.compose(source));
        }
        
        /**
         * Connects the {@code upstream} operator before this operator. Returns a composed operator that applies this
         * operator to the result of the {@code upstream} operator.
         *
         * @param upstream the upstream operator
         * @return a composed operator that applies this operator to the result of the {@code upstream} operator
         * @param <T> the upstream element type
         * @throws NullPointerException if upstream is null
         */
        default <T> SourceOperator<T, Out> compose(SourceOperator<? super T, ? extends In> upstream) {
            Objects.requireNonNull(upstream);
            return source -> compose(upstream.compose(source));
        }
        
        /**
         * Connects the {@code downstream} operator after this operator. Returns a composed operator that applies the
         * {@code downstream} operator to the result of this operator.
         *
         * @param downstream the downstream operator
         * @return a composed operator that applies the {@code downstream} operator to the result of this operator
         * @param <T> the downstream element type
         * @throws NullPointerException if downstream is null
         */
        @SuppressWarnings("unchecked")
        default <T> SourceOperator<In, T> andThen(SourceOperator<? super Out, ? extends T> downstream) {
            Objects.requireNonNull(downstream);
            return source -> (Source<T>) downstream.compose(compose(source));
        }
        
        /**
         * Connects the {@code downstream} operator after this operator. Returns a composed operator that applies the
         * {@code downstream} operator to the result of this operator.
         *
         * @param downstream the downstream operator
         * @return a composed operator that applies the {@code downstream} operator to the result of this operator
         * @param <T> the downstream element type
         * @throws NullPointerException if downstream is null
         */
        @SuppressWarnings("unchecked")
        default <T> SourceToStepOperator<In, T> andThen(SourceToStepOperator<? super Out, ? extends T> downstream) {
            Objects.requireNonNull(downstream);
            return source -> (StepSource<T>) downstream.compose(compose(source));
        }
    }
    
    /**
     * Represents an operation on an upstream {@link StepSource StepSource} that produces a downstream
     * {@link Source Source}.
     *
     * <p>This is a functional interface whose functional method is {@link #compose(StepSource)}.
     *
     * @param <In> the upstream element type
     * @param <Out> the downstream element type
     */
    @FunctionalInterface
    public interface StepToSourceOperator<In, Out> {
        /**
         * Connects the {@code upstream} source upstream this operator. Returns a downstream source.
         *
         * @implSpec The downstream source is generally expected to internally delegate to the upstream source in some
         * way. This allows for transforming elements or signals as they travel from upstream to downstream.
         *
         * @param upstream the upstream source
         * @return the downstream source
         */
        Source<Out> compose(StepSource<? extends In> upstream);
        
        /**
         * Connects the {@code upstream} segue before this operator. Returns a new segue that pairs the {@code upstream}
         * segue's sink with the downstream source obtained by applying this operator to the {@code upstream} segue's
         * source.
         *
         * @param upstream the upstream segue
         * @return a new segue with source transformed
         * @param <T> the upstream sink element type
         * @throws NullPointerException if upstream is null
         */
        default <T> Segue<T, Out> compose(SinkStepSource<? super T, ? extends In> upstream) {
            return compose(upstream.source()).compose(upstream.sink());
        }
        
        /**
         * Connects the {@code upstream} segue before this operator. Returns a new segue that pairs the {@code upstream}
         * segue's sink with the downstream source obtained by applying this operator to the {@code upstream} segue's
         * source.
         *
         * @param upstream the upstream segue
         * @return a new segue with source transformed
         * @param <T> the upstream sink element type
         * @throws NullPointerException if upstream is null
         */
        default <T> StepSinkSource<T, Out> compose(StepSegue<? super T, ? extends In> upstream) {
            return compose(upstream.source()).compose(upstream.sink());
        }
        
        /**
         * Connects the {@code upstream} operator before this operator. Returns a composed operator that applies this
         * operator to the result of the {@code upstream} operator.
         *
         * @param upstream the upstream operator
         * @return a composed operator that applies this operator to the result of the {@code upstream} operator
         * @param <T> the upstream element type
         * @throws NullPointerException if upstream is null
         */
        default <T> SourceOperator<T, Out> compose(SourceToStepOperator<? super T, ? extends In> upstream) {
            Objects.requireNonNull(upstream);
            return source -> compose(upstream.compose(source));
        }
        
        /**
         * Connects the {@code upstream} operator before this operator. Returns a composed operator that applies this
         * operator to the result of the {@code upstream} operator.
         *
         * @param upstream the upstream operator
         * @return a composed operator that applies this operator to the result of the {@code upstream} operator
         * @param <T> the upstream element type
         * @throws NullPointerException if upstream is null
         */
        default <T> StepToSourceOperator<T, Out> compose(StepSourceOperator<? super T, ? extends In> upstream) {
            Objects.requireNonNull(upstream);
            return source -> compose(upstream.compose(source));
        }
        
        /**
         * Connects the {@code downstream} operator after this operator. Returns a composed operator that applies the
         * {@code downstream} operator to the result of this operator.
         *
         * @param downstream the downstream operator
         * @return a composed operator that applies the {@code downstream} operator to the result of this operator
         * @param <T> the downstream element type
         * @throws NullPointerException if downstream is null
         */
        @SuppressWarnings("unchecked")
        default <T> StepToSourceOperator<In, T> andThen(SourceOperator<? super Out, ? extends T> downstream) {
            Objects.requireNonNull(downstream);
            return source -> (Source<T>) downstream.compose(compose(source));
        }
        
        /**
         * Connects the {@code downstream} operator after this operator. Returns a composed operator that applies the
         * {@code downstream} operator to the result of this operator.
         *
         * @param downstream the downstream operator
         * @return a composed operator that applies the {@code downstream} operator to the result of this operator
         * @param <T> the downstream element type
         * @throws NullPointerException if downstream is null
         */
        @SuppressWarnings("unchecked")
        default <T> StepSourceOperator<In, T> andThen(SourceToStepOperator<? super Out, ? extends T> downstream) {
            Objects.requireNonNull(downstream);
            return source -> (StepSource<T>) downstream.compose(compose(source));
        }
    }
    
    /**
     * Represents an operation on an upstream {@link Source Source} that produces a downstream
     * {@link StepSource StepSource}.
     *
     * <p>This is a functional interface whose functional method is {@link #compose(Source)}.
     *
     * @param <In> the upstream element type
     * @param <Out> the downstream element type
     */
    @FunctionalInterface
    public interface SourceToStepOperator<In, Out> extends SourceOperator<In, Out> {
        /**
         * Connects the {@code upstream} source upstream this operator. Returns a downstream source.
         *
         * @implSpec The downstream source is generally expected to internally delegate to the upstream source in some
         * way. This allows for transforming elements or signals as they travel from upstream to downstream.
         *
         * @param upstream the upstream source
         * @return the downstream source
         */
        @Override
        StepSource<Out> compose(Source<? extends In> upstream);
        
        /**
         * Connects the {@code upstream} segue before this operator. Returns a new segue that pairs the {@code upstream}
         * segue's sink with the downstream source obtained by applying this operator to the {@code upstream} segue's
         * source.
         *
         * @param upstream the upstream segue
         * @return a new segue with source transformed
         * @param <T> the upstream sink element type
         * @throws NullPointerException if before is null
         */
        @Override
        default <T> SinkStepSource<T, Out> compose(Segue<? super T, ? extends In> upstream) {
            return compose(upstream.source()).compose(upstream.sink());
        }
        
        /**
         * Connects the {@code upstream} segue before this operator. Returns a new segue that pairs the {@code upstream}
         * segue's sink with the downstream source obtained by applying this operator to the {@code upstream} segue's
         * source.
         *
         * @param upstream the upstream segue
         * @return a new segue with source transformed
         * @param <T> the upstream sink element type
         * @throws NullPointerException if before is null
         */
        @Override
        default <T> StepSegue<T, Out> compose(StepSinkSource<? super T, ? extends In> upstream) {
            return compose(upstream.source()).compose(upstream.sink());
        }
        
        /**
         * Connects the {@code upstream} operator before this operator. Returns a composed operator that applies this
         * operator to the result of the {@code upstream} operator.
         *
         * @param upstream the upstream operator
         * @return a composed operator that applies this operator to the result of the {@code before} operator
         * @param <T> the upstream element type
         * @throws NullPointerException if before is null
         */
        @Override
        default <T> SourceToStepOperator<T, Out> compose(SourceOperator<? super T, ? extends In> upstream) {
            Objects.requireNonNull(upstream);
            return source -> compose(upstream.compose(source));
        }
        
        /**
         * Connects the {@code upstream} operator before this operator. Returns a composed operator that applies this
         * operator to the result of the {@code upstream} operator.
         *
         * @param upstream the upstream operator
         * @return a composed operator that applies this operator to the result of the {@code before} operator
         * @param <T> the upstream element type
         * @throws NullPointerException if before is null
         */
        @Override
        default <T> StepSourceOperator<T, Out> compose(StepToSourceOperator<? super T, ? extends In> upstream) {
            Objects.requireNonNull(upstream);
            return source -> compose(upstream.compose(source));
        }
        
        /**
         * Connects the {@code downstream} operator after this operator. Returns a composed operator that applies the
         * {@code downstream} operator to the result of this operator.
         *
         * @param downstream the downstream operator
         * @return a composed operator that applies the {@code downstream} operator to the result of this operator
         * @param <T> the downstream element type
         * @throws NullPointerException if downstream is null
         */
        @SuppressWarnings("unchecked")
        default <T> SourceOperator<In, T> andThen(StepToSourceOperator<? super Out, ? extends T> downstream) {
            Objects.requireNonNull(downstream);
            return source -> (Source<T>) downstream.compose(compose(source));
        }
        
        /**
         * Connects the {@code downstream} operator after this operator. Returns a composed operator that applies the
         * {@code downstream} operator to the result of this operator.
         *
         * @param downstream the downstream operator
         * @return a composed operator that applies the {@code downstream} operator to the result of this operator
         * @param <T> the downstream element type
         * @throws NullPointerException if downstream is null
         */
        @SuppressWarnings("unchecked")
        default <T> SourceToStepOperator<In, T> andThen(StepSourceOperator<? super Out, ? extends T> downstream) {
            Objects.requireNonNull(downstream);
            return source -> (StepSource<T>) downstream.compose(compose(source));
        }
    }
    
    /**
     * Represents an operation on an upstream {@link StepSource StepSource} that produces a downstream
     * {@code StepSource}.
     *
     * <p>This is a functional interface whose functional method is {@link #compose(StepSource)}.
     *
     * @param <In> the upstream element type
     * @param <Out> the downstream element type
     */
    @FunctionalInterface
    public interface StepSourceOperator<In, Out> extends StepToSourceOperator<In, Out> {
        /**
         * Connects the {@code upstream} source upstream this operator. Returns a downstream source.
         *
         * @implSpec The downstream source is generally expected to internally delegate to the upstream source in some
         * way. This allows for transforming elements or signals as they travel from upstream to downstream.
         *
         * @param upstream the upstream source
         * @return the downstream source
         */
        @Override
        StepSource<Out> compose(StepSource<? extends In> upstream);
        
        /**
         * Connects the {@code upstream} segue before this operator. Returns a new segue that pairs the {@code upstream}
         * segue's sink with the downstream source obtained by applying this operator to the {@code upstream} segue's
         * source.
         *
         * @param upstream the upstream segue
         * @return a new segue with source transformed
         * @param <T> the upstream sink element type
         * @throws NullPointerException if before is null
         */
        @Override
        default <T> SinkStepSource<T, Out> compose(SinkStepSource<? super T, ? extends In> upstream) {
            return compose(upstream.source()).compose(upstream.sink());
        }
        
        /**
         * Connects the {@code upstream} segue before this operator. Returns a new segue that pairs the {@code upstream}
         * segue's sink with the downstream source obtained by applying this operator to the {@code upstream} segue's
         * source.
         *
         * @param upstream the upstream segue
         * @return a new segue with source transformed
         * @param <T> the upstream sink element type
         * @throws NullPointerException if before is null
         */
        @Override
        default <T> StepSegue<T, Out> compose(StepSegue<? super T, ? extends In> upstream) {
            return compose(upstream.source()).compose(upstream.sink());
        }
        
        /**
         * Connects the {@code upstream} operator before this operator. Returns a composed operator that applies this
         * operator to the result of the {@code upstream} operator.
         *
         * @param upstream the upstream operator
         * @return a composed operator that applies this operator to the result of the {@code before} operator
         * @param <T> the upstream element type
         * @throws NullPointerException if before is null
         */
        @Override
        default <T> SourceToStepOperator<T, Out> compose(SourceToStepOperator<? super T, ? extends In> upstream) {
            Objects.requireNonNull(upstream);
            return source -> compose(upstream.compose(source));
        }
        
        /**
         * Connects the {@code upstream} operator before this operator. Returns a composed operator that applies this
         * operator to the result of the {@code upstream} operator.
         *
         * @param upstream the upstream operator
         * @return a composed operator that applies this operator to the result of the {@code before} operator
         * @param <T> the upstream element type
         * @throws NullPointerException if before is null
         */
        @Override
        default <T> StepSourceOperator<T, Out> compose(StepSourceOperator<? super T, ? extends In> upstream) {
            Objects.requireNonNull(upstream);
            return source -> compose(upstream.compose(source));
        }
        
        /**
         * Connects the {@code downstream} operator after this operator. Returns a composed operator that applies the
         * {@code downstream} operator to the result of this operator.
         *
         * @param downstream the downstream operator
         * @return a composed operator that applies the {@code downstream} operator to the result of this operator
         * @param <T> the downstream element type
         * @throws NullPointerException if downstream is null
         */
        @SuppressWarnings("unchecked")
        default <T> StepToSourceOperator<In, T> andThen(StepToSourceOperator<? super Out, ? extends T> downstream) {
            Objects.requireNonNull(downstream);
            return source -> (Source<T>) downstream.compose(compose(source));
        }
        
        /**
         * Connects the {@code downstream} operator after this operator. Returns a composed operator that applies the
         * {@code downstream} operator to the result of this operator.
         *
         * @param downstream the downstream operator
         * @return a composed operator that applies the {@code downstream} operator to the result of this operator
         * @param <T> the downstream element type
         * @throws NullPointerException if downstream is null
         */
        @SuppressWarnings("unchecked")
        default <T> StepSourceOperator<In, T> andThen(StepSourceOperator<? super Out, ? extends T> downstream) {
            Objects.requireNonNull(downstream);
            return source -> (StepSource<T>) downstream.compose(compose(source));
        }
    }
}