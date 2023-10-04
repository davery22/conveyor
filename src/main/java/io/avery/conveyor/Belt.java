package io.avery.conveyor;

import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.stream.Collector;
import java.util.stream.Stream;

/**
 * TODO: Stages, Segues, Operators
 *  intermediate vs boundary vs terminal
 *  fan-out vs fan-in
 *  exception handling ('up' to nearest boundary, then 'down' across boundaries)
 */
public class Belt {
    private Belt() {}
    
    // --- Stages ---
    
    /**
     * Sealed interface over {@link Source Source}, {@link Sink Sink}, and {@link Silo Silo}. Stages have different
     * input/output configurations, or "shapes", which can be connected to form a processing pipeline.
     *
     * <ul>
     *     <li>{@code Source} - yields output elements
     *     <li>{@code Sink} - accepts input elements
     *     <li>{@code Silo} - connects {@code Sources} to {@code Sinks}
     * </ul>
     *
     * (For a "stage" that accepts input elements and yields output elements, see {@link Segue Segue}.)
     *
     * <p>Stages can be {@link #run run}, which will traverse each silo encapsulated by the stage and execute the
     * processing pipeline.
     */
    public sealed interface Stage {
        /**
         * Executes each silo encapsulated by this stage. Executions proceeds by recursively running the source and sink
         * of the silo, and submitting a task to the executor that:
         * <ol>
         *     <li>drains the source to the sink
         *     <li>completes the sink normally if no exception was thrown
         *     <li>closes the source
         *     <li>completes the sink abruptly if an exception was thrown, suppressing further exceptions
         * </ol>
         *
         * <p>If any of steps 1-3 throw an exception, the initial exception will be caught and wrapped in a
         * {@link java.util.concurrent.CompletionException CompletionException} thrown at the end of the task.
         *
         * <p>If the initial exception is an {@link InterruptedException}, or if any completions throw
         * {@code InterruptedException}, the thread interrupt status will be set when the exception is caught, and will
         * remain set until any subsequent throw of {@code InterruptedException}. (This ensures that recovery operators
         * see the interrupt, and do not unintentionally interfere with interrupt responsiveness.)
         *
         * <p>The given executor should ideally spawn a new thread for each task. An executor with insufficient threads
         * to run all encapsulated silos concurrently may cause deadlock.
         *
         * <p>If a silo has already started running on any executor, subsequent runs of that silo will short-circuit and
         * do nothing.
         *
         * @implSpec A stage that delegates to other stages should call {@code run} on each stage before returning from
         * this method.
         *
         * @implNote The default implementation does nothing.
         *
         * @param executor the executor to submit tasks to
         */
        default void run(Executor executor) { }
    }
    
    /**
     * A {@link Stage Stage} that represents connected {@link Source Source} and {@link Sink Sink} stages. A silo may
     * encapsulate:
     * <ul>
     *     <li>A {@code StepSource} connected to a {@code Sink}
     *     <li>A {@code Source} connected to a {@code StepSink}
     *     <li>A {@code Silo} connected to a {@code Silo} (and thereby, any sequence of {@code Silos})
     * </ul>
     *
     * <p>A silo itself accepts no input and yields no output. The {@link #run run} method interacts with silos by
     * draining encapsulated sources to sinks.
     *
     * @see #run(Executor)
     */
    public sealed interface Silo extends Stage permits Belts.ClosedSilo, Belts.ChainSilo {
        /**
         * Connects the {@code upstream} silo before this silo. Returns a new silo that, when {@link #run run}, runs
         * this silo and the {@code upstream} silo.
         *
         * @param upstream the upstream silo
         * @return a new silo that runs this silo and the {@code upstream} silo
         * @throws NullPointerException if upstream is null
         */
        default Silo compose(Silo upstream) {
            return new Belts.ChainSilo(upstream, this);
        }
        
        /**
         * Connects the {@code upstream} sink before this silo. Returns a new sink that behaves like the
         * {@code upstream} sink, except that {@link #run running} it will also run this silo.
         *
         * @param upstream the upstream sink
         * @return a new sink that also runs this silo
         * @param <T> the upstream sink element type
         * @throws NullPointerException if upstream is null
         */
        default <T> Sink<T> compose(Sink<? super T> upstream) {
            return new Belts.ChainSink<>(upstream, this);
        }
        
        /**
         * Connects the {@code upstream} sink before this silo. Returns a new sink that behaves like the
         * {@code upstream} sink, except that {@link #run running} it will also run this silo.
         *
         * @param upstream the upstream sink
         * @return a new sink that also runs this silo
         * @param <T> the upstream sink element type
         * @throws NullPointerException if upstream is null
         */
        default <T> StepSink<T> compose(StepSink<? super T> upstream) {
            return new Belts.ChainStepSink<>(upstream, this);
        }
        
        /**
         * Connects the {@code downstream} silo after this silo. Returns a new silo that, when {@link #run run}, runs
         * this silo and the {@code downstream} silo.
         *
         * @param downstream the downstream silo
         * @return a new silo that runs this silo and the {@code before} silo
         * @throws NullPointerException if downstream is null
         */
        default Silo andThen(Silo downstream) {
            return new Belts.ChainSilo(this, downstream);
        }
        
        /**
         * Connects the {@code downstream} source after this silo. Returns a new source that behaves like the
         * {@code downstream} source, except that {@link #run running} it will also run this silo.
         *
         * @param downstream the downstream source
         * @return a new source that also runs this silo
         * @param <T> the downstream source element type
         * @throws NullPointerException if downstream is null
         */
        default <T> Source<T> andThen(Source<? extends T> downstream) {
            return new Belts.ChainSource<>(this, downstream);
        }
        
        /**
         * Connects the {@code downstream} source after this silo. Returns a new source that behaves like the
         * {@code downstream} source, except that {@link #run running} it will also run this silo.
         *
         * @param downstream the downstream source
         * @return a new source that also runs this silo
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
     * <p>A sink may encapsulate a downstream silo, which will {@link #run run} when the sink runs. Sinks generally
     * should be run before accepting elements, in case the sink connects across a downstream boundary, to avoid the
     * effects of unmitigated buffer saturation (including potential deadlock).
     *
     * <p>This is a functional interface whose functional method is {@link #drainFromSource(StepSource)}.
     *
     * @param <In> the input element type
     */
    @FunctionalInterface
    public non-sealed interface Sink<In> extends Stage {
        /**
         * Polls as many elements as possible from the source to this sink. This proceeds until either an exception is
         * thrown, this sink cancels, or this sink is unable to accept more elements from the source (which does not
         * necessarily mean the source drained). Returns {@code true} if the source definitely drained, meaning a call
         * to {@link StepSource#poll poll} returned {@code null}; else returns {@code false}.
         *
         * @implSpec Implementors should restrict to {@link StepSource#poll polling} from the source. Closing the source
         * is the caller's responsibility, as the source may be reused after this method is called.
         *
         * @param source the source to drain from
         * @return {@code true} if the source drained
         * @throws Exception if unable to drain
         */
        boolean drainFromSource(StepSource<? extends In> source) throws Exception;
        
        /**
         * Signals any nearest downstream boundary sources to stop yielding elements that arrive after this signal.
         *
         * @implSpec A boundary sink should implement its {@link StepSink#offer offer} and
         * {@link #drainFromSource drainFromSource} methods to discard elements and return {@code false} after this
         * method is called, to prevent unbounded buffering or deadlock. The connected boundary source should return
         * {@code null} from {@link StepSource#poll poll} and {@code false} from {@link Source#drainToSink drainToSink}
         * after yielding all values that arrived before it received this signal.
         *
         * <p>A sink that delegates to downstream sinks should call {@code complete} on each downstream sink before
         * returning from this method, unless this method throws before completing any sinks. If completing any sink
         * throws an exception, subsequent exceptions should be suppressed onto the first exception. If completing any
         * sink throws {@link InterruptedException}, the thread interrupt status should be set when the exception is
         * caught, and remain set until any subsequent throw of {@code InterruptedException} (in accordance with
         * {@link #run Stage.run}). The utility method {@link Belts#composedComplete(Stream)} is provided for common use
         * cases.
         *
         * @implNote The default implementation does nothing.
         *
         * @throws Exception if unable to complete
         */
        default void complete() throws Exception { }
        
        /**
         * Signals any nearest downstream boundary sources to stop yielding elements and throw
         * {@link UpstreamException}.
         *
         * @implSpec A boundary sink should implement its {@link StepSink#offer offer} and
         * {@link #drainFromSource drainFromSource} methods to discard elements and return {@code false} after this
         * method is called, to prevent unbounded buffering or deadlock. The connected boundary source should throw an
         * {@link UpstreamException}, wrapping the cause passed to this method, upon initiating any subsequent calls to
         * {@link StepSource#poll poll} or subsequent offers in {@link Source#drainToSink drainToSink}.
         *
         * <p>A sink that delegates to downstream sinks should call {@code completeAbruptly} on each downstream sink
         * before returning from this method, <strong>even if this method throws</strong>. If completing any sink throws
         * an exception, subsequent exceptions should be suppressed onto the first exception. If completing any sink
         * throws {@link InterruptedException}, the thread interrupt status should be set when the exception is caught,
         * and remain set until any subsequent throw of {@code InterruptedException} (in accordance with
         * {@link #run Stage.run}). The utility method {@link Belts#composedCompleteAbruptly(Stream, Throwable)} is
         * provided for common use cases.
         *
         * @implNote The default implementation does nothing.
         *
         * @param cause the causal exception
         * @throws Exception if unable to complete
         */
        default void completeAbruptly(Throwable cause) throws Exception { }
        
        /**
         * Connects the {@code upstream} source before this sink. Returns a new silo that, when {@link #run run}, will
         * drain from the {@code upstream} source to this sink.
         *
         * @param upstream the upstream source
         * @return a new silo that will drain from the source to this sink
         * @throws NullPointerException if upstream is null
         */
        default Silo compose(StepSource<? extends In> upstream) {
            return new Belts.ClosedSilo<>(upstream, this);
        }
        
        /**
         * Connects the {@code upstream} segue before this sink. Returns a new sink that behaves like the segue's sink,
         * except that {@link #run running} it will also run the silo formed by connecting the segue's source to this
         * sink.
         *
         * @param upstream the upstream segue
         * @return a new sink that also runs a downstream silo
         * @param <T> the upstream sink element type
         * @throws NullPointerException if upstream is null
         */
        default <T> Sink<T> compose(SinkStepSource<? super T, ? extends In> upstream) {
            return new Belts.ChainSink<>(upstream.sink(), new Belts.ClosedSilo<>(upstream.source(), this));
        }
        
        /**
         * Connects the {@code upstream} segue before this sink. Returns a new sink that behaves like the segue's sink,
         * except that {@link #run running} it will also run the silo formed by connecting the segue's source to this
         * sink.
         *
         * @param upstream the upstream segue
         * @return a new sink that also runs a downstream silo
         * @param <T> the upstream sink element type
         * @throws NullPointerException if upstream is null
         */
        default <T> StepSink<T> compose(StepSegue<? super T, ? extends In> upstream) {
            return new Belts.ChainStepSink<>(upstream.sink(), new Belts.ClosedSilo<>(upstream.source(), this));
        }
        
        /**
         * Connects the {@code downstream} silo after this sink. Returns a new sink that behaves like this sink, except
         * that {@link #run running} it will also run the {@code downstream} silo.
         *
         * @param downstream the downstream silo
         * @return a new sink that also runs a downstream silo
         * @throws NullPointerException if downstream is null
         */
        default Sink<In> andThen(Silo downstream) {
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
     * <p>A source may encapsulate an upstream silo, which will {@link #run run} when the source runs. Sources generally
     * should be run before yielding elements, in case the source connects across an upstream boundary, to avoid the
     * effects of unmitigated buffer depletion (including potential deadlock).
     *
     * <p>This is a functional interface whose functional method is {@link #drainToSink(StepSink)}.
     *
     * @param <Out> the output element type
     */
    @FunctionalInterface
    public non-sealed interface Source<Out> extends Stage, AutoCloseable {
        /**
         * Offers as many elements as possible from this source to the sink. This proceeds until either an exception is
         * thrown, this source is drained, or this source is unable to offer more elements to the sink (which does not
         * necessarily mean the sink cancelled). Returns {@code false} if the sink definitely cancelled, meaning a call
         * to {@link StepSink#offer offer} returned {@code false}; else returns {@code true}.
         *
         * @implSpec Implementors should restrict to {@link StepSink#offer offering} to the sink. Completing the sink is
         * the caller's responsibility, as the sink may be reused after this method is called.
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
         * {@link StepSource#poll poll} and {@link #drainToSink drainToSink}. In that case, if this is a boundary
         * source, the connected boundary sink should implement its {@link StepSink#offer offer} and
         * {@link Sink#drainFromSource drainFromSource} methods to discard elements and return {@code false} after this
         * method is called, to prevent unbounded buffering or deadlock.
         *
         * <p>A source that delegates to upstream sources should call {@code close} on each upstream source before
         * returning from this method, <strong>even if this method throws</strong>. If closing any source throws an
         * exception, subsequent exceptions should be suppressed onto the first exception. The utility method
         * {@link Belts#composedClose(Stream)} is provided for common use cases.
         *
         * @implNote The default implementation does nothing.
         *
         * @throws Exception if unable to close
         */
        default void close() throws Exception { }
        
        /**
         * Performs the given action for each remaining element of the source. This proceeds until either an exception
         * is thrown, this source is drained, or this source is unable to offer more elements to the {@code Consumer}.
         *
         * @param action the action to be performed for each element
         * @throws Exception if unable to drain
         */
        default void forEach(Consumer<? super Out> action) throws Exception {
            Objects.requireNonNull(action);
            
            class ConsumerSink implements StepSink<Out> {
                @Override
                public boolean offer(Out input) {
                    action.accept(input);
                    return true;
                }
            }
            
            drainToSink(new ConsumerSink());
        }
        
        /**
         * Performs a mutable reduction operation on the remaining elements of this source using a {@code Collector}.
         * This proceeds until either an exception is thrown, this source is drained, or this source is unable to offer
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
                public boolean offer(Out input) {
                    accumulator.accept(acc, input);
                    return true;
                }
            }
            
            drainToSink(new CollectorSink());
            return finisher.apply(acc);
        }
        
        /**
         * Connects the {@code upstream} silo before this source. Returns a new source that behaves like this source,
         * except that {@link #run running} it will also run the {@code upstream} silo.
         *
         * @param upstream the upstream silo
         * @return a new source that also runs an upstream silo
         * @throws NullPointerException if upstream is null
         */
        default Source<Out> compose(Silo upstream) {
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
         * Connects the {@code downstream} sink after this source. Returns a new silo that, when {@link #run run}, will
         * drain from this source to the {@code downstream} sink.
         *
         * @param downstream the downstream sink
         * @return a new silo that will drain from this source to the sink
         * @throws NullPointerException if downstream is null
         */
        default Silo andThen(StepSink<? super Out> downstream) {
            return new Belts.ClosedSilo<>(this, downstream);
        }
        
        /**
         * Connects the {@code downstream} segue after this source. Returns a new source that behaves like the segue's
         * source, except that {@link #run running} it will also run the silo formed by connecting this source before
         * the segue's sink.
         *
         * @param downstream the downstream segue
         * @return a new source that also runs an upstream silo
         * @param <T> the downstream source element type
         * @throws NullPointerException if downstream is null
         */
        default <T> Source<T> andThen(StepSinkSource<? super Out, ? extends T> downstream) {
            return new Belts.ChainSource<>(new Belts.ClosedSilo<>(this, downstream.sink()), downstream.source());
        }
        
        /**
         * Connects the {@code downstream} segue after this source. Returns a new source that behaves like the segue's
         * source, except that {@link #run running} it will also run the silo formed by connecting this source before
         * the segue's sink.
         *
         * @param downstream the downstream segue
         * @return a new source that also runs an upstream silo
         * @param <T> the downstream source element type
         * @throws NullPointerException if downstream is null
         */
        default <T> StepSource<T> andThen(StepSegue<? super Out, ? extends T> downstream) {
            return new Belts.ChainStepSource<>(new Belts.ClosedSilo<>(this, downstream.sink()), downstream.source());
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
     * <p>This is a functional interface whose functional method is {@link #offer(Object)}.
     *
     * @param <In> the input element type
     */
    @FunctionalInterface
    public interface StepSink<In> extends Sink<In> {
        /**
         * Offers the input element to this sink for processing. Returns {@code false} if this sink cancelled during or
         * prior to this call, in which case the element may not have been fully processed.
         *
         * @implSpec Once this method returns {@code false}, subsequent calls should also discard the input element and
         * return {@code false}, to indicate the sink is permanently cancelled and no longer accepting elements.
         *
         * @param input the input element
         * @return {@code false} if this sink cancelled, else {@code true}
         * @throws Exception if unable to offer
         */
        boolean offer(In input) throws Exception;
        
        /**
         * {@inheritDoc}
         *
         * @implSpec The implementation loops, polling from the source and offering to this sink, until either the
         * source drains or this sink cancels. This is equivalent to the implementation of
         * {@link StepSource#drainToSink StepSource.drainToSink(StepSink)}.
         */
        @Override
        default boolean drainFromSource(StepSource<? extends In> source) throws Exception {
            for (In e; (e = source.poll()) != null; ) {
                if (!offer(e)) {
                    return false;
                }
            }
            return true;
        }
        
        /**
         * Connects the {@code upstream} source before this sink. Returns a new silo that, when {@link #run run}, will
         * drain from the {@code upstream} source to this sink.
         *
         * @param upstream the upstream source
         * @return a new silo that will drain from the source to this sink
         * @throws NullPointerException if upstream is null
         */
        default Silo compose(Source<? extends In> upstream) {
            return new Belts.ClosedSilo<>(upstream, this);
        }
        
        /**
         * Connects the {@code upstream} segue before this sink. Returns a new sink that behaves like the segue's sink,
         * except that {@link #run running} it will also run the silo formed by connecting the segue's source before
         * this sink.
         *
         * @param upstream the upstream segue
         * @return a new sink that also runs a downstream silo
         * @param <T> the upstream sink element type
         * @throws NullPointerException if upstream is null
         */
        default <T> Sink<T> compose(Segue<? super T, ? extends In> upstream) {
            return new Belts.ChainSink<>(upstream.sink(), new Belts.ClosedSilo<>(upstream.source(), this));
        }
        
        /**
         * Connects the {@code upstream} segue before this sink. Returns a new sink that behaves like the segue's sink,
         * except that {@link #run running} it will also run the silo formed by connecting the segue's source before
         * this sink.
         *
         * @param upstream the upstream segue
         * @return a new sink that also runs a downstream silo
         * @param <T> the upstream sink element type
         * @throws NullPointerException if upstream is null
         */
        default <T> StepSink<T> compose(StepSinkSource<T, ? extends In> upstream) {
            return new Belts.ChainStepSink<>(upstream.sink(), new Belts.ClosedSilo<>(upstream.source(), this));
        }
        
        @Override
        default StepSink<In> andThen(Silo downstream) {
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
     * <p>This is a functional interface whose functional method is {@link #poll()}.
     *
     * @param <Out> the output element type
     */
    @FunctionalInterface
    public interface StepSource<Out> extends Source<Out> {
        /**
         * Polls this source for the next element. Returns {@code null} if this source is drained.
         *
         * @implSpec Once this method returns {@code null}, subsequent calls should also return {@code null}, to indicate
         * the source is permanently drained and no longer yielding elements.
         *
         * @return the next element from this source, or {@code null} if this source is drained
         * @throws Exception if unable to poll
         */
        Out poll() throws Exception;
        
        /**
         * {@inheritDoc}
         *
         * @implSpec The implementation loops, polling from this source and offering to the sink, until either this
         * source drains or the sink cancels. This is equivalent to the implementation of
         * {@link StepSink#drainFromSource StepSink.drainFromSource(StepSource)}.
         */
        @Override
        default boolean drainToSink(StepSink<? super Out> sink) throws Exception {
            for (Out e; (e = poll()) != null; ) {
                if (!sink.offer(e)) {
                    return false;
                }
            }
            return true;
        }
        
        @Override
        default StepSource<Out> compose(Silo upstream) {
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
         * Connects the {@code downstream} sink after this source. Returns a new silo that, when {@link #run run}, will
         * drain from this source to the {@code downstream} sink.
         *
         * @param downstream the downstream sink
         * @return a new silo that will drain from this source to the sink
         * @throws NullPointerException if downstream is null
         */
        default Silo andThen(Sink<? super Out> downstream) {
            return new Belts.ClosedSilo<>(this, downstream);
        }
        
        /**
         * Connects the {@code downstream} segue after this source. Returns a new source that behaves like the segue's
         * source, except that {@link #run running} it will also run the silo formed by connecting this source before
         * the segue's sink.
         *
         * @param downstream the downstream segue
         * @return a new source that also runs an upstream silo
         * @param <T> the downstream source element type
         * @throws NullPointerException if downstream is null
         */
        default <T> Source<T> andThen(Segue<? super Out, ? extends T> downstream) {
            return new Belts.ChainSource<>(new Belts.ClosedSilo<>(this, downstream.sink()), downstream.source());
        }
        
        /**
         * Connects the {@code downstream} segue after this source. Returns a new source that behaves like the segue's
         * source, except that {@link #run running} it will also run the silo formed by connecting this source before
         * the segue's sink.
         *
         * @param downstream the downstream segue
         * @return a new source that also runs an upstream silo
         * @param <T> the downstream source element type
         * @throws NullPointerException if downstream is null
         */
        default <T> StepSource<T> andThen(SinkStepSource<? super Out, ? extends T> downstream) {
            return new Belts.ChainStepSource<>(new Belts.ClosedSilo<>(this, downstream.sink()), downstream.source());
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
     * connected, such that the elements input to the sink determine or influence the elements output from the source.
     * This allows data to transition across the "asynchronous boundary" between threads, if the thread(s) draining to
     * the sink differ from the thread(s) draining from the source.
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
         * source, except that {@link Stage#run running} it will also run the silo formed by connecting the
         * {@code upstream} source before this segue's sink.
         *
         * @param upstream the upstream source
         * @return a new source that also runs an upstream silo
         * @throws NullPointerException if upstream is null
         */
        default Source<Out> compose(StepSource<? extends In> upstream) {
            return new Belts.ChainSource<>(new Belts.ClosedSilo<>(upstream, sink()), source());
        }
        
        /**
         * Connects the {@code upstream} segue before this segue. Returns a new segue that pairs the {@code upstream}
         * segue's sink with a new source that behaves like this segue's source, except that {@link Stage#run running}
         * it will also run the silo formed between segues.
         *
         * <p>This method behaves equivalently to:
         * {@snippet :
         * this.compose(upstream.source()).compose(upstream.sink())
         * }
         *
         * @param upstream the upstream segue
         * @return a new segue whose source also runs an upstream silo
         * @param <T> the upstream sink element type
         * @throws NullPointerException if upstream is null
         */
        default <T> Segue<T, Out> compose(SinkStepSource<? super T, ? extends In> upstream) {
            return new Belts.ChainSegue<>(upstream.sink(), new Belts.ChainSource<>(new Belts.ClosedSilo<>(upstream.source(), sink()), source()));
        }
        
        /**
         * Connects the {@code upstream} segue before this segue. Returns a new segue that pairs the {@code upstream}
         * segue's sink with a new source that behaves like this segue's source, except that {@link Stage#run running}
         * it will also run the silo formed between segues.
         *
         * <p>This method behaves equivalently to:
         * {@snippet :
         * this.compose(upstream.source()).compose(upstream.sink())
         * }
         *
         * @param upstream the upstream segue
         * @return a new segue whose source also runs an upstream silo
         * @param <T> the upstream sink element type
         * @throws NullPointerException if upstream is null
         */
        default <T> StepSinkSource<T, Out> compose(StepSegue<? super T, ? extends In> upstream) {
            return new Belts.ChainStepSinkSource<>(upstream.sink(), new Belts.ChainSource<>(new Belts.ClosedSilo<>(upstream.source(), sink()), source()));
        }
        
        /**
         * Connects the {@code downstream} sink after this segue. Returns a new sink that behaves like this segue's
         * sink, except that {@link Stage#run running} it will also run the silo formed by connecting this segue's
         * source before the {@code downstream} sink.
         *
         * @param downstream the downstream sink
         * @return a new sink that also runs a downstream silo
         * @throws NullPointerException if downstream is null
         */
        default Sink<In> andThen(StepSink<? super Out> downstream) {
            return new Belts.ChainSink<>(sink(), new Belts.ClosedSilo<>(source(), downstream));
        }
        
        /**
         * Connects the {@code downstream} segue after this segue. Returns a new segue that pairs the {@code downstream}
         * segue's source with a new sink that behaves like this segue's sink, except that {@link Stage#run running}
         * it will also run the silo formed between segues.
         *
         * <p>This method behaves equivalently to:
         * {@snippet :
         * this.andThen(downstream.sink()).andThen(downstream.source())
         * }
         *
         * @param downstream the downstream segue
         * @return a new segue whose sink also runs a downstream silo
         * @param <T> the downstream source element type
         * @throws NullPointerException if downstream is null
         */
        default <T> Segue<In, T> andThen(StepSinkSource<? super Out, ? extends T> downstream) {
            return new Belts.ChainSegue<>(new Belts.ChainSink<>(sink(), new Belts.ClosedSilo<>(source(), downstream.sink())), downstream.source());
        }
        
        /**
         * Connects the {@code downstream} segue after this segue. Returns a new segue that pairs the {@code downstream}
         * segue's source with a new sink that behaves like this segue's sink, except that {@link Stage#run running}
         * it will also run the silo formed between segues.
         *
         * <p>This method behaves equivalently to:
         * {@snippet :
         * this.andThen(downstream.sink()).andThen(downstream.source())
         * }
         *
         * @param downstream the downstream segue
         * @return a new segue whose sink also runs a downstream silo
         * @param <T> the downstream source element type
         * @throws NullPointerException if downstream is null
         */
        default <T> SinkStepSource<In, T> andThen(StepSegue<? super Out, ? extends T> downstream) {
            return new Belts.ChainSinkStepSource<>(new Belts.ChainSink<>(sink(), new Belts.ClosedSilo<>(source(), downstream.sink())), downstream.source());
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
         * source, except that {@link Stage#run running} it will also run the silo formed by connecting the
         * {@code upstream} source before this segue's sink.
         *
         * @param upstream the upstream source
         * @return a new source that also runs an upstream silo
         * @throws NullPointerException if upstream is null
         */
        default Source<Out> compose(Source<? extends In> upstream) {
            return new Belts.ChainSource<>(new Belts.ClosedSilo<>(upstream, sink()), source());
        }
        
        /**
         * Connects the {@code upstream} segue before this segue. Returns a new segue that pairs the {@code upstream}
         * segue's sink with a new source that behaves like this segue's source, except that {@link Stage#run running}
         * it will also run the silo formed between segues.
         *
         * <p>This method behaves equivalently to:
         * {@snippet :
         * this.compose(upstream.source()).compose(upstream.sink())
         * }
         *
         * @param upstream the upstream segue
         * @return a new segue whose source also runs an upstream silo
         * @param <T> the upstream sink element type
         * @throws NullPointerException if upstream is null
         */
        default <T> Segue<T, Out> compose(Segue<? super T, ? extends In> upstream) {
            return new Belts.ChainSegue<>(upstream.sink(), new Belts.ChainSource<>(new Belts.ClosedSilo<>(upstream.source(), sink()), source()));
        }
        
        /**
         * Connects the {@code upstream} segue before this segue. Returns a new segue that pairs the {@code upstream}
         * segue's sink with a new source that behaves like this segue's source, except that {@link Stage#run running}
         * it will also run the silo formed between segues.
         *
         * <p>This method behaves equivalently to:
         * {@snippet :
         * this.compose(upstream.source()).compose(upstream.sink())
         * }
         *
         * @param upstream the upstream segue
         * @return a new segue whose source also runs an upstream silo
         * @param <T> the upstream sink element type
         * @throws NullPointerException if upstream is null
         */
        default <T> StepSinkSource<T, Out> compose(StepSinkSource<? super T, ? extends In> upstream) {
            return new Belts.ChainStepSinkSource<>(upstream.sink(), new Belts.ChainSource<>(new Belts.ClosedSilo<>(upstream.source(), sink()), source()));
        }
        
        @Override
        default StepSink<In> andThen(StepSink<? super Out> downstream) {
            return new Belts.ChainStepSink<>(sink(), new Belts.ClosedSilo<>(source(), downstream));
        }
        
        @Override
        default <T> StepSinkSource<In, T> andThen(StepSinkSource<? super Out, ? extends T> downstream) {
            return new Belts.ChainStepSinkSource<>(new Belts.ChainStepSink<>(sink(), new Belts.ClosedSilo<>(source(), downstream.sink())), downstream.source());
        }
        
        @Override
        default <T> StepSegue<In, T> andThen(StepSegue<? super Out, ? extends T> downstream) {
            return new Belts.ChainStepSegue<>(new Belts.ChainStepSink<>(sink(), new Belts.ClosedSilo<>(source(), downstream.sink())), downstream.source());
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
            return new Belts.ChainStepSource<>(new Belts.ClosedSilo<>(upstream, sink()), source());
        }
        
        @Override
        default <T> SinkStepSource<T, Out> compose(SinkStepSource<? super T, ? extends In> upstream) {
            return new Belts.ChainSinkStepSource<>(upstream.sink(), new Belts.ChainStepSource<>(new Belts.ClosedSilo<>(upstream.source(), sink()), source()));
        }
        
        @Override
        default <T> StepSegue<T, Out> compose(StepSegue<? super T, ? extends In> upstream) {
            return new Belts.ChainStepSegue<>(upstream.sink(), new Belts.ChainStepSource<>(new Belts.ClosedSilo<>(upstream.source(), sink()), source()));
        }
        
        /**
         * Connects the {@code downstream} sink after this segue. Returns a new sink that behaves like this segue's
         * sink, except that {@link Stage#run running} it will also run the silo formed by connecting this segue's
         * source before the {@code downstream} sink.
         *
         * @param downstream the downstream sink
         * @return a new sink that also runs a downstream silo
         * @throws NullPointerException if downstream is null
         */
        default Sink<In> andThen(Sink<? super Out> downstream) {
            return new Belts.ChainSink<>(sink(), new Belts.ClosedSilo<>(source(), downstream));
        }
        
        /**
         * Connects the {@code downstream} segue after this segue. Returns a new segue that pairs the {@code downstream}
         * segue's source with a new sink that behaves like this segue's sink, except that {@link Stage#run running}
         * it will also run the silo formed between segues.
         *
         * <p>This method behaves equivalently to:
         * {@snippet :
         * this.andThen(downstream.sink()).andThen(downstream.source())
         * }
         *
         * @param downstream the downstream segue
         * @return a new segue whose sink also runs a downstream silo
         * @param <T> the downstream source element type
         * @throws NullPointerException if downstream is null
         */
        default <T> Segue<In, T> andThen(Segue<? super Out, ? extends T> downstream) {
            return new Belts.ChainSegue<>(new Belts.ChainSink<>(sink(), new Belts.ClosedSilo<>(source(), downstream.sink())), downstream.source());
        }
        
        /**
         * Connects the {@code downstream} segue after this segue. Returns a new segue that pairs the {@code downstream}
         * segue's source with a new sink that behaves like this segue's sink, except that {@link Stage#run running}
         * it will also run the silo formed between segues.
         *
         * <p>This method behaves equivalently to:
         * {@snippet :
         * this.andThen(downstream.sink()).andThen(downstream.source())
         * }
         *
         * @param downstream the downstream segue
         * @return a new segue whose sink also runs a downstream silo
         * @param <T> the downstream source element type
         * @throws NullPointerException if downstream is null
         */
        default <T> SinkStepSource<In, T> andThen(SinkStepSource<? super Out, ? extends T> downstream) {
            return new Belts.ChainSinkStepSource<>(new Belts.ChainSink<>(sink(), new Belts.ClosedSilo<>(source(), downstream.sink())), downstream.source());
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
            return new Belts.ChainStepSource<>(new Belts.ClosedSilo<>(upstream, sink()), source());
        }
        
        @Override
        default <T> SinkStepSource<T, Out> compose(Segue<? super T, ? extends In> upstream) {
            return new Belts.ChainSinkStepSource<>(upstream.sink(), new Belts.ChainStepSource<>(new Belts.ClosedSilo<>(upstream.source(), sink()), source()));
        }
        
        @Override
        default <T> StepSegue<T, Out> compose(StepSinkSource<? super T, ? extends In> upstream) {
            return new Belts.ChainStepSegue<>(upstream.sink(), new Belts.ChainStepSource<>(new Belts.ClosedSilo<>(upstream.source(), sink()), source()));
        }
        
        @Override
        default StepSink<In> andThen(Sink<? super Out> downstream) {
            return new Belts.ChainStepSink<>(sink(), new Belts.ClosedSilo<>(source(), downstream));
        }
        
        @Override
        default <T> StepSinkSource<In, T> andThen(Segue<? super Out, ? extends T> downstream) {
            return new Belts.ChainStepSinkSource<>(new Belts.ChainStepSink<>(sink(), new Belts.ClosedSilo<>(source(), downstream.sink())), downstream.source());
        }
        
        @Override
        default <T> StepSegue<In, T> andThen(SinkStepSource<? super Out, ? extends T> downstream) {
            return new Belts.ChainStepSegue<>(new Belts.ChainStepSink<>(sink(), new Belts.ClosedSilo<>(source(), downstream.sink())), downstream.source());
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
    // Sink<T> = SinkOperator<T, U> andThen Sink<U>
    //         = Sink<U> compose SinkOperator<T, U>
    
    /**
     * Represents an operation on a downstream {@link Sink Sink} that produces an upstream {@code Sink}.
     *
     * <p>Note that methods on this interface are named according to the direction of data flow, not the direction of
     * function application. Methods named "andThen" take an argument that applies downstream of this operator. Methods
     * named "compose" take an argument that applies upstream of this operator.
     *
     * <p>This is a functional interface whose functional method is {@link #andThen(Sink)}.
     *
     * @param <T> the upstream element type
     * @param <U> the downstream element type
     */
    @FunctionalInterface
    public interface SinkOperator<T, U> {
        /**
         * Connects the {@code downstream} sink after this operator. Returns an upstream sink.
         *
         * @implSpec The upstream sink is generally expected to internally delegate to the downstream sink in some way.
         * This allows for transforming elements or signals as they travel from upstream to downstream.
         *
         * @param downstream the downstream sink
         * @return the upstream sink
         */
        Sink<T> andThen(Sink<? super U> downstream);
        
        /**
         * Connects the {@code downstream} segue after this operator. Returns a new segue that pairs the
         * {@code downstream} segue's source with the upstream sink obtained by applying this operator to the
         * {@code downstream} segue's sink.
         *
         * @param downstream the downstream segue
         * @return a new segue with sink transformed
         * @param <Out> the downstream source element type
         * @throws NullPointerException if downstream is null
         */
        default <Out> Segue<T, Out> andThen(Segue<? super U, ? extends Out> downstream) {
            return andThen(downstream.sink()).andThen(downstream.source());
        }
        
        /**
         * Connects the {@code downstream} segue after this operator. Returns a new segue that pairs the
         * {@code downstream} segue's source with the upstream sink obtained by applying this operator to the
         * {@code downstream} segue's sink.
         *
         * @param downstream the downstream segue
         * @return a new segue with sink transformed
         * @param <Out> the downstream source element type
         * @throws NullPointerException if downstream is null
         */
        default <Out> SinkStepSource<T, Out> andThen(SinkStepSource<? super U, ? extends Out> downstream) {
            return andThen(downstream.sink()).andThen(downstream.source());
        }
        
        /**
         * Connects the {@code upstream} operator before this operator. Returns a composed operator that applies the
         * {@code upstream} operator to the result of this operator.
         *
         * @param upstream the upstream operator
         * @return a composed operator that applies the {@code upstream} operator to the result of this operator
         * @param <V> the upstream element type
         * @throws NullPointerException if upstream is null
         */
        @SuppressWarnings("unchecked")
        default <V> SinkOperator<V, U> compose(SinkOperator<? super V, ? extends T> upstream) {
            Objects.requireNonNull(upstream);
            return sink -> (Sink<V>) upstream.andThen(andThen(sink));
        }
        
        /**
         * Connects the {@code upstream} operator before this operator. Returns a composed operator that applies the
         * {@code upstream} operator to the result of this operator.
         *
         * @param upstream the upstream operator
         * @return a composed operator that applies the {@code upstream} operator to the result of this operator
         * @param <V> the upstream element type
         * @throws NullPointerException if upstream is null
         */
        @SuppressWarnings("unchecked")
        default <V> StepToSinkOperator<V, U> compose(StepToSinkOperator<? super V, ? extends T> upstream) {
            Objects.requireNonNull(upstream);
            return sink -> (StepSink<V>) upstream.andThen(andThen(sink));
        }
        
        /**
         * Connects the {@code downstream} operator after this operator. Returns a composed operator that applies this
         * operator to the result of the {@code downstream} operator.
         *
         * @param downstream the downstream operator
         * @return a composed operator that applies this operator to the result of the {@code downstream} operator
         * @param <V> the downstream element type
         * @throws NullPointerException if downstream is null
         */
        default <V> SinkOperator<T, V> andThen(SinkOperator<? super U, ? extends V> downstream) {
            Objects.requireNonNull(downstream);
            return sink -> andThen(downstream.andThen(sink));
        }
        
        /**
         * Connects the {@code downstream} operator after this operator. Returns a composed operator that applies this
         * operator to the result of the {@code downstream} operator.
         *
         * @param downstream the downstream operator
         * @return a composed operator that applies this operator to the result of the {@code downstream} operator
         * @param <V> the downstream element type
         * @throws NullPointerException if downstream is null
         */
        default <V> SinkToStepOperator<T, V> andThen(SinkToStepOperator<? super U, ? extends V> downstream) {
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
     * @param <T> the upstream element type
     * @param <U> the downstream element type
     */
    @FunctionalInterface
    public interface SinkToStepOperator<T, U> {
        /**
         * Connects the {@code downstream} sink after this operator. Returns an upstream sink.
         *
         * @implSpec The upstream sink is generally expected to internally delegate to the downstream sink in some way.
         * This allows for transforming elements or signals as they travel from upstream to downstream.
         *
         * @param downstream the downstream sink
         * @return the upstream sink
         */
        Sink<T> andThen(StepSink<? super U> downstream);
        
        /**
         * Connects the {@code downstream} segue after this operator. Returns a new segue that pairs the
         * {@code downstream} segue's source with the upstream sink obtained by applying this operator to the
         * {@code downstream} segue's sink.
         *
         * @param downstream the downstream segue
         * @return a new segue with sink transformed
         * @param <Out> the downstream source element type
         * @throws NullPointerException if downstream is null
         */
        default <Out> Segue<T, Out> andThen(StepSinkSource<? super U, ? extends Out> downstream) {
            return andThen(downstream.sink()).andThen(downstream.source());
        }
        
        /**
         * Connects the {@code downstream} segue after this operator. Returns a new segue that pairs the
         * {@code downstream} segue's source with the upstream sink obtained by applying this operator to the
         * {@code downstream} segue's sink.
         *
         * @param downstream the downstream segue
         * @return a new segue with sink transformed
         * @param <Out> the downstream source element type
         * @throws NullPointerException if downstream is null
         */
        default <Out> SinkStepSource<T, Out> andThen(StepSegue<? super U, ? extends Out> downstream) {
            return andThen(downstream.sink()).andThen(downstream.source());
        }
        
        /**
         * Connects the {@code upstream} operator before this operator. Returns a composed operator that applies the
         * {@code upstream} operator to the result of this operator.
         *
         * @param upstream the upstream operator
         * @return a composed operator that applies the {@code upstream} operator to the result of this operator
         * @param <V> the upstream element type
         * @throws NullPointerException if upstream is null
         */
        @SuppressWarnings("unchecked")
        default <V> SinkToStepOperator<V, U> compose(SinkOperator<? super V, ? extends T> upstream) {
            Objects.requireNonNull(upstream);
            return sink -> (Sink<V>) upstream.andThen(andThen(sink));
        }
        
        /**
         * Connects the {@code upstream} operator before this operator. Returns a composed operator that applies the
         * {@code upstream} operator to the result of this operator.
         *
         * @param upstream the upstream operator
         * @return a composed operator that applies the {@code upstream} operator to the result of this operator
         * @param <V> the upstream element type
         * @throws NullPointerException if upstream is null
         */
        @SuppressWarnings("unchecked")
        default <V> StepSinkOperator<V, U> compose(StepToSinkOperator<? super V, ? extends T> upstream) {
            Objects.requireNonNull(upstream);
            return sink -> (StepSink<V>) upstream.andThen(andThen(sink));
        }
        
        /**
         * Connects the {@code downstream} operator after this operator. Returns a composed operator that applies this
         * operator to the result of the {@code downstream} operator.
         *
         * @param downstream the downstream operator
         * @return a composed operator that applies this operator to the result of the {@code downstream} operator
         * @param <V> the downstream element type
         * @throws NullPointerException if downstream is null
         */
        default <V> SinkOperator<T, V> andThen(StepToSinkOperator<? super U, ? extends V> downstream) {
            Objects.requireNonNull(downstream);
            return sink -> andThen(downstream.andThen(sink));
        }
        
        /**
         * Connects the {@code downstream} operator after this operator. Returns a composed operator that applies this
         * operator to the result of the {@code downstream} operator.
         *
         * @param downstream the downstream operator
         * @return a composed operator that applies this operator to the result of the {@code downstream} operator
         * @param <V> the downstream element type
         * @throws NullPointerException if downstream is null
         */
        default <V> SinkToStepOperator<T, V> andThen(StepSinkOperator<? super U, ? extends V> downstream) {
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
     * @param <T> the upstream element type
     * @param <U> the downstream element type
     */
    @FunctionalInterface
    public interface StepToSinkOperator<T, U> extends SinkOperator<T, U> {
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
        StepSink<T> andThen(Sink<? super U> downstream);
        
        /**
         * Connects the {@code downstream} segue after this operator. Returns a new segue that pairs the
         * {@code downstream} segue's source with the upstream sink obtained by applying this operator to the
         * {@code downstream} segue's sink.
         *
         * @param downstream the downstream segue
         * @return a new segue with sink transformed
         * @param <Out> the downstream source element type
         * @throws NullPointerException if after is null
         */
        @Override
        default <Out> StepSinkSource<T, Out> andThen(Segue<? super U, ? extends Out> downstream) {
            return andThen(downstream.sink()).andThen(downstream.source());
        }
        
        /**
         * Connects the {@code downstream} segue after this operator. Returns a new segue that pairs the
         * {@code downstream} segue's source with the upstream sink obtained by applying this operator to the
         * {@code downstream} segue's sink.
         *
         * @param downstream the downstream segue
         * @return a new segue with sink transformed
         * @param <Out> the downstream source element type
         * @throws NullPointerException if after is null
         */
        @Override
        default <Out> StepSegue<T, Out> andThen(SinkStepSource<? super U, ? extends Out> downstream) {
            return andThen(downstream.sink()).andThen(downstream.source());
        }
        
        /**
         * Connects the {@code upstream} operator before this operator. Returns a composed operator that applies the
         * {@code upstream} operator to the result of this operator.
         *
         * @param upstream the upstream operator
         * @return a composed operator that applies the {@code upstream} operator to the result of this operator
         * @param <V> the upstream element type
         * @throws NullPointerException if upstream is null
         */
        @SuppressWarnings("unchecked")
        default <V> SinkOperator<V, U> compose(SinkToStepOperator<? super V, ? extends T> upstream) {
            Objects.requireNonNull(upstream);
            return sink -> (Sink<V>) upstream.andThen(andThen(sink));
        }
        
        /**
         * Connects the {@code upstream} operator before this operator. Returns a composed operator that applies the
         * {@code upstream} operator to the result of this operator.
         *
         * @param upstream the upstream operator
         * @return a composed operator that applies the {@code upstream} operator to the result of this operator
         * @param <V> the upstream element type
         * @throws NullPointerException if upstream is null
         */
        @SuppressWarnings("unchecked")
        default <V> StepToSinkOperator<V, U> compose(StepSinkOperator<? super V, ? extends T> upstream) {
            Objects.requireNonNull(upstream);
            return sink -> (StepSink<V>) upstream.andThen(andThen(sink));
        }
        
        /**
         * Connects the {@code downstream} operator after this operator. Returns a composed operator that applies this
         * operator to the result of the {@code downstream} operator.
         *
         * @param downstream the downstream operator
         * @return a composed operator that applies this operator to the result of the {@code downstream} operator
         * @param <V> the downstream element type
         * @throws NullPointerException if downstream is null
         */
        @Override
        default <V> StepToSinkOperator<T, V> andThen(SinkOperator<? super U, ? extends V> downstream) {
            Objects.requireNonNull(downstream);
            return sink -> andThen(downstream.andThen(sink));
        }
        
        /**
         * Connects the {@code downstream} operator after this operator. Returns a composed operator that applies this
         * operator to the result of the {@code downstream} operator.
         *
         * @param downstream the downstream operator
         * @return a composed operator that applies this operator to the result of the {@code downstream} operator
         * @param <V> the downstream element type
         * @throws NullPointerException if downstream is null
         */
        @Override
        default <V> StepSinkOperator<T, V> andThen(SinkToStepOperator<? super U, ? extends V> downstream) {
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
     * @param <T> the upstream element type
     * @param <U> the downstream element type
     */
    @FunctionalInterface
    public interface StepSinkOperator<T, U> extends SinkToStepOperator<T, U> {
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
        StepSink<T> andThen(StepSink<? super U> downstream);
        
        /**
         * Connects the {@code downstream} segue after this operator. Returns a new segue that pairs the
         * {@code downstream} segue's source with the upstream sink obtained by applying this operator to the
         * {@code downstream} segue's sink.
         *
         * @param downstream the downstream segue
         * @return a new segue with sink transformed
         * @param <Out> the downstream source element type
         * @throws NullPointerException if downstream is null
         */
        @Override
        default <Out> StepSinkSource<T, Out> andThen(StepSinkSource<? super U, ? extends Out> downstream) {
            return andThen(downstream.sink()).andThen(downstream.source());
        }
        
        /**
         * Connects the {@code downstream} segue after this operator. Returns a new segue that pairs the
         * {@code downstream} segue's source with the upstream sink obtained by applying this operator to the
         * {@code downstream} segue's sink.
         *
         * @param downstream the downstream segue
         * @return a new segue with sink transformed
         * @param <Out> the downstream source element type
         * @throws NullPointerException if downstream is null
         */
        @Override
        default <Out> StepSegue<T, Out> andThen(StepSegue<? super U, ? extends Out> downstream) {
            return andThen(downstream.sink()).andThen(downstream.source());
        }
        
        /**
         * Connects the {@code upstream} operator before this operator. Returns a composed operator that applies the
         * {@code upstream} operator to the result of this operator.
         *
         * @param upstream the upstream operator
         * @return a composed operator that applies the {@code upstream} operator to the result of this operator
         * @param <V> the upstream element type
         * @throws NullPointerException if upstream is null
         */
        @SuppressWarnings("unchecked")
        default <V> SinkToStepOperator<V, U> compose(SinkToStepOperator<? super V, ? extends T> upstream) {
            Objects.requireNonNull(upstream);
            return sink -> (Sink<V>) upstream.andThen(andThen(sink));
        }
        
        /**
         * Connects the {@code upstream} operator before this operator. Returns a composed operator that applies the
         * {@code upstream} operator to the result of this operator.
         *
         * @param upstream the upstream operator
         * @return a composed operator that applies the {@code upstream} operator to the result of this operator
         * @param <V> the upstream element type
         * @throws NullPointerException if upstream is null
         */
        @SuppressWarnings("unchecked")
        default <V> StepSinkOperator<V, U> compose(StepSinkOperator<? super V, ? extends T> upstream) {
            Objects.requireNonNull(upstream);
            return sink -> (StepSink<V>) upstream.andThen(andThen(sink));
        }
        
        /**
         * Connects the {@code downstream} operator after this operator. Returns a composed operator that applies this
         * operator to the result of the {@code downstream} operator.
         *
         * @param downstream the downstream operator
         * @return a composed operator that applies this operator to the result of the {@code downstream} operator
         * @param <V> the downstream element type
         * @throws NullPointerException if downstream is null
         */
        @Override
        default <V> StepToSinkOperator<T, V> andThen(StepToSinkOperator<? super U, ? extends V> downstream) {
            Objects.requireNonNull(downstream);
            return sink -> andThen(downstream.andThen(sink));
        }
        
        /**
         * Connects the {@code downstream} operator after this operator. Returns a composed operator that applies this
         * operator to the result of the {@code downstream} operator.
         *
         * @param downstream the downstream operator
         * @return a composed operator that applies this operator to the result of the {@code downstream} operator
         * @param <V> the downstream element type
         * @throws NullPointerException if downstream is null
         */
        @Override
        default <V> StepSinkOperator<T, V> andThen(StepSinkOperator<? super U, ? extends V> downstream) {
            Objects.requireNonNull(downstream);
            return sink -> andThen(downstream.andThen(sink));
        }
    }

    // --- Source Operators ---
    // Source<U> = SourceOperator<T, U> compose Source<T>
    //           = Source<T> andThen SourceOperator<T, U>
    
    /**
     * Represents an operation on an upstream {@link Source Source} that produces a downstream {@code Source}.
     *
     * <p>This is a functional interface whose functional method is {@link #compose(Source)}.
     *
     * @param <T> the upstream element type
     * @param <U> the downstream element type
     */
    @FunctionalInterface
    public interface SourceOperator<T, U> {
        /**
         * Connects the {@code upstream} source before this operator. Returns a downstream source.
         *
         * @implSpec The downstream source is generally expected to internally delegate to the upstream source in some
         * way. This allows for transforming elements or signals as they travel from upstream to downstream.
         *
         * @param upstream the upstream source
         * @return the downstream source
         */
        Source<U> compose(Source<? extends T> upstream);
        
        /**
         * Connects the {@code upstream} segue before this operator. Returns a new segue that pairs the {@code upstream}
         * segue's sink with the downstream source obtained by applying this operator to the {@code upstream} segue's
         * source.
         *
         * @param upstream the upstream segue
         * @return a new segue with source transformed
         * @param <In> the upstream sink element type
         * @throws NullPointerException if upstream is null
         */
        default <In> Segue<In, U> compose(Segue<? super In, ? extends T> upstream) {
            return compose(upstream.source()).compose(upstream.sink());
        }
        
        /**
         * Connects the {@code upstream} segue before this operator. Returns a new segue that pairs the {@code upstream}
         * segue's sink with the downstream source obtained by applying this operator to the {@code upstream} segue's
         * source.
         *
         * @param upstream the upstream segue
         * @return a new segue with source transformed
         * @param <In> the upstream sink element type
         * @throws NullPointerException if upstream is null
         */
        default <In> StepSinkSource<In, U> compose(StepSinkSource<? super In, ? extends T> upstream) {
            return compose(upstream.source()).compose(upstream.sink());
        }
        
        /**
         * Connects the {@code upstream} operator before this operator. Returns a composed operator that applies this
         * operator to the result of the {@code upstream} operator.
         *
         * @param upstream the upstream operator
         * @return a composed operator that applies this operator to the result of the {@code upstream} operator
         * @param <V> the upstream element type
         * @throws NullPointerException if upstream is null
         */
        default <V> StepToSourceOperator<V, U> compose(StepToSourceOperator<? super V, ? extends T> upstream) {
            Objects.requireNonNull(upstream);
            return source -> compose(upstream.compose(source));
        }
        
        /**
         * Connects the {@code upstream} operator before this operator. Returns a composed operator that applies this
         * operator to the result of the {@code upstream} operator.
         *
         * @param upstream the upstream operator
         * @return a composed operator that applies this operator to the result of the {@code upstream} operator
         * @param <V> the upstream element type
         * @throws NullPointerException if upstream is null
         */
        default <V> SourceOperator<V, U> compose(SourceOperator<? super V, ? extends T> upstream) {
            Objects.requireNonNull(upstream);
            return source -> compose(upstream.compose(source));
        }
        
        /**
         * Connects the {@code downstream} operator after this operator. Returns a composed operator that applies the
         * {@code downstream} operator to the result of this operator.
         *
         * @param downstream the downstream operator
         * @return a composed operator that applies the {@code downstream} operator to the result of this operator
         * @param <V> the downstream element type
         * @throws NullPointerException if downstream is null
         */
        @SuppressWarnings("unchecked")
        default <V> SourceOperator<T, V> andThen(SourceOperator<? super U, ? extends V> downstream) {
            Objects.requireNonNull(downstream);
            return source -> (Source<V>) downstream.compose(compose(source));
        }
        
        /**
         * Connects the {@code downstream} operator after this operator. Returns a composed operator that applies the
         * {@code downstream} operator to the result of this operator.
         *
         * @param downstream the downstream operator
         * @return a composed operator that applies the {@code downstream} operator to the result of this operator
         * @param <V> the downstream element type
         * @throws NullPointerException if downstream is null
         */
        @SuppressWarnings("unchecked")
        default <V> SourceToStepOperator<T, V> andThen(SourceToStepOperator<? super U, ? extends V> downstream) {
            Objects.requireNonNull(downstream);
            return source -> (StepSource<V>) downstream.compose(compose(source));
        }
    }
    
    /**
     * Represents an operation on an upstream {@link StepSource StepSource} that produces a downstream
     * {@link Source Source}.
     *
     * <p>This is a functional interface whose functional method is {@link #compose(StepSource)}.
     *
     * @param <T> the upstream element type
     * @param <U> the downstream element type
     */
    @FunctionalInterface
    public interface StepToSourceOperator<T, U> {
        /**
         * Connects the {@code upstream} source upstream this operator. Returns a downstream source.
         *
         * @implSpec The downstream source is generally expected to internally delegate to the upstream source in some
         * way. This allows for transforming elements or signals as they travel from upstream to downstream.
         *
         * @param upstream the upstream source
         * @return the downstream source
         */
        Source<U> compose(StepSource<? extends T> upstream);
        
        /**
         * Connects the {@code upstream} segue before this operator. Returns a new segue that pairs the {@code upstream}
         * segue's sink with the downstream source obtained by applying this operator to the {@code upstream} segue's
         * source.
         *
         * @param upstream the upstream segue
         * @return a new segue with source transformed
         * @param <In> the upstream sink element type
         * @throws NullPointerException if upstream is null
         */
        default <In> Segue<In, U> compose(SinkStepSource<? super In, ? extends T> upstream) {
            return compose(upstream.source()).compose(upstream.sink());
        }
        
        /**
         * Connects the {@code upstream} segue before this operator. Returns a new segue that pairs the {@code upstream}
         * segue's sink with the downstream source obtained by applying this operator to the {@code upstream} segue's
         * source.
         *
         * @param upstream the upstream segue
         * @return a new segue with source transformed
         * @param <In> the upstream sink element type
         * @throws NullPointerException if upstream is null
         */
        default <In> StepSinkSource<In, U> compose(StepSegue<? super In, ? extends T> upstream) {
            return compose(upstream.source()).compose(upstream.sink());
        }
        
        /**
         * Connects the {@code upstream} operator before this operator. Returns a composed operator that applies this
         * operator to the result of the {@code upstream} operator.
         *
         * @param upstream the upstream operator
         * @return a composed operator that applies this operator to the result of the {@code upstream} operator
         * @param <V> the upstream element type
         * @throws NullPointerException if upstream is null
         */
        default <V> SourceOperator<V, U> compose(SourceToStepOperator<? super V, ? extends T> upstream) {
            Objects.requireNonNull(upstream);
            return source -> compose(upstream.compose(source));
        }
        
        /**
         * Connects the {@code upstream} operator before this operator. Returns a composed operator that applies this
         * operator to the result of the {@code upstream} operator.
         *
         * @param upstream the upstream operator
         * @return a composed operator that applies this operator to the result of the {@code upstream} operator
         * @param <V> the upstream element type
         * @throws NullPointerException if upstream is null
         */
        default <V> StepToSourceOperator<V, U> compose(StepSourceOperator<? super V, ? extends T> upstream) {
            Objects.requireNonNull(upstream);
            return source -> compose(upstream.compose(source));
        }
        
        /**
         * Connects the {@code downstream} operator after this operator. Returns a composed operator that applies the
         * {@code downstream} operator to the result of this operator.
         *
         * @param downstream the downstream operator
         * @return a composed operator that applies the {@code downstream} operator to the result of this operator
         * @param <V> the downstream element type
         * @throws NullPointerException if downstream is null
         */
        @SuppressWarnings("unchecked")
        default <V> StepToSourceOperator<T, V> andThen(SourceOperator<? super U, ? extends V> downstream) {
            Objects.requireNonNull(downstream);
            return source -> (Source<V>) downstream.compose(compose(source));
        }
        
        /**
         * Connects the {@code downstream} operator after this operator. Returns a composed operator that applies the
         * {@code downstream} operator to the result of this operator.
         *
         * @param downstream the downstream operator
         * @return a composed operator that applies the {@code downstream} operator to the result of this operator
         * @param <V> the downstream element type
         * @throws NullPointerException if downstream is null
         */
        @SuppressWarnings("unchecked")
        default <V> StepSourceOperator<T, V> andThen(SourceToStepOperator<? super U, ? extends V> downstream) {
            Objects.requireNonNull(downstream);
            return source -> (StepSource<V>) downstream.compose(compose(source));
        }
    }
    
    /**
     * Represents an operation on an upstream {@link Source Source} that produces a downstream
     * {@link StepSource StepSource}.
     *
     * <p>This is a functional interface whose functional method is {@link #compose(Source)}.
     *
     * @param <T> the upstream element type
     * @param <U> the downstream element type
     */
    @FunctionalInterface
    public interface SourceToStepOperator<T, U> extends SourceOperator<T, U> {
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
        StepSource<U> compose(Source<? extends T> upstream);
        
        /**
         * Connects the {@code upstream} segue before this operator. Returns a new segue that pairs the {@code upstream}
         * segue's sink with the downstream source obtained by applying this operator to the {@code upstream} segue's
         * source.
         *
         * @param upstream the upstream segue
         * @return a new segue with source transformed
         * @param <In> the upstream sink element type
         * @throws NullPointerException if before is null
         */
        @Override
        default <In> SinkStepSource<In, U> compose(Segue<? super In, ? extends T> upstream) {
            return compose(upstream.source()).compose(upstream.sink());
        }
        
        /**
         * Connects the {@code upstream} segue before this operator. Returns a new segue that pairs the {@code upstream}
         * segue's sink with the downstream source obtained by applying this operator to the {@code upstream} segue's
         * source.
         *
         * @param upstream the upstream segue
         * @return a new segue with source transformed
         * @param <In> the upstream sink element type
         * @throws NullPointerException if before is null
         */
        @Override
        default <In> StepSegue<In, U> compose(StepSinkSource<? super In, ? extends T> upstream) {
            return compose(upstream.source()).compose(upstream.sink());
        }
        
        /**
         * Connects the {@code upstream} operator before this operator. Returns a composed operator that applies this
         * operator to the result of the {@code upstream} operator.
         *
         * @param upstream the upstream operator
         * @return a composed operator that applies this operator to the result of the {@code before} operator
         * @param <V> the upstream element type
         * @throws NullPointerException if before is null
         */
        @Override
        default <V> SourceToStepOperator<V, U> compose(SourceOperator<? super V, ? extends T> upstream) {
            Objects.requireNonNull(upstream);
            return source -> compose(upstream.compose(source));
        }
        
        /**
         * Connects the {@code upstream} operator before this operator. Returns a composed operator that applies this
         * operator to the result of the {@code upstream} operator.
         *
         * @param upstream the upstream operator
         * @return a composed operator that applies this operator to the result of the {@code before} operator
         * @param <V> the upstream element type
         * @throws NullPointerException if before is null
         */
        @Override
        default <V> StepSourceOperator<V, U> compose(StepToSourceOperator<? super V, ? extends T> upstream) {
            Objects.requireNonNull(upstream);
            return source -> compose(upstream.compose(source));
        }
        
        /**
         * Connects the {@code downstream} operator after this operator. Returns a composed operator that applies the
         * {@code downstream} operator to the result of this operator.
         *
         * @param downstream the downstream operator
         * @return a composed operator that applies the {@code downstream} operator to the result of this operator
         * @param <V> the downstream element type
         * @throws NullPointerException if downstream is null
         */
        @SuppressWarnings("unchecked")
        default <V> SourceOperator<T, V> andThen(StepToSourceOperator<? super U, ? extends V> downstream) {
            Objects.requireNonNull(downstream);
            return source -> (Source<V>) downstream.compose(compose(source));
        }
        
        /**
         * Connects the {@code downstream} operator after this operator. Returns a composed operator that applies the
         * {@code downstream} operator to the result of this operator.
         *
         * @param downstream the downstream operator
         * @return a composed operator that applies the {@code downstream} operator to the result of this operator
         * @param <V> the downstream element type
         * @throws NullPointerException if downstream is null
         */
        @SuppressWarnings("unchecked")
        default <V> SourceToStepOperator<T, V> andThen(StepSourceOperator<? super U, ? extends V> downstream) {
            Objects.requireNonNull(downstream);
            return source -> (StepSource<V>) downstream.compose(compose(source));
        }
    }
    
    /**
     * Represents an operation on an upstream {@link StepSource StepSource} that produces a downstream
     * {@code StepSource}.
     *
     * <p>This is a functional interface whose functional method is {@link #compose(StepSource)}.
     *
     * @param <T> the upstream element type
     * @param <U> the downstream element type
     */
    @FunctionalInterface
    public interface StepSourceOperator<T, U> extends StepToSourceOperator<T, U> {
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
        StepSource<U> compose(StepSource<? extends T> upstream);
        
        /**
         * Connects the {@code upstream} segue before this operator. Returns a new segue that pairs the {@code upstream}
         * segue's sink with the downstream source obtained by applying this operator to the {@code upstream} segue's
         * source.
         *
         * @param upstream the upstream segue
         * @return a new segue with source transformed
         * @param <In> the upstream sink element type
         * @throws NullPointerException if before is null
         */
        @Override
        default <In> SinkStepSource<In, U> compose(SinkStepSource<? super In, ? extends T> upstream) {
            return compose(upstream.source()).compose(upstream.sink());
        }
        
        /**
         * Connects the {@code upstream} segue before this operator. Returns a new segue that pairs the {@code upstream}
         * segue's sink with the downstream source obtained by applying this operator to the {@code upstream} segue's
         * source.
         *
         * @param upstream the upstream segue
         * @return a new segue with source transformed
         * @param <In> the upstream sink element type
         * @throws NullPointerException if before is null
         */
        @Override
        default <In> StepSegue<In, U> compose(StepSegue<? super In, ? extends T> upstream) {
            return compose(upstream.source()).compose(upstream.sink());
        }
        
        /**
         * Connects the {@code upstream} operator before this operator. Returns a composed operator that applies this
         * operator to the result of the {@code upstream} operator.
         *
         * @param upstream the upstream operator
         * @return a composed operator that applies this operator to the result of the {@code before} operator
         * @param <V> the upstream element type
         * @throws NullPointerException if before is null
         */
        @Override
        default <V> SourceToStepOperator<V, U> compose(SourceToStepOperator<? super V, ? extends T> upstream) {
            Objects.requireNonNull(upstream);
            return source -> compose(upstream.compose(source));
        }
        
        /**
         * Connects the {@code upstream} operator before this operator. Returns a composed operator that applies this
         * operator to the result of the {@code upstream} operator.
         *
         * @param upstream the upstream operator
         * @return a composed operator that applies this operator to the result of the {@code before} operator
         * @param <V> the upstream element type
         * @throws NullPointerException if before is null
         */
        @Override
        default <V> StepSourceOperator<V, U> compose(StepSourceOperator<? super V, ? extends T> upstream) {
            Objects.requireNonNull(upstream);
            return source -> compose(upstream.compose(source));
        }
        
        /**
         * Connects the {@code downstream} operator after this operator. Returns a composed operator that applies the
         * {@code downstream} operator to the result of this operator.
         *
         * @param downstream the downstream operator
         * @return a composed operator that applies the {@code downstream} operator to the result of this operator
         * @param <V> the downstream element type
         * @throws NullPointerException if downstream is null
         */
        @SuppressWarnings("unchecked")
        default <V> StepToSourceOperator<T, V> andThen(StepToSourceOperator<? super U, ? extends V> downstream) {
            Objects.requireNonNull(downstream);
            return source -> (Source<V>) downstream.compose(compose(source));
        }
        
        /**
         * Connects the {@code downstream} operator after this operator. Returns a composed operator that applies the
         * {@code downstream} operator to the result of this operator.
         *
         * @param downstream the downstream operator
         * @return a composed operator that applies the {@code downstream} operator to the result of this operator
         * @param <V> the downstream element type
         * @throws NullPointerException if downstream is null
         */
        @SuppressWarnings("unchecked")
        default <V> StepSourceOperator<T, V> andThen(StepSourceOperator<? super U, ? extends V> downstream) {
            Objects.requireNonNull(downstream);
            return source -> (StepSource<V>) downstream.compose(compose(source));
        }
    }
}