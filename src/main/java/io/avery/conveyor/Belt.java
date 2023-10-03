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
         * Traverses each silo encapsulated by this stage, recursively running the source and sink of the silo, and
         * submitting a task to the executor that:
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
         * Returns a composed silo that, when {@link #run run}, runs this silo and the {@code before} silo. This method
         * is effectively equivalent to {@link #andThen(Silo) Silo.andThen(Silo)}.
         *
         * @param before the other silo
         * @return a composed silo that runs this silo and the {@code before} silo
         * @throws NullPointerException if before is null
         */
        default Silo compose(Silo before) {
            return new Belts.ChainSilo(before, this);
        }
        
        /**
         * Returns a composed sink that behaves like the {@code before} sink, except that {@link #run running} it will
         * also run this silo.
         *
         * @param before the upstream sink
         * @return a composed sink that also runs this silo
         * @param <T> the upstream sink element type
         * @throws NullPointerException if before is null
         */
        default <T> Sink<T> compose(Sink<? super T> before) {
            return new Belts.ChainSink<>(before, this);
        }
        
        /**
         * Returns a composed sink that behaves like the {@code before} sink, except that {@link #run running} it will
         * also run this silo.
         *
         * @param before the upstream sink
         * @return a composed sink that also runs this silo
         * @param <T> the upstream sink element type
         * @throws NullPointerException if before is null
         */
        default <T> StepSink<T> compose(StepSink<? super T> before) {
            return new Belts.ChainStepSink<>(before, this);
        }
        
        /**
         * Returns a composed silo that, when {@link #run run}, runs this silo and the {@code after} silo. This method
         * is effectively equivalent to {@link #compose(Silo) Silo.compose(Silo)}.
         *
         * @param after the other silo
         * @return a composed silo that runs this silo and the {@code before} silo
         * @throws NullPointerException if after is null
         */
        default Silo andThen(Silo after) {
            return new Belts.ChainSilo(this, after);
        }
        
        /**
         * Returns a composed source that behaves like the {@code after} source, except that {@link #run running} it
         * will also run this silo.
         *
         * @param after the downstream source
         * @return a composed source that also runs this silo
         * @param <T> the downstream source element type
         * @throws NullPointerException if after is null
         */
        default <T> Source<T> andThen(Source<? extends T> after) {
            return new Belts.ChainSource<>(this, after);
        }
        
        /**
         * Returns a composed source that behaves like the {@code after} source, except that {@link #run running} it
         * will also run this silo.
         *
         * @param after the downstream source
         * @return a composed source that also runs this silo
         * @param <T> the downstream source element type
         * @throws NullPointerException if after is null
         */
        default <T> StepSource<T> andThen(StepSource<? extends T> after) {
            return new Belts.ChainStepSource<>(this, after);
        }
    }
    
    /**
     * A {@link Stage Stage} that accepts input elements.
     *
     * <p>A sink may encapsulate a downstream silo, which will {@link #run run} when the sink runs. Sinks generally
     * should be run before accepting elements, in case the sink connects across a downstream boundary, to avoid the
     * effects of unmitigated buffer saturation (including potential deadlock).
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
         * Notifies any nearest downstream boundary sources to stop yielding elements that arrive after this signal.
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
         * Notifies any nearest downstream boundary sources to stop yielding elements and throw
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
         * Returns a silo that, when {@link #run run}, will drain from the {@code before} source to this sink.
         *
         * @param before the upstream source
         * @return a silo that will drain from the source to this sink
         * @throws NullPointerException if before is null
         */
        default Silo compose(StepSource<? extends In> before) {
            return new Belts.ClosedSilo<>(before, this);
        }
        
        /**
         * Returns a composed sink that behaves like the sink-side of the {@code before} segue, except that
         * {@link #run running} it will also run the silo formed by connecting the source-side of the {@code before}
         * segue to this sink.
         *
         * @param before the upstream segue
         * @return a composed sink that also runs a downstream silo
         * @param <T> the upstream sink element type
         * @throws NullPointerException if before is null
         */
        default <T> Sink<T> compose(SinkStepSource<? super T, ? extends In> before) {
            return new Belts.ChainSink<>(before.sink(), new Belts.ClosedSilo<>(before.source(), this));
        }
        
        /**
         * Returns a composed sink that behaves like the sink-side of the {@code before} segue, except that
         * {@link #run running} it will also run the silo formed by connecting the source-side of the {@code before}
         * segue to this sink.
         *
         * @param before the upstream segue
         * @return a composed sink that also runs a downstream silo
         * @param <T> the upstream sink element type
         * @throws NullPointerException if before is null
         */
        default <T> StepSink<T> compose(StepSegue<? super T, ? extends In> before) {
            return new Belts.ChainStepSink<>(before.sink(), new Belts.ClosedSilo<>(before.source(), this));
        }
        
        /**
         * Returns a composed sink that behaves like this sink, except that {@link #run running} it will also run the
         * {@code after} silo.
         *
         * @param after the downstream silo
         * @return a composed sink that also runs a downstream silo
         * @throws NullPointerException if after is null
         */
        default Sink<In> andThen(Silo after) {
            return new Belts.ChainSink<>(this, after);
        }
        
        /**
         * Returns a segue that bundles this sink with the {@code after} source.
         *
         * @param after the downstream source
         * @return a segue that bundles this sink with the {@code after} source
         * @param <T> the downstream source element type
         * @throws NullPointerException if after is null
         */
        default <T> Segue<In, T> andThen(Source<? extends T> after) {
            return new Belts.ChainSegue<>(this, after);
        }
        
        /**
         * Returns a segue that bundles this sink with the {@code after} source.
         *
         * @param after the downstream source
         * @return a segue that bundles this sink with the {@code after} source
         * @param <T> the downstream source element type
         * @throws NullPointerException if after is null
         */
        default <T> SinkStepSource<In, T> andThen(StepSource<? extends T> after) {
            return new Belts.ChainSinkStepSource<>(this, after);
        }
        
        /**
         * Returns an upstream sink obtained by applying the {@code mapper} to this sink.
         *
         * @param mapper a function that creates an upstream sink from a downstream sink
         * @return an upstream sink obtained by applying the {@code mapper} to this sink
         * @param <T> the upstream sink element type
         * @throws NullPointerException if mapper is null
         */
        @SuppressWarnings("unchecked")
        default <T> Sink<T> compose(SinkOperator<? super T, ? extends In> mapper) {
            return (Sink<T>) mapper.andThen(this);
        }
        
        /**
         * Returns an upstream sink obtained by applying the {@code mapper} to this sink.
         *
         * @param mapper a function that creates an upstream sink from a downstream sink
         * @return an upstream sink obtained by applying the {@code mapper} to this sink
         * @param <T> the upstream sink element type
         * @throws NullPointerException if mapper is null
         */
        @SuppressWarnings("unchecked")
        default <T> StepSink<T> compose(StepToSinkOperator<? super T, ? extends In> mapper) {
            return (StepSink<T>) mapper.andThen(this);
        }
    }
    
    /**
     * A {@link Stage Stage} that yields output elements.
     *
     * <p>A source may encapsulate an upstream silo, which will {@link #run run} when the source runs. Sources generally
     * should be run before yielding elements, in case the source connects across an upstream boundary, to avoid the
     * effects of unmitigated buffer depletion (including potential deadlock).
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
         * Returns a composed source that behaves like this source, except that {@link #run running} it will also run
         * the {@code before} silo.
         *
         * @param before the upstream silo
         * @return a composed source that also runs an upstream silo
         * @throws NullPointerException if before is null
         */
        default Source<Out> compose(Silo before) {
            return new Belts.ChainSource<>(before, this);
        }
        
        /**
         * Returns a segue that bundles the {@code before} sink with this source.
         *
         * @param before the upstream sink
         * @return a segue that bundles the {@code before} sink with this source
         * @param <T> the upstream sink element type
         * @throws NullPointerException if before is null
         */
        default <T> Segue<T, Out> compose(Sink<? super T> before) {
            return new Belts.ChainSegue<>(before, this);
        }
        
        /**
         * Returns a segue that bundles the {@code before} sink with this source.
         *
         * @param before the upstream sink
         * @return a segue that bundles the {@code before} sink with this source
         * @param <T> the upstream sink element type
         * @throws NullPointerException if before is null
         */
        default <T> StepSinkSource<T, Out> compose(StepSink<? super T> before) {
            return new Belts.ChainStepSinkSource<>(before, this);
        }
        
        /**
         * Returns a silo that, when {@link #run run}, will drain from this source to the {@code after} sink.
         *
         * @param after the downstream sink
         * @return a silo that will drain from this source to the sink
         * @throws NullPointerException if after is null
         */
        default Silo andThen(StepSink<? super Out> after) {
            return new Belts.ClosedSilo<>(this, after);
        }
        
        /**
         * Returns a composed source that behaves like the source-side of the {@code after} segue, except that
         * {@link #run running} it will also run the silo formed by connecting this source to the sink-side of the
         * {@code after} segue.
         *
         * @param after the downstream segue
         * @return a composed source that also runs an upstream silo
         * @param <T> the downstream source element type
         * @throws NullPointerException if after is null
         */
        default <T> Source<T> andThen(StepSinkSource<? super Out, ? extends T> after) {
            return new Belts.ChainSource<>(new Belts.ClosedSilo<>(this, after.sink()), after.source());
        }
        
        /**
         * Returns a composed source that behaves like the source-side of the {@code after} segue, except that
         * {@link #run running} it will also run the silo formed by connecting this source to the sink-side of the
         * {@code after} segue.
         *
         * @param after the downstream segue
         * @return a composed source that also runs an upstream silo
         * @param <T> the downstream source element type
         * @throws NullPointerException if after is null
         */
        default <T> StepSource<T> andThen(StepSegue<? super Out, ? extends T> after) {
            return new Belts.ChainStepSource<>(new Belts.ClosedSilo<>(this, after.sink()), after.source());
        }
        
        /**
         * Returns a downstream source obtained by applying the {@code mapper} to this source.
         *
         * @param mapper a function that creates a downstream source from an upstream source
         * @return a downstream source obtained by applying the {@code mapper} to this source
         * @param <T> the downstream source element type
         * @throws NullPointerException if mapper is null
         */
        @SuppressWarnings("unchecked")
        default <T> Source<T> andThen(SourceOperator<? super Out, ? extends T> mapper) {
            return (Source<T>) mapper.compose(this);
        }
        
        /**
         * Returns a downstream source obtained by applying the {@code mapper} to this source.
         *
         * @param mapper a function that creates a downstream source from an upstream source
         * @return a downstream source obtained by applying the {@code mapper} to this source
         * @param <T> the downstream source element type
         * @throws NullPointerException if mapper is null
         */
        @SuppressWarnings("unchecked")
        default <T> StepSource<T> andThen(SourceToStepOperator<? super Out, ? extends T> mapper) {
            return (StepSource<T>) mapper.compose(this);
        }
    }
    
    /**
     * A {@link Sink Sink} that can accept input elements one at a time.
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
         * Returns a silo that, when {@link #run run}, will drain from the {@code before} source to this sink.
         *
         * @param before the upstream source
         * @return a silo that will drain from the source to this sink
         * @throws NullPointerException if before is null
         */
        default Silo compose(Source<? extends In> before) {
            return new Belts.ClosedSilo<>(before, this);
        }
        
        /**
         * Returns a composed sink that behaves like the sink-side of the {@code before} segue, except that
         * {@link #run running} it will also run the silo formed by connecting the source-side of the {@code before}
         * segue to this sink.
         *
         * @param before the upstream segue
         * @return a composed sink that also runs a downstream silo
         * @param <T> the upstream sink element type
         * @throws NullPointerException if before is null
         */
        default <T> Sink<T> compose(Segue<? super T, ? extends In> before) {
            return new Belts.ChainSink<>(before.sink(), new Belts.ClosedSilo<>(before.source(), this));
        }
        
        /**
         * Returns a composed sink that behaves like the sink-side of the {@code before} segue, except that
         * {@link #run running} it will also run the silo formed by connecting the source-side of the {@code before}
         * segue to this sink.
         *
         * @param before the upstream segue
         * @return a composed sink that also runs a downstream silo
         * @param <T> the upstream sink element type
         * @throws NullPointerException if before is null
         */
        default <T> StepSink<T> compose(StepSinkSource<T, ? extends In> before) {
            return new Belts.ChainStepSink<>(before.sink(), new Belts.ClosedSilo<>(before.source(), this));
        }
        
        @Override
        default StepSink<In> andThen(Silo after) {
            return new Belts.ChainStepSink<>(this, after);
        }
        
        @Override
        default <T> StepSinkSource<In, T> andThen(Source<? extends T> after) {
            return new Belts.ChainStepSinkSource<>(this, after);
        }
        
        @Override
        default <T> StepSegue<In, T> andThen(StepSource<? extends T> after) {
            return new Belts.ChainStepSegue<>(this, after);
        }
        
        /**
         * Returns an upstream sink obtained by applying the {@code mapper} to this sink.
         *
         * @param mapper a function that creates an upstream sink from a downstream sink
         * @return an upstream sink obtained by applying the {@code mapper} to this sink
         * @param <T> the upstream sink element type
         * @throws NullPointerException if mapper is null
         */
        @SuppressWarnings("unchecked")
        default <T> Sink<T> compose(SinkToStepOperator<? super T, ? extends In> mapper) {
            return (Sink<T>) mapper.andThen(this);
        }
        
        /**
         * Returns an upstream sink obtained by applying the {@code mapper} to this sink.
         *
         * @param mapper a function that creates an upstream sink from a downstream sink
         * @return an upstream sink obtained by applying the {@code mapper} to this sink
         * @param <T> the upstream sink element type
         * @throws NullPointerException if mapper is null
         */
        @SuppressWarnings("unchecked")
        default <T> StepSink<T> compose(StepSinkOperator<? super T, ? extends In> mapper) {
            return (StepSink<T>) mapper.andThen(this);
        }
    }
    
    /**
     * A {@link Source Source} that can yield output elements one at a time.
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
        default StepSource<Out> compose(Silo before) {
            return new Belts.ChainStepSource<>(before, this);
        }
        
        @Override
        default <T> SinkStepSource<T, Out> compose(Sink<? super T> before) {
            return new Belts.ChainSinkStepSource<>(before, this);
        }
        
        @Override
        default <T> StepSegue<T, Out> compose(StepSink<? super T> before) {
            return new Belts.ChainStepSegue<>(before, this);
        }
        
        /**
         * Returns a silo that, when {@link #run run}, will drain from this source to the {@code after} sink.
         *
         * @param after the downstream sink
         * @return a silo that will drain from this source to the sink
         * @throws NullPointerException if after is null
         */
        default Silo andThen(Sink<? super Out> after) {
            return new Belts.ClosedSilo<>(this, after);
        }
        
        /**
         * Returns a composed source that behaves like the source-side of the {@code after} segue, except that
         * {@link #run running} it will also run the silo formed by connecting this source to the sink-side of the
         * {@code after} segue.
         *
         * @param after the downstream segue
         * @return a composed source that also runs an upstream silo
         * @param <T> the downstream source element type
         * @throws NullPointerException if after is null
         */
        default <T> Source<T> andThen(Segue<? super Out, ? extends T> after) {
            return new Belts.ChainSource<>(new Belts.ClosedSilo<>(this, after.sink()), after.source());
        }
        
        /**
         * Returns a composed source that behaves like the source-side of the {@code after} segue, except that
         * {@link #run running} it will also run the silo formed by connecting this source to the sink-side of the
         * {@code after} segue.
         *
         * @param after the downstream segue
         * @return a composed source that also runs an upstream silo
         * @param <T> the downstream source element type
         * @throws NullPointerException if after is null
         */
        default <T> StepSource<T> andThen(SinkStepSource<? super Out, ? extends T> after) {
            return new Belts.ChainStepSource<>(new Belts.ClosedSilo<>(this, after.sink()), after.source());
        }
        
        /**
         * Returns a downstream source obtained by applying the {@code mapper} to this source.
         *
         * @param mapper a function that creates a downstream source from an upstream source
         * @return a downstream source obtained by applying the {@code mapper} to this source
         * @param <T> the downstream source element type
         * @throws NullPointerException if mapper is null
         */
        @SuppressWarnings("unchecked")
        default <T> Source<T> andThen(StepToSourceOperator<? super Out, ? extends T> mapper) {
            return (Source<T>) mapper.compose(this);
        }
        
        /**
         * Returns a downstream source obtained by applying the {@code mapper} to this source.
         *
         * @param mapper a function that creates a downstream source from an upstream source
         * @return a downstream source obtained by applying the {@code mapper} to this source
         * @param <T> the downstream source element type
         * @throws NullPointerException if mapper is null
         */
        @SuppressWarnings("unchecked")
        default <T> StepSource<T> andThen(StepSourceOperator<? super Out, ? extends T> mapper) {
            return (StepSource<T>) mapper.compose(this);
        }
    }
    
    // --- Segues ---
    
    /**
     * A bundled {@link Sink Sink} and {@link Source Source}. The sink and source need not be related. However, it is
     * common for the sink and source to be internally connected, such that the elements input to the sink determine or
     * influence the elements output from the source. This allows data to transition across the "asynchronous boundary"
     * between threads, if the thread(s) draining to the sink differ from the thread(s) draining from the source.
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
         * Returns a composed source that behaves like the source-side of this segue, except that
         * {@link Stage#run running} it will also run the silo formed by connecting the {@code before} source to the
         * sink-side of this segue.
         *
         * @param before the upstream source
         * @return a composed source that also runs an upstream silo
         * @throws NullPointerException if before is null
         */
        default Source<Out> compose(StepSource<? extends In> before) {
            return new Belts.ChainSource<>(new Belts.ClosedSilo<>(before, sink()), source());
        }
        
        /**
         * Returns a composed segue that bundles the sink-side of the {@code before} segue with a composed source that
         * behaves like the source-side of this segue, as if by calling
         * {@snippet :
         * this.compose(before.source()).compose(before.sink())
         * }
         * The composition is right-associative; the composed source {@link Stage#run runs} the interior silo.
         *
         * @param before the upstream segue
         * @return a composed segue whose source also runs an interior silo
         * @param <T> the upstream sink element type
         * @throws NullPointerException if before is null
         */
        default <T> Segue<T, Out> compose(SinkStepSource<? super T, ? extends In> before) {
            return new Belts.ChainSegue<>(before.sink(), new Belts.ChainSource<>(new Belts.ClosedSilo<>(before.source(), sink()), source()));
        }
        
        /**
         * Returns a composed segue that bundles the sink-side of the {@code before} segue with a composed source that
         * behaves like the source-side of this segue, as if by calling
         * {@snippet :
         * this.compose(before.source()).compose(before.sink())
         * }
         * The composition is right-associative; the composed source {@link Stage#run runs} the interior silo.
         *
         * @param before the upstream segue
         * @return a composed segue whose source also runs an interior silo
         * @param <T> the upstream sink element type
         * @throws NullPointerException if before is null
         */
        default <T> StepSinkSource<T, Out> compose(StepSegue<? super T, ? extends In> before) {
            return new Belts.ChainStepSinkSource<>(before.sink(), new Belts.ChainSource<>(new Belts.ClosedSilo<>(before.source(), sink()), source()));
        }
        
        /**
         * Returns a composed sink that behaves like the sink-side of this segue, except that {@link Stage#run running}
         * it will also run the silo formed by connecting the source-side of this segue to the {@code after} sink.
         *
         * @param after the downstream sink
         * @return a composed sink that also runs a downstream silo
         * @throws NullPointerException if after is null
         */
        default Sink<In> andThen(StepSink<? super Out> after) {
            return new Belts.ChainSink<>(sink(), new Belts.ClosedSilo<>(source(), after));
        }
        
        /**
         * Returns a composed segue that bundles the source-side of the {@code after} segue with a composed sink that
         * behaves like the sink-side of this segue, as if by calling
         * {@snippet :
         * this.andThen(after.sink()).andThen(after.source())
         * }
         * The composition is left-associative; the composed sink {@link Stage#run runs} the interior silo.
         *
         * @param after the downstream segue
         * @return a composed segue whose sink also runs an interior silo
         * @param <T> the downstream source element type
         * @throws NullPointerException if after is null
         */
        default <T> Segue<In, T> andThen(StepSinkSource<? super Out, ? extends T> after) {
            return new Belts.ChainSegue<>(new Belts.ChainSink<>(sink(), new Belts.ClosedSilo<>(source(), after.sink())), after.source());
        }
        
        /**
         * Returns a composed segue that bundles the source-side of the {@code after} segue with a composed sink that
         * behaves like the sink-side of this segue, as if by calling
         * {@snippet :
         * this.andThen(after.sink()).andThen(after.source())
         * }
         * The composition is left-associative; the composed sink {@link Stage#run runs} the interior silo.
         *
         * @param after the downstream segue
         * @return a composed segue whose sink also runs an interior silo
         * @param <T> the downstream source element type
         * @throws NullPointerException if after is null
         */
        default <T> SinkStepSource<In, T> andThen(StepSegue<? super Out, ? extends T> after) {
            return new Belts.ChainSinkStepSource<>(new Belts.ChainSink<>(sink(), new Belts.ClosedSilo<>(source(), after.sink())), after.source());
        }
        
        /**
         * Returns a segue that bundles the source-side of this segue with the upstream sink obtained by applying the
         * {@code mapper} to the sink-side of this segue.
         *
         * @param mapper a function that creates an upstream sink from a downstream sink
         * @return a new segue with sink transformed
         * @param <T> the upstream sink element type
         * @throws NullPointerException if mapper is null
         */
        @SuppressWarnings("unchecked")
        default <T> Segue<T, Out> compose(SinkOperator<? super T, ? extends In> mapper) {
            return (Segue<T, Out>) mapper.andThen(sink()).andThen(source());
        }
        
        /**
         * Returns a segue that bundles the source-side of this segue with the upstream sink obtained by applying the
         * {@code mapper} to the sink-side of this segue.
         *
         * @param mapper a function that creates an upstream sink from a downstream sink
         * @return a new segue with sink transformed
         * @param <T> the upstream sink element type
         * @throws NullPointerException if mapper is null
         */
        @SuppressWarnings("unchecked")
        default <T> StepSinkSource<T, Out> compose(StepToSinkOperator<? super T, ? extends In> mapper) {
            return (StepSinkSource<T, Out>) mapper.andThen(sink()).andThen(source());
        }
        
        /**
         * Returns a segue that bundles the sink-side of this segue with the downstream source obtained by applying the
         * {@code mapper} to the source-side of this segue.
         *
         * @param mapper a function that creates a downstream source from an upstream source
         * @return a new segue with source transformed
         * @param <T> the downstream source element type
         * @throws NullPointerException if mapper is null
         */
        default <T> Segue<In, T> andThen(SourceOperator<? super Out, ? extends T> mapper) {
            return sink().andThen(mapper.compose(source()));
        }
        
        /**
         * Returns a segue that bundles the sink-side of this segue with the downstream source obtained by applying the
         * {@code mapper} to the source-side of this segue.
         *
         * @param mapper a function that creates a downstream source from an upstream source
         * @return a new segue with source transformed
         * @param <T> the downstream source element type
         * @throws NullPointerException if mapper is null
         */
        default <T> SinkStepSource<In, T> andThen(SourceToStepOperator<? super Out, ? extends T> mapper) {
            return sink().andThen(mapper.compose(source()));
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
         * Returns a composed source that behaves like the source-side of this segue, except that
         * {@link Stage#run running} it will also run the silo formed by connecting the {@code before} source to the
         * sink-side of this segue.
         *
         * @param before the upstream source
         * @return a composed source that also runs an upstream silo
         * @throws NullPointerException if before is null
         */
        default Source<Out> compose(Source<? extends In> before) {
            return new Belts.ChainSource<>(new Belts.ClosedSilo<>(before, sink()), source());
        }
        
        /**
         * Returns a composed segue that bundles the sink-side of the {@code before} segue with a composed source that
         * behaves like the source-side of this segue, as if by calling
         * {@snippet :
         * this.compose(before.source()).compose(before.sink())
         * }
         * The composition is right-associative; the composed source {@link Stage#run runs} the interior silo.
         *
         * @param before the upstream segue
         * @return a composed segue whose source also runs an interior silo
         * @param <T> the upstream sink element type
         * @throws NullPointerException if before is null
         */
        default <T> Segue<T, Out> compose(Segue<? super T, ? extends In> before) {
            return new Belts.ChainSegue<>(before.sink(), new Belts.ChainSource<>(new Belts.ClosedSilo<>(before.source(), sink()), source()));
        }
        
        /**
         * Returns a composed segue that bundles the sink-side of the {@code before} segue with a composed source that
         * behaves like the source-side of this segue, as if by calling
         * {@snippet :
         * this.compose(before.source()).compose(before.sink())
         * }
         * The composition is right-associative; the composed source {@link Stage#run runs} the interior silo.
         *
         * @param before the upstream segue
         * @return a composed segue whose source also runs an interior silo
         * @param <T> the upstream sink element type
         * @throws NullPointerException if before is null
         */
        default <T> StepSinkSource<T, Out> compose(StepSinkSource<? super T, ? extends In> before) {
            return new Belts.ChainStepSinkSource<>(before.sink(), new Belts.ChainSource<>(new Belts.ClosedSilo<>(before.source(), sink()), source()));
        }
        
        @Override
        default StepSink<In> andThen(StepSink<? super Out> after) {
            return new Belts.ChainStepSink<>(sink(), new Belts.ClosedSilo<>(source(), after));
        }
        
        @Override
        default <T> StepSinkSource<In, T> andThen(StepSinkSource<? super Out, ? extends T> after) {
            return new Belts.ChainStepSinkSource<>(new Belts.ChainStepSink<>(sink(), new Belts.ClosedSilo<>(source(), after.sink())), after.source());
        }
        
        @Override
        default <T> StepSegue<In, T> andThen(StepSegue<? super Out, ? extends T> after) {
            return new Belts.ChainStepSegue<>(new Belts.ChainStepSink<>(sink(), new Belts.ClosedSilo<>(source(), after.sink())), after.source());
        }
        
        /**
         * Returns a segue that bundles the source-side of this segue with the upstream sink obtained by applying the
         * {@code mapper} to the sink-side of this segue.
         *
         * @param mapper a function that creates an upstream sink from a downstream sink
         * @return a new segue with sink transformed
         * @param <T> the upstream sink element type
         * @throws NullPointerException if mapper is null
         */
        @SuppressWarnings("unchecked")
        default <T> Segue<T, Out> compose(SinkToStepOperator<? super T, ? extends In> mapper) {
            return (Segue<T, Out>) mapper.andThen(sink()).andThen(source());
        }
        
        /**
         * Returns a segue that bundles the source-side of this segue with the upstream sink obtained by applying the
         * {@code mapper} to the sink-side of this segue.
         *
         * @param mapper a function that creates an upstream sink from a downstream sink
         * @return a new segue with sink transformed
         * @param <T> the upstream sink element type
         * @throws NullPointerException if mapper is null
         */
        @SuppressWarnings("unchecked")
        default <T> StepSinkSource<T, Out> compose(StepSinkOperator<? super T, ? extends In> mapper) {
            return (StepSinkSource<T, Out>) mapper.andThen(sink()).andThen(source());
        }
        
        @Override
        default <T> StepSinkSource<In, T> andThen(SourceOperator<? super Out, ? extends T> mapper) {
            return sink().andThen(mapper.compose(source()));
        }
        
        @Override
        default <T> StepSegue<In, T> andThen(SourceToStepOperator<? super Out, ? extends T> mapper) {
            return sink().andThen(mapper.compose(source()));
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
        default StepSource<Out> compose(StepSource<? extends In> before) {
            return new Belts.ChainStepSource<>(new Belts.ClosedSilo<>(before, sink()), source());
        }
        
        @Override
        default <T> SinkStepSource<T, Out> compose(SinkStepSource<? super T, ? extends In> before) {
            return new Belts.ChainSinkStepSource<>(before.sink(), new Belts.ChainStepSource<>(new Belts.ClosedSilo<>(before.source(), sink()), source()));
        }
        
        @Override
        default <T> StepSegue<T, Out> compose(StepSegue<? super T, ? extends In> before) {
            return new Belts.ChainStepSegue<>(before.sink(), new Belts.ChainStepSource<>(new Belts.ClosedSilo<>(before.source(), sink()), source()));
        }
        
        /**
         * Returns a composed sink that behaves like the sink-side of this segue, except that {@link Stage#run running}
         * it will also run the silo formed by connecting the source-side of this segue to the {@code after} sink.
         *
         * @param after the downstream sink
         * @return a composed sink that also runs a downstream silo
         * @throws NullPointerException if after is null
         */
        default Sink<In> andThen(Sink<? super Out> after) {
            return new Belts.ChainSink<>(sink(), new Belts.ClosedSilo<>(source(), after));
        }
        
        /**
         * Returns a composed segue that bundles the source-side of the {@code after} segue with a composed sink that
         * behaves like the sink-side of this segue, as if by calling
         * {@snippet :
         * this.andThen(after.sink()).andThen(after.source())
         * }
         * The composition is left-associative; the composed sink {@link Stage#run runs} the interior silo.
         *
         * @param after the downstream segue
         * @return a composed segue whose sink also runs an interior silo
         * @param <T> the downstream source element type
         * @throws NullPointerException if after is null
         */
        default <T> Segue<In, T> andThen(Segue<? super Out, ? extends T> after) {
            return new Belts.ChainSegue<>(new Belts.ChainSink<>(sink(), new Belts.ClosedSilo<>(source(), after.sink())), after.source());
        }
        
        /**
         * Returns a composed segue that bundles the source-side of the {@code after} segue with a composed sink that
         * behaves like the sink-side of this segue, as if by calling
         * {@snippet :
         * this.andThen(after.sink()).andThen(after.source())
         * }
         * The composition is left-associative; the composed sink {@link Stage#run runs} the interior silo.
         *
         * @param after the downstream segue
         * @return a composed segue whose sink also runs an interior silo
         * @param <T> the downstream source element type
         * @throws NullPointerException if after is null
         */
        default <T> SinkStepSource<In, T> andThen(SinkStepSource<? super Out, ? extends T> after) {
            return new Belts.ChainSinkStepSource<>(new Belts.ChainSink<>(sink(), new Belts.ClosedSilo<>(source(), after.sink())), after.source());
        }
        
        @Override
        @SuppressWarnings("unchecked")
        default <T> SinkStepSource<T, Out> compose(SinkOperator<? super T, ? extends In> mapper) {
            return (SinkStepSource<T, Out>) mapper.andThen(sink()).andThen(source());
        }
        
        @Override
        @SuppressWarnings("unchecked")
        default <T> StepSegue<T, Out> compose(StepToSinkOperator<? super T, ? extends In> mapper) {
            return (StepSegue<T, Out>) mapper.andThen(sink()).andThen(source());
        }
        
        /**
         * Returns a segue that bundles the sink-side of this segue with the downstream source obtained by applying the
         * {@code mapper} to the source-side of this segue.
         *
         * @param mapper a function that creates a downstream source from an upstream source
         * @return a new segue with source transformed
         * @param <T> the downstream source element type
         * @throws NullPointerException if mapper is null
         */
        default <T> Segue<In, T> andThen(StepToSourceOperator<? super Out, ? extends T> mapper) {
            return sink().andThen(mapper.compose(source()));
        }
        
        /**
         * Returns a segue that bundles the sink-side of this segue with the downstream source obtained by applying the
         * {@code mapper} to the source-side of this segue.
         *
         * @param mapper a function that creates a downstream source from an upstream source
         * @return a new segue with source transformed
         * @param <T> the downstream source element type
         * @throws NullPointerException if mapper is null
         */
        default <T> SinkStepSource<In, T> andThen(StepSourceOperator<? super Out, ? extends T> mapper) {
            return sink().andThen(mapper.compose(source()));
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
        default StepSource<Out> compose(Source<? extends In> before) {
            return new Belts.ChainStepSource<>(new Belts.ClosedSilo<>(before, sink()), source());
        }
        
        @Override
        default <T> SinkStepSource<T, Out> compose(Segue<? super T, ? extends In> before) {
            return new Belts.ChainSinkStepSource<>(before.sink(), new Belts.ChainStepSource<>(new Belts.ClosedSilo<>(before.source(), sink()), source()));
        }
        
        @Override
        default <T> StepSegue<T, Out> compose(StepSinkSource<? super T, ? extends In> before) {
            return new Belts.ChainStepSegue<>(before.sink(), new Belts.ChainStepSource<>(new Belts.ClosedSilo<>(before.source(), sink()), source()));
        }
        
        @Override
        default StepSink<In> andThen(Sink<? super Out> after) {
            return new Belts.ChainStepSink<>(sink(), new Belts.ClosedSilo<>(source(), after));
        }
        
        @Override
        default <T> StepSinkSource<In, T> andThen(Segue<? super Out, ? extends T> after) {
            return new Belts.ChainStepSinkSource<>(new Belts.ChainStepSink<>(sink(), new Belts.ClosedSilo<>(source(), after.sink())), after.source());
        }
        
        @Override
        default <T> StepSegue<In, T> andThen(SinkStepSource<? super Out, ? extends T> after) {
            return new Belts.ChainStepSegue<>(new Belts.ChainStepSink<>(sink(), new Belts.ClosedSilo<>(source(), after.sink())), after.source());
        }
        
        @Override
        @SuppressWarnings("unchecked")
        default <T> SinkStepSource<T, Out> compose(SinkToStepOperator<? super T, ? extends In> mapper) {
            return (SinkStepSource<T, Out>) mapper.andThen(sink()).andThen(source());
        }
        
        @Override
        @SuppressWarnings("unchecked")
        default <T> StepSegue<T, Out> compose(StepSinkOperator<? super T, ? extends In> mapper) {
            return (StepSegue<T, Out>) mapper.andThen(sink()).andThen(source());
        }
        
        @Override
        default <T> StepSinkSource<In, T> andThen(StepToSourceOperator<? super Out, ? extends T> mapper) {
            return sink().andThen(mapper.compose(source()));
        }
        
        @Override
        default <T> StepSegue<In, T> andThen(StepSourceOperator<? super Out, ? extends T> mapper) {
            return sink().andThen(mapper.compose(source()));
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
         * Returns an upstream {@link Sink Sink} from a downstream {@code Sink}.
         *
         * @implSpec The upstream sink is generally expected to internally delegate to the downstream sink in some way.
         * This allows for transforming elements or signals as they travel from upstream to downstream.
         *
         * @param after the downstream sink
         * @return the upstream sink
         */
        Sink<T> andThen(Sink<? super U> after);
        
        /**
         * Returns a segue that bundles the source-side of the {@code after} segue with the sink obtained by applying
         * this operator to the sink-side of the {@code after} segue.
         *
         * @param after the downstream segue
         * @return a new segue with sink transformed
         * @param <Out> the downstream source element type
         * @throws NullPointerException if after is null
         */
        default <Out> Segue<T, Out> andThen(Segue<? super U, ? extends Out> after) {
            return andThen(after.sink()).andThen(after.source());
        }
        
        /**
         * Returns a segue that bundles the source-side of the {@code after} segue with the sink obtained by applying
         * this operator to the sink-side of the {@code after} segue.
         *
         * @param after the downstream segue
         * @return a new segue with sink transformed
         * @param <Out> the downstream source element type
         * @throws NullPointerException if after is null
         */
        default <Out> SinkStepSource<T, Out> andThen(SinkStepSource<? super U, ? extends Out> after) {
            return andThen(after.sink()).andThen(after.source());
        }
        
        /**
         * Returns a composed operator that applies the {@code before} operator to the result of this operator.
         *
         * @param before the upstream operator
         * @return a composed operator that applies the {@code before} operator to the result of this operator
         * @param <V> the upstream element type
         * @throws NullPointerException if before is null
         */
        @SuppressWarnings("unchecked")
        default <V> SinkOperator<V, U> compose(SinkOperator<? super V, ? extends T> before) {
            Objects.requireNonNull(before);
            return sink -> (Sink<V>) before.andThen(andThen(sink));
        }
        
        /**
         * Returns a composed operator that applies the {@code before} operator to the result of this operator.
         *
         * @param before the upstream operator
         * @return a composed operator that applies the {@code before} operator to the result of this operator
         * @param <V> the upstream element type
         * @throws NullPointerException if before is null
         */
        @SuppressWarnings("unchecked")
        default <V> StepToSinkOperator<V, U> compose(StepToSinkOperator<? super V, ? extends T> before) {
            Objects.requireNonNull(before);
            return sink -> (StepSink<V>) before.andThen(andThen(sink));
        }
        
        /**
         * Returns a composed operator that applies this operator to the result of the {@code after} operator.
         *
         * @param after the downstream operator
         * @return a composed operator that applies this operator to the result of the {@code after} operator
         * @param <V> the downstream element type
         * @throws NullPointerException if after is null
         */
        default <V> SinkOperator<T, V> andThen(SinkOperator<? super U, ? extends V> after) {
            Objects.requireNonNull(after);
            return sink -> andThen(after.andThen(sink));
        }
        
        /**
         * Returns a composed operator that applies this operator to the result of the {@code after} operator.
         *
         * @param after the downstream operator
         * @return a composed operator that applies this operator to the result of the {@code after} operator
         * @param <V> the downstream element type
         * @throws NullPointerException if after is null
         */
        default <V> SinkToStepOperator<T, V> andThen(SinkToStepOperator<? super U, ? extends V> after) {
            Objects.requireNonNull(after);
            return sink -> andThen(after.andThen(sink));
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
         * Returns an upstream {@link Sink Sink} from a downstream {@link StepSink StepSink}.
         *
         * @implSpec The upstream sink is generally expected to internally delegate to the downstream sink in some way.
         * This allows for transforming elements or signals as they travel from upstream to downstream.
         *
         * @param after the downstream sink
         * @return the upstream sink
         */
        Sink<T> andThen(StepSink<? super U> after);
        
        /**
         * Returns a segue that bundles the source-side of the {@code after} segue with the sink obtained by applying
         * this operator to the sink-side of the {@code after} segue.
         *
         * @param after the downstream segue
         * @return a new segue with sink transformed
         * @param <Out> the downstream source element type
         * @throws NullPointerException if after is null
         */
        default <Out> Segue<T, Out> andThen(StepSinkSource<? super U, ? extends Out> after) {
            return andThen(after.sink()).andThen(after.source());
        }
        
        /**
         * Returns a segue that bundles the source-side of the {@code after} segue with the sink obtained by applying
         * this operator to the sink-side of the {@code after} segue.
         *
         * @param after the downstream segue
         * @return a new segue with sink transformed
         * @param <Out> the downstream source element type
         * @throws NullPointerException if after is null
         */
        default <Out> SinkStepSource<T, Out> andThen(StepSegue<? super U, ? extends Out> after) {
            return andThen(after.sink()).andThen(after.source());
        }
        
        /**
         * Returns a composed operator that applies the {@code before} operator to the result of this operator.
         *
         * @param before the upstream operator
         * @return a composed operator that applies the {@code before} operator to the result of this operator
         * @param <V> the upstream element type
         * @throws NullPointerException if before is null
         */
        @SuppressWarnings("unchecked")
        default <V> SinkToStepOperator<V, U> compose(SinkOperator<? super V, ? extends T> before) {
            Objects.requireNonNull(before);
            return sink -> (Sink<V>) before.andThen(andThen(sink));
        }
        
        /**
         * Returns a composed operator that applies the {@code before} operator to the result of this operator.
         *
         * @param before the upstream operator
         * @return a composed operator that applies the {@code before} operator to the result of this operator
         * @param <V> the upstream element type
         * @throws NullPointerException if before is null
         */
        @SuppressWarnings("unchecked")
        default <V> StepSinkOperator<V, U> compose(StepToSinkOperator<? super V, ? extends T> before) {
            Objects.requireNonNull(before);
            return sink -> (StepSink<V>) before.andThen(andThen(sink));
        }
        
        /**
         * Returns a composed operator that applies this operator to the result of the {@code after} operator.
         *
         * @param after the downstream operator
         * @return a composed operator that applies this operator to the result of the {@code after} operator
         * @param <V> the downstream element type
         * @throws NullPointerException if after is null
         */
        default <V> SinkOperator<T, V> andThen(StepToSinkOperator<? super U, ? extends V> after) {
            Objects.requireNonNull(after);
            return sink -> andThen(after.andThen(sink));
        }
        
        /**
         * Returns a composed operator that applies this operator to the result of the {@code after} operator.
         *
         * @param after the downstream operator
         * @return a composed operator that applies this operator to the result of the {@code after} operator
         * @param <V> the downstream element type
         * @throws NullPointerException if after is null
         */
        default <V> SinkToStepOperator<T, V> andThen(StepSinkOperator<? super U, ? extends V> after) {
            Objects.requireNonNull(after);
            return sink -> andThen(after.andThen(sink));
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
         * Returns an upstream {@link StepSink StepSink} from a downstream {@link Sink Sink}.
         *
         * @implSpec The upstream sink is generally expected to internally delegate to the downstream sink in some way.
         * This allows for transforming elements or signals as they travel from upstream to downstream.
         *
         * @param after the downstream sink
         * @return the upstream sink
         */
        @Override
        StepSink<T> andThen(Sink<? super U> after);
        
        /**
         * Returns a segue that bundles the source-side of the {@code after} segue with the sink obtained by applying
         * this operator to the sink-side of the {@code after} segue.
         *
         * @param after the downstream segue
         * @return a new segue with sink transformed
         * @param <Out> the downstream source element type
         * @throws NullPointerException if after is null
         */
        @Override
        default <Out> StepSinkSource<T, Out> andThen(Segue<? super U, ? extends Out> after) {
            return andThen(after.sink()).andThen(after.source());
        }
        
        /**
         * Returns a segue that bundles the source-side of the {@code after} segue with the sink obtained by applying
         * this operator to the sink-side of the {@code after} segue.
         *
         * @param after the downstream segue
         * @return a new segue with sink transformed
         * @param <Out> the downstream source element type
         * @throws NullPointerException if after is null
         */
        @Override
        default <Out> StepSegue<T, Out> andThen(SinkStepSource<? super U, ? extends Out> after) {
            return andThen(after.sink()).andThen(after.source());
        }
        
        /**
         * Returns a composed operator that applies the {@code before} operator to the result of this operator.
         *
         * @param before the upstream operator
         * @return a composed operator that applies the {@code before} operator to the result of this operator
         * @param <V> the upstream element type
         * @throws NullPointerException if before is null
         */
        @SuppressWarnings("unchecked")
        default <V> SinkOperator<V, U> compose(SinkToStepOperator<? super V, ? extends T> before) {
            Objects.requireNonNull(before);
            return sink -> (Sink<V>) before.andThen(andThen(sink));
        }
        
        /**
         * Returns a composed operator that applies the {@code before} operator to the result of this operator.
         *
         * @param before the upstream operator
         * @return a composed operator that applies the {@code before} operator to the result of this operator
         * @param <V> the upstream element type
         * @throws NullPointerException if before is null
         */
        @SuppressWarnings("unchecked")
        default <V> StepToSinkOperator<V, U> compose(StepSinkOperator<? super V, ? extends T> before) {
            Objects.requireNonNull(before);
            return sink -> (StepSink<V>) before.andThen(andThen(sink));
        }
        
        /**
         * Returns a composed operator that applies this operator to the result of the {@code after} operator.
         *
         * @param after the downstream operator
         * @return a composed operator that applies this operator to the result of the {@code after} operator
         * @param <V> the downstream element type
         * @throws NullPointerException if after is null
         */
        @Override
        default <V> StepToSinkOperator<T, V> andThen(SinkOperator<? super U, ? extends V> after) {
            Objects.requireNonNull(after);
            return sink -> andThen(after.andThen(sink));
        }
        
        /**
         * Returns a composed operator that applies this operator to the result of the {@code after} operator.
         *
         * @param after the downstream operator
         * @return a composed operator that applies this operator to the result of the {@code after} operator
         * @param <V> the downstream element type
         * @throws NullPointerException if after is null
         */
        @Override
        default <V> StepSinkOperator<T, V> andThen(SinkToStepOperator<? super U, ? extends V> after) {
            Objects.requireNonNull(after);
            return sink -> andThen(after.andThen(sink));
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
         * Returns an upstream {@link StepSink StepSink} from a downstream {@code StepSink}.
         *
         * @implSpec The upstream sink is generally expected to internally delegate to the downstream sink in some way.
         * This allows for transforming elements or signals as they travel from upstream to downstream.
         *
         * @param after the downstream sink
         * @return the upstream sink
         */
        @Override
        StepSink<T> andThen(StepSink<? super U> after);
        
        /**
         * Returns a segue that bundles the source-side of the {@code after} segue with the sink obtained by applying
         * this operator to the sink-side of the {@code after} segue.
         *
         * @param after the downstream segue
         * @return a new segue with sink transformed
         * @param <Out> the downstream source element type
         * @throws NullPointerException if after is null
         */
        @Override
        default <Out> StepSinkSource<T, Out> andThen(StepSinkSource<? super U, ? extends Out> after) {
            return andThen(after.sink()).andThen(after.source());
        }
        
        /**
         * Returns a segue that bundles the source-side of the {@code after} segue with the sink obtained by applying
         * this operator to the sink-side of the {@code after} segue.
         *
         * @param after the downstream segue
         * @return a new segue with sink transformed
         * @param <Out> the downstream source element type
         * @throws NullPointerException if after is null
         */
        @Override
        default <Out> StepSegue<T, Out> andThen(StepSegue<? super U, ? extends Out> after) {
            return andThen(after.sink()).andThen(after.source());
        }
        
        /**
         * Returns a composed operator that applies the {@code before} operator to the result of this operator.
         *
         * @param before the upstream operator
         * @return a composed operator that applies the {@code before} operator to the result of this operator
         * @param <V> the upstream element type
         * @throws NullPointerException if before is null
         */
        @SuppressWarnings("unchecked")
        default <V> SinkToStepOperator<V, U> compose(SinkToStepOperator<? super V, ? extends T> before) {
            Objects.requireNonNull(before);
            return sink -> (Sink<V>) before.andThen(andThen(sink));
        }
        
        /**
         * Returns a composed operator that applies the {@code before} operator to the result of this operator.
         *
         * @param before the upstream operator
         * @return a composed operator that applies the {@code before} operator to the result of this operator
         * @param <V> the upstream element type
         * @throws NullPointerException if before is null
         */
        @SuppressWarnings("unchecked")
        default <V> StepSinkOperator<V, U> compose(StepSinkOperator<? super V, ? extends T> before) {
            Objects.requireNonNull(before);
            return sink -> (StepSink<V>) before.andThen(andThen(sink));
        }
        
        /**
         * Returns a composed operator that applies this operator to the result of the {@code after} operator.
         *
         * @param after the downstream operator
         * @return a composed operator that applies this operator to the result of the {@code after} operator
         * @param <V> the downstream element type
         * @throws NullPointerException if after is null
         */
        @Override
        default <V> StepToSinkOperator<T, V> andThen(StepToSinkOperator<? super U, ? extends V> after) {
            Objects.requireNonNull(after);
            return sink -> andThen(after.andThen(sink));
        }
        
        /**
         * Returns a composed operator that applies this operator to the result of the {@code after} operator.
         *
         * @param after the downstream operator
         * @return a composed operator that applies this operator to the result of the {@code after} operator
         * @param <V> the downstream element type
         * @throws NullPointerException if after is null
         */
        @Override
        default <V> StepSinkOperator<T, V> andThen(StepSinkOperator<? super U, ? extends V> after) {
            Objects.requireNonNull(after);
            return sink -> andThen(after.andThen(sink));
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
         * Returns a downstream {@link Source Source} from an upstream {@code Source}.
         *
         * @implSpec The downstream source is generally expected to internally delegate to the upstream source in some
         * way. This allows for transforming elements or signals as they travel from upstream to downstream.
         *
         * @param before the upstream source
         * @return the downstream source
         */
        Source<U> compose(Source<? extends T> before);
        
        /**
         * Returns a segue that bundles the sink-side of the {@code before} segue with the sink obtained by applying
         * this operator to the source-side of the {@code after} segue.
         *
         * @param before the upstream segue
         * @return a new segue with source transformed
         * @param <In> the upstream sink element type
         * @throws NullPointerException if before is null
         */
        default <In> Segue<In, U> compose(Segue<? super In, ? extends T> before) {
            return compose(before.source()).compose(before.sink());
        }
        
        /**
         * Returns a segue that bundles the sink-side of the {@code before} segue with the sink obtained by applying
         * this operator to the source-side of the {@code after} segue.
         *
         * @param before the upstream segue
         * @return a new segue with source transformed
         * @param <In> the upstream sink element type
         * @throws NullPointerException if before is null
         */
        default <In> StepSinkSource<In, U> compose(StepSinkSource<? super In, ? extends T> before) {
            return compose(before.source()).compose(before.sink());
        }
        
        /**
         * Returns a composed operator that applies this operator to the result of the {@code before} operator.
         *
         * @param before the upstream operator
         * @return a composed operator that applies this operator to the result of the {@code before} operator
         * @param <V> the upstream element type
         * @throws NullPointerException if before is null
         */
        default <V> StepToSourceOperator<V, U> compose(StepToSourceOperator<? super V, ? extends T> before) {
            Objects.requireNonNull(before);
            return source -> compose(before.compose(source));
        }
        
        /**
         * Returns a composed operator that applies this operator to the result of the {@code before} operator.
         *
         * @param before the upstream operator
         * @return a composed operator that applies this operator to the result of the {@code before} operator
         * @param <V> the upstream element type
         * @throws NullPointerException if before is null
         */
        default <V> SourceOperator<V, U> compose(SourceOperator<? super V, ? extends T> before) {
            Objects.requireNonNull(before);
            return source -> compose(before.compose(source));
        }
        
        /**
         * Returns a composed operator that applies the {@code after} operator to the result of this operator.
         *
         * @param after the downstream operator
         * @return a composed operator that applies the {@code after} operator to the result of this operator
         * @param <V> the downstream element type
         * @throws NullPointerException if after is null
         */
        @SuppressWarnings("unchecked")
        default <V> SourceOperator<T, V> andThen(SourceOperator<? super U, ? extends V> after) {
            Objects.requireNonNull(after);
            return source -> (Source<V>) after.compose(compose(source));
        }
        
        /**
         * Returns a composed operator that applies the {@code after} operator to the result of this operator.
         *
         * @param after the downstream operator
         * @return a composed operator that applies the {@code after} operator to the result of this operator
         * @param <V> the downstream element type
         * @throws NullPointerException if after is null
         */
        @SuppressWarnings("unchecked")
        default <V> SourceToStepOperator<T, V> andThen(SourceToStepOperator<? super U, ? extends V> after) {
            Objects.requireNonNull(after);
            return source -> (StepSource<V>) after.compose(compose(source));
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
         * Returns a downstream {@link Source Source} from an upstream {@link StepSource StepSource}.
         *
         * @implSpec The downstream source is generally expected to internally delegate to the upstream source in some
         * way. This allows for transforming elements or signals as they travel from upstream to downstream.
         *
         * @param before the upstream source
         * @return the downstream source
         */
        Source<U> compose(StepSource<? extends T> before);
        
        /**
         * Returns a segue that bundles the sink-side of the {@code before} segue with the sink obtained by applying
         * this operator to the source-side of the {@code after} segue.
         *
         * @param before the upstream segue
         * @return a new segue with source transformed
         * @param <In> the upstream sink element type
         * @throws NullPointerException if before is null
         */
        default <In> Segue<In, U> compose(SinkStepSource<? super In, ? extends T> before) {
            return compose(before.source()).compose(before.sink());
        }
        
        /**
         * Returns a segue that bundles the sink-side of the {@code before} segue with the sink obtained by applying
         * this operator to the source-side of the {@code after} segue.
         *
         * @param before the upstream segue
         * @return a new segue with source transformed
         * @param <In> the upstream sink element type
         * @throws NullPointerException if before is null
         */
        default <In> StepSinkSource<In, U> compose(StepSegue<? super In, ? extends T> before) {
            return compose(before.source()).compose(before.sink());
        }
        
        /**
         * Returns a composed operator that applies this operator to the result of the {@code before} operator.
         *
         * @param before the upstream operator
         * @return a composed operator that applies this operator to the result of the {@code before} operator
         * @param <V> the upstream element type
         * @throws NullPointerException if before is null
         */
        default <V> SourceOperator<V, U> compose(SourceToStepOperator<? super V, ? extends T> before) {
            Objects.requireNonNull(before);
            return source -> compose(before.compose(source));
        }
        
        /**
         * Returns a composed operator that applies this operator to the result of the {@code before} operator.
         *
         * @param before the upstream operator
         * @return a composed operator that applies this operator to the result of the {@code before} operator
         * @param <V> the upstream element type
         * @throws NullPointerException if before is null
         */
        default <V> StepToSourceOperator<V, U> compose(StepSourceOperator<? super V, ? extends T> before) {
            Objects.requireNonNull(before);
            return source -> compose(before.compose(source));
        }
        
        /**
         * Returns a composed operator that applies the {@code after} operator to the result of this operator.
         *
         * @param after the downstream operator
         * @return a composed operator that applies the {@code after} operator to the result of this operator
         * @param <V> the downstream element type
         * @throws NullPointerException if after is null
         */
        @SuppressWarnings("unchecked")
        default <V> StepToSourceOperator<T, V> andThen(SourceOperator<? super U, ? extends V> after) {
            Objects.requireNonNull(after);
            return source -> (Source<V>) after.compose(compose(source));
        }
        
        /**
         * Returns a composed operator that applies the {@code after} operator to the result of this operator.
         *
         * @param after the downstream operator
         * @return a composed operator that applies the {@code after} operator to the result of this operator
         * @param <V> the downstream element type
         * @throws NullPointerException if after is null
         */
        @SuppressWarnings("unchecked")
        default <V> StepSourceOperator<T, V> andThen(SourceToStepOperator<? super U, ? extends V> after) {
            Objects.requireNonNull(after);
            return source -> (StepSource<V>) after.compose(compose(source));
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
         * Returns a downstream {@link StepSource StepSource} from an upstream {@link Source Source}.
         *
         * @implSpec The downstream source is generally expected to internally delegate to the upstream source in some
         * way. This allows for transforming elements or signals as they travel from upstream to downstream.
         *
         * @param before the upstream source
         * @return the downstream source
         */
        @Override
        StepSource<U> compose(Source<? extends T> before);
        
        /**
         * Returns a segue that bundles the sink-side of the {@code before} segue with the sink obtained by applying
         * this operator to the source-side of the {@code after} segue.
         *
         * @param before the upstream segue
         * @return a new segue with source transformed
         * @param <In> the upstream sink element type
         * @throws NullPointerException if before is null
         */
        @Override
        default <In> SinkStepSource<In, U> compose(Segue<? super In, ? extends T> before) {
            return compose(before.source()).compose(before.sink());
        }
        
        /**
         * Returns a segue that bundles the sink-side of the {@code before} segue with the sink obtained by applying
         * this operator to the source-side of the {@code after} segue.
         *
         * @param before the upstream segue
         * @return a new segue with source transformed
         * @param <In> the upstream sink element type
         * @throws NullPointerException if before is null
         */
        @Override
        default <In> StepSegue<In, U> compose(StepSinkSource<? super In, ? extends T> before) {
            return compose(before.source()).compose(before.sink());
        }
        
        /**
         * Returns a composed operator that applies this operator to the result of the {@code before} operator.
         *
         * @param before the upstream operator
         * @return a composed operator that applies this operator to the result of the {@code before} operator
         * @param <V> the upstream element type
         * @throws NullPointerException if before is null
         */
        @Override
        default <V> SourceToStepOperator<V, U> compose(SourceOperator<? super V, ? extends T> before) {
            Objects.requireNonNull(before);
            return source -> compose(before.compose(source));
        }
        
        /**
         * Returns a composed operator that applies this operator to the result of the {@code before} operator.
         *
         * @param before the upstream operator
         * @return a composed operator that applies this operator to the result of the {@code before} operator
         * @param <V> the upstream element type
         * @throws NullPointerException if before is null
         */
        @Override
        default <V> StepSourceOperator<V, U> compose(StepToSourceOperator<? super V, ? extends T> before) {
            Objects.requireNonNull(before);
            return source -> compose(before.compose(source));
        }
        
        /**
         * Returns a composed operator that applies the {@code after} operator to the result of this operator.
         *
         * @param after the downstream operator
         * @return a composed operator that applies the {@code after} operator to the result of this operator
         * @param <V> the downstream element type
         * @throws NullPointerException if after is null
         */
        @SuppressWarnings("unchecked")
        default <V> SourceOperator<T, V> andThen(StepToSourceOperator<? super U, ? extends V> after) {
            Objects.requireNonNull(after);
            return source -> (Source<V>) after.compose(compose(source));
        }
        
        /**
         * Returns a composed operator that applies the {@code after} operator to the result of this operator.
         *
         * @param after the downstream operator
         * @return a composed operator that applies the {@code after} operator to the result of this operator
         * @param <V> the downstream element type
         * @throws NullPointerException if after is null
         */
        @SuppressWarnings("unchecked")
        default <V> SourceToStepOperator<T, V> andThen(StepSourceOperator<? super U, ? extends V> after) {
            Objects.requireNonNull(after);
            return source -> (StepSource<V>) after.compose(compose(source));
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
         * Returns a downstream {@link StepSource StepSource} from an upstream {@code StepSource}.
         *
         * @implSpec The downstream source is generally expected to internally delegate to the upstream source in some
         * way. This allows for transforming elements or signals as they travel from upstream to downstream.
         *
         * @param before the upstream source
         * @return the downstream source
         */
        @Override
        StepSource<U> compose(StepSource<? extends T> before);
        
        /**
         * Returns a segue that bundles the sink-side of the {@code before} segue with the sink obtained by applying
         * this operator to the source-side of the {@code after} segue.
         *
         * @param before the upstream segue
         * @return a new segue with source transformed
         * @param <In> the upstream sink element type
         * @throws NullPointerException if before is null
         */
        @Override
        default <In> SinkStepSource<In, U> compose(SinkStepSource<? super In, ? extends T> before) {
            return compose(before.source()).compose(before.sink());
        }
        
        /**
         * Returns a segue that bundles the sink-side of the {@code before} segue with the sink obtained by applying
         * this operator to the source-side of the {@code after} segue.
         *
         * @param before the upstream segue
         * @return a new segue with source transformed
         * @param <In> the upstream sink element type
         * @throws NullPointerException if before is null
         */
        @Override
        default <In> StepSegue<In, U> compose(StepSegue<? super In, ? extends T> before) {
            return compose(before.source()).compose(before.sink());
        }
        
        /**
         * Returns a composed operator that applies this operator to the result of the {@code before} operator.
         *
         * @param before the upstream operator
         * @return a composed operator that applies this operator to the result of the {@code before} operator
         * @param <V> the upstream element type
         * @throws NullPointerException if before is null
         */
        @Override
        default <V> SourceToStepOperator<V, U> compose(SourceToStepOperator<? super V, ? extends T> before) {
            Objects.requireNonNull(before);
            return source -> compose(before.compose(source));
        }
        
        /**
         * Returns a composed operator that applies this operator to the result of the {@code before} operator.
         *
         * @param before the upstream operator
         * @return a composed operator that applies this operator to the result of the {@code before} operator
         * @param <V> the upstream element type
         * @throws NullPointerException if before is null
         */
        @Override
        default <V> StepSourceOperator<V, U> compose(StepSourceOperator<? super V, ? extends T> before) {
            Objects.requireNonNull(before);
            return source -> compose(before.compose(source));
        }
        
        /**
         * Returns a composed operator that applies the {@code after} operator to the result of this operator.
         *
         * @param after the downstream operator
         * @return a composed operator that applies the {@code after} operator to the result of this operator
         * @param <V> the downstream element type
         * @throws NullPointerException if after is null
         */
        @SuppressWarnings("unchecked")
        default <V> StepToSourceOperator<T, V> andThen(StepToSourceOperator<? super U, ? extends V> after) {
            Objects.requireNonNull(after);
            return source -> (Source<V>) after.compose(compose(source));
        }
        
        /**
         * Returns a composed operator that applies the {@code after} operator to the result of this operator.
         *
         * @param after the downstream operator
         * @return a composed operator that applies the {@code after} operator to the result of this operator
         * @param <V> the downstream element type
         * @throws NullPointerException if after is null
         */
        @SuppressWarnings("unchecked")
        default <V> StepSourceOperator<T, V> andThen(StepSourceOperator<? super U, ? extends V> after) {
            Objects.requireNonNull(after);
            return source -> (StepSource<V>) after.compose(compose(source));
        }
    }
}