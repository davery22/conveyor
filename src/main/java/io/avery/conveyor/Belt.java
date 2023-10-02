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
     * TODO
     */
    public sealed interface Stage {
        /**
         * Runs each silo encapsulated by this stage, by recursively running the source and sink of the silo, and
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
         * @implSpec A stage that delegates to other stages must call {@code run} on each stage before returning from
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
        // Stage/Segue chaining
        default Silo compose(Silo before) { return new Belts.ChainSilo(before, this); }
        default <T> Sink<T> compose(Sink<T> before) { return new Belts.ChainSink<>(before, this); }
        default <T> StepSink<T> compose(StepSink<T> before) { return new Belts.ChainStepSink<>(before, this); }
        default Silo andThen(Silo after) { return new Belts.ChainSilo(this, after); }
        default <T> Source<T> andThen(Source<T> after) { return new Belts.ChainSource<>(this, after); }
        default <T> StepSource<T> andThen(StepSource<T> after) { return new Belts.ChainStepSource<>(this, after); }
    }
    
    /**
     * A {@link Stage Stage} that accepts input elements.
     *
     * <p>A sink may encapsulate a downstream silo, which will {@link #run run} when the sink runs. Sinks generally
     * should be run before accepting elements, in case the sink connects across a downstream boundary, to avoid
     * unmitigated buffer saturation and consequent deadlock or dropped elements.
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
         * @implSpec A boundary sink must implement its {@link StepSink#offer offer} and
         * {@link #drainFromSource drainFromSource} methods to discard elements and return {@code false} after this
         * method is called, to prevent unbounded buffering or deadlock. The connected boundary source must return
         * {@code null} from {@link StepSource#poll poll} and {@code false} from {@link Source#drainToSink drainToSink}
         * after yielding all values that arrived before it received this signal.
         *
         * <p>A sink that delegates to downstream sinks must call {@code complete} on each downstream sink before
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
         * @implSpec A boundary sink must implement its {@link StepSink#offer offer} and
         * {@link #drainFromSource drainFromSource} methods to discard elements and return {@code false} after this
         * method is called, to prevent unbounded buffering or deadlock. The connected boundary source must throw an
         * {@link UpstreamException}, wrapping the cause passed to this method, upon initiating any subsequent calls to
         * {@link StepSource#poll poll} or subsequent offers in {@link Source#drainToSink drainToSink}.
         *
         * <p>A sink that delegates to downstream sinks must call {@code completeAbruptly} on each downstream sink
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
        
        // Stage/Segue chaining
        default Silo compose(StepSource<? extends In> before) { return new Belts.ClosedSilo<>(before, this); }
        default <T> Sink<T> compose(SinkStepSource<T, ? extends In> before) { return new Belts.ChainSink<>(before.sink(), new Belts.ClosedSilo<>(before.source(), this)); }
        default <T> StepSink<T> compose(StepSegue<T, ? extends In> before) { return new Belts.ChainStepSink<>(before.sink(), new Belts.ClosedSilo<>(before.source(), this)); }
        default Sink<In> andThen(Silo after) { return new Belts.ChainSink<>(this, after); }
        default <T> Segue<In, T> andThen(Source<T> after) { return new Belts.ChainSegue<>(this, after); }
        default <T> SinkStepSource<In, T> andThen(StepSource<T> after) { return new Belts.ChainSinkStepSource<>(this, after); }
        
        // Operator chaining
        default <T> Sink<T> compose(SinkOperator<T, In> mapper) { return mapper.andThen(this); }
        default <T> StepSink<T> compose(StepToSinkOperator<T, In> mapper) { return mapper.andThen(this); }
    }
    
    /**
     * A {@link Stage Stage} that yields output elements.
     *
     * <p>A source may encapsulate an upstream silo, which will {@link #run run} when the source runs. Sources generally
     * should be run before yielding elements, in case the source connects across an upstream boundary, to avoid
     * unmitigated buffer depletion and consequent deadlock or inserted elements.
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
         * source, the connected boundary sink must implement its {@link StepSink#offer offer} and
         * {@link Sink#drainFromSource drainFromSource} methods to discard elements and return {@code false} after this
         * method is called, to prevent unbounded buffering or deadlock.
         *
         * <p>A source that delegates to upstream sources must call {@code close} on each upstream source before
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
        
        // Stage/Segue chaining
        default Source<Out> compose(Silo before) { return new Belts.ChainSource<>(before, this); }
        default <T> Segue<T, Out> compose(Sink<T> before) { return new Belts.ChainSegue<>(before, this); }
        default <T> StepSinkSource<T, Out> compose(StepSink<T> before) { return new Belts.ChainStepSinkSource<>(before, this); }
        default Silo andThen(StepSink<? super Out> after) { return new Belts.ClosedSilo<>(this, after); }
        default <T> Source<T> andThen(StepSinkSource<? super Out, T> after) { return new Belts.ChainSource<>(new Belts.ClosedSilo<>(this, after.sink()), after.source()); }
        default <T> StepSource<T> andThen(StepSegue<? super Out, T> after) { return new Belts.ChainStepSource<>(new Belts.ClosedSilo<>(this, after.sink()), after.source()); }
        
        // Operator chaining
        default <T> Source<T> andThen(SourceOperator<Out, T> mapper) { return mapper.compose(this); }
        default <T> StepSource<T> andThen(SourceToStepOperator<Out, T> mapper) { return mapper.compose(this); }
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
         * @implSpec Once this method returns {@code false}, subsequent calls must also discard the input element and
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
         * @implNote The default implementation loops, polling from the source and offering to this sink, until either
         * the source drains or this sink cancels. This is equivalent to the default implementation of
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
        
        // Stage/Segue chaining
        default Silo compose(Source<? extends In> before) { return new Belts.ClosedSilo<>(before, this); }
        default <T> Sink<T> compose(Segue<T, ? extends In> before) { return new Belts.ChainSink<>(before.sink(), new Belts.ClosedSilo<>(before.source(), this)); }
        default <T> StepSink<T> compose(StepSinkSource<T, ? extends In> before) { return new Belts.ChainStepSink<>(before.sink(), new Belts.ClosedSilo<>(before.source(), this)); }
        @Override default StepSink<In> andThen(Silo after) { return new Belts.ChainStepSink<>(this, after); }
        @Override default <T> StepSinkSource<In, T> andThen(Source<T> after) { return new Belts.ChainStepSinkSource<>(this, after); }
        @Override default <T> StepSegue<In, T> andThen(StepSource<T> after) { return new Belts.ChainStepSegue<>(this, after); }
        
        // Operator chaining
        default <T> Sink<T> compose(SinkToStepOperator<T, In> mapper) { return mapper.andThen(this); }
        default <T> StepSink<T> compose(StepSinkOperator<T, In> mapper) { return mapper.andThen(this); }
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
         * @implSpec Once this method returns {@code null}, subsequent calls must also return {@code null}, to indicate
         * the source is permanently drained and no longer yielding elements.
         *
         * @return the next element from this source, or {@code null} if this source is drained
         * @throws Exception if unable to poll
         */
        Out poll() throws Exception;
        
        /**
         * {@inheritDoc}
         *
         * @implNote The default implementation loops, polling from this source and offering to the sink, until either
         * this source drains or the sink cancels. This is equivalent to the default implementation of
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
        
        // Stage/Segue chaining
        @Override default StepSource<Out> compose(Silo before) { return new Belts.ChainStepSource<>(before, this); }
        @Override default <T> SinkStepSource<T, Out> compose(Sink<T> before) { return new Belts.ChainSinkStepSource<>(before, this); }
        @Override default <T> StepSegue<T, Out> compose(StepSink<T> before) { return new Belts.ChainStepSegue<>(before, this); }
        default Silo andThen(Sink<? super Out> after) { return new Belts.ClosedSilo<>(this, after); }
        default <T> Source<T> andThen(Segue<? super Out, T> after) { return new Belts.ChainSource<>(new Belts.ClosedSilo<>(this, after.sink()), after.source()); }
        default <T> StepSource<T> andThen(SinkStepSource<? super Out, T> after) { return new Belts.ChainStepSource<>(new Belts.ClosedSilo<>(this, after.sink()), after.source()); }
        
        // Operator chaining
        default <T> Source<T> andThen(StepToSourceOperator<Out, T> mapper) { return mapper.compose(this); }
        default <T> StepSource<T> andThen(StepSourceOperator<Out, T> mapper) { return mapper.compose(this); }
    }
    
    // --- Segues ---
    
    /**
     * TODO
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
        
        // Stage/Segue chaining
        default Source<Out> compose(StepSource<? extends In> before) { return new Belts.ChainSource<>(new Belts.ClosedSilo<>(before, sink()), source()); }
        default <T> Segue<T, Out> compose(SinkStepSource<T, ? extends In> before) { return new Belts.ChainSegue<>(before.sink(), new Belts.ChainSource<>(new Belts.ClosedSilo<>(before.source(), sink()), source())); }
        default <T> StepSinkSource<T, Out> compose(StepSegue<T, ? extends In> before) { return new Belts.ChainStepSinkSource<>(before.sink(), new Belts.ChainSource<>(new Belts.ClosedSilo<>(before.source(), sink()), source())); }
        default Sink<In> andThen(StepSink<? super Out> after) { return new Belts.ChainSink<>(sink(), new Belts.ClosedSilo<>(source(), after)); }
        default <T> Segue<In, T> andThen(StepSinkSource<? super Out, T> after) { return new Belts.ChainSegue<>(new Belts.ChainSink<>(sink(), new Belts.ClosedSilo<>(source(), after.sink())), after.source()); }
        default <T> SinkStepSource<In, T> andThen(StepSegue<? super Out, T> after) { return new Belts.ChainSinkStepSource<>(new Belts.ChainSink<>(sink(), new Belts.ClosedSilo<>(source(), after.sink())), after.source()); }
        
        // Operator chaining
        default <T> Segue<T, Out> compose(SinkOperator<T, In> mapper) { return mapper.andThen(sink()).andThen(source()); }
        default <T> StepSinkSource<T, Out> compose(StepToSinkOperator<T, In> mapper) { return mapper.andThen(sink()).andThen(source()); }
        default <T> Segue<In, T> andThen(SourceOperator<Out, T> mapper) { return sink().andThen(mapper.compose(source())); }
        default <T> SinkStepSource<In, T> andThen(SourceToStepOperator<Out, T> mapper) { return sink().andThen(mapper.compose(source())); }
    }
    
    /**
     * A {@link Segue Segue} where the sink is a {@link StepSink StepSink}.
     *
     * @param <In> the input element type
     * @param <Out> the output element type
     */
    public interface StepSinkSource<In, Out> extends Segue<In, Out> {
        @Override StepSink<In> sink();
        
        // Stage/Segue chaining
        default Source<Out> compose(Source<? extends In> before) { return new Belts.ChainSource<>(new Belts.ClosedSilo<>(before, sink()), source()); }
        default <T> Segue<T, Out> compose(Segue<T, ? extends In> before) { return new Belts.ChainSegue<>(before.sink(), new Belts.ChainSource<>(new Belts.ClosedSilo<>(before.source(), sink()), source())); }
        default <T> StepSinkSource<T, Out> compose(StepSinkSource<T, ? extends In> before) { return new Belts.ChainStepSinkSource<>(before.sink(), new Belts.ChainSource<>(new Belts.ClosedSilo<>(before.source(), sink()), source())); }
        @Override default StepSink<In> andThen(StepSink<? super Out> after) { return new Belts.ChainStepSink<>(sink(), new Belts.ClosedSilo<>(source(), after)); }
        @Override default <T> StepSinkSource<In, T> andThen(StepSinkSource<? super Out, T> after) { return new Belts.ChainStepSinkSource<>(new Belts.ChainStepSink<>(sink(), new Belts.ClosedSilo<>(source(), after.sink())), after.source()); }
        @Override default <T> StepSegue<In, T> andThen(StepSegue<? super Out, T> after) { return new Belts.ChainStepSegue<>(new Belts.ChainStepSink<>(sink(), new Belts.ClosedSilo<>(source(), after.sink())), after.source()); }
        
        // Operator chaining
        default <T> Segue<T, Out> compose(SinkToStepOperator<T, In> mapper) { return mapper.andThen(sink()).andThen(source()); }
        default <T> StepSinkSource<T, Out> compose(StepSinkOperator<T, In> mapper) { return mapper.andThen(sink()).andThen(source()); }
        @Override default <T> StepSinkSource<In, T> andThen(SourceOperator<Out, T> mapper) { return sink().andThen(mapper.compose(source())); }
        @Override default <T> StepSegue<In, T> andThen(SourceToStepOperator<Out, T> mapper) { return sink().andThen(mapper.compose(source())); }
    }
    
    /**
     * A {@link Segue Segue} where the source is a {@link StepSource StepSource}.
     *
     * @param <In> the input element type
     * @param <Out> the output element type
     */
    public interface SinkStepSource<In, Out> extends Segue<In, Out> {
        @Override StepSource<Out> source();
        
        // Stage/Segue chaining
        @Override default StepSource<Out> compose(StepSource<? extends In> before) { return new Belts.ChainStepSource<>(new Belts.ClosedSilo<>(before, sink()), source()); }
        @Override default <T> SinkStepSource<T, Out> compose(SinkStepSource<T, ? extends In> before) { return new Belts.ChainSinkStepSource<>(before.sink(), new Belts.ChainStepSource<>(new Belts.ClosedSilo<>(before.source(), sink()), source())); }
        @Override default <T> StepSegue<T, Out> compose(StepSegue<T, ? extends In> before) { return new Belts.ChainStepSegue<>(before.sink(), new Belts.ChainStepSource<>(new Belts.ClosedSilo<>(before.source(), sink()), source())); }
        default Sink<In> andThen(Sink<? super Out> after) { return new Belts.ChainSink<>(sink(), new Belts.ClosedSilo<>(source(), after)); }
        default <T> Segue<In, T> andThen(Segue<? super Out, T> after) { return new Belts.ChainSegue<>(new Belts.ChainSink<>(sink(), new Belts.ClosedSilo<>(source(), after.sink())), after.source()); }
        default <T> SinkStepSource<In, T> andThen(SinkStepSource<? super Out, T> after) { return new Belts.ChainSinkStepSource<>(new Belts.ChainSink<>(sink(), new Belts.ClosedSilo<>(source(), after.sink())), after.source()); }
        
        // Operator chaining
        @Override default <T> SinkStepSource<T, Out> compose(SinkOperator<T, In> mapper) { return mapper.andThen(sink()).andThen(source()); }
        @Override default <T> StepSegue<T, Out> compose(StepToSinkOperator<T, In> mapper) { return mapper.andThen(sink()).andThen(source()); }
        default <T> Segue<In, T> andThen(StepToSourceOperator<Out, T> mapper) { return sink().andThen(mapper.compose(source())); }
        default <T> SinkStepSource<In, T> andThen(StepSourceOperator<Out, T> mapper) { return sink().andThen(mapper.compose(source())); }
    }
    
    /**
     * A {@link Segue Segue} where the sink is {@link StepSink StepSink} and the source is a {@link StepSource StepSource}.
     *
     * @param <In> the input element type
     * @param <Out> the output element type
     */
    public interface StepSegue<In, Out> extends StepSinkSource<In, Out>, SinkStepSource<In, Out> {
        // Stage/Segue chaining
        @Override default StepSource<Out> compose(Source<? extends In> before) { return new Belts.ChainStepSource<>(new Belts.ClosedSilo<>(before, sink()), source()); }
        @Override default <T> SinkStepSource<T, Out> compose(Segue<T, ? extends In> before) { return new Belts.ChainSinkStepSource<>(before.sink(), new Belts.ChainStepSource<>(new Belts.ClosedSilo<>(before.source(), sink()), source())); }
        @Override default <T> StepSegue<T, Out> compose(StepSinkSource<T, ? extends In> before) { return new Belts.ChainStepSegue<>(before.sink(), new Belts.ChainStepSource<>(new Belts.ClosedSilo<>(before.source(), sink()), source())); }
        @Override default StepSink<In> andThen(Sink<? super Out> after) { return new Belts.ChainStepSink<>(sink(), new Belts.ClosedSilo<>(source(), after)); }
        @Override default <T> StepSinkSource<In, T> andThen(Segue<? super Out, T> after) { return new Belts.ChainStepSinkSource<>(new Belts.ChainStepSink<>(sink(), new Belts.ClosedSilo<>(source(), after.sink())), after.source()); }
        @Override default <T> StepSegue<In, T> andThen(SinkStepSource<? super Out, T> after) { return new Belts.ChainStepSegue<>(new Belts.ChainStepSink<>(sink(), new Belts.ClosedSilo<>(source(), after.sink())), after.source()); }
        
        // Operator chaining
        @Override default <T> SinkStepSource<T, Out> compose(SinkToStepOperator<T, In> mapper) { return mapper.andThen(sink()).andThen(source()); }
        @Override default <T> StepSegue<T, Out> compose(StepSinkOperator<T, In> mapper) { return mapper.andThen(sink()).andThen(source()); }
        @Override default <T> StepSinkSource<In, T> andThen(StepToSourceOperator<Out, T> mapper) { return sink().andThen(mapper.compose(source())); }
        @Override default <T> StepSegue<In, T> andThen(StepSourceOperator<Out, T> mapper) { return sink().andThen(mapper.compose(source())); }
    }
    
    // --- Sink Operators ---
    // Sink<T> = SinkOperator<T, U> andThen Sink<U>
    //         = Sink<U> compose SinkOperator<T, U>
    
    @FunctionalInterface
    public interface SinkOperator<T, U> {
        // Stage/Segue chaining
        Sink<T> andThen(Sink<U> sink);
        default <Out> Segue<T, Out> andThen(Segue<U, Out> ss) { return andThen(ss.sink()).andThen(ss.source()); }
        default <Out> SinkStepSource<T, Out> andThen(SinkStepSource<U, Out> ss) { return andThen(ss.sink()).andThen(ss.source()); }
        
        // Operator chaining
        default <V> SinkOperator<V, U> compose(SinkOperator<V, T> before) { return sink -> before.andThen(andThen(sink)); }
        default <V> StepToSinkOperator<V, U> compose(StepToSinkOperator<V, T> before) { return sink -> before.andThen(andThen(sink)); }
        default <V> SinkOperator<T, V> andThen(SinkOperator<U, V> after) { return sink -> andThen(after.andThen(sink)); }
        default <V> SinkToStepOperator<T, V> andThen(SinkToStepOperator<U, V> after) { return sink -> andThen(after.andThen(sink)); }
    }
    
    @FunctionalInterface
    public interface SinkToStepOperator<T, U> {
        // Stage/Segue chaining
        Sink<T> andThen(StepSink<U> sink);
        default <Out> Segue<T, Out> andThen(StepSinkSource<U, Out> ss) { return andThen(ss.sink()).andThen(ss.source()); }
        default <Out> SinkStepSource<T, Out> andThen(StepSegue<U, Out> ss) { return andThen(ss.sink()).andThen(ss.source()); }
        
        // Operator chaining
        default <V> SinkToStepOperator<V, U> compose(SinkOperator<V, T> before) { return sink -> before.andThen(andThen(sink)); }
        default <V> StepSinkOperator<V, U> compose(StepToSinkOperator<V, T> before) { return sink -> before.andThen(andThen(sink)); }
        default <V> SinkOperator<T, V> andThen(StepToSinkOperator<U, V> after) { return sink -> andThen(after.andThen(sink)); }
        default <V> SinkToStepOperator<T, V> andThen(StepSinkOperator<U, V> after) { return sink -> andThen(after.andThen(sink)); }
    }
    
    @FunctionalInterface
    public interface StepToSinkOperator<T, U> extends SinkOperator<T, U> {
        // Stage/Segue chaining
        @Override StepSink<T> andThen(Sink<U> sink);
        @Override default <Out> StepSinkSource<T, Out> andThen(Segue<U, Out> ss) { return andThen(ss.sink()).andThen(ss.source()); }
        @Override default <Out> StepSegue<T, Out> andThen(SinkStepSource<U, Out> ss) { return andThen(ss.sink()).andThen(ss.source()); }
        
        // Operator chaining
        default <V> SinkOperator<V, U> compose(SinkToStepOperator<V, T> before) { return sink -> before.andThen(andThen(sink)); }
        default <V> StepToSinkOperator<V, U> compose(StepSinkOperator<V, T> before) { return sink -> before.andThen(andThen(sink)); }
        @Override default <V> StepToSinkOperator<T, V> andThen(SinkOperator<U, V> after) { return sink -> andThen(after.andThen(sink)); }
        @Override default <V> StepSinkOperator<T, V> andThen(SinkToStepOperator<U, V> after) { return sink -> andThen(after.andThen(sink)); }
    }
    
    @FunctionalInterface
    public interface StepSinkOperator<T, U> extends SinkToStepOperator<T, U> {
        // Stage/Segue chaining
        @Override StepSink<T> andThen(StepSink<U> sink);
        @Override default <Out> StepSinkSource<T, Out> andThen(StepSinkSource<U, Out> ss) { return andThen(ss.sink()).andThen(ss.source()); }
        @Override default <Out> StepSegue<T, Out> andThen(StepSegue<U, Out> ss) { return andThen(ss.sink()).andThen(ss.source()); }
        
        // Operator chaining
        default <V> SinkToStepOperator<V, U> compose(SinkToStepOperator<V, T> before) { return sink -> before.andThen(andThen(sink)); }
        default <V> StepSinkOperator<V, U> compose(StepSinkOperator<V, T> before) { return sink -> before.andThen(andThen(sink)); }
        @Override default <V> StepToSinkOperator<T, V> andThen(StepToSinkOperator<U, V> after) { return sink -> andThen(after.andThen(sink)); }
        @Override default <V> StepSinkOperator<T, V> andThen(StepSinkOperator<U, V> after) { return sink -> andThen(after.andThen(sink)); }
    }

    // --- Source Operators ---
    // Source<U> = SourceOperator<T, U> compose Source<T>
    //           = Source<T> andThen SourceOperator<T, U>
    
    @FunctionalInterface
    public interface SourceOperator<T, U> {
        // Stage/Segue chaining
        Source<U> compose(Source<T> source);
        default <In> Segue<In, U> compose(Segue<In, T> ss) { return ss.sink().andThen(compose(ss.source())); }
        default <In> StepSinkSource<In, U> compose(StepSinkSource<In, T> ss) { return ss.sink().andThen(compose(ss.source())); }
        
        // Operator chaining
        default <V> StepToSourceOperator<V, U> compose(StepToSourceOperator<V, T> before) { return source -> compose(before.compose(source)); }
        default <V> SourceOperator<V, U> compose(SourceOperator<V, T> before) { return source -> compose(before.compose(source)); }
        default <V> SourceOperator<T, V> andThen(SourceOperator<U, V> after) { return source -> after.compose(compose(source)); }
        default <V> SourceToStepOperator<T, V> andThen(SourceToStepOperator<U, V> after) { return source -> after.compose(compose(source)); }
    }
    
    @FunctionalInterface
    public interface StepToSourceOperator<T, U> {
        // Stage/Segue chaining
        Source<U> compose(StepSource<T> source);
        default <In> Segue<In, U> compose(SinkStepSource<In, T> ss) { return ss.sink().andThen(compose(ss.source())); }
        default <In> StepSinkSource<In, U> compose(StepSegue<In, T> ss) { return ss.sink().andThen(compose(ss.source())); }
        
        // Operator chaining
        default <V> SourceOperator<V, U> compose(SourceToStepOperator<V, T> before) { return source -> compose(before.compose(source)); }
        default <V> StepToSourceOperator<V, U> compose(StepSourceOperator<V, T> before) { return source -> compose(before.compose(source)); }
        default <V> StepToSourceOperator<T, V> andThen(SourceOperator<U, V> after) { return source -> after.compose(compose(source)); }
        default <V> StepSourceOperator<T, V> andThen(SourceToStepOperator<U, V> after) { return source -> after.compose(compose(source)); }
    }
    
    @FunctionalInterface
    public interface SourceToStepOperator<T, U> extends SourceOperator<T, U> {
        // Stage/Segue chaining
        @Override StepSource<U> compose(Source<T> source);
        @Override default <In> SinkStepSource<In, U> compose(Segue<In, T> ss) { return ss.sink().andThen(compose(ss.source())); }
        @Override default <In> StepSegue<In, U> compose(StepSinkSource<In, T> ss) { return ss.sink().andThen(compose(ss.source())); }
        
        // Operator chaining
        @Override default <V> SourceToStepOperator<V, U> compose(SourceOperator<V, T> before) { return source -> compose(before.compose(source)); }
        @Override default <V> StepSourceOperator<V, U> compose(StepToSourceOperator<V, T> before) { return source -> compose(before.compose(source)); }
        default <V> SourceOperator<T, V> andThen(StepToSourceOperator<U, V> after) { return source -> after.compose(compose(source)); }
        default <V> SourceToStepOperator<T, V> andThen(StepSourceOperator<U, V> after) { return source -> after.compose(compose(source)); }
    }
    
    @FunctionalInterface
    public interface StepSourceOperator<T, U> extends StepToSourceOperator<T, U> {
        // Stage/Segue chaining
        @Override StepSource<U> compose(StepSource<T> source);
        @Override default <In> SinkStepSource<In, U> compose(SinkStepSource<In, T> ss) { return ss.sink().andThen(compose(ss.source())); }
        @Override default <In> StepSegue<In, U> compose(StepSegue<In, T> ss) { return ss.sink().andThen(compose(ss.source())); }
        
        // Operator chaining
        @Override default <V> SourceToStepOperator<V, U> compose(SourceToStepOperator<V, T> before) { return source -> compose(before.compose(source)); }
        @Override default <V> StepSourceOperator<V, U> compose(StepSourceOperator<V, T> before) { return source -> compose(before.compose(source)); }
        default <V> StepToSourceOperator<T, V> andThen(StepToSourceOperator<U, V> after) { return source -> after.compose(compose(source)); }
        default <V> StepSourceOperator<T, V> andThen(StepSourceOperator<U, V> after) { return source -> after.compose(compose(source)); }
    }
}