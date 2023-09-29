package io.avery.conveyor;

import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collector;

public class Belt {
    private Belt() {}
    
    // --- Stages ---
    
    public sealed interface Stage {
        /**
         *
         * @param executor
         */
        default void run(Executor executor) { }
    }
    
    public sealed interface Silo extends Stage permits Belts.ClosedSilo, Belts.ChainSilo {
        // Stage/Segue chaining
        default Silo compose(Silo before) { return new Belts.ChainSilo(before, this); }
        default <T> Sink<T> compose(Sink<T> before) { return new Belts.ChainSink<>(before, this); }
        default <T> StepSink<T> compose(StepSink<T> before) { return new Belts.ChainStepSink<>(before, this); }
        default Silo andThen(Silo after) { return new Belts.ChainSilo(this, after); }
        default <T> Source<T> andThen(Source<T> after) { return new Belts.ChainSource<>(this, after); }
        default <T> StepSource<T> andThen(StepSource<T> after) { return new Belts.ChainStepSource<>(this, after); }
    }
    
    @FunctionalInterface
    public non-sealed interface Sink<In> extends Stage {
        /**
         *
         * @param source
         * @return {@code true} if the source drained, meaning a call to {@link StepSource#poll poll} returned {@code null}.
         * @throws Exception
         */
        boolean drainFromSource(StepSource<? extends In> source) throws Exception;
        
        /**
         * Notifies any nearest downstream boundary sources to stop yielding elements that arrive after this signal.
         *
         * @implSpec To prevent unbounded buffering or deadlock, a boundary sink must implement its
         * {@link StepSink#offer offer} and {@link #drainFromSource drainFromSource} methods to discard elements and
         * return {@code false} after this method is called. The connected boundary source must return {@code null} from
         * {@link StepSource#poll poll} and {@code false} from {@link Source#drainToSink drainToSink} after yielding all
         * values that arrived before it received this signal.
         *
         * <p>A sink that delegates to downstream sinks must call {@code complete} on each downstream sink before
         * returning from this method, unless this method throws.
         *
         * <p>The default implementation does nothing.
         *
         * @throws Exception
         */
        default void complete() throws Exception { }
        
        /**
         * Notifies any nearest downstream boundary sources to stop yielding elements and throw
         * {@link UpstreamException}.
         *
         * @implSpec To prevent unbounded buffering or deadlock, a boundary sink must implement its
         * {@link StepSink#offer offer} and {@link #drainFromSource drainFromSource} methods to discard elements and
         * return {@code false} after this method is called. The connected boundary source must throw an
         * {@link UpstreamException}, wrapping the exception passed to this method, upon initiating any subsequent calls
         * to {@link StepSource#poll poll} or subsequent offers in {@link Source#drainToSink drainToSink}.
         *
         * <p>A sink that delegates to downstream sinks must call {@code completeAbruptly} on each downstream sink
         * before returning from this method, <strong>even if this method throws</strong>.
         *
         * <p>The default implementation does nothing.
         *
         * @param exception
         * @throws Exception
         */
        default void completeAbruptly(Throwable exception) throws Exception { }
        
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
    
    @FunctionalInterface
    public non-sealed interface Source<Out> extends Stage, AutoCloseable {
        /**
         *
         * @param sink
         * @return {@code false} if the sink cancelled, meaning a call to {@link StepSink#offer offer} returned {@code false}.
         * @throws Exception
         */
        boolean drainToSink(StepSink<? super Out> sink) throws Exception;
        
        /**
         * Relinquishes any underlying resources held by this source.
         *
         * @implSpec A source that delegates to upstream sources must call {@code close} on each upstream source before
         * returning from this method, <strong>even if this method throws</strong>.
         *
         * <p>The default implementation does nothing.
         *
         * @throws Exception
         */
        default void close() throws Exception { }
        
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
        
        default <A, R> R collect(Collector<? super Out, A, R> collector) throws Exception {
            BiConsumer<A, ? super Out> accumulator = collector.accumulator();
            Function<A, R> finisher = collector.finisher();
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
    
    @FunctionalInterface
    public interface StepSink<In> extends Sink<In> {
        /**
         *
         * @param input
         * @return
         * @throws Exception
         */
        boolean offer(In input) throws Exception;
        
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
    
    @FunctionalInterface
    public interface StepSource<Out> extends Source<Out> {
        /**
         *
         * @return
         * @throws Exception
         */
        Out poll() throws Exception;
        
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
    
    public interface Segue<In, Out> {
        Sink<In> sink();
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
        default <V> SinkOperator<T, V> compose(SinkOperator<U, V> before) { return sink -> andThen(before.andThen(sink)); }
        default <V> SinkToStepOperator<T, V> compose(SinkToStepOperator<U, V> before) { return sink -> andThen(before.andThen(sink)); }
        default <V> SinkOperator<V, U> andThen(SinkOperator<V, T> after) { return sink -> after.andThen(andThen(sink)); }
        default <V> StepToSinkOperator<V, U> andThen(StepToSinkOperator<V, T> after) { return sink -> after.andThen(andThen(sink)); }
    }
    
    @FunctionalInterface
    public interface SinkToStepOperator<T, U> {
        // Stage/Segue chaining
        Sink<T> andThen(StepSink<U> sink);
        default <Out> Segue<T, Out> andThen(StepSinkSource<U, Out> ss) { return andThen(ss.sink()).andThen(ss.source()); }
        default <Out> SinkStepSource<T, Out> andThen(StepSegue<U, Out> ss) { return andThen(ss.sink()).andThen(ss.source()); }
        
        // Operator chaining
        default <V> SinkOperator<T, V> compose(StepToSinkOperator<U, V> before) { return sink -> andThen(before.andThen(sink)); }
        default <V> SinkToStepOperator<T, V> compose(StepSinkOperator<U, V> before) { return sink -> andThen(before.andThen(sink)); }
        default <V> SinkToStepOperator<V, U> andThen(SinkOperator<V, T> after) { return sink -> after.andThen(andThen(sink)); }
        default <V> StepSinkOperator<V, U> andThen(StepToSinkOperator<V, T> after) { return sink -> after.andThen(this.andThen(sink)); }
    }
    
    @FunctionalInterface
    public interface StepToSinkOperator<T, U> extends SinkOperator<T, U> {
        // Stage/Segue chaining
        @Override StepSink<T> andThen(Sink<U> sink);
        @Override default <Out> StepSinkSource<T, Out> andThen(Segue<U, Out> ss) { return andThen(ss.sink()).andThen(ss.source()); }
        @Override default <Out> StepSegue<T, Out> andThen(SinkStepSource<U, Out> ss) { return andThen(ss.sink()).andThen(ss.source()); }
        
        // Operator chaining
        @Override default <V> StepToSinkOperator<T, V> compose(SinkOperator<U, V> before) { return sink -> andThen(before.andThen(sink)); }
        @Override default <V> StepSinkOperator<T, V> compose(SinkToStepOperator<U, V> before) { return sink -> andThen(before.andThen(sink)); }
        default <V> SinkOperator<V, U> andThen(SinkToStepOperator<V, T> after) { return sink -> after.andThen(andThen(sink)); }
        default <V> StepToSinkOperator<V, U> andThen(StepSinkOperator<V, T> after) { return sink -> after.andThen(andThen(sink)); }
    }
    
    @FunctionalInterface
    public interface StepSinkOperator<T, U> extends SinkToStepOperator<T, U> {
        // Stage/Segue chaining
        @Override StepSink<T> andThen(StepSink<U> sink);
        @Override default <Out> StepSinkSource<T, Out> andThen(StepSinkSource<U, Out> ss) { return andThen(ss.sink()).andThen(ss.source()); }
        @Override default <Out> StepSegue<T, Out> andThen(StepSegue<U, Out> ss) { return andThen(ss.sink()).andThen(ss.source()); }
        
        // Operator chaining
        @Override default <V> StepToSinkOperator<T, V> compose(StepToSinkOperator<U, V> before) { return sink -> andThen(before.andThen(sink)); }
        @Override default <V> StepSinkOperator<T, V> compose(StepSinkOperator<U, V> before) { return sink -> andThen(before.andThen(sink)); }
        default <V> SinkToStepOperator<V, U> andThen(SinkToStepOperator<V, T> after) { return sink -> after.andThen(andThen(sink)); }
        default <V> StepSinkOperator<V, U> andThen(StepSinkOperator<V, T> after) { return sink -> after.andThen(andThen(sink)); }
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