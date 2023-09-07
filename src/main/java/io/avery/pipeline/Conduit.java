package io.avery.pipeline;

import java.util.concurrent.StructuredTaskScope;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collector;

public class Conduit {
    private Conduit() {}
    
    public sealed interface Stage {
        default void run(StructuredTaskScope<?> scope) { }
    }
    
    public sealed interface Silo extends Stage permits Conduits.ClosedSilo, Conduits.ChainSilo {
        // Chaining
        default Silo compose(Silo before) { return new Conduits.ChainSilo(before, this); }
        default <T> Sink<T> compose(Sink<T> before) { return new Conduits.ChainSink<>(before, this); }
        default <T> StepSink<T> compose(StepSink<T> before) { return new Conduits.ChainStepSink<>(before, this); }
        default Silo andThen(Silo after) { return new Conduits.ChainSilo(this, after); }
        default <T> Source<T> andThen(Source<T> after) { return new Conduits.ChainSource<>(this, after); }
        default <T> StepSource<T> andThen(StepSource<T> after) { return new Conduits.ChainStepSource<>(this, after); }
    }
    
    @FunctionalInterface
    public non-sealed interface Sink<In> extends Stage {
        boolean drainFromSource(StepSource<? extends In> source) throws Exception;
        
        default void complete(Throwable error) throws Exception {
            // Default impl handles the case where the Sink has no async downstream.
            // Implementations that have an async downstream should override this method to propagate error downstream.
            if (error != null) {
                throw new UpstreamException(error);
            }
        }
        
        // Chaining
        default Silo compose(StepSource<? extends In> before) { return new Conduits.ClosedSilo<>(before, this); }
        default <T> Sink<T> compose(SinkStepSource<T, ? extends In> before) { return new Conduits.ChainSink<>(before.sink(), new Conduits.ClosedSilo<>(before.source(), this)); }
        default <T> StepSink<T> compose(Segue<T, ? extends In> before) { return new Conduits.ChainStepSink<>(before.sink(), new Conduits.ClosedSilo<>(before.source(),  this)); }
        default Sink<In> andThen(Silo after) { return new Conduits.ChainSink<>(this, after); }
        default <T> SinkSource<In, T> andThen(Source<T> after) { return new Conduits.ChainSinkSource<>(this, after); }
        default <T> SinkStepSource<In, T> andThen(StepSource<T> after) { return new Conduits.ChainSinkStepSource<>(this, after); }
        
        // Mapping
        default <T> Sink<T> mapSink(SinkOperator<In, T> mapper) { return mapper.apply(this); }
        default <T> StepSink<T> mapSink(SinkToStepOperator<In, T> mapper) { return mapper.apply(this); }
    }
    
    @FunctionalInterface
    public non-sealed interface Source<Out> extends Stage, AutoCloseable {
        boolean drainToSink(StepSink<? super Out> sink) throws Exception;
        
        default void close() throws Exception { }
        
        default void forEach(Consumer<? super Out> action) throws Exception {
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
        
        // Chaining
        default Source<Out> compose(Silo before) { return new Conduits.ChainSource<>(before, this); }
        default <T> SinkSource<T, Out> compose(Sink<T> before) { return new Conduits.ChainSinkSource<>(before, this); }
        default <T> StepSinkSource<T, Out> compose(StepSink<T> before) { return new Conduits.ChainStepSinkSource<>(before, this); }
        default Silo andThen(StepSink<? super Out> after) { return new Conduits.ClosedSilo<>(this, after); }
        default <T> Source<T> andThen(StepSinkSource<? super Out, T> after) { return new Conduits.ChainSource<>(new Conduits.ClosedSilo<>(this, after.sink()), after.source()); }
        default <T> StepSource<T> andThen(Segue<? super Out, T> after) { return new Conduits.ChainStepSource<>(new Conduits.ClosedSilo<>(this, after.sink()), after.source()); }
        
        // Mapping
        default <T> Source<T> mapSource(SourceOperator<Out, T> mapper) { return mapper.apply(this); }
        default <T> StepSource<T> mapSource(SourceToStepOperator<Out, T> mapper) { return mapper.apply(this); }
    }
    
    @FunctionalInterface
    public interface StepSink<In> extends Sink<In> {
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
        
        // Chaining
        default Silo compose(Source<? extends In> before) { return new Conduits.ClosedSilo<>(before, this); }
        default <T> Sink<T> compose(SinkSource<T, ? extends In> before) { return new Conduits.ChainSink<>(before.sink(), new Conduits.ClosedSilo<>(before.source(), this)); }
        default <T> StepSink<T> compose(StepSinkSource<T, ? extends In> before) { return new Conduits.ChainStepSink<>(before.sink(), new Conduits.ClosedSilo<>(before.source(), this)); }
        @Override default StepSink<In> andThen(Silo after) { return new Conduits.ChainStepSink<>(this, after); }
        @Override default <T> StepSinkSource<In, T> andThen(Source<T> after) { return new Conduits.ChainStepSinkSource<>(this, after); }
        @Override default <T> Segue<In, T> andThen(StepSource<T> after) { return new Conduits.ChainSegue<>(this, after); }
        
        // Mapping
        default <T> Sink<T> mapSink(StepToSinkOperator<In, T> mapper) { return mapper.apply(this); }
        default <T> StepSink<T> mapSink(StepSinkOperator<In, T> mapper) { return mapper.apply(this); }
    }
    
    @FunctionalInterface
    public interface StepSource<Out> extends Source<Out> {
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
        
        // Chaining
        @Override default StepSource<Out> compose(Silo before) { return new Conduits.ChainStepSource<>(before, this); }
        @Override default <T> SinkStepSource<T, Out> compose(Sink<T> before) { return new Conduits.ChainSinkStepSource<>(before, this); }
        @Override default <T> Segue<T, Out> compose(StepSink<T> before) { return new Conduits.ChainSegue<>(before, this); }
        default Silo andThen(Sink<? super Out> after) { return new Conduits.ClosedSilo<>(this, after); }
        default <T> Source<T> andThen(SinkSource<? super Out, T> after) { return new Conduits.ChainSource<>(new Conduits.ClosedSilo<>(this, after.sink()), after.source()); }
        default <T> StepSource<T> andThen(SinkStepSource<? super Out, T> after) { return new Conduits.ChainStepSource<>(new Conduits.ClosedSilo<>(this, after.sink()), after.source()); }
        
        // Mapping
        default <T> Source<T> mapSource(StepToSourceOperator<Out, T> mapper) { return mapper.apply(this); }
        default <T> StepSource<T> mapSource(StepSourceOperator<Out, T> mapper) { return mapper.apply(this); }
    }
    
    public interface SinkSource<In, Out> {
        Sink<In> sink();
        Source<Out> source();
        
        // Chaining
        default Source<Out> compose(StepSource<? extends In> before) { return new Conduits.ChainSource<>(new Conduits.ClosedSilo<>(before, sink()), source()); }
        default <T> SinkSource<T, Out> compose(SinkStepSource<T, ? extends In> before) { return new Conduits.ChainSinkSource<>(before.sink(), new Conduits.ChainSource<>(new Conduits.ClosedSilo<>(before.source(), sink()), source())); }
        default <T> StepSinkSource<T, Out> compose(Segue<T, ? extends In> before) { return new Conduits.ChainStepSinkSource<>(before.sink(), new Conduits.ChainSource<>(new Conduits.ClosedSilo<>(before.source(), sink()), source())); }
        default Sink<In> andThen(StepSink<? super Out> after) { return new Conduits.ChainSink<>(sink(), new Conduits.ClosedSilo<>(source(), after)); }
        default <T> SinkSource<In, T> andThen(StepSinkSource<? super Out, T> after) { return new Conduits.ChainSinkSource<>(new Conduits.ChainSink<>(sink(), new Conduits.ClosedSilo<>(source(), after.sink())), after.source()); }
        default <T> SinkStepSource<In, T> andThen(Segue<? super Out, T> after) { return new Conduits.ChainSinkStepSource<>(new Conduits.ChainSink<>(sink(), new Conduits.ClosedSilo<>(source(), after.sink())), after.source()); }
        
        // Mapping
        default <T> SinkSource<T, Out> mapSink(SinkOperator<In, T> mapper) { return mapper.apply(sink()).andThen(source()); }
        default <T> StepSinkSource<T, Out> mapSink(SinkToStepOperator<In, T> mapper) { return mapper.apply(sink()).andThen(source()); }
        default <T> SinkSource<In, T> mapSource(SourceOperator<Out, T> mapper) { return sink().andThen(mapper.apply(source())); }
        default <T> SinkStepSource<In, T> mapSource(SourceToStepOperator<Out, T> mapper) { return sink().andThen(mapper.apply(source())); }
    }
    
    public interface StepSinkSource<In, Out> extends SinkSource<In, Out> {
        @Override StepSink<In> sink();
        
        // Chaining
        default Source<Out> compose(Source<? extends In> before) { return new Conduits.ChainSource<>(new Conduits.ClosedSilo<>(before, sink()), source()); }
        default <T> SinkSource<T, Out> compose(SinkSource<T, ? extends In> before) { return new Conduits.ChainSinkSource<>(before.sink(), new Conduits.ChainSource<>(new Conduits.ClosedSilo<>(before.source(), sink()), source())); }
        default <T> StepSinkSource<T, Out> compose(StepSinkSource<T, ? extends In> before) { return new Conduits.ChainStepSinkSource<>(before.sink(), new Conduits.ChainSource<>(new Conduits.ClosedSilo<>(before.source(), sink()), source())); }
        @Override default StepSink<In> andThen(StepSink<? super Out> after) { return new Conduits.ChainStepSink<>(sink(), new Conduits.ClosedSilo<>(source(), after)); }
        @Override default <T> StepSinkSource<In, T> andThen(StepSinkSource<? super Out, T> after) { return new Conduits.ChainStepSinkSource<>(new Conduits.ChainStepSink<>(sink(), new Conduits.ClosedSilo<>(source(), after.sink())), after.source()); }
        @Override default <T> Segue<In, T> andThen(Segue<? super Out, T> after) { return new Conduits.ChainSegue<>(new Conduits.ChainStepSink<>(sink(), new Conduits.ClosedSilo<>(source(), after.sink())), after.source()); }
        
        // Mapping
        default <T> SinkSource<T, Out> mapSink(StepToSinkOperator<In, T> mapper) { return mapper.apply(sink()).andThen(source()); }
        default <T> StepSinkSource<T, Out> mapSink(StepSinkOperator<In, T> mapper) { return mapper.apply(sink()).andThen(source()); }
        @Override default <T> StepSinkSource<In, T> mapSource(SourceOperator<Out, T> mapper) { return sink().andThen(mapper.apply(source())); }
        @Override default <T> Segue<In, T> mapSource(SourceToStepOperator<Out, T> mapper) { return sink().andThen(mapper.apply(source())); }
    }
    
    public interface SinkStepSource<In, Out> extends SinkSource<In, Out> {
        @Override StepSource<Out> source();
        
        // Chaining
        @Override default StepSource<Out> compose(StepSource<? extends In> before) { return new Conduits.ChainStepSource<>(new Conduits.ClosedSilo<>(before, sink()), source()); }
        @Override default <T> SinkStepSource<T, Out> compose(SinkStepSource<T, ? extends In> before) { return new Conduits.ChainSinkStepSource<>(before.sink(), new Conduits.ChainStepSource<>(new Conduits.ClosedSilo<>(before.source(), sink()), source())); }
        @Override default <T> Segue<T, Out> compose(Segue<T, ? extends In> before) { return new Conduits.ChainSegue<>(before.sink(), new Conduits.ChainStepSource<>(new Conduits.ClosedSilo<>(before.source(), sink()), source())); }
        default Sink<In> andThen(Sink<? super Out> after) { return new Conduits.ChainSink<>(sink(), new Conduits.ClosedSilo<>(source(), after)); }
        default <T> SinkSource<In, T> andThen(SinkSource<? super Out, T> after) { return new Conduits.ChainSinkSource<>(new Conduits.ChainSink<>(sink(), new Conduits.ClosedSilo<>(source(), after.sink())), after.source()); }
        default <T> SinkStepSource<In, T> andThen(SinkStepSource<? super Out, T> after) { return new Conduits.ChainSinkStepSource<>(new Conduits.ChainSink<>(sink(), new Conduits.ClosedSilo<>(source(), after.sink())), after.source()); }
        
        // Mapping
        @Override default <T> SinkStepSource<T, Out> mapSink(SinkOperator<In, T> mapper) { return mapper.apply(sink()).andThen(source()); }
        @Override default <T> Segue<T, Out> mapSink(SinkToStepOperator<In, T> mapper) { return mapper.apply(sink()).andThen(source()); }
        default <T> SinkSource<In, T> mapSource(StepToSourceOperator<Out, T> mapper) { return sink().andThen(mapper.apply(source())); }
        default <T> SinkStepSource<In, T> mapSource(StepSourceOperator<Out, T> mapper) { return sink().andThen(mapper.apply(source())); }
    }
    
    public interface Segue<In, Out> extends StepSinkSource<In, Out>, SinkStepSource<In, Out> {
        // Chaining
        @Override default StepSource<Out> compose(Source<? extends In> before) { return new Conduits.ChainStepSource<>(new Conduits.ClosedSilo<>(before, sink()), source()); }
        @Override default <T> SinkStepSource<T, Out> compose(SinkSource<T, ? extends In> before) { return new Conduits.ChainSinkStepSource<>(before.sink(), new Conduits.ChainStepSource<>(new Conduits.ClosedSilo<>(before.source(), sink()), source())); }
        @Override default <T> Segue<T, Out> compose(StepSinkSource<T, ? extends In> before) { return new Conduits.ChainSegue<>(before.sink(), new Conduits.ChainStepSource<>(new Conduits.ClosedSilo<>(before.source(), sink()), source())); }
        @Override default StepSink<In> andThen(Sink<? super Out> after) { return new Conduits.ChainStepSink<>(sink(), new Conduits.ClosedSilo<>(source(), after)); }
        @Override default <T> StepSinkSource<In, T> andThen(SinkSource<? super Out, T> after) { return new Conduits.ChainStepSinkSource<>(new Conduits.ChainStepSink<>(sink(), new Conduits.ClosedSilo<>(source(), after.sink())), after.source()); }
        @Override default <T> Segue<In, T> andThen(SinkStepSource<? super Out, T> after) { return new Conduits.ChainSegue<>(new Conduits.ChainStepSink<>(sink(), new Conduits.ClosedSilo<>(source(), after.sink())), after.source()); }
        
        // Mapping
        @Override default <T> SinkStepSource<T, Out> mapSink(StepToSinkOperator<In, T> mapper) { return mapper.apply(sink()).andThen(source()); }
        @Override default <T> Segue<T, Out> mapSink(StepSinkOperator<In, T> mapper) { return mapper.apply(sink()).andThen(source()); }
        @Override default <T> StepSinkSource<In, T> mapSource(StepToSourceOperator<Out, T> mapper) { return sink().andThen(mapper.apply(source())); }
        @Override default <T> Segue<In, T> mapSource(StepSourceOperator<Out, T> mapper) { return sink().andThen(mapper.apply(source())); }
    }
    
    @FunctionalInterface
    public interface SinkOperator<T, U> {
        Sink<U> apply(Sink<T> sink);
        default <Out> SinkSource<U, Out> apply(SinkSource<T, Out> ss) { return apply(ss.sink()).andThen(ss.source()); }
        default <Out> SinkStepSource<U, Out> apply(SinkStepSource<T, Out> ss) { return apply(ss.sink()).andThen(ss.source()); }
        
        // Chaining
        default <V> SinkOperator<V, U> compose(SinkOperator<V, T> before) { return sink -> apply(before.apply(sink)); }
        default <V> StepToSinkOperator<V, U> compose(StepToSinkOperator<V, T> before) { return sink -> apply(before.apply(sink)); }
        default <V> SinkOperator<T, V> andThen(SinkOperator<U, V> after) { return sink -> after.apply(apply(sink)); }
        default <V> SinkToStepOperator<T, V> andThen(SinkToStepOperator<U, V> after) { return sink -> after.apply(apply(sink)); }
    }
    
    @FunctionalInterface
    public interface StepToSinkOperator<T, U> {
        Sink<U> apply(StepSink<T> sink);
        default <Out> SinkSource<U, Out> apply(StepSinkSource<T, Out> ss) { return apply(ss.sink()).andThen(ss.source()); }
        default <Out> SinkStepSource<U, Out> apply(Segue<T, Out> ss) { return apply(ss.sink()).andThen(ss.source()); }
        
        // Chaining
        default <V> SinkOperator<V, U> compose(SinkToStepOperator<V, T> before) { return sink -> apply(before.apply(sink)); }
        default <V> StepToSinkOperator<V, U> compose(StepSinkOperator<V, T> before) { return sink -> apply(before.apply(sink)); }
        default <V> StepToSinkOperator<T, V> andThen(SinkOperator<U, V> after) { return sink -> after.apply(apply(sink)); }
        default <V> StepSinkOperator<T, V> andThen(SinkToStepOperator<U, V> after) { return sink -> after.apply(apply(sink)); }
    }
    
    @FunctionalInterface
    public interface SinkToStepOperator<T, U> extends SinkOperator<T, U> {
        @Override StepSink<U> apply(Sink<T> sink);
        @Override default <Out> StepSinkSource<U, Out> apply(SinkSource<T, Out> ss) { return apply(ss.sink()).andThen(ss.source()); }
        @Override default <Out> Segue<U, Out> apply(SinkStepSource<T, Out> ss) { return apply(ss.sink()).andThen(ss.source()); }
        
        // Chaining
        @Override default <V> SinkToStepOperator<V, U> compose(SinkOperator<V, T> before) { return sink -> apply(before.apply(sink)); }
        @Override default <V> StepSinkOperator<V, U> compose(StepToSinkOperator<V, T> before) { return sink -> apply(before.apply(sink)); }
        default <V> SinkOperator<T, V> andThen(StepToSinkOperator<U, V> after) { return sink -> after.apply(apply(sink)); }
        default <V> SinkToStepOperator<T, V> andThen(StepSinkOperator<U, V> after) { return sink -> after.apply(apply(sink)); }
    }
    
    @FunctionalInterface
    public interface StepSinkOperator<T, U> extends StepToSinkOperator<T, U> {
        @Override StepSink<U> apply(StepSink<T> sink);
        @Override default <Out> StepSinkSource<U, Out> apply(StepSinkSource<T, Out> ss) { return apply(ss.sink()).andThen(ss.source()); }
        @Override default <Out> Segue<U, Out> apply(Segue<T, Out> ss) { return apply(ss.sink()).andThen(ss.source()); }
        
        // Chaining
        @Override default <V> SinkToStepOperator<V, U> compose(SinkToStepOperator<V, T> before) { return sink -> apply(before.apply(sink)); }
        @Override default <V> StepSinkOperator<V, U> compose(StepSinkOperator<V, T> before) { return sink -> apply(before.apply(sink)); }
        default <V> StepToSinkOperator<T, V> andThen(StepToSinkOperator<U, V> after) { return sink -> after.apply(apply(sink)); }
        default <V> StepSinkOperator<T, V> andThen(StepSinkOperator<U, V> after) { return sink -> after.apply(apply(sink)); }
    }

    @FunctionalInterface
    public interface SourceOperator<T, U> {
        Source<U> apply(Source<T> source);
        default <In> SinkSource<In, U> apply(SinkSource<In, T> ss) { return ss.sink().andThen(apply(ss.source())); }
        default <In> StepSinkSource<In, U> apply(StepSinkSource<In, T> ss) { return ss.sink().andThen(apply(ss.source())); }
        
        // Chaining
        default <V> StepToSourceOperator<V, U> compose(StepToSourceOperator<V, T> before) { return source -> apply(before.apply(source)); }
        default <V> SourceOperator<V, U> compose(SourceOperator<V, T> before) { return source -> apply(before.apply(source)); }
        default <V> SourceOperator<T, V> andThen(SourceOperator<U, V> after) { return source -> after.apply(apply(source)); }
        default <V> SourceToStepOperator<T, V> andThen(SourceToStepOperator<U, V> after) { return source -> after.apply(apply(source)); }
    }
    
    @FunctionalInterface
    public interface StepToSourceOperator<T, U> {
        Source<U> apply(StepSource<T> source);
        default <In> SinkSource<In, U> apply(SinkStepSource<In, T> ss) { return ss.sink().andThen(apply(ss.source())); }
        default <In> StepSinkSource<In, U> apply(Segue<In, T> ss) { return ss.sink().andThen(apply(ss.source())); }
        
        // Chaining
        default <V> SourceOperator<V, U> compose(SourceToStepOperator<V, T> before) { return source -> apply(before.apply(source)); }
        default <V> StepToSourceOperator<V, U> compose(StepSourceOperator<V, T> before) { return source -> apply(before.apply(source)); }
        default <V> StepToSourceOperator<T, V> andThen(SourceOperator<U, V> after) { return source -> after.apply(apply(source)); }
        default <V> StepSourceOperator<T, V> andThen(SourceToStepOperator<U, V> after) { return source -> after.apply(apply(source)); }
    }
    
    @FunctionalInterface
    public interface SourceToStepOperator<T, U> extends SourceOperator<T, U> {
        @Override StepSource<U> apply(Source<T> source);
        @Override default <In> SinkStepSource<In, U> apply(SinkSource<In, T> ss) { return ss.sink().andThen(apply(ss.source())); }
        @Override default <In> Segue<In, U> apply(StepSinkSource<In, T> ss) { return ss.sink().andThen(apply(ss.source())); }
        
        // Chaining
        @Override default <V> SourceToStepOperator<V, U> compose(SourceOperator<V, T> before) { return source -> apply(before.apply(source)); }
        @Override default <V> StepSourceOperator<V, U> compose(StepToSourceOperator<V, T> before) { return source -> apply(before.apply(source)); }
        default <V> SourceOperator<T, V> andThen(StepToSourceOperator<U, V> after) { return source -> after.apply(apply(source)); }
        default <V> SourceToStepOperator<T, V> andThen(StepSourceOperator<U, V> after) { return source -> after.apply(apply(source)); }
    }
    
    @FunctionalInterface
    public interface StepSourceOperator<T, U> extends StepToSourceOperator<T, U> {
        @Override StepSource<U> apply(StepSource<T> source);
        @Override default <In> SinkStepSource<In, U> apply(SinkStepSource<In, T> ss) { return ss.sink().andThen(apply(ss.source())); }
        @Override default <In> Segue<In, U> apply(Segue<In, T> ss) { return ss.sink().andThen(apply(ss.source())); }
        
        // Chaining
        @Override default <V> SourceToStepOperator<V, U> compose(SourceToStepOperator<V, T> before) { return source -> apply(before.apply(source)); }
        @Override default <V> StepSourceOperator<V, U> compose(StepSourceOperator<V, T> before) { return source -> apply(before.apply(source)); }
        default <V> StepToSourceOperator<T, V> andThen(StepToSourceOperator<U, V> after) { return source -> after.apply(apply(source)); }
        default <V> StepSourceOperator<T, V> andThen(StepSourceOperator<U, V> after) { return source -> after.apply(apply(source)); }
    }
}