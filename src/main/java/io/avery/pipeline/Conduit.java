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
        
        default Silo compose(StepSource<? extends In> before) { return new Conduits.ClosedSilo<>(before, this); }
        default <T> Sink<T> compose(SinkStepSource<T, ? extends In> before) { return new Conduits.ChainSink<>(before.sink(), new Conduits.ClosedSilo<>(before.source(), this)); }
        default <T> StepSink<T> compose(Segue<T, ? extends In> before) { return new Conduits.ChainStepSink<>(before.sink(), new Conduits.ClosedSilo<>(before.source(),  this)); }
        default Sink<In> andThen(Silo after) { return new Conduits.ChainSink<>(this, after); }
        default <T> SinkSource<In, T> andThen(Source<T> after) { return new Conduits.ChainSinkSource<>(this, after); }
        default <T> SinkStepSource<In, T> andThen(StepSource<T> after) { return new Conduits.ChainSinkStepSource<>(this, after); }
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
        
        default Source<Out> compose(Silo before) { return new Conduits.ChainSource<>(before, this); }
        default <T> SinkSource<T, Out> compose(Sink<T> before) { return new Conduits.ChainSinkSource<>(before, this); }
        default <T> StepSinkSource<T, Out> compose(StepSink<T> before) { return new Conduits.ChainStepSinkSource<>(before, this); }
        default Silo andThen(StepSink<? super Out> after) { return new Conduits.ClosedSilo<>(this, after); }
        default <T> Source<T> andThen(StepSinkSource<? super Out, T> after) { return new Conduits.ChainSource<>(new Conduits.ClosedSilo<>(this, after.sink()), after.source()); }
        default <T> StepSource<T> andThen(Segue<? super Out, T> after) { return new Conduits.ChainStepSource<>(new Conduits.ClosedSilo<>(this, after.sink()), after.source()); }
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
        
        default Silo compose(Source<? extends In> before) { return new Conduits.ClosedSilo<>(before, this); }
        default <T> Sink<T> compose(SinkSource<T, ? extends In> before) { return new Conduits.ChainSink<>(before.sink(), new Conduits.ClosedSilo<>(before.source(), this)); }
        default <T> StepSink<T> compose(StepSinkSource<T, ? extends In> before) { return new Conduits.ChainStepSink<>(before.sink(), new Conduits.ClosedSilo<>(before.source(), this)); }
        @Override default StepSink<In> andThen(Silo after) { return new Conduits.ChainStepSink<>(this, after); }
        @Override default <T> StepSinkSource<In, T> andThen(Source<T> after) { return new Conduits.ChainStepSinkSource<>(this, after); }
        @Override default <T> Segue<In, T> andThen(StepSource<T> after) { return new Conduits.ChainSegue<>(this, after); }
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
        
        @Override default StepSource<Out> compose(Silo before) { return new Conduits.ChainStepSource<>(before, this); }
        @Override default <T> SinkStepSource<T, Out> compose(Sink<T> before) { return new Conduits.ChainSinkStepSource<>(before, this); }
        @Override default <T> Segue<T, Out> compose(StepSink<T> before) { return new Conduits.ChainSegue<>(before, this); }
        default Silo andThen(Sink<? super Out> after) { return new Conduits.ClosedSilo<>(this, after); }
        default <T> Source<T> andThen(SinkSource<? super Out, T> after) { return new Conduits.ChainSource<>(new Conduits.ClosedSilo<>(this, after.sink()), after.source()); }
        default <T> StepSource<T> andThen(SinkStepSource<? super Out, T> after) { return new Conduits.ChainStepSource<>(new Conduits.ClosedSilo<>(this, after.sink()), after.source()); }
    }
    
    public interface SinkSource<In, Out> {
        Sink<In> sink();
        Source<Out> source();
        
        default Source<Out> compose(StepSource<? extends In> before) { return new Conduits.ChainSource<>(new Conduits.ClosedSilo<>(before, sink()), source()); }
        default <T> SinkSource<T, Out> compose(SinkStepSource<T, ? extends In> before) { return new Conduits.ChainSinkSource<>(before.sink(), new Conduits.ChainSource<>(new Conduits.ClosedSilo<>(before.source(), sink()), source())); }
        default <T> StepSinkSource<T, Out> compose(Segue<T, ? extends In> before) { return new Conduits.ChainStepSinkSource<>(before.sink(), new Conduits.ChainSource<>(new Conduits.ClosedSilo<>(before.source(), sink()), source())); }
        default Sink<In> andThen(StepSink<? super Out> after) { return new Conduits.ChainSink<>(sink(), new Conduits.ClosedSilo<>(source(), after)); }
        default <T> SinkSource<In, T> andThen(StepSinkSource<? super Out, T> after) { return new Conduits.ChainSinkSource<>(new Conduits.ChainSink<>(sink(), new Conduits.ClosedSilo<>(source(), after.sink())), after.source()); }
        default <T> SinkStepSource<In, T> andThen(Segue<? super Out, T> after) { return new Conduits.ChainSinkStepSource<>(new Conduits.ChainSink<>(sink(), new Conduits.ClosedSilo<>(source(), after.sink())), after.source()); }
    }
    
    public interface StepSinkSource<In, Out> extends SinkSource<In, Out> {
        @Override StepSink<In> sink();
        
        default Source<Out> compose(Source<? extends In> before) { return new Conduits.ChainSource<>(new Conduits.ClosedSilo<>(before, sink()), source()); }
        default <T> SinkSource<T, Out> compose(SinkSource<T, ? extends In> before) { return new Conduits.ChainSinkSource<>(before.sink(), new Conduits.ChainSource<>(new Conduits.ClosedSilo<>(before.source(), sink()), source())); }
        default <T> StepSinkSource<T, Out> compose(StepSinkSource<T, ? extends In> before) { return new Conduits.ChainStepSinkSource<>(before.sink(), new Conduits.ChainSource<>(new Conduits.ClosedSilo<>(before.source(), sink()), source())); }
        @Override default StepSink<In> andThen(StepSink<? super Out> after) { return new Conduits.ChainStepSink<>(sink(), new Conduits.ClosedSilo<>(source(), after)); }
        @Override default <T> StepSinkSource<In, T> andThen(StepSinkSource<? super Out, T> after) { return new Conduits.ChainStepSinkSource<>(new Conduits.ChainStepSink<>(sink(), new Conduits.ClosedSilo<>(source(), after.sink())), after.source()); }
        @Override default <T> Segue<In, T> andThen(Segue<? super Out, T> after) { return new Conduits.ChainSegue<>(new Conduits.ChainStepSink<>(sink(), new Conduits.ClosedSilo<>(source(), after.sink())), after.source()); }
    }
    
    public interface SinkStepSource<In, Out> extends SinkSource<In, Out> {
        @Override StepSource<Out> source();
        
        @Override default StepSource<Out> compose(StepSource<? extends In> before) { return new Conduits.ChainStepSource<>(new Conduits.ClosedSilo<>(before, sink()), source()); }
        @Override default <T> SinkStepSource<T, Out> compose(SinkStepSource<T, ? extends In> before) { return new Conduits.ChainSinkStepSource<>(before.sink(), new Conduits.ChainStepSource<>(new Conduits.ClosedSilo<>(before.source(), sink()), source())); }
        @Override default <T> Segue<T, Out> compose(Segue<T, ? extends In> before) { return new Conduits.ChainSegue<>(before.sink(), new Conduits.ChainStepSource<>(new Conduits.ClosedSilo<>(before.source(), sink()), source())); }
        default Sink<In> andThen(Sink<? super Out> after) { return new Conduits.ChainSink<>(sink(), new Conduits.ClosedSilo<>(source(), after)); }
        default <T> SinkSource<In, T> andThen(SinkSource<? super Out, T> after) { return new Conduits.ChainSinkSource<>(new Conduits.ChainSink<>(sink(), new Conduits.ClosedSilo<>(source(), after.sink())), after.source()); }
        default <T> SinkStepSource<In, T> andThen(SinkStepSource<? super Out, T> after) { return new Conduits.ChainSinkStepSource<>(new Conduits.ChainSink<>(sink(), new Conduits.ClosedSilo<>(source(), after.sink())), after.source()); }
    }
    
    public interface Segue<In, Out> extends StepSinkSource<In, Out>, SinkStepSource<In, Out> {
        @Override default StepSource<Out> compose(Source<? extends In> before) { return new Conduits.ChainStepSource<>(new Conduits.ClosedSilo<>(before, sink()), source()); }
        @Override default <T> SinkStepSource<T, Out> compose(SinkSource<T, ? extends In> before) { return new Conduits.ChainSinkStepSource<>(before.sink(), new Conduits.ChainStepSource<>(new Conduits.ClosedSilo<>(before.source(), sink()), source())); }
        @Override default <T> Segue<T, Out> compose(StepSinkSource<T, ? extends In> before) { return new Conduits.ChainSegue<>(before.sink(), new Conduits.ChainStepSource<>(new Conduits.ClosedSilo<>(before.source(), sink()), source())); }
        @Override default StepSink<In> andThen(Sink<? super Out> after) { return new Conduits.ChainStepSink<>(sink(), new Conduits.ClosedSilo<>(source(), after)); }
        @Override default <T> StepSinkSource<In, T> andThen(SinkSource<? super Out, T> after) { return new Conduits.ChainStepSinkSource<>(new Conduits.ChainStepSink<>(sink(), new Conduits.ClosedSilo<>(source(), after.sink())), after.source()); }
        @Override default <T> Segue<In, T> andThen(SinkStepSource<? super Out, T> after) { return new Conduits.ChainSegue<>(new Conduits.ChainStepSink<>(sink(), new Conduits.ClosedSilo<>(source(), after.sink())), after.source()); }
    }
}