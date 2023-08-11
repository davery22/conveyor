package io.avery.pipeline;

import java.util.concurrent.Callable;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collector;

public class Conduit {
    private Conduit() {}
    
    public interface Operator {
        default void drainWithin(Consumer<Callable<?>> fork) {}
    }
    
    @FunctionalInterface
    public interface Source<Out> extends Operator, AutoCloseable {
        boolean drainToSink(StepSink<? super Out> sink) throws Exception;
        
        default void close() throws Exception {}
        
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
        
        default <T> StepSource<T> andThen(Stage<? super Out, T> after) {
            return new Conduits.ChainedStepSource<>(this, after);
        }
        
        default Operator andThen(StepSink<? super Out> after) {
            return new Conduits.ChainedOperator(this, after);
        }
    }
    
    @FunctionalInterface
    public interface Sink<In> extends Operator {
        boolean drainFromSource(StepSource<? extends In> source) throws Exception;
        
        default void complete(Throwable error) throws Exception {
            // Default impl handles the case where the Sink has no async downstream.
            // Implementations that have an async downstream should override this method to propagate error downstream.
            if (error != null) {
                throw new UpstreamException(error);
            }
        }
        
        default <T> StepSink<T> compose(Stage<T, ? extends In> before) {
            return new Conduits.ChainedStepSink<>(before, this);
        }
        
        default Operator compose(StepSource<? extends In> before) {
            return new Conduits.ChainedOperator(before, this);
        }
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
        
        default Operator andThen(Sink<? super Out> after) {
            return new Conduits.ChainedOperator(this, after);
        }
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
        
        default Operator compose(Source<? extends In> before) {
            return new Conduits.ChainedOperator(before, this);
        }
    }
    
    public interface Stage<In, Out> extends StepSink<In>, StepSource<Out> {
        @Override default <T> Stage<T, Out> compose(Stage<T, ? extends In> before) { return new Conduits.ChainedStage<>(before, this); }
        @Override default StepSource<Out> compose(StepSource<? extends In> before) { return new Conduits.ChainedStepSource<>(before, this); }
        @Override default StepSource<Out> compose(Source<? extends In> before) { return new Conduits.ChainedStepSource<>(before, this); }
        
        @Override default <T> Stage<In, T> andThen(Stage<? super Out, T> after) { return new Conduits.ChainedStage<>(this, after); }
        @Override default StepSink<In> andThen(StepSink<? super Out> after) { return new Conduits.ChainedStepSink<>(this, after); }
        @Override default StepSink<In> andThen(Sink<? super Out> after) { return new Conduits.ChainedStepSink<>(this, after); }
    }
}