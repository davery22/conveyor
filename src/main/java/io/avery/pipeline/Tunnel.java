package io.avery.pipeline;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collector;

public class Tunnel {
    private Tunnel() {}
    
    public interface Source<T> extends AutoCloseable {
        void drainToSink(StepSink<? super T> sink) throws Exception;
        
        default void close() throws Exception {}
        
        default Pipeline.Source<T> pipeline() {
            return Pipelines.source(this);
        }
        
        default void forEach(Consumer<? super T> action) throws Exception {
            class ConsumerSink implements StepSink<T> {
                @Override
                public boolean offer(T input) {
                    action.accept(input);
                    return true;
                }
            }
            
            drainToSink(new ConsumerSink());
        }
        
        default <A, R> R collect(Collector<? super T, A, R> collector) throws Exception {
            BiConsumer<A, ? super T> accumulator = collector.accumulator();
            Function<A, R> finisher = collector.finisher();
            A acc = collector.supplier().get();
            
            class CollectorSink implements StepSink<T> {
                @Override
                public boolean offer(T input) {
                    accumulator.accept(acc, input);
                    return true;
                }
            }
            
            drainToSink(new CollectorSink());
            return finisher.apply(acc);
        }
    }
    
    public interface Sink<T> {
        void drainFromSource(StepSource<? extends T> source) throws Exception;
        
        default void complete(Throwable error) throws Exception {}
        
        default Pipeline.Sink<T> pipeline() {
            return Pipelines.sink(this);
        }
    }
    
    public interface StepSource<T> extends Source<T> {
        T poll() throws Exception;
        
        @Override
        default void drainToSink(StepSink<? super T> sink) throws Exception {
            for (T e; (e = poll()) != null && sink.offer(e); ) { }
        }
        
        @Override
        default Pipeline.StepSource<T> pipeline() {
            return Pipelines.stepSource(this);
        }
    }
    
    public interface StepSink<T> extends Sink<T> {
        boolean offer(T input) throws Exception;
        
        @Override
        default void drainFromSource(StepSource<? extends T> source) throws Exception {
            for (T e; (e = source.poll()) != null && offer(e); ) { }
        }
        
        @Override
        default Pipeline.StepSink<T> pipeline() {
            return Pipelines.stepSink(this);
        }
    }
    
    public interface Stage<In, Out> extends StepSink<In>, StepSource<Out> {
        @Override
        default Pipeline.Stage<In, Out> pipeline() {
            return Pipelines.stage(this);
        }
    }
}