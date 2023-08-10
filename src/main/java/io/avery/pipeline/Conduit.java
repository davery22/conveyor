package io.avery.pipeline;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collector;

public class Conduit {
    private Conduit() {}
    
    @FunctionalInterface
    public interface Source<T> extends AutoCloseable {
        boolean drainToSink(StepSink<? super T> sink) throws Exception;
        
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
    
    @FunctionalInterface
    public interface Sink<T> {
        boolean drainFromSource(StepSource<? extends T> source) throws Exception;
        
        default void complete(Throwable error) throws Exception {
            // Default impl handles the case where the Sink has no async downstream.
            // Implementations that have an async downstream should override this method to propagate error downstream.
            // TODO: What about eg balance(), where one of several Sinks might be synchronous, causing a throw...
            //  In general, when/how can it be safe for close()/complete(err) to actually throw?
            //  Maybe ShutdownOnFailure is not the right scope for pipelines?
            if (error != null) {
                throw new UpstreamException(error);
            }
        }
        
        default Pipeline.Sink<T> pipeline() {
            return Pipelines.sink(this);
        }
    }
    
    @FunctionalInterface
    public interface StepSource<T> extends Source<T> {
        T poll() throws Exception;
        
        @Override
        default boolean drainToSink(StepSink<? super T> sink) throws Exception {
            for (T e; (e = poll()) != null; ) {
                if (!sink.offer(e)) {
                    return false;
                }
            }
            return true;
        }
        
        @Override
        default Pipeline.StepSource<T> pipeline() {
            return Pipelines.stepSource(this);
        }
    }
    
    @FunctionalInterface
    public interface StepSink<T> extends Sink<T> {
        boolean offer(T input) throws Exception;
        
        @Override
        default boolean drainFromSource(StepSource<? extends T> source) throws Exception {
            for (T e; (e = source.poll()) != null; ) {
                if (!offer(e)) {
                    return false;
                }
            }
            return true;
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