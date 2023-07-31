package io.avery.pipeline;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collector;

public class Tunnel {
    private Tunnel() {}
    
    public interface Source<T> extends AutoCloseable {
        void drainToSink(GatedSink<? super T> action) throws Exception;
        default void close() throws Exception {}
        
        default void forEach(Consumer<? super T> action) throws Exception {
            class ConsumerSink implements GatedSink<T> {
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
            
            class CollectorSink implements GatedSink<T> {
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
        void drainFromSource(GatedSource<? extends T> source) throws Exception;
        default void complete(Throwable error) throws Exception {}
    }
    
    public interface GatedSource<T> extends Source<T> {
        T poll() throws Exception;
        
        @Override
        default void drainToSink(GatedSink<? super T> sink) throws Exception {
            for (T e; (e = poll()) != null && sink.offer(e); ) { }
        }
    }
    
    public interface GatedSink<T> extends Sink<T> {
        boolean offer(T input) throws Exception;
        
        @Override
        default void drainFromSource(GatedSource<? extends T> source) throws Exception {
            for (T e; (e = source.poll()) != null && offer(e); ) { }
        }
    }
    
    // "Segueway"
    
    public interface Gate<In, Out> extends GatedSink<In>, GatedSource<Out> {
    }
}