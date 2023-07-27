package io.avery.pipeline;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collector;

public class Tunnel {
    private Tunnel() {}
    
    public interface Push<T> extends AutoCloseable {
        void forEachUntilCancel(Sink<? super T> action) throws Exception;
        default void close() throws Exception {}
        
        default void forEach(Consumer<? super T> action) throws Exception {
            class ConsumerSink implements Sink<T> {
                @Override
                public boolean offer(T input) {
                    action.accept(input);
                    return true;
                }
            }
            
            forEachUntilCancel(new ConsumerSink());
        }
        
        default <A, R> R collect(Collector<? super T, A, R> collector) throws Exception {
            BiConsumer<A, ? super T> accumulator = collector.accumulator();
            Function<A, R> finisher = collector.finisher();
            A acc = collector.supplier().get();
            
            class CollectorSink implements Sink<T> {
                @Override
                public boolean offer(T input) {
                    accumulator.accept(acc, input);
                    return true;
                }
            }
            
            forEachUntilCancel(new CollectorSink());
            return finisher.apply(acc);
        }
    }
    
    public interface Source<T> extends Push<T> {
        T poll() throws Exception;
        
        default void forEachUntilCancel(Sink<? super T> sink) throws Exception {
            for (T e; (e = poll()) != null; ) {
                if (!sink.offer(e)) {
                    return;
                }
            }
        }
    }
    
    public interface Sink<T> {
        boolean offer(T input) throws Exception;
        default void complete(Throwable error) throws Exception {}
    }
    
    public interface Stage<In, Out> extends Sink<In>, Source<Out> {
    }
}