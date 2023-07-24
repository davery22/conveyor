package io.avery.pipeline;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collector;

public interface TunnelSource<T> extends AutoCloseable {
    T poll() throws Exception;
    default void close() throws Exception {}
    
    default void forEach(TunnelSink<? super T> sink) throws Exception {
        for (T e; (e = poll()) != null; ) {
            if (!sink.offer(e)) {
                return;
            }
        }
    }
    
    default void forEach(Consumer<? super T> action) throws Exception {
        for (T e; (e = poll()) != null; ) {
            action.accept(e);
        }
    }
    
    default <A, R> R collect(Collector<? super T, A, R> collector) throws Exception {
        BiConsumer<A, ? super T> accumulator = collector.accumulator();
        Function<A, R> finisher = collector.finisher();
        A acc = collector.supplier().get();
        for (T e; (e = poll()) != null; ) {
            accumulator.accept(acc, e);
        }
        return finisher.apply(acc);
    }
}
