package io.avery.pipeline;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collector;

public interface Tunnel<In, Out> extends AutoCloseable {
    boolean offer(In input) throws InterruptedException;
    void complete() throws InterruptedException;
    
    Out poll() throws InterruptedException;
    void close();
    
    default void forEach(Consumer<? super Out> action) throws InterruptedException {
        for (Out e; (e = poll()) != null; ) {
            action.accept(e);
        }
    }
    
    default <A, R> R collect(Collector<? super Out, A, R> collector) throws InterruptedException {
        BiConsumer<A, ? super Out> accumulator = collector.accumulator();
        Function<A, R> finisher = collector.finisher();
        A acc = collector.supplier().get();
        for (Out e; (e = poll()) != null; ) {
            accumulator.accept(acc, e);
        }
        return finisher.apply(acc);
    }
}