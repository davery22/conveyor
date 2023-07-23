package io.avery.pipeline;

import java.util.concurrent.ExecutionException;

public interface TunnelSink<T> {
    boolean offer(T input) throws ExecutionException, InterruptedException;
    default void complete() throws ExecutionException, InterruptedException {}
    
    // 'Gatherers' can be used to prepend transformations, creating a new sink
    default <U, A> TunnelSink<U> compose(Gatherer<U, A, T> gatherer) {
        return Tunnels.compose(gatherer, this);
    }
}
