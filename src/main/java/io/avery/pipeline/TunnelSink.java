package io.avery.pipeline;

import java.util.concurrent.locks.ReentrantLock;

public interface TunnelSink<T> {
    boolean offer(T input) throws Exception;
    default void complete() throws Exception {}
    
    default ReentrantLock lock() { return null; }
    
    // 'Gatherers' can be used to prepend transformations, creating a new sink
    default <U, A> TunnelSink<U> prepend(Gatherer<U, A, T> gatherer) {
        return Tunnels.prepend(gatherer, this);
    }
}
