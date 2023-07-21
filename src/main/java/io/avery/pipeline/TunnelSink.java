package io.avery.pipeline;

import java.util.concurrent.ExecutionException;

public interface TunnelSink<T> {
    boolean offer(T input) throws ExecutionException, InterruptedException;
    void complete() throws ExecutionException, InterruptedException;
}
