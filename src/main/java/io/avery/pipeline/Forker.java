package io.avery.pipeline;

import java.util.concurrent.Callable;

@FunctionalInterface
public interface Forker {
    void fork(Callable<?> callable);
}
