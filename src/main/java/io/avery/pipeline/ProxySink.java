package io.avery.pipeline;

import java.util.concurrent.Executor;

public abstract class ProxySink<T> implements Conduit.Sink<T> {
    protected abstract Conduit.Sink<?> sink();
    
    @Override
    public void complete() throws Exception {
        sink().complete();
    }
    
    @Override
    public void completeExceptionally(Throwable ex) throws Exception {
        sink().completeExceptionally(ex);
    }
    
    @Override
    public void run(Executor executor) {
        sink().run(executor);
    }
}
