package io.avery.pipeline;

import java.util.concurrent.Executor;

public abstract class ProxySource<T> implements Conduit.Source<T> {
    protected abstract Conduit.Source<?> source();
    
    @Override
    public void close() throws Exception {
        source().close();
    }
    
    @Override
    public void run(Executor executor) {
        source().run(executor);
    }
}
