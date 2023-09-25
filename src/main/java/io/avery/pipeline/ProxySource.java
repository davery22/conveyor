package io.avery.pipeline;

import java.util.concurrent.Executor;
import java.util.stream.Stream;

public abstract class ProxySource<T> implements Conduit.Source<T> {
    protected abstract Stream<? extends Conduit.Source<?>> sources();
    
    @Override
    public void close() throws Exception {
        Conduits.composedClose(sources());
    }
    
    @Override
    public void run(Executor executor) {
        sources().sequential().forEach(source -> source.run(executor));
    }
}
