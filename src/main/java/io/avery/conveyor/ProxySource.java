package io.avery.conveyor;

import java.util.concurrent.Executor;
import java.util.stream.Stream;

public abstract class ProxySource<T> implements Belt.Source<T> {
    protected abstract Stream<? extends Belt.Source<?>> sources();
    
    @Override
    public void close() throws Exception {
        Belts.composedClose(sources());
    }
    
    @Override
    public void run(Executor executor) {
        sources().sequential().forEach(source -> source.run(executor));
    }
}
