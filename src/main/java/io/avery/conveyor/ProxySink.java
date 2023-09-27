package io.avery.conveyor;

import java.util.concurrent.Executor;
import java.util.stream.Stream;

public abstract class ProxySink<T> implements Belt.Sink<T> {
    protected abstract Stream<? extends Belt.Sink<?>> sinks();
    
    @Override
    public void complete() throws Exception {
        Belts.composedComplete(sinks());
    }
    
    @Override
    public void completeAbruptly(Throwable exception) throws Exception {
        Belts.composedCompleteAbruptly(sinks(), exception);
    }
    
    @Override
    public void run(Executor executor) {
        sinks().sequential().forEach(sink -> sink.run(executor));
    }
}
