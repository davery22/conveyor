package io.avery.pipeline;

import java.util.concurrent.Executor;
import java.util.stream.Stream;

public abstract class ProxySink<T> implements Conduit.Sink<T> {
    protected abstract Stream<? extends Conduit.Sink<?>> sinks();
    
    @Override
    public void complete() throws Exception {
        Conduits.composedComplete(sinks());
    }
    
    @Override
    public void completeAbruptly(Throwable exception) throws Exception {
        Conduits.composedCompleteAbruptly(sinks(), exception);
    }
    
    @Override
    public void run(Executor executor) {
        sinks().sequential().forEach(sink -> sink.run(executor));
    }
}
