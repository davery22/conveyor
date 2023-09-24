package io.avery.pipeline;

import java.util.concurrent.Executor;
import java.util.stream.Stream;

public abstract class FanOutSink<T> implements Conduit.Sink<T> {
    protected abstract Stream<? extends Conduit.Sink<?>> sinks();
    
    @Override
    public void complete() throws Exception {
        Conduits.composedComplete(sinks());
    }
    
    @Override
    public void completeExceptionally(Throwable exception) throws Exception {
        Conduits.composedCompleteExceptionally(sinks(), exception);
    }
    
    @Override
    public void run(Executor executor) {
        sinks().sequential().forEach(sink -> sink.run(executor));
    }
}
