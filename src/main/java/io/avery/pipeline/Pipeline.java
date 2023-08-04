package io.avery.pipeline;

import java.util.concurrent.Callable;
import java.util.function.Consumer;

public class Pipeline {
    private Pipeline() {}
    
    public sealed interface System permits Head, Tail, Pipelines.ClosedSystem {
        void runInternals(Consumer<Callable<?>> fork);
    }
    
    public sealed interface Head<In> extends System permits Body, Pipelines.ChainedHead {
        Tunnel.GatedSink<In> sink();
    }
    
    public sealed interface Tail<Out> extends System permits Body, Pipelines.ChainedTail {
        Tunnel.GatedSource<Out> source();
        <T> Tail<T> connect(Body<? super Out, T> body);
        System connect(Head<? super Out> head);
    }
    
    public sealed interface Body<In, Out> extends Head<In>, Tail<Out> permits Pipelines.ChainedBody {
        <T> Body<In, T> connect(Body<? super Out, T> body);
        Head<In> connect(Head<? super Out> head);
    }
}
