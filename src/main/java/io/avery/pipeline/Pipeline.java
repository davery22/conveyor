package io.avery.pipeline;

import java.util.concurrent.Callable;
import java.util.function.Consumer;

public class Pipeline {
    private Pipeline() {}
    
    public interface Source<Out> {
        Tunnel.GatedSource<Out> mouth();
        void run(Consumer<? super Callable<?>> fork);
        <T> Source<T> connect(Segment<? super Out, T> segment);
        System connect(Sink<? super Out> sink);
    }
    
    public interface Segment<In, Out> {
        Tunnel.GatedSink<In> head();
        Tunnel.GatedSource<Out> mouth();
        void run(Consumer<? super Callable<?>> fork);
        <T> Segment<In, T> connect(Segment<? super Out, T> segment);
        Sink<In> connect(Sink<? super Out> sink);
    }
    
    public interface Sink<In> {
        Tunnel.GatedSink<In> head();
        void run(Consumer<? super Callable<?>> fork);
    }
    
    public interface System {
        void run(Consumer<? super Callable<?>> fork);
    }
}
