package io.avery.pipeline;

import java.util.concurrent.Callable;
import java.util.function.Consumer;

public class Pipeline {
    private Pipeline() {}
    
    public sealed interface System permits Source, Sink, Pipelines.ClosedSystem {
        void run(Consumer<Callable<?>> fork);
    }
    
    public sealed interface Source<Out> extends System permits StepSource, Pipelines.ChainedSource {
        Tunnel.Source<Out> source();
        
        <T> StepSource<T> andThen(Stage<? super Out, T> stage);
        System andThen(StepSink<? super Out> sink);
    }
    
    public sealed interface Sink<In> extends System permits StepSink, Pipelines.ChainedSink {
        Tunnel.Sink<In> sink();
        
        <T> StepSink<T> compose(Stage<T, ? extends In> stage);
        System compose(StepSource<? extends In> source);
    }
    
    public sealed interface StepSource<Out> extends Source<Out> permits Stage, Pipelines.ChainedStepSource {
        @Override Tunnel.GatedSource<Out> source();
        
        System andThen(Sink<? super Out> sink);
    }
    
    public sealed interface StepSink<In> extends Sink<In> permits Stage, Pipelines.ChainedStepSink {
        @Override Tunnel.GatedSink<In> sink();
        
        System compose(Source<? extends In> source);
    }
    
    public sealed interface Stage<In, Out> extends StepSink<In>, StepSource<Out> permits Pipelines.ChainedStage {
        @Override <T> Stage<T, Out> compose(Stage<T, ? extends In> stage);
        @Override StepSource<Out> compose(StepSource<? extends In> source);
        @Override StepSource<Out> compose(Source<? extends In> source);
        
        @Override <T> Stage<In, T> andThen(Stage<? super Out, T> stage);
        @Override StepSink<In> andThen(StepSink<? super Out> sink);
        @Override StepSink<In> andThen(Sink<? super Out> sink);
    }
}
