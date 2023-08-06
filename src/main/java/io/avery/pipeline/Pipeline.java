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
        
        <T> StepSource<T> andThen(Stage<? super Out, T> after);
        System andThen(StepSink<? super Out> after);
    }
    
    public sealed interface Sink<In> extends System permits StepSink, Pipelines.ChainedSink {
        Tunnel.Sink<In> sink();
        
        <T> StepSink<T> compose(Stage<T, ? extends In> before);
        System compose(StepSource<? extends In> before);
    }
    
    public sealed interface StepSource<Out> extends Source<Out> permits Stage, Pipelines.ChainedStepSource {
        @Override
        Tunnel.StepSource<Out> source();
        
        System andThen(Sink<? super Out> after);
    }
    
    public sealed interface StepSink<In> extends Sink<In> permits Stage, Pipelines.ChainedStepSink {
        @Override
        Tunnel.StepSink<In> sink();
        
        System compose(Source<? extends In> before);
    }
    
    public sealed interface Stage<In, Out> extends StepSink<In>, StepSource<Out> permits Pipelines.ChainedStage {
        @Override <T> Stage<T, Out> compose(Stage<T, ? extends In> before);
        @Override StepSource<Out> compose(StepSource<? extends In> before);
        @Override StepSource<Out> compose(Source<? extends In> before);
        
        @Override <T> Stage<In, T> andThen(Stage<? super Out, T> after);
        @Override StepSink<In> andThen(StepSink<? super Out> after);
        @Override StepSink<In> andThen(Sink<? super Out> after);
    }
}
