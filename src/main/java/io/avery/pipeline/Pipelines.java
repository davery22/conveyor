package io.avery.pipeline;

import java.util.concurrent.Callable;
import java.util.function.Consumer;

public class Pipelines {
    private Pipelines() {} // Utility
    
    public static <T> Pipeline.Source<T> source(Tunnel.Source<T> source) {
        return new ChainedSource<>(new Leaf(source));
    }
    
    public static <T> Pipeline.Sink<T> sink(Tunnel.Sink<T> sink) {
        return new ChainedSink<>(new Leaf(sink));
    }
    
    public static <T> Pipeline.StepSource<T> stepSource(Tunnel.GatedSource<T> source) {
        return new ChainedStepSource<>(new Leaf(source));
    }
    
    public static <T> Pipeline.StepSink<T> stepSink(Tunnel.GatedSink<T> sink) {
        return new ChainedStepSink<>(new Leaf(sink));
    }
    
    public static <T, U> Pipeline.Stage<T, U> stage(Tunnel.FullGate<T, U> gate) {
        return new ChainedStage<>(new Leaf(gate));
    }
    
    static sealed class ClosedSystem implements Pipeline.System {
        final TinyTree stages;
        
        private ClosedSystem(TinyTree stages) {
            this.stages = stages;
        }
        
        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        public void run(Consumer<Callable<?>> fork) {
            var box = new Object(){ Object stage = null; };
            stages.forEach(stage -> {
                if (box.stage == null) {
                    box.stage = stage;
                    return;
                }
                var prev = (Tunnel.Source<?>) box.stage;
                var curr = (Tunnel.Sink<?>) (box.stage = stage);
                fork.accept(() -> {
                    try (prev) {
                        if (curr instanceof Tunnel.GatedSink gs) {
                            prev.drainToSink(gs);
                        } else {
                            curr.drainFromSource((Tunnel.GatedSource) prev);
                        }
                        curr.complete(null);
                    } catch (Throwable error) {
                        curr.complete(error);
                    }
                    return null;
                });
            });
        }
    }
    
    static sealed class ChainedSource<Out> extends ClosedSystem implements Pipeline.Source<Out> {
        ChainedSource(TinyTree stages) {
            super(stages);
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public Tunnel.Source<Out> source() {
            return (Tunnel.Source<Out>) stages.last();
        }
        
        @Override
        public <T> Pipeline.StepSource<T> andThen(Pipeline.Stage<? super Out, T> stage) {
            return new ChainedStepSource<>(combineStages(this, (ClosedSystem) stage));
        }
        
        @Override
        public Pipeline.System andThen(Pipeline.StepSink<? super Out> sink) {
            return new ClosedSystem(combineStages(this, (ClosedSystem) sink));
        }
    }
    
    static sealed class ChainedSink<In> extends ClosedSystem implements Pipeline.Sink<In> {
        ChainedSink(TinyTree stages) {
            super(stages);
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public Tunnel.Sink<In> sink() {
            return (Tunnel.Sink<In>) stages.first();
        }
        
        @Override
        public <T> Pipeline.StepSink<T> compose(Pipeline.Stage<T, ? extends In> stage) {
            return new ChainedStepSink<>(combineStages((ClosedSystem) stage, this));
        }
        
        @Override
        public Pipeline.System compose(Pipeline.StepSource<? extends In> source) {
            return new ClosedSystem(combineStages((ClosedSystem) source, this));
        }
    }
    
    static final class ChainedStepSource<Out> extends ChainedSource<Out> implements Pipeline.StepSource<Out> {
        ChainedStepSource(TinyTree stages) {
            super(stages);
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public Tunnel.GatedSource<Out> source() {
            return (Tunnel.GatedSource<Out>) stages.last();
        }
        
        @Override
        public Pipeline.System andThen(Pipeline.Sink<? super Out> sink) {
            return new ClosedSystem(combineStages(this, (ClosedSystem) sink));
        }
    }
    
    static final class ChainedStepSink<In> extends ChainedSink<In> implements Pipeline.StepSink<In> {
        ChainedStepSink(TinyTree stages) {
            super(stages);
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public Tunnel.GatedSink<In> sink() {
            return (Tunnel.GatedSink<In>) stages.first();
        }
        
        @Override
        public Pipeline.System compose(Pipeline.Source<? extends In> source) {
            return new ClosedSystem(combineStages((ClosedSystem) source, this));
        }
    }
    
    static final class ChainedStage<In, Out> extends ClosedSystem implements Pipeline.Stage<In, Out> {
        ChainedStage(TinyTree stages) {
            super(stages);
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public Tunnel.GatedSink<In> sink() {
            return (Tunnel.GatedSink<In>) stages.first();
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public Tunnel.GatedSource<Out> source() {
            return (Tunnel.GatedSource<Out>) stages.last();
        }
        
        @Override
        public <T> Pipeline.Stage<In, T> andThen(Pipeline.Stage<? super Out, T> stage) {
            return new ChainedStage<>(combineStages(this, (ClosedSystem) stage));
        }
        
        @Override
        public Pipeline.StepSink<In> andThen(Pipeline.StepSink<? super Out> sink) {
            return new ChainedStepSink<>(combineStages(this, (ClosedSystem) sink));
        }
        
        @Override
        public Pipeline.StepSink<In> andThen(Pipeline.Sink<? super Out> sink) {
            return new ChainedStepSink<>(combineStages(this, (ClosedSystem) sink));
        }
        
        @Override
        public <T> Pipeline.Stage<T, Out> compose(Pipeline.Stage<T, ? extends In> stage) {
            return new ChainedStage<>(combineStages((ClosedSystem) stage, this));
        }
        
        @Override
        public Pipeline.StepSource<Out> compose(Pipeline.StepSource<? extends In> source) {
            return new ChainedStepSource<>(combineStages((ClosedSystem) source, this));
        }
        
        @Override
        public Pipeline.StepSource<Out> compose(Pipeline.Source<? extends In> source) {
            return new ChainedStepSource<>(combineStages((ClosedSystem) source, this));
        }
    }
    
    private static TinyTree combineStages(ClosedSystem a, ClosedSystem b) {
        return new Branch(a.stages, b.stages);
    }
    
    private sealed interface TinyTree {
        void forEach(Consumer<Object> action);
        Object first();
        Object last();
    }
    
    private record Branch(TinyTree left, TinyTree right) implements TinyTree {
        @Override public void forEach(Consumer<Object> action) { left.forEach(action); right.forEach(action); }
        @Override public Object first() { return left.first(); }
        @Override public Object last() { return right.last(); }
    }
    
    private record Leaf(Object value) implements TinyTree {
        @Override public void forEach(Consumer<Object> action) { action.accept(value); }
        @Override public Object first() { return value; }
        @Override public Object last() { return value; }
    }
}
