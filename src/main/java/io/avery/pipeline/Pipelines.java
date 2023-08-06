package io.avery.pipeline;

import java.util.concurrent.Callable;
import java.util.function.Consumer;

public class Pipelines {
    private Pipelines() {} // Utility
    
    public static <T> Pipeline.Source<T> source(Conduit.Source<T> source) {
        return new ChainedSource<>(new Leaf(source));
    }
    
    public static <T> Pipeline.Sink<T> sink(Conduit.Sink<T> sink) {
        return new ChainedSink<>(new Leaf(sink));
    }
    
    public static <T> Pipeline.StepSource<T> stepSource(Conduit.StepSource<T> source) {
        return new ChainedStepSource<>(new Leaf(source));
    }
    
    public static <T> Pipeline.StepSink<T> stepSink(Conduit.StepSink<T> sink) {
        return new ChainedStepSink<>(new Leaf(sink));
    }
    
    public static <T, U> Pipeline.Stage<T, U> stage(Conduit.Stage<T, U> stage) {
        return new ChainedStage<>(new Leaf(stage));
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
                var prev = (Conduit.Source<?>) box.stage;
                var curr = (Conduit.Sink<?>) (box.stage = stage);
                fork.accept(() -> {
                    try (prev) {
                        if (curr instanceof Conduit.StepSink ss) {
                            prev.drainToSink(ss);
                        } else {
                            curr.drainFromSource((Conduit.StepSource) prev);
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
        public Conduit.Source<Out> source() {
            return (Conduit.Source<Out>) stages.last();
        }
        
        @Override
        public <T> Pipeline.StepSource<T> andThen(Pipeline.Stage<? super Out, T> after) {
            return new ChainedStepSource<>(combineStages(this, (ClosedSystem) after));
        }
        
        @Override
        public Pipeline.System andThen(Pipeline.StepSink<? super Out> after) {
            return new ClosedSystem(combineStages(this, (ClosedSystem) after));
        }
    }
    
    static sealed class ChainedSink<In> extends ClosedSystem implements Pipeline.Sink<In> {
        ChainedSink(TinyTree stages) {
            super(stages);
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public Conduit.Sink<In> sink() {
            return (Conduit.Sink<In>) stages.first();
        }
        
        @Override
        public <T> Pipeline.StepSink<T> compose(Pipeline.Stage<T, ? extends In> before) {
            return new ChainedStepSink<>(combineStages((ClosedSystem) before, this));
        }
        
        @Override
        public Pipeline.System compose(Pipeline.StepSource<? extends In> before) {
            return new ClosedSystem(combineStages((ClosedSystem) before, this));
        }
    }
    
    static final class ChainedStepSource<Out> extends ChainedSource<Out> implements Pipeline.StepSource<Out> {
        ChainedStepSource(TinyTree stages) {
            super(stages);
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public Conduit.StepSource<Out> source() {
            return (Conduit.StepSource<Out>) stages.last();
        }
        
        @Override
        public Pipeline.System andThen(Pipeline.Sink<? super Out> after) {
            return new ClosedSystem(combineStages(this, (ClosedSystem) after));
        }
    }
    
    static final class ChainedStepSink<In> extends ChainedSink<In> implements Pipeline.StepSink<In> {
        ChainedStepSink(TinyTree stages) {
            super(stages);
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public Conduit.StepSink<In> sink() {
            return (Conduit.StepSink<In>) stages.first();
        }
        
        @Override
        public Pipeline.System compose(Pipeline.Source<? extends In> before) {
            return new ClosedSystem(combineStages((ClosedSystem) before, this));
        }
    }
    
    static final class ChainedStage<In, Out> extends ClosedSystem implements Pipeline.Stage<In, Out> {
        ChainedStage(TinyTree stages) {
            super(stages);
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public Conduit.StepSink<In> sink() {
            return (Conduit.StepSink<In>) stages.first();
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public Conduit.StepSource<Out> source() {
            return (Conduit.StepSource<Out>) stages.last();
        }
        
        @Override
        public <T> Pipeline.Stage<In, T> andThen(Pipeline.Stage<? super Out, T> after) {
            return new ChainedStage<>(combineStages(this, (ClosedSystem) after));
        }
        
        @Override
        public Pipeline.StepSink<In> andThen(Pipeline.StepSink<? super Out> after) {
            return new ChainedStepSink<>(combineStages(this, (ClosedSystem) after));
        }
        
        @Override
        public Pipeline.StepSink<In> andThen(Pipeline.Sink<? super Out> after) {
            return new ChainedStepSink<>(combineStages(this, (ClosedSystem) after));
        }
        
        @Override
        public <T> Pipeline.Stage<T, Out> compose(Pipeline.Stage<T, ? extends In> before) {
            return new ChainedStage<>(combineStages((ClosedSystem) before, this));
        }
        
        @Override
        public Pipeline.StepSource<Out> compose(Pipeline.StepSource<? extends In> before) {
            return new ChainedStepSource<>(combineStages((ClosedSystem) before, this));
        }
        
        @Override
        public Pipeline.StepSource<Out> compose(Pipeline.Source<? extends In> before) {
            return new ChainedStepSource<>(combineStages((ClosedSystem) before, this));
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
