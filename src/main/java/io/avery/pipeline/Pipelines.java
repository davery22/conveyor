package io.avery.pipeline;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

public class Pipelines {
    private Pipelines() {} // Utility
    
    static sealed class ClosedSystem implements Pipeline.System {
        final List<Object> stages;
        
        private ClosedSystem(List<Object> stages) {
            this.stages = stages;
        }
        
        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        public void run(Consumer<Callable<?>> fork) {
            var iter = stages.iterator();
            for (var cursor = iter.next(); iter.hasNext(); ) {
                var curr = (Tunnel.Source<?>) cursor;
                var next = (Tunnel.Sink<?>) iter.next();
                fork.accept(() -> {
                    try (curr) {
                        if (next instanceof Tunnel.GatedSink gs) {
                            curr.drainToSink(gs);
                        } else {
                            next.drainFromSource((Tunnel.GatedSource) curr);
                        }
                        next.complete(null);
                    } catch (Throwable error) {
                        next.complete(error);
                    }
                    return null;
                });
                cursor = next;
            }
        }
    }
    
    static sealed class ChainedSink<In> extends ClosedSystem implements Pipeline.Sink<In> {
        ChainedSink(List<Object> stages) {
            super(stages);
        }
        
        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        public Tunnel.Sink<In> sink() {
            return (Tunnel.Sink) stages.get(0);
        }
        
        @Override
        public <T> Pipeline.StepSink<T> compose(Pipeline.Stage<T, ? extends In> stage) {
            return new ChainedStepSink<>(combineStages((ClosedSystem) stage, this));
        }
        
        @Override
        public Pipeline.System compose(Pipeline.StepSource<? extends In> stepSource) {
            return new ClosedSystem(combineStages((ClosedSystem) stepSource, this));
        }
    }
    
    static final class ChainedStepSink<In> extends ChainedSink<In> implements Pipeline.StepSink<In> {
        ChainedStepSink(List<Object> stages) {
            super(stages);
        }
        
        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        public Tunnel.GatedSink<In> sink() {
            return (Tunnel.GatedSink) stages.get(0);
        }
        
        @Override
        public Pipeline.System compose(Pipeline.Source<? extends In> source) {
            return new ClosedSystem(combineStages((ClosedSystem) source, this));
        }
    }
    
    static sealed class ChainedSource<Out> extends ClosedSystem implements Pipeline.Source<Out> {
        ChainedSource(List<Object> stages) {
            super(stages);
        }
        
        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        public Tunnel.Source<Out> source() {
            return (Tunnel.Source) stages.get(stages.size()-1);
        }
        
        @Override
        public <T> Pipeline.StepSource<T> andThen(Pipeline.Stage<? super Out, T> stage) {
            return new ChainedStepSource<>(combineStages(this, (ClosedSystem) stage));
        }
        
        @Override
        public Pipeline.System andThen(Pipeline.StepSink<? super Out> stepSink) {
            return new ClosedSystem(combineStages(this, (ClosedSystem) stepSink));
        }
    }
    
    static final class ChainedStepSource<Out> extends ChainedSource<Out> implements Pipeline.StepSource<Out> {
        ChainedStepSource(List<Object> stages) {
            super(stages);
        }
        
        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        public Tunnel.GatedSource<Out> source() {
            return (Tunnel.GatedSource) stages.get(stages.size()-1);
        }
        
        @Override
        public Pipeline.System andThen(Pipeline.Sink<? super Out> sink) {
            return new ClosedSystem(combineStages(this, (ClosedSystem) sink));
        }
    }
    
    static final class ChainedStage<In, Out> extends ClosedSystem implements Pipeline.Stage<In, Out> {
        ChainedStage(List<Object> stages) {
            super(stages);
        }
        
        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        public Tunnel.GatedSink<In> sink() {
            return (Tunnel.GatedSink) stages.get(0);
        }
        
        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        public Tunnel.GatedSource<Out> source() {
            return (Tunnel.GatedSource) stages.get(stages.size()-1);
        }
        
        @Override
        public <T> Pipeline.Stage<In, T> andThen(Pipeline.Stage<? super Out, T> stage) {
            return new ChainedStage<>(combineStages(this, (ClosedSystem) stage));
        }
        
        @Override
        public Pipeline.StepSink<In> andThen(Pipeline.StepSink<? super Out> stepSink) {
            return new ChainedStepSink<>(combineStages(this, (ClosedSystem) stepSink));
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
        public Pipeline.StepSource<Out> compose(Pipeline.StepSource<? extends In> stepSource) {
            return new ChainedStepSource<>(combineStages((ClosedSystem) stepSource, this));
        }
        
        @Override
        public Pipeline.StepSource<Out> compose(Pipeline.Source<? extends In> source) {
            return new ChainedStepSource<>(combineStages((ClosedSystem) source, this));
        }
    }
    
    private static List<Object> combineStages(ClosedSystem a, ClosedSystem b) {
        // TODO: Avoid N^2 copying, possibly by:
        // TODO: Track "linked or consumed"
        List<Object> stages = new ArrayList<>(a.stages.size() + b.stages.size());
        stages.addAll(a.stages);
        stages.addAll(b.stages);
        return stages;
    }
    
    public static <T> Pipeline.Source<T> source(Tunnel.Source<T> source) {
        return new ChainedSource<>(List.of(source));
    }

    public static <T> Pipeline.StepSource<T> stepSource(Tunnel.GatedSource<T> source) {
        return new ChainedStepSource<>(List.of(source));
    }

    public static <T> Pipeline.Sink<T> sink(Tunnel.Sink<T> sink) {
        return new ChainedSink<>(List.of(sink));
    }

    public static <T> Pipeline.StepSink<T> stepSink(Tunnel.GatedSink<T> sink) {
        return new ChainedStepSink<>(List.of(sink));
    }

    public static <T, U> Pipeline.Stage<T, U> stage(Tunnel.FullGate<T, U> gate) {
        return new ChainedStage<>(List.of(gate));
    }
}
