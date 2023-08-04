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
        public void runInternals(Consumer<Callable<?>> fork) {
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
    
    static final class ChainedHead<In> extends ClosedSystem implements Pipeline.Head<In> {
        ChainedHead(List<Object> stages) {
            super(stages);
        }
        
        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        public Tunnel.GatedSink<In> sink() {
            return (Tunnel.GatedSink) stages.get(0);
        }
    }
    
    static final class ChainedTail<Out> extends ClosedSystem implements Pipeline.Tail<Out> {
        ChainedTail(List<Object> stages) {
            super(stages);
        }
        
        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        public Tunnel.GatedSource<Out> source() {
            return (Tunnel.GatedSource) stages.get(stages.size()-1);
        }
        
        @Override
        public <T> Pipeline.Tail<T> connect(Pipeline.Body<? super Out, T> body) {
            return new ChainedTail<>(combineStages(this, (ClosedSystem) body));
        }
        
        @Override
        public Pipeline.System connect(Pipeline.Head<? super Out> head) {
            return new ClosedSystem(combineStages(this, (ClosedSystem) head));
        }
    }
    
    static final class ChainedBody<In, Out> extends ClosedSystem implements Pipeline.Body<In, Out> {
        ChainedBody(List<Object> stages) {
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
        public <T> Pipeline.Body<In, T> connect(Pipeline.Body<? super Out, T> body) {
            return new ChainedBody<>(combineStages(this, (ClosedSystem) body));
        }
        
        @Override
        public Pipeline.Head<In> connect(Pipeline.Head<? super Out> head) {
            return new ChainedHead<>(combineStages(this, (ClosedSystem) head));
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
    
    public static <T, U> Pipeline.Tail<U> tail(Tunnel.Source<T> source, Tunnel.FullGate<? super T, U> gate) {
        return new ChainedTail<>(List.of(source, gate));
    }

    public static <T> Pipeline.Tail<T> tail(Tunnel.GatedSource<T> source) {
        return new ChainedTail<>(List.of(source));
    }

    public static <T, U> Pipeline.Head<T> head(Tunnel.FullGate<T, U> gate, Tunnel.Sink<? super U> sink) {
        return new ChainedHead<>(List.of(gate, sink));
    }

    public static <T> Pipeline.Head<T> head(Tunnel.GatedSink<T> sink) {
        return new ChainedHead<>(List.of(sink));
    }

    public static <T, U> Pipeline.Body<T, U> body(Tunnel.FullGate<T, U> gate) {
        return new ChainedBody<>(List.of(gate));
    }
}
