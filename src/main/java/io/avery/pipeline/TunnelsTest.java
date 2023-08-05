package io.avery.pipeline;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.StructuredTaskScope;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

class TunnelsTest {
    // TODO: Remove
    public static void main(String[] args) throws Exception {
        test2();
    }
    
    public static void test2() throws Exception {
        try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
            var buffer = TunnelsTest.buffer(10);
            
            Pipelines
                .<String>source(sink -> {
                    try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
                        for (String line; !"stop".equalsIgnoreCase(line = reader.readLine()); ) {
                            sink.offer(line);
                        }
                    }
                })
                .andThen(TunnelsTest.<String>buffer(4).pipeline())
                .andThen(Tunnels.balance(
                    IntStream.range(0, 4).mapToObj(i -> Tunnels.gatedFuse(
                        TunnelsTest.flatMap((String s) -> Stream.of(s.repeat(i+1))),
                        buffer
                    )).toList()
                ).pipeline())
                .run(scope::fork);
            
            Pipelines
                .stepSource(buffer)
                .andThen(Pipelines.sink(source -> source.forEach(System.out::println)))
                .run(scope::fork);
            
//            scope.fork(() -> {
//                try (buffer) {
//                    buffer.forEach(System.out::println);
//                }
//                return null;
//            });
            
            scope.join().throwIfFailed();
        }
    }
    
    public static void test1() throws Exception {
        try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
            var stage = Tunnels.gatedFuse(
                flatMap((String s) -> IntStream.range(0, 3).mapToObj(i -> s)),
                Tunnels.tokenBucket(
                    Duration.ofSeconds(1),
                    String::length,
                    10,
                    100
                )
            );
            
            // Producer
            scope.fork(() -> {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
                    for (String line; !"stop".equalsIgnoreCase(line = reader.readLine()); ) {
                        stage.offer(line);
                    }
                    stage.complete(null);
                } catch (Throwable error) {
                    stage.complete(error);
                }
                return null;
            });
            
            // Consumer
            scope.fork(() -> {
                try (stage) {
                    stage.forEach(System.out::println);
                }
                return null;
            });
            
            scope.join().throwIfFailed();
        }
    }
    
    private static <T> Tunnel.FullGate<T, T> buffer(int bufferLimit) {
        return Tunnels.extrapolate(e -> Collections.emptyIterator(), bufferLimit);
    }
    
    private static <T, R> Gatherer<T, ?, R> flatMap(Function<? super T, ? extends Stream<? extends R>> mapper) {
        return new Gatherer<T, Void, R>() {
            @Override
            public Supplier<Void> supplier() {
                return () -> (Void) null;
            }
            
            @Override
            public Integrator<Void, T, R> integrator() {
                return (state, element, downstream) -> {
                    try (var s = mapper.apply(element)) {
                        return s == null || s.sequential().allMatch(downstream::flush);
                    }
                };
            }
            
            @Override
            public BinaryOperator<Void> combiner() {
                return (l, r) -> l;
            }
            
            @Override
            public BiConsumer<Void, Sink<? super R>> finisher() {
                return (state, downstream) -> {};
            }
            
            @Override
            public Set<Characteristics> characteristics() {
                return Set.of(Characteristics.GREEDY, Characteristics.STATELESS);
            }
        };
    }
}