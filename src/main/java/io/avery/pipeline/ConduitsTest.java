package io.avery.pipeline;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.StructuredTaskScope;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

class ConduitsTest {
    // TODO: Remove
    public static void main(String[] args) throws Exception {
        test1();
//        test2();
//        testBidi();

//        try (var in = new Scanner(System.in)) {
//            for (String line; in.hasNextLine() && !"stop".equalsIgnoreCase(line = in.nextLine()); ) {
//                System.out.println(line);
//            }
//        }
    }
    
    static void test1() throws Exception {
        try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
            var buffer = ConduitsTest.buffer(10);
            var lines = Stream.generate(new Scanner(System.in)::nextLine)
                .takeWhile(line -> !"stop".equalsIgnoreCase(line))
                .iterator();
            
            Conduits
                .stepSource(() -> lines.hasNext() ? lines.next() : null)
                .andThen(Conduits.mapAsyncPartitioned(
                    10, 3, 15,
                    (String s) -> s.isEmpty() ? '*' : s.charAt(0),
                    (s, c) -> () -> c + ":" + s,
                    buffer
                ))
                .run(Conduits.drainToCompletion(scope::fork));
            buffer
                .andThen(Conduits.sink(source -> { source.forEach(System.out::println); return true; }))
                .run(Conduits.drainToCompletion(scope::fork));
            
            scope.join().throwIfFailed();

//            Conduits
//                .<String>source(sink -> {
//                    try (var in = new Scanner(System.in)) {
//                        for (String line; in.hasNextLine() && !"stop".equalsIgnoreCase(line = in.nextLine()); ) {
//                            if (!sink.offer(line)) {
//                                return false;
//                            }
//                        }
//                        return true;
//                    }
//                })
//                .andThen(ConduitsTest.buffer(4))
//                .andThen(Conduits.balance(
//                    IntStream.range(0, 4).mapToObj(i -> Conduits.stepFuse(
//                        ConduitsTest.flatMap((String s) -> Stream.of(s.repeat(i+1))),
//                        buffer
//                    )).toList()
//                ))
        }
    }
    
    static void test2() throws Exception {
        try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
//            var segue = Conduits.stepFuse(
//                ConduitsTest.flatMap((String s) -> IntStream.range(0, 3).mapToObj(i -> s)),
//                Conduits.tokenBucket(
//                    Duration.ofSeconds(1),
//                    String::length,
//                    10,
//                    100
//                )
//            );
//
//            // Producer
//            scope.fork(() -> {
//                try (var in = new Scanner(System.in)) {
//                    for (String line; in.hasNextLine() && !"stop".equalsIgnoreCase(line = in.nextLine()) && segue.offer(line); ) { }
//                    segue.complete(null);
//                } catch (Throwable error) {
//                    segue.complete(error);
//                }
//                return null;
//            });
//
//            // Consumer
//            scope.fork(() -> {
//                try (segue) {
//                    segue.forEach(System.out::println);
//                }
//                return null;
//            });
            
            Conduits
                .<String>source(sink -> {
                    try (var in = new Scanner(System.in)) {
                        for (String line; in.hasNextLine() && !"stop".equalsIgnoreCase(line = in.nextLine()); ) {
                            if (!sink.offer(line)) {
                                return false;
                            }
                        }
                        return true;
                    }
                })
                .andThen(Conduits.stepFuse(
                    ConduitsTest.flatMap((String s) -> IntStream.range(0, 3).mapToObj(i -> s)),
                    Conduits.tokenBucket(
                        Duration.ofSeconds(1),
                        String::length,
                        10,
                        100
                    )
                ))
                .andThen(Conduits.fuse(
                    ConduitsTest.flatMap((String s) -> Stream.of(s+"22")),
                    16,
                    source -> { source.forEach(System.out::println); return true; }
                ))
                .run(Conduits.drainToCompletion(scope::fork));
            
            scope.join().throwIfFailed();
        }
    }
    
    static void testBidi() throws InterruptedException, ExecutionException {
        try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
            var buffer = Conduits.extrapolate(0, e -> Collections.emptyIterator(), 256);
            var probe = Conduits.<Integer>source(sink -> {
                try (var in = new Scanner(System.in)) {
                    for (String line; in.hasNextLine() && !"stop".equalsIgnoreCase(line = in.nextLine()); ) {
                        if (!sink.offer(line.length())) {
                            return false;
                        }
                    }
                    return true;
                }
            });
            Conduits
                .zip(buffer, probe, Integer::sum)
                .andThen(Conduits.stepBroadcast(List.of(
                    buffer,
                    Conduits.stepSink(e -> { System.out.println(e); return true; })
                )))
                .run(Conduits.drainToCompletion(scope::fork));
            scope.join().throwIfFailed();
        }
    }
    
    private static <T> Conduit.Segue<T, T> buffer(int bufferLimit) {
        return Conduits.extrapolate(null, e -> Collections.emptyIterator(), bufferLimit);
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