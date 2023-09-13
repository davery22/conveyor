package io.avery.pipeline;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

class ConduitsTest {
    
    // TODO: Remove
    public static void main(String[] args) throws Exception {
//        testMapAsyncVsMapBalanced();
//        testNostepVsBuffer();
//        testSpeed();
//        test1();
//        test2();
//        testBidi();

//        try (var in = new Scanner(System.in)) {
//            for (String line; in.hasNextLine() && !"stop".equalsIgnoreCase(line = in.nextLine()); ) {
//                System.out.println(line);
//            }
//        }
    }
    
    static void testGroupBy() throws Exception {
        try (var scope = new SlowFailScope()) {
//            Conduits.invert(lineSource(), sink -> Conduits.fuse(ConduitsTest.flatMap((String line) -> Stream.of(line.length())), sink))
            lineSource()
                .mapSource(Conduits.adaptSinkOfSource(Conduits.gather(ConduitsTest.flatMap((String line) -> Stream.of(line.length())))))
                .andThen(Conduits.stepSink(e -> { System.out.println(e); return true; }))
                .run(Conduits.scopedExecutor(scope));
            
            scope.join().throwIfFailed();
        }
    }
    
    static void testMapAsyncVsMapBalanced() throws Exception {
        var start = Instant.now();
        try (var scope = new SlowFailScope()) {
            long[] a = { 0 };
            var iter = Stream.iterate(0L, i -> i+1).limit(1_000_000).iterator();
            
            Conduits.stepSource(() -> iter.hasNext() ? iter.next() : null)
//                .andThen(Conduits.mapBalanceOrdered(
//                    i -> () -> i * 2,
//                    IntStream.range(0, 4).mapToObj(i -> Conduits.<Long>stepSink(e -> { a[0] += e; return true; })).toList()
//                ))
                .andThen(Conduits.mapAsyncOrdered(
                    4, 400,
                    i -> () -> i * 2
                ))
                .andThen(Conduits.stepSink(e -> { a[0] += e; return true; }))
                .run(Conduits.scopedExecutor(scope));
            
            scope.join().throwIfFailed();
            System.out.println(a[0]);
        }
        var end = Instant.now();
        System.out.println(Duration.between(start, end));
    }
    
    static void testNostepVsBuffer() throws Exception {
        var start = Instant.now();
        try (var scope = new SlowFailScope()) {
            long[] a = { 0 }, b = { 0 }, c = { 0 };
            var iter = Stream.iterate(0L, i -> i+1).limit(1_000_000).iterator();
            
            Conduits
                .stepSource(() -> iter.hasNext() ? iter.next() : null)
//                .<Long>source(sink -> {
//                    for (long i = 0; i < 1_000_000; i++) {
//                        if (!sink.offer(i)) {
//                            return false;
//                        }
//                    }
//                    return true;
//                })
//                .andThen(Conduits.stepBroadcast(List.of(
//                    Conduits.stepSink(e -> { a[0] += e+1; return true; }),
//                    Conduits.stepSink(e -> { b[0] += e+2; return true; }),
//                    Conduits.stepSink(e -> { c[0] += e+3; return true; })
//                )))
//                .andThen(Conduits.broadcast(List.of(
//                    Conduits.sink(source -> { source.forEach(e -> a[0] += e+1); return true; }),
//                    Conduits.sink(source -> { source.forEach(e -> b[0] += e+2); return true; }),
//                    Conduits.sink(source -> { source.forEach(e -> c[0] += e+3); return true; })
//                )))
                .andThen(Conduits.broadcast(List.of(
                    ConduitsTest.<Long>buffer(4).andThen(Conduits.sink(source -> { source.forEach(e -> a[0] += e+1); return true; })),
                    ConduitsTest.<Long>buffer(4).andThen(Conduits.sink(source -> { source.forEach(e -> b[0] += e+2); return true; })),
                    ConduitsTest.<Long>buffer(4).andThen(Conduits.sink(source -> { source.forEach(e -> c[0] += e+3); return true; }))
                )))
                .run(Conduits.scopedExecutor(scope));
            
            scope.join().throwIfFailed();
            System.out.println(a[0] + b[0] + c[0]);
        }
        var end = Instant.now();
        System.out.println(Duration.between(start, end));
    }
    
    static void testSpeed() throws Exception {
        var start = Instant.now();
        try (var scope = new SlowFailScope()) {
            long[] res = { 0 };
            
            Conduits
                .<Long>source(sink -> {
                    for (long i = 0; i < 1_000_000; i++) {
                        if (!sink.offer(i)) {
                            return false;
                        }
                    }
                    return true;
                })
                .andThen(Conduits.stepSink(e -> { res[0] += e; return true; }))
                .run(Conduits.scopedExecutor(scope));
            
            scope.join().throwIfFailed();
            System.out.println(res[0]);
        }
        var end = Instant.now();
        System.out.println(Duration.between(start, end));
    }
    
    static void test1() throws Exception {
        try (var scope = new SlowFailScope()) {
            lineSource()
                .andThen(Conduits.mapAsyncPartitioned(
                    10, 3, 15,
                    (String s) -> s.isEmpty() ? '*' : s.charAt(0),
                    (s, c) -> () -> c + ":" + s
                ))
                .andThen(Conduits.sink(source -> { source.forEach(System.out::println); return true; }))
                .run(Conduits.scopedExecutor(scope));
            
            scope.join().throwIfFailed();

//            lineSource()
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
        try (var scope = new SlowFailScope()) {
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
            
            lineSource()
                .andThen(Conduits
                    .gather(ConduitsTest.flatMap((String s) -> IntStream.range(0, 3).mapToObj(i -> s)))
                    .apply(Conduits.tokenBucket(
                        Duration.ofSeconds(1),
                        String::length,
                        10,
                        100
                    ))
                )
                .andThen(Conduits
                    .gather(ConduitsTest.flatMap((String s) -> Stream.of(s+"22")))
                    .apply(ConduitsTest.buffer(16))
                )
                .andThen(Conduits.sink(source -> { source.forEach(System.out::println); return true; }))
                .run(Conduits.scopedExecutor(scope));
            
            scope.join().throwIfFailed();
        }
    }
    
    static void testBidi() throws InterruptedException, ExecutionException {
        try (var scope = new SlowFailScope()) {
            var buffer = Conduits.extrapolate(0, e -> Collections.emptyIterator(), 256);
            var iter = Stream.generate(new Scanner(System.in)::nextLine)
                .takeWhile(line -> !"stop".equalsIgnoreCase(line))
                .iterator();
            var probe = Conduits.stepSource(() -> iter.hasNext() ? iter.next().length() : null);
            
            Conduits
                .zip(buffer.source(), probe, Integer::sum)
                .andThen(Conduits.broadcast(List.of(
                    buffer.sink(),
                    Conduits.stepSink(e -> { System.out.println(e); return true; })
                )))
                .run(Conduits.scopedExecutor(scope));
            
            scope.join().throwIfFailed();
        }
    }
    
    private static Conduit.StepSource<String> lineSource() {
        var iter = Stream.generate(new Scanner(System.in)::nextLine)
            .takeWhile(line -> !"stop".equalsIgnoreCase(line))
            .iterator();
        return Conduits.stepSource(() -> iter.hasNext() ? iter.next() : null);
    }
    
    private static <T> Conduit.StepSegue<T, T> buffer(int bufferLimit) {
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