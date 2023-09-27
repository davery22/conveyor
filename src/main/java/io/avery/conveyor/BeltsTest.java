package io.avery.conveyor;

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

class BeltsTest {
    
    // TODO: Remove
    public static void main(String[] args) throws Exception {
        testGroupBy();
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
        try (var scope = new FailureHandlingScope(Throwable::printStackTrace)) {
            var buffer = Belts.buffer(16);
            var noCompleteBuffer = Belts.stepSink(buffer.sink()::offer);
            
            lineSource()
                .andThen(Belts
                    .groupBy(
                        (String line) -> {
                            if (line.isEmpty()) return '*';
                            if ("HALT".equals(line)) throw new IllegalStateException("HALTED!");
                            return line.charAt(0);
                        },
                        true,
                        t -> { },
                        (k, v) -> Belts
                            .flatMap(e -> Belts.source(Stream.of(k, e, v)), t -> { })
                            .andThen(Belts.buffer(16))
                            .andThen(Belts
                                .gather(flatMap(e -> {
                                    if ("CEASE".equals(e)) throw new IllegalStateException("CEASED!");
                                    return Stream.of(e);
                                }))
                                .andThen(noCompleteBuffer)
                            )
                    )
                    .compose(Belts.alsoComplete(buffer.sink()))
                )
                .andThen(buffer.source())
                .andThen(Belts.sink(source -> { source.forEach(System.out::println); return true; }))
                .run(Belts.scopeExecutor(scope));
            
            scope.join();
        }
    }
    
    static void testAdaptSinkOfSource() throws Exception {
        try (var scope = new FailureHandlingScope(Throwable::printStackTrace)) {
            lineSource()
                .andThen(Belts.adaptSinkOfSource(Belts.gather(BeltsTest.flatMap((String line) -> Stream.of(line.length()))),
                                                    t -> { }))
                .andThen(Belts.stepSink(e -> { System.out.println(e); return true; }))
                .run(Belts.scopeExecutor(scope));
            
            scope.join();
        }
    }
    
    static void testMapAsyncVsMapBalanced() throws Exception {
        var start = Instant.now();
        try (var scope = new FailureHandlingScope(Throwable::printStackTrace)) {
            long[] a = { 0 };
            var iter = Stream.iterate(0L, i -> i+1).limit(1_000_000).iterator();
            
            Belts.stepSource(() -> iter.hasNext() ? iter.next() : null)
                .andThen(Belts.mapBalanceOrdered(
                    4, 400,
                    i -> () -> i * 2
                ))
                .andThen(Belts.stepSink(e -> { a[0] += e; return true; }))
                .run(Belts.scopeExecutor(scope));
            
            scope.join();
            System.out.println(a[0]);
        }
        var end = Instant.now();
        System.out.println(Duration.between(start, end));
    }
    
    static void testNostepVsBuffer() throws Exception {
        var start = Instant.now();
        try (var scope = new FailureHandlingScope(Throwable::printStackTrace)) {
            long[] a = { 0 }, b = { 0 }, c = { 0 };
            var iter = Stream.iterate(0L, i -> i+1).limit(1_000_000).iterator();
            
            Belts
                .stepSource(() -> iter.hasNext() ? iter.next() : null)
//                .<Long>source(sink -> {
//                    for (long i = 0; i < 1_000_000; i++) {
//                        if (!sink.offer(i)) {
//                            return false;
//                        }
//                    }
//                    return true;
//                })
//                .andThen(Belts.stepBroadcast(List.of(
//                    Belts.stepSink(e -> { a[0] += e+1; return true; }),
//                    Belts.stepSink(e -> { b[0] += e+2; return true; }),
//                    Belts.stepSink(e -> { c[0] += e+3; return true; })
//                )))
//                .andThen(Belts.broadcast(List.of(
//                    Belts.sink(source -> { source.forEach(e -> a[0] += e+1); return true; }),
//                    Belts.sink(source -> { source.forEach(e -> b[0] += e+2); return true; }),
//                    Belts.sink(source -> { source.forEach(e -> c[0] += e+3); return true; })
//                )))
                .andThen(Belts.broadcast(List.of(
                    Belts.<Long>buffer(4).andThen(Belts.sink(source -> { source.forEach(e -> a[0] += e+1); return true; })),
                    Belts.<Long>buffer(4).andThen(Belts.sink(source -> { source.forEach(e -> b[0] += e+2); return true; })),
                    Belts.<Long>buffer(4).andThen(Belts.sink(source -> { source.forEach(e -> c[0] += e+3); return true; }))
                )))
                .run(Belts.scopeExecutor(scope));
            
            scope.join();
            System.out.println(a[0] + b[0] + c[0]);
        }
        var end = Instant.now();
        System.out.println(Duration.between(start, end));
    }
    
    static void testSpeed() throws Exception {
        var start = Instant.now();
        try (var scope = new FailureHandlingScope(Throwable::printStackTrace)) {
            long[] res = { 0 };
            
            Belts
                .<Long>source(sink -> {
                    for (long i = 0; i < 1_000_000; i++) {
                        if (!sink.offer(i)) {
                            return false;
                        }
                    }
                    return true;
                })
                .andThen(Belts.stepSink(e -> { res[0] += e; return true; }))
                .run(Belts.scopeExecutor(scope));
            
            scope.join();
            System.out.println(res[0]);
        }
        var end = Instant.now();
        System.out.println(Duration.between(start, end));
    }
    
    static void test1() throws Exception {
        try (var scope = new FailureHandlingScope(Throwable::printStackTrace)) {
            lineSource()
                .andThen(Belts.mapBalancePartitioned(
                    10, 3, 15,
                    (String s) -> s.isEmpty() ? '*' : s.charAt(0),
                    (s, c) -> () -> c + ":" + s
                ))
                .andThen(Belts.sink(source -> { source.forEach(System.out::println); return true; }))
                .run(Belts.scopeExecutor(scope));
            
            scope.join();

//            lineSource()
//                .andThen(Belts.buffer(4))
//                .andThen(Belts.balance(
//                    IntStream.range(0, 4).mapToObj(i -> Belts.stepFuse(
//                        BeltsTest.flatMap((String s) -> Stream.of(s.repeat(i+1))),
//                        buffer
//                    )).toList()
//                ))
        }
    }
    
    static void test2() throws Exception {
        try (var scope = new FailureHandlingScope(Throwable::printStackTrace)) {
//            var segue = Belts.stepFuse(
//                BeltsTest.flatMap((String s) -> IntStream.range(0, 3).mapToObj(i -> s)),
//                Belts.tokenBucket(
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
                .andThen(Belts
                    .gather(BeltsTest.flatMap((String s) -> IntStream.range(0, 3).mapToObj(i -> s)))
                    .andThen(Belts.tokenBucket(
                        Duration.ofSeconds(1),
                        String::length,
                        10,
                        100
                    ))
                )
                .andThen(Belts
                    .gather(BeltsTest.flatMap((String s) -> Stream.of(s+"22")))
                    .andThen(Belts.buffer(16))
                )
                .andThen(Belts.sink(source -> { source.forEach(System.out::println); return true; }))
                .run(Belts.scopeExecutor(scope));
            
            scope.join();
        }
    }
    
    static void testBidi() throws InterruptedException, ExecutionException {
        try (var scope = new FailureHandlingScope(Throwable::printStackTrace)) {
            var buffer = Belts.extrapolate(0, e -> Collections.emptyIterator(), 256);
            var iter = Stream.generate(new Scanner(System.in)::nextLine)
                .takeWhile(line -> !"stop".equalsIgnoreCase(line))
                .iterator();
            var probe = Belts.stepSource(() -> iter.hasNext() ? iter.next().length() : null);
            
            Belts
                .zip(buffer.source(), probe, Integer::sum)
                .andThen(Belts.broadcast(List.of(
                    buffer.sink(),
                    Belts.stepSink(e -> { System.out.println(e); return true; })
                )))
                .run(Belts.scopeExecutor(scope));
            
            scope.join();
        }
    }
    
    private static Belt.StepSource<String> lineSource() {
        var iter = Stream.generate(new Scanner(System.in)::nextLine)
            .takeWhile(line -> !"stop".equalsIgnoreCase(line))
            .iterator();
//        return Belts.stepSource(() -> iter.hasNext() ? iter.next() : null);
        return Belts.stepSource(() -> {
            if (iter.hasNext()) return iter.next();
            throw new IllegalStateException("TRIP");
        });
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