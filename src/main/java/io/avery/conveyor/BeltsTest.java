package io.avery.conveyor;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

class BeltsTest {
    
    // TODO: Remove
    public static void main(String[] args) throws Exception {
        testRecoverStep();
        testRecover();
//        testSynchronizeStepSource();
//        testSynchronizeStepSink();
//        testGroupBy();
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
    
    static void testSynchronizeStepSource() throws Exception {
        try (var scope = new StructuredTaskScope<>()) {
            List<Integer> list = new ArrayList<>();
            Belt.StepSource<Integer> source = Belts.iteratorSource(List.of(0, 1, 2).iterator()).andThen(Belts.synchronizeStepSource());
            Belt.StepSink<Integer> sink = ((Belt.StepSink<Integer>) list::add).compose(Belts.synchronizeStepSink());
            Belt.StepSource<Integer> noCloseSource = source::poll;
            
            Belts
                .merge(List.of(
                    // These 2 sources will concurrently poll from the same upstream, effectively "balancing"
                    noCloseSource.andThen(Belts.filterMap(i -> i + 1)),
                    noCloseSource.andThen(Belts.filterMap(i -> i + 4))
                ))
                .andThen(Belts.alsoClose(source))
                .andThen(sink)
                .run(Belts.scopeExecutor(scope));
            
            scope.join();
            
            String result = list.stream().map(String::valueOf).collect(Collectors.joining());
            assertIn(
                Set.of(
                    "156", "516", "561", "246", "426", "462", "345", "435", "453", "456",
                    "423", "243", "234", "513", "153", "135", "612", "162", "126", "123"
                ),
                result
            );
        }
    }
    
    static void testSynchronizeStepSink() throws Exception {
        try (var scope = new StructuredTaskScope<>()) {
            List<Integer> list = new ArrayList<>();
            Belt.StepSource<Integer> source = Belts.iteratorSource(List.of(0, 1, 2).iterator()).andThen(Belts.synchronizeStepSource());
            Belt.StepSink<Integer> sink = ((Belt.StepSink<Integer>) list::add).compose(Belts.synchronizeStepSink());
            Belt.StepSink<Integer> noCompleteSink = sink::offer;
            
            Belts
                .balance(List.of(
                    // These 2 sinks will concurrently offer to the same downstream, effectively "merging"
                    noCompleteSink.compose(Belts.gather(map((Integer i) -> i + 1))),
                    noCompleteSink.compose(Belts.gather(map((Integer i) -> i + 4)))
                ))
                .compose(Belts.alsoComplete(sink))
                .compose(source)
                .run(Belts.scopeExecutor(scope));
            
            scope.join();
            
            String result = list.stream().map(String::valueOf).collect(Collectors.joining());
            assertIn(
                Set.of(
                    "156", "516", "561", "246", "426", "462", "345", "435", "453", "456",
                    "423", "243", "234", "513", "153", "135", "612", "162", "126", "123"
                ),
                result
            );
        }
    }
    
    static void testRecoverStep() throws Exception {
        try (var scope = new StructuredTaskScope<>()) {
            List<Integer> list = new ArrayList<>();
            Belt.Source<Integer> source = Belts.streamSource(
                Stream.iterate(1, i -> {
                    if (i < 3) {
                        return i + 1;
                    }
                    throw new IllegalStateException();
                })
            );
            
            source
                .andThen(Belts
                    .recoverStep(
                        cause -> Belts.streamSource(Stream.of(7, 8, 9)),
                        Throwable::printStackTrace
                    )
                    .andThen((Belt.StepSink<Integer>) list::add)
                )
                .run(Belts.scopeExecutor(scope));
                
            scope.join();
            
            assertEquals(List.of(1, 2, 3, 7, 8, 9), list);
        }
    }
    
    static void testRecover() throws Exception {
        try (var scope = new StructuredTaskScope<>()) {
            List<Integer> list = new ArrayList<>();
            Iterator<Integer> iter = List.of(1, 2, 3).iterator();
            Belt.StepSource<Integer> source = () -> {
                if (iter.hasNext()) {
                    return iter.next();
                }
                throw new IllegalStateException();
            };
            
            source
                .andThen(Belts
                    .recover(
                        cause -> Belts.iteratorSource(List.of(7, 8, 9).iterator()),
                        Throwable::printStackTrace
                    )
                    .andThen((Belt.Sink<Integer>) src -> {
                        src.forEach(list::add);
                        return true;
                    })
                )
                .run(Belts.scopeExecutor(scope));
            
            scope.join();
            
            assertEquals(List.of(1, 2, 3, 7, 8, 9), list);
        }
    }
    
    static void testFilterMap() throws Exception {
        try (var scope = new StructuredTaskScope<>()) {
            List<String> list = new ArrayList<>();
            scope.join();
        }
    }
    
    static void testBalanceMerge() throws Exception {
        try (var scope = new FailureHandlingScope(Throwable::printStackTrace)) {
            lineSource()
                .andThen(Belts.filterMap(s -> (Callable<String>) () -> s))
                .andThen(Belts.<String>balanceMergeSink(4)
                    .andThen(Belts.buffer(16))
                )
                .run(Belts.scopeExecutor(scope));
            scope.join();
        }
    }
    
    static void testGroupBy() throws Exception {
        try (var scope = new FailureHandlingScope(Throwable::printStackTrace)) {
            lineSource()
                .andThen(
                    ((Belt.SinkToStepOperator<String, String>) buffer -> Belts
                        .groupBy(
                            (String line) -> {
                                if (line.isEmpty()) return "*";
                                if ("HALT".equals(line)) throw new IllegalStateException("HALTED!");
                                return String.valueOf(line.charAt(0));
                            },
                            true,
                            t -> { },
                            (k, v) -> Belts
                                .flatMap((String e) -> Belts.streamSource(Stream.of(k, e, v)), t -> { })
                                .andThen(Belts.buffer(16))
                                .andThen(Belts
                                    .gather(flatMap((String e) -> {
                                        if ("CEASE".equals(e)) throw new IllegalStateException("CEASED!");
                                        return Stream.of(e);
                                    }))
                                    .andThen(buffer::offer)
                                )
                        )
                        .compose(Belts.alsoComplete(buffer))
                    )
                    .andThen(Belts.buffer(16))
                )
                .andThen((Belt.Sink<String>) source -> { source.forEach(System.out::println); return true; })
                .run(Belts.scopeExecutor(scope));
            
            scope.join();
        }
    }
    
    static void testAdaptSinkOfSource() throws Exception {
        try (var scope = new FailureHandlingScope(Throwable::printStackTrace)) {
            lineSource()
                .andThen(Belts.adaptSinkOfSource(
                    Belts.gather(BeltsTest.flatMap((String line) -> Stream.of(line.length()))),
                    t -> { }
                ))
                .andThen((Belt.StepSink<Integer>) e -> { System.out.println(e); return true; })
                .run(Belts.scopeExecutor(scope));
            
            scope.join();
        }
    }
    
    static void testMapAsyncVsMapBalanced() throws Exception {
        var start = Instant.now();
        try (var scope = new FailureHandlingScope(Throwable::printStackTrace)) {
            long[] a = { 0 };
            var iter = Stream.iterate(0L, i -> i+1).limit(1_000_000).iterator();
            
            ((Belt.StepSource<Long>) () -> iter.hasNext() ? iter.next() : null)
                .andThen(Belts.mapBalanceOrdered(
                    4, 400,
                    i -> () -> i * 2
                ))
                .andThen((Belt.StepSink<Long>) e -> { a[0] += e; return true; })
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
            
            ((Belt.StepSource<Long>) () -> iter.hasNext() ? iter.next() : null)
//                ((Belt.Source<Long>) sink -> {
//                    for (long i = 0; i < 1_000_000; i++) {
//                        if (!sink.offer(i)) {
//                            return false;
//                        }
//                    }
//                    return true;
//                })
//                .andThen(Belts.stepBroadcast(List.of(
//                    (Belt.StepSink<Long>) e -> { a[0] += e+1; return true; },
//                    (Belt.StepSink<Long>) e -> { b[0] += e+2; return true; },
//                    (Belt.StepSink<Long>) e -> { c[0] += e+3; return true; }
//                )))
//                .andThen(Belts.broadcast(List.of(
//                    (Belt.Sink<Long>) source -> { source.forEach(e -> a[0] += e+1); return true; },
//                    (Belt.Sink<Long>) source -> { source.forEach(e -> b[0] += e+2); return true; },
//                    (Belt.Sink<Long>) source -> { source.forEach(e -> c[0] += e+3); return true; }
//                )))
                .andThen(Belts.broadcast(List.of(
                    Belts.<Long>buffer(4).andThen((Belt.Sink<Long>) source -> { source.forEach(e -> a[0] += e+1); return true; }),
                    Belts.<Long>buffer(4).andThen((Belt.Sink<Long>) source -> { source.forEach(e -> b[0] += e+2); return true; }),
                    Belts.<Long>buffer(4).andThen((Belt.Sink<Long>) source -> { source.forEach(e -> c[0] += e+3); return true; })
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
            
            ((Belt.Source<Long>)
                sink -> {
                    for (long i = 0; i < 1_000_000; i++) {
                        if (!sink.offer(i)) {
                            return false;
                        }
                    }
                    return true;
                })
                .andThen((Belt.StepSink<Long>) e -> { res[0] += e; return true; })
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
                .andThen((Belt.Sink<String>) source -> { source.forEach(System.out::println); return true; })
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
                .andThen((Belt.Sink<String>) source -> { source.forEach(System.out::println); return true; })
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
            Belt.StepSource<Integer> probe = () -> iter.hasNext() ? iter.next().length() : null;
            
            Belts
                .zip(buffer.source(), probe, Integer::sum)
                .andThen(Belts.broadcast(List.of(
                    buffer.sink(),
                    e -> { System.out.println(e); return true; })
                ))
                .run(Belts.scopeExecutor(scope));
            
            scope.join();
        }
    }
    
    private static Belt.StepSource<String> lineSource() {
        var iter = Stream.generate(new Scanner(System.in)::nextLine)
            .takeWhile(line -> !"stop".equalsIgnoreCase(line))
            .iterator();
//        return () -> iter.hasNext() ? iter.next() : null;
        return () -> {
            if (iter.hasNext()) return iter.next();
            throw new IllegalStateException("TRIP");
        };
    }
    
    private static <T, R> Gatherer<T, ?, R> flatMap(Function<? super T, ? extends Stream<? extends R>> mapper) {
        return new Gatherer<T, Void, R>() {
            @Override
            public Supplier<Void> initializer() {
                return () -> (Void) null;
            }
            
            @Override
            public Integrator<Void, T, R> integrator() {
                return (state, element, downstream) -> {
                    try (var s = mapper.apply(element)) {
                        return s == null || s.sequential().allMatch(downstream::push);
                    }
                };
            }
            
            @Override
            public BinaryOperator<Void> combiner() {
                return (l, r) -> l;
            }
            
            @Override
            public BiConsumer<Void, Downstream<R>> finisher() {
                return (state, downstream) -> {};
            }
            
            @Override
            public Set<Characteristics> characteristics() {
                return Set.of(Characteristics.GREEDY, Characteristics.STATELESS);
            }
        };
    }
    
    private static <T, U> Gatherer<T, ?, U> map(Function<? super T, ? extends U> mapper) {
        return new Gatherer<T, Void, U>() {
            @Override
            public Supplier<Void> initializer() {
                return () -> (Void) null;
            }
            
            @Override
            public Integrator<Void, T, U> integrator() {
                return (state, element, downstream) -> downstream.push(mapper.apply(element));
            }
            
            @Override
            public BinaryOperator<Void> combiner() {
                return (l, r) -> l;
            }
            
            @Override
            public BiConsumer<Void, Downstream<U>> finisher() {
                return (state, downstream) -> {};
            }
            
            @Override
            public Set<Characteristics> characteristics() {
                return Set.of(Characteristics.GREEDY, Characteristics.STATELESS);
            }
        };
    }
    
    public static void assertIn(Set<?> expected, Object actual) {
        if (!expected.contains(actual)) {
            throw new AssertionError("Expected one of <" + expected + "> but was <" + actual + ">");
        }
    }
    
    public static void assertEquals(Object expected, Object actual) {
        if (!Objects.equals(expected, actual)) {
            throw new AssertionError("Expected <" + expected + "> but was <" + actual + ">");
        }
    }
}