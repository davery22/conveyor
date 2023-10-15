package io.avery.conveyor;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.StructuredTaskScope;
import java.util.function.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

class BeltsTest {
    
    // TODO: Remove
    public static void main(String[] args) throws Exception {
        testSynchronizeStepSource();
        testSynchronizeStepSink();
        testRecoverStep();
        testRecover();
        testFilterMap();
        testAlsoClose();
        testAlsoComplete();
        testSplit();
        testGroupBy();
        testFlatMap();
        testAdaptSourceOfSink();
        testAdaptSinkOfSource();
        testGather();
        
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
            List<Integer> list = new ArrayList<>();
            
            Belts.iteratorSource(List.of(1, 2, 3, 4, 5, 6).iterator())
                .andThen(Belts.filterMap(i -> i % 2 == 0 ? null : -i))
                .andThen((Belt.StepSink<Integer>) list::add)
                .run(Belts.scopeExecutor(scope));
            
            scope.join();
            
            assertEquals(List.of(-1, -3, -5), list);
        }
    }
    
    static void testAlsoClose() throws Exception {
        try (var scope = new StructuredTaskScope<>()) {
            List<Integer> list = new ArrayList<>();
            Iterator<Integer> iter = List.of(0, 1, 2).iterator();
            Belt.StepSource<Integer> source = new Belt.StepSource<Integer>() {
                @Override public Integer poll() { return iter.hasNext() ? iter.next() : null; }
                @Override public void close() { list.addAll(List.of(0, 0, 0)); }
            }.andThen(Belts.synchronizeStepSource());
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
                    "156000", "516000", "561000", "246000", "426000", "462000", "345000", "435000", "453000", "456000",
                    "423000", "243000", "234000", "513000", "153000", "135000", "612000", "162000", "126000", "123000"
                ),
                result
            );
        }
    }
    
    static void testAlsoComplete() throws Exception {
        try (var scope = new StructuredTaskScope<>()) {
            List<Integer> list = new ArrayList<>();
            Belt.StepSource<Integer> source = Belts.iteratorSource(List.of(0, 1, 2).iterator()).andThen(Belts.synchronizeStepSource());
            Belt.StepSink<Integer> sink = new Belt.StepSink<Integer>() {
                @Override public boolean offer(Integer input) { return list.add(input); }
                @Override public void complete() { list.addAll(List.of(0, 0, 0)); }
            }.compose(Belts.synchronizeStepSink());
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
                    "156000", "516000", "561000", "246000", "426000", "462000", "345000", "435000", "453000", "456000",
                    "423000", "243000", "234000", "513000", "153000", "135000", "612000", "162000", "126000", "123000"
                ),
                result
            );
        }
    }
    
    static void testSplit() throws Exception {
        try (var scope = new StructuredTaskScope<>()) {
           // In this example, the consecutive inner sinks offer to a shared sink, effectively "concatenating"
           List<Integer> list = new ArrayList<>();
           Belt.StepSink<Integer> sink = list::add;
           Belt.StepSink<Integer> noCompleteSink = sink::offer;
      
           Belts.iteratorSource(List.of(0, 1, 2, 3, 4, 5).iterator())
               .andThen(Belts
                   .split(
                       (Integer i) -> i % 2 == 0, // Splits a new sink when element is even
                       false, false,
                       Throwable::printStackTrace,
                       i -> noCompleteSink.compose(Belts.flatMap(j -> Belts.streamSource(Stream.of(j, i)),
                                                                 Throwable::printStackTrace))
                   )
                   .compose(Belts.alsoComplete(sink))
               )
               .run(Belts.scopeExecutor(scope));
      
           scope.join();
           assertEquals(List.of(0, 0, 1, 0, 2, 2, 3, 2, 4, 4, 5, 4), list);
       }
    }
    
    static void testGroupBy() throws Exception {
        try (var scope = new StructuredTaskScope<>()) {
            // In this example, the concurrent inner sinks offer to a shared sink, effectively "merging"
            List<String> list = new ArrayList<>();
            Belt.StepSink<String> sink = list::add;
            Belt.StepSink<String> noCompleteSink = sink::offer;
            
            Belts.iteratorSource(List.of("now", "or", "never").iterator())
                .andThen(Belts
                    .groupBy(
                        (String s) -> s.substring(0, 1),
                        false,
                        Throwable::printStackTrace,
                        (k, first) -> noCompleteSink.compose(Belts.flatMap(s -> Belts.streamSource(Stream.of(k, s, first)),
                                                                           Throwable::printStackTrace))
                    )
                    .compose(Belts.alsoComplete(sink))
                )
                .run(Belts.scopeExecutor(scope));
            
            scope.join();
            assertEquals(List.of("n", "now", "now", "o", "or", "or", "n", "never", "now"), list);
        }
    }
    
    static void testFlatMap() throws Exception {
        try (var scope = new StructuredTaskScope<>()) {
            List<String> list = new ArrayList<>();
            
            Belts.iteratorSource(List.of("red", "blue", "green").iterator())
                .andThen(Belts
                    .flatMap(
                        (String color) -> Belts.streamSource(Stream.of("color", color))
                            .andThen(Belts.buffer(256)),
                        Throwable::printStackTrace
                    )
                    .andThen((Belt.StepSink<String>) list::add)
                )
                .run(Belts.scopeExecutor(scope));
            
            scope.join();
            assertEquals(List.of("color", "red", "color", "blue", "color", "green"), list);
        }
    }
    
    static void testAdaptSourceOfSink() throws Exception {
        try (var scope = new StructuredTaskScope<>()) {
            List<Integer> list1 = new ArrayList<>();
            List<Integer> list2 = new ArrayList<>();
            
            Belts.iteratorSource(List.of(1, 2, 3).iterator())
                .andThen(Belts.synchronizeStepSource())
                .andThen(Belts.balance(List.of(
                    (Belt.StepSink<Integer>) list1::add,
                    ((Belt.StepSink<Integer>) list2::add)
                        .compose(Belts.adaptSourceOfSink(
                            Belts.filterMap(i -> 10 - i),
                            Throwable::printStackTrace
                        ))
                )))
                .run(Belts.scopeExecutor(scope));
            
            scope.join();
            
            String result = Stream.concat(list1.stream(), list2.stream()).map(String::valueOf).collect(Collectors.joining());
            assertIn(
                Set.of(
                    "187", "817", "871", "297", "927", "972", "398", "938", "983", "987",
                    "923", "293", "239", "813", "183", "138", "712", "172", "127", "123"
                ),
                result
            );
        }
    }
    
    static void testAdaptSinkOfSource() throws Exception {
        try (var scope = new StructuredTaskScope<>()) {
            List<Integer> list = new ArrayList<>();
            Belt.StepSink<Integer> sink = ((Belt.StepSink<Integer>) list::add).compose(Belts.synchronizeStepSink());
            
            Belts
                .merge(List.of(
                    Belts.streamSource(Stream.of(9)),
                    Belts.streamSource(Stream.of(2))
                        .andThen(Belts.adaptSinkOfSource(
                            Belts.flatMap(i -> Belts.streamSource(Stream.of(i, i+1, i+2)),
                                          Throwable::printStackTrace),
                            Throwable::printStackTrace
                        ))
                ))
                .andThen(sink)
                .run(Belts.scopeExecutor(scope));
            
            scope.join();
            
            String result = list.stream().map(String::valueOf).collect(Collectors.joining());
            assertIn(Set.of("9234", "2934", "2394", "2349"), result);
        }
    }
    
    static void testGather() throws Exception {
        try (var scope = new StructuredTaskScope<>()) {
            List<Integer> list = new ArrayList<>();
            
            Belts.streamSource(Stream.of(1, 2, 3))
                .andThen(
                    Belts.gather(map((Integer i) -> i * 2))
                        .andThen((Belt.StepSink<Integer>) list::add)
                )
                .run(Belts.scopeExecutor(scope));
            
            scope.join();
            assertEquals(List.of(2, 4, 6), list);
        }
    }
    
    static void testDelay() throws Exception {
        try (var scope = new StructuredTaskScope<>()) {
            List<String> list = new ArrayList<>();
            
            Belts.iteratorSource(List.of("World").iterator())
                .andThen(Belts
                             .flatMap(
                                 (String s) -> Belts.streamSource(Stream.of("Hello", s))
                                     .andThen(Belts.delay(i -> Instant.now().plusSeconds(1), 256)),
                                 Throwable::printStackTrace
                             )
                             .andThen((Belt.StepSink<String>) s -> {
                                 System.out.println(s);
                                 return true;
                             })
//                    .andThen((Belt.StepSink<String>) list::add)
                )
                .run(Belts.scopeExecutor(scope));
            
            scope.join();
            assertEquals(List.of(), list);
        }
    }
    
    static void testBalanceMerge2() throws Exception {
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
    
    static void testGroupBy2() throws Exception {
        try (var scope = new FailureHandlingScope(Throwable::printStackTrace)) {
            Belt.StepSegue<String, String> buffer = Belts.buffer(256);
            
            lineSource()
                .andThen(Belts
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
                                .andThen(buffer.sink()::offer)
                            )
                    )
                    .compose(Belts.alsoComplete(buffer.sink()))
                )
                .andThen(buffer.source())
                .andThen((Belt.Sink<String>) source -> { source.forEach(System.out::println); return true; })
                .run(Belts.scopeExecutor(scope));
            
            scope.join();
        }
    }
    
    static void testAdaptSinkOfSource2() throws Exception {
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
                    .andThen(Belts.throttle(
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