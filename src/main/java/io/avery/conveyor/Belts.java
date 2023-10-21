package io.avery.conveyor;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.*;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * Implementations of various useful {@link Belt.Stage Stages}, {@link Belt.Segue Segues}, and {@code Operators}.
 *
 * <p>In addition to any documented contracts, implementations here strictly follow structured concurrency - threads
 * are spawned within lexical scopes that await their completion before exiting.
 */
public class Belts {
    private Belts() {} // Utility
    
    static final Throwable NULL_EXCEPTION = new Throwable();
    
    /**
     * Returns an operator that synchronizes access to an upstream source's {@code poll} and {@code close} methods. The
     * resultant downstream source can be safely polled concurrently, making it suitable for ad-hoc "balancing" use
     * cases.
     *
     * <p>Example:
     * {@snippet :
     * try (var scope = new StructuredTaskScope<>()) {
     *     List<Integer> list = new ArrayList<>();
     *     Belt.StepSource<Integer> source = Belts.iteratorSource(List.of(0, 1, 2).iterator()).andThen(Belts.synchronizeStepSource());
     *     Belt.StepSink<Integer> sink = ((Belt.StepSink<Integer>) list::add).compose(Belts.synchronizeStepSink());
     *     Belt.StepSource<Integer> noCloseSource = source::poll;
     *
     *     Belts
     *         .merge(List.of(
     *             // These 2 sources will concurrently poll from the same upstream, effectively "balancing"
     *             noCloseSource.andThen(Belts.filterMap(i -> i + 1)),
     *             noCloseSource.andThen(Belts.filterMap(i -> i + 4))
     *         ))
     *         .andThen(Belts.alsoClose(source))
     *         .andThen(sink)
     *         .run(Belts.scopeExecutor(scope));
     *
     *     scope.join();
     *
     *     String result = list.stream().map(String::valueOf).collect(Collectors.joining());
     *     System.out.println(result);
     *     // Possible outputs:
     *     // 156; 516; 561; 246; 426; 462; 345; 435; 453; 456;
     *     // 423; 243; 234; 513; 153; 135; 612; 162; 126; 123;
     * }
     * }
     *
     * @return an operator that synchronizes access to an upstream source
     * @param <T> the source element type
     */
    public static <T> Belt.StepSourceOperator<T, T> synchronizeStepSource() {
        class SynchronizedStepSource implements Belt.StepSource<T> {
            final Belt.StepSource<? extends T> source;
            final ReentrantLock lock = new ReentrantLock();
            
            SynchronizedStepSource(Belt.StepSource<? extends T> source) {
                this.source = Objects.requireNonNull(source);
            }
            
            @Override
            public T poll() throws Exception {
                lock.lockInterruptibly();
                try {
                    return source.poll();
                } finally {
                    lock.unlock();
                }
            }
            
            @Override
            public void close() throws Exception {
                lock.lock();
                try {
                    source.close();
                } finally {
                    lock.unlock();
                }
            }
            
            @Override
            public void run(Executor executor) {
                source.run(executor);
            }
        }
        
        return SynchronizedStepSource::new;
    }
    
    /**
     * Returns an operator that synchronizes access to a downstream sink's {@code offer}, {@code complete}, and
     * {@code completeAbruptly} methods. The resultant upstream sink can be safely offered to concurrently, making it
     * suitable for ad-hoc "merging" use cases.
     *
     * <p>Example:
     * {@snippet :
     * try (var scope = new StructuredTaskScope<>()) {
     *     List<Integer> list = new ArrayList<>();
     *     Belt.StepSource<Integer> source = Belts.iteratorSource(List.of(0, 1, 2).iterator()).andThen(Belts.synchronizeStepSource());
     *     Belt.StepSink<Integer> sink = ((Belt.StepSink<Integer>) list::add).compose(Belts.synchronizeStepSink());
     *     Belt.StepSink<Integer> noCompleteSink = sink::offer;
     *
     *     Belts
     *         .balance(List.of(
     *             // These 2 sinks will concurrently offer to the same downstream, effectively "merging"
     *             noCompleteSink.compose(Belts.gather(Gatherers.map((Integer i) -> i + 1))),
     *             noCompleteSink.compose(Belts.gather(Gatherers.map((Integer i) -> i + 4)))
     *         ))
     *         .compose(Belts.alsoComplete(sink))
     *         .compose(source)
     *         .run(Belts.scopeExecutor(scope));
     *
     *     scope.join();
     *
     *     String result = list.stream().map(String::valueOf).collect(Collectors.joining());
     *     System.out.println(result);
     *     // Possible outputs:
     *     // 156; 516; 561; 246; 426; 462; 345; 435; 453; 456;
     *     // 423; 243; 234; 513; 153; 135; 612; 162; 126; 123;
     * }
     * }
     *
     * @return an operator that synchronizes access to a downstream sink
     * @param <T> the sink element type
     */
    public static <T> Belt.StepSinkOperator<T, T> synchronizeStepSink() {
        class SynchronizedStepSink implements Belt.StepSink<T> {
            final Belt.StepSink<? super T> sink;
            final ReentrantLock lock = new ReentrantLock();
            
            SynchronizedStepSink(Belt.StepSink<? super T> sink) {
                this.sink = Objects.requireNonNull(sink);
            }
            
            @Override
            public boolean offer(T input) throws Exception {
                lock.lockInterruptibly();
                try {
                    return sink.offer(input);
                } finally {
                    lock.unlock();
                }
            }
            
            @Override
            public void complete() throws Exception {
                lock.lockInterruptibly();
                try {
                    sink.complete();
                } finally {
                    lock.unlock();
                }
            }
            
            @Override
            public void completeAbruptly(Throwable cause) throws Exception {
                lock.lock();
                try {
                    sink.completeAbruptly(cause);
                } finally {
                    lock.unlock();
                }
            }
            
            @Override
            public void run(Executor executor) {
                sink.run(executor);
            }
        }
        
        return SynchronizedStepSink::new;
    }
    
    /**
     * Returns an operator that attempts to recover from abrupt completion before it reaches a downstream sink. When the
     * resultant upstream sink is completed abruptly, the {@code mapper} is applied to the cause to produce a source,
     * which is then run and drained to the downstream sink. The downstream sink is then completed normally, and any
     * running silos from the source are awaited.
     *
     * <p>If the {@code mapper} throws an exception, the downstream is completed abruptly with the original cause.
     * Otherwise, if draining the created source or completing the downstream sink throws an exception, the downstream
     * is completed abruptly with that exception as the cause.
     *
     * <p>Example:
     * {@snippet :
     * try (var scope = new StructuredTaskScope<>()) {
     *     List<Integer> list = new ArrayList<>();
     *     Belt.Source<Integer> source = Belts.streamSource(
     *         Stream.iterate(1, i -> {
     *             if (i < 3) {
     *                 return i + 1;
     *             }
     *             throw new IllegalStateException();
     *         })
     *     );
     *
     *     source
     *         .andThen(Belts
     *             .recoverStep(
     *                 cause -> Belts.streamSource(Stream.of(7, 8, 9)),
     *                 Throwable::printStackTrace
     *             )
     *             .andThen((Belt.StepSink<Integer>) list::add)
     *         )
     *         .run(Belts.scopeExecutor(scope));
     *
     *     scope.join();
     *
     *     System.out.println(list);
     *     // Prints: [1, 2, 3, 7, 8, 9]
     * }
     * }
     *
     * @param mapper a function that creates a source from an exception
     * @param asyncExceptionHandler a function that consumes any exceptions thrown when asynchronously running silos
     *                              encapsulated by the created source
     * @return an operator that attempts to recover from abrupt completion before it reaches a downstream sink
     * @param <T> the sink element type
     */
    public static <T> Belt.StepSinkOperator<T, T> recoverStep(Function<? super Throwable, ? extends Belt.Source<? extends T>> mapper,
                                                              Consumer<? super Throwable> asyncExceptionHandler) {
        Objects.requireNonNull(mapper);
        Objects.requireNonNull(asyncExceptionHandler);
        
        class RecoverStep implements Belt.StepSink<T> {
            final Belt.StepSink<? super T> sink;
            
            RecoverStep(Belt.StepSink<? super T> sink) {
                this.sink = Objects.requireNonNull(sink);
            }
            
            @Override
            public boolean offer(T input) throws Exception {
                return sink.offer(input);
            }
            
            @Override
            public void complete() throws Exception {
                sink.complete();
            }
            
            @Override
            public void completeAbruptly(Throwable cause) throws Exception {
                boolean running = false;
                try {
                    var source = Objects.requireNonNull(mapper.apply(cause));
                    try (var scope = new FailureHandlingScope("recoverStep-completeAbruptly",
                                                              Thread.ofVirtual().name("thread-", 0).factory(),
                                                              asyncExceptionHandler)) {
                        source.run(scopeExecutor(scope));
                        running = true;
                        try (source) {
                            source.drainToSink(sink);
                            sink.complete(); // Note: This may not be the first time calling...
                        }
                        scope.join();
                    }
                } catch (Throwable e) {
                    if (e instanceof InterruptedException) { Thread.currentThread().interrupt(); }
                    try { sink.completeAbruptly(running ? e : cause); }
                    catch (Throwable t) { if (t instanceof InterruptedException) { Thread.currentThread().interrupt(); } e.addSuppressed(t); }
                    if (e instanceof InterruptedException) { Thread.interrupted(); }
                    throw e;
                }
            }
            
            @Override
            public void run(Executor executor) {
                sink.run(executor);
            }
        }
        
        return RecoverStep::new;
    }
    
    /**
     * Returns an operator that attempts to recover from abrupt completion before it reaches a downstream sink. When the
     * resultant upstream sink is completed abruptly, the {@code mapper} is applied to the cause to produce a source,
     * which is then run and drained to the downstream sink. The downstream sink is then completed normally, and any
     * running silos from the source are awaited.
     *
     * <p>If the {@code mapper} throws an exception, the downstream is completed abruptly with the original cause.
     * Otherwise, if draining the created source or completing the downstream sink throws an exception, the downstream
     * is completed abruptly with that exception as the cause.
     *
     * <p>Example:
     * {@snippet :
     * try (var scope = new StructuredTaskScope<>()) {
     *     List<Integer> list = new ArrayList<>();
     *     Iterator<Integer> iter = List.of(1, 2, 3).iterator();
     *     Belt.StepSource<Integer> source = () -> {
     *         if (iter.hasNext()) {
     *             return iter.next();
     *         }
     *         throw new IllegalStateException();
     *     };
     *
     *     source
     *         .andThen(Belts
     *             .recover(
     *                 cause -> Belts.iteratorSource(List.of(7, 8, 9).iterator()),
     *                 Throwable::printStackTrace
     *             )
     *             .andThen((Belt.Sink<Integer>) src -> {
     *                 src.forEach(list::add);
     *                 return true;
     *             })
     *         )
     *         .run(Belts.scopeExecutor(scope));
     *
     *     scope.join();
     *
     *     System.out.println(list);
     *     // Prints: [1, 2, 3, 7, 8, 9]
     * }
     * }
     *
     * @param mapper a function that creates a source from an exception
     * @param asyncExceptionHandler a function that consumes any exceptions thrown when asynchronously running silos
     *                              encapsulated by the created source
     * @return an operator that attempts to recover from abrupt completion before it reaches a downstream sink
     * @param <T> the sink element type
     */
    public static <T> Belt.SinkOperator<T, T> recover(Function<? super Throwable, ? extends Belt.StepSource<? extends T>> mapper,
                                                      Consumer<? super Throwable> asyncExceptionHandler) {
        Objects.requireNonNull(mapper);
        Objects.requireNonNull(asyncExceptionHandler);
        
        class Recover implements Belt.Sink<T> {
            final Belt.Sink<? super T> sink;
            
            Recover(Belt.Sink<? super T> sink) {
                this.sink = Objects.requireNonNull(sink);
            }
            
            @Override
            public boolean drainFromSource(Belt.StepSource<? extends T> source) throws Exception {
                return sink.drainFromSource(source);
            }
            
            @Override
            public void complete() throws Exception {
                sink.complete();
            }
            
            @Override
            public void completeAbruptly(Throwable cause) throws Exception {
                boolean running = false;
                try {
                    var source = Objects.requireNonNull(mapper.apply(cause));
                    try (var scope = new FailureHandlingScope("recover-completeAbruptly",
                                                              Thread.ofVirtual().name("thread-", 0).factory(),
                                                              asyncExceptionHandler)) {
                        source.run(scopeExecutor(scope));
                        running = true;
                        try (source) {
                            sink.drainFromSource(source);
                            sink.complete(); // Note: This may not be the first time calling...
                        }
                        scope.join();
                    }
                } catch (Throwable e) {
                    if (e instanceof InterruptedException) { Thread.currentThread().interrupt(); }
                    try { sink.completeAbruptly(running ? e : cause); }
                    catch (Throwable t) { if (t instanceof InterruptedException) { Thread.currentThread().interrupt(); } e.addSuppressed(t); }
                    if (e instanceof InterruptedException) { Thread.interrupted(); }
                    throw e;
                }
            }
            
            @Override
            public void run(Executor executor) {
                sink.run(executor);
            }
        }
        
        return Recover::new;
    }
    
    /**
     * Returns an operator that applies the {@code mapper} to adapt or discard elements from an upstream source. For
     * each upstream element, the resultant downstream source yields the result of applying the {@code mapper} to that
     * element, or discards the element if the {@code mapper} returned {@code null}.
     *
     * <p>Example:
     * {@snippet :
     * try (var scope = new StructuredTaskScope<>()) {
     *     List<Integer> list = new ArrayList<>();
     *
     *     Belts.iteratorSource(List.of(1, 2, 3, 4, 5, 6).iterator())
     *         .andThen(Belts.filterMap(i -> i % 2 == 0 ? null : -i))
     *         .andThen((Belt.StepSink<Integer>) list::add)
     *         .run(Belts.scopeExecutor(scope));
     *
     *     scope.join();
     *
     *     System.out.println(list);
     *     // Prints: [-1, -3, -5]
     * }
     * }
     *
     * @param mapper a function to be applied to the upstream elements
     * @return an operator that applies the {@code mapper} to adapt or discard elements from an upstream source
     * @param <T> the upstream element type
     * @param <U> the downstream element type
     */
    public static <T, U> Belt.StepSourceOperator<T, U> filterMap(Function<? super T, ? extends U> mapper) {
        Objects.requireNonNull(mapper);
        
        class FilterMap extends ProxySource<U> implements Belt.StepSource<U> {
            final Belt.StepSource<? extends T> source;
            
            FilterMap(Belt.StepSource<? extends T> source) {
                this.source = Objects.requireNonNull(source);
            }
            
            @Override
            public U poll() throws Exception {
                for (T t; (t = source.poll()) != null; ) {
                    U u = mapper.apply(t);
                    if (u != null) {
                        return u;
                    }
                }
                return null;
            }
            
            @Override
            protected Stream<? extends Belt.Source<?>> sources() {
                return Stream.of(source);
            }
        }
        
        return FilterMap::new;
    }
    
    /**
     * Returns an operator that also closes the given {@code sourceToClose} when an upstream source closes. The
     * resultant downstream source will delegate to the upstream source, but also close the {@code sourceToClose} when
     * closed, and run the {@code sourceToClose} when run.
     *
     * <p>This is useful when a source is shared among several downstream sources: Instead of each downstream source
     * attempting to close the shared source when it closes - and potentially interfering with others - this operator
     * allows a common parent of the downstream sources to handle closing the shared source when all are done using it.
     * This is particularly important when the downstream sources may be closed at different times, or when closing the
     * shared source is not idempotent.
     *
     * <p>Example:
     * {@snippet :
     * try (var scope = new StructuredTaskScope<>()) {
     *     List<Integer> list = new ArrayList<>();
     *     Iterator<Integer> iter = List.of(0, 1, 2).iterator();
     *     Belt.StepSource<Integer> source = new Belt.StepSource<Integer>() {
     *         @Override public Integer poll() { return iter.hasNext() ? iter.next() : null; }
     *         @Override public void close() { System.out.print("000"); }
     *     }.andThen(Belts.synchronizeStepSource());
     *     Belt.StepSink<Integer> sink = ((Belt.StepSink<Integer>) list::add).compose(Belts.synchronizeStepSink());
     *     Belt.StepSource<Integer> noCloseSource = source::poll;
     *
     *     Belts
     *         .merge(List.of(
     *             // These 2 sources will concurrently poll from the same upstream, effectively "balancing"
     *             noCloseSource.andThen(Belts.filterMap(i -> i + 1)),
     *             noCloseSource.andThen(Belts.filterMap(i -> i + 4))
     *         ))
     *         .andThen(Belts.alsoClose(source))
     *         .andThen(sink)
     *         .run(Belts.scopeExecutor(scope));
     *
     *     scope.join();
     *
     *     String result = list.stream().map(String::valueOf).collect(Collectors.joining());
     *     System.out.println(result);
     *     // Possible outputs:
     *     // 000156; 000516; 000561; 000246; 000426; 000462; 000345; 000435; 000453; 000456;
     *     // 000423; 000243; 000234; 000513; 000153; 000135; 000612; 000162; 000126; 000123
     * }
     * }
     *
     * @param sourceToClose the additional source to close
     * @return an operator that also closes the given {@code sourceToClose} when an upstream source closes
     * @param <T> the element type
     */
    public static <T> Belt.SourceOperator<T, T> alsoClose(Belt.Source<?> sourceToClose) {
        Objects.requireNonNull(sourceToClose);
        
        class AlsoClose extends ProxySource<T> {
            final Belt.Source<? extends T> source;
            
            AlsoClose(Belt.Source<? extends T> source) {
                this.source = Objects.requireNonNull(source);
            }
            
            @Override
            public boolean drainToSink(Belt.StepSink<? super T> sink) throws Exception {
                return source.drainToSink(sink);
            }
            
            @Override
            protected Stream<? extends Belt.Source<?>> sources() {
                return Stream.of(source, sourceToClose);
            }
        }
        
        return AlsoClose::new;
    }
    
    /**
     * Returns an operator that also completes the given {@code sinkToComplete} when a downstream sink completes. The
     * resultant upstream sink will delegate to the downstream sink, but also complete the {@code sinkToComplete} when
     * completed (normally or abruptly), and run the {@code sinkToComplete} when run.
     *
     * <p>This is useful when a sink is shared among several upstream sinks: Instead of each upstream sink attempting to
     * complete the shared sink when it completes - and potentially interfering with others - this operator allows a
     * common parent of the upstream sinks to handle closing the shared sink when all are done using it. This is
     * particularly important when the upstream sinks may be completed at different times, or when completing the shared
     * sink is not idempotent.
     *
     * <p>Example:
     * {@snippet :
     * try (var scope = new StructuredTaskScope<>()) {
     *     List<Integer> list = new ArrayList<>();
     *     Belt.StepSource<Integer> source = Belts.iteratorSource(List.of(0, 1, 2).iterator()).andThen(Belts.synchronizeStepSource());
     *     Belt.StepSink<Integer> sink = new Belt.StepSink<Integer>() {
     *         @Override public boolean offer(Integer input) { return list.add(input); }
     *         @Override public void complete() { list.addAll(List.of(0, 0, 0)); }
     *     }.compose(Belts.synchronizeStepSink());
     *     Belt.StepSink<Integer> noCompleteSink = sink::offer;
     *
     *     Belts
     *         .balance(List.of(
     *             // These 2 sinks will concurrently offer to the same downstream, effectively "merging"
     *             noCompleteSink.compose(Belts.gather(map((Integer i) -> i + 1))),
     *             noCompleteSink.compose(Belts.gather(map((Integer i) -> i + 4)))
     *         ))
     *         .compose(Belts.alsoComplete(sink))
     *         .compose(source)
     *         .run(Belts.scopeExecutor(scope));
     *
     *     scope.join();
     *
     *     String result = list.stream().map(String::valueOf).collect(Collectors.joining());
     *     System.out.println(result);
     *     // Possible outputs:
     *     // 156000; 516000; 561000; 246000; 426000; 462000; 345000; 435000; 453000; 456000;
     *     // 423000; 243000; 234000; 513000; 153000; 135000; 612000; 162000; 126000; 123000
     * }
     * }
     *
     * @param sinkToComplete the additional sink to complete
     * @return an operator that also completes the given {@code sinkToComplete} when a downstream sink completes
     * @param <T> the element type
     */
    public static <T> Belt.SinkOperator<T, T> alsoComplete(Belt.Sink<?> sinkToComplete) {
        Objects.requireNonNull(sinkToComplete);
        
        class AlsoComplete extends ProxySink<T> {
            final Belt.Sink<? super T> sink;
            
            AlsoComplete(Belt.Sink<? super T> sink) {
                this.sink = Objects.requireNonNull(sink);
            }
            
            @Override
            public boolean drainFromSource(Belt.StepSource<? extends T> source) throws Exception {
                return sink.drainFromSource(source);
            }
            
            @Override
            protected Stream<? extends Belt.Sink<?>> sinks() {
                return Stream.of(sink, sinkToComplete);
            }
        }
        
        return AlsoComplete::new;
    }
    
    /**
     * Returns a sink that offers input elements to consecutive inner sinks, using the {@code predicate} to determine
     * when to create each new sink. When the {@code predicate} returns {@code true} for an element, the current inner
     * sink is completed and its running silos awaited, then a new inner sink is created by passing the element to the
     * {@code sinkFactory}, and the new sink is run.
     *
     * <p>Example:
     * {@snippet :
     * try (var scope = new StructuredTaskScope<>()) {
     *     // In this example, the consecutive inner sinks offer to a shared sink, effectively "concatenating"
     *     List<Integer> list = new ArrayList<>();
     *     Belt.StepSink<Integer> sink = list::add;
     *     Belt.StepSink<Integer> noCompleteSink = sink::offer;
     *
     *     Belts.iteratorSource(List.of(0, 1, 2, 3, 4, 5).iterator())
     *         .andThen(Belts
     *             .split(
     *                 (Integer i) -> i % 2 == 0, // Starts a new sink when element is even
     *                 false, false,
     *                 Throwable::printStackTrace,
     *                 i -> noCompleteSink.compose(Belts.flatMap(j -> Belts.streamSource(Stream.of(j, i)),
     *                                                           Throwable::printStackTrace))
     *             )
     *             .compose(Belts.alsoComplete(sink))
     *         )
     *         .run(Belts.scopeExecutor(scope));
     *
     *     scope.join();
     *     System.out.println(list);
     *     // Prints: [0, 0, 1, 0, 2, 2, 3, 2, 4, 4, 5, 4]
     * }
     * }
     *
     * @param predicate a predicate to be applied to the input elements
     * @param splitAfter if {@code true}, an element that passes the {@code predicate} will cause a new inner sink to be
     *                   created starting with the next element, rather than the current element
     * @param eagerCancel if {@code true}, cancels draining when the first inner sink cancels; else never cancels
     * @param asyncExceptionHandler a function that consumes any exceptions thrown when asynchronously running silos
     *                              encapsulated by created sinks
     * @param sinkFactory a function that creates a sink, using the first element that will be offered to that sink
     * @return a sink that offers input elements to consecutive inner sinks, delimited by passing {@code predicate}
     * @param <T> the element type
     */
    public static <T> Belt.Sink<T> split(Predicate<? super T> predicate,
                                         boolean splitAfter,
                                         boolean eagerCancel,
                                         Consumer<? super Throwable> asyncExceptionHandler,
                                         Function<? super T, ? extends Belt.StepSink<? super T>> sinkFactory) {
        Objects.requireNonNull(predicate);
        Objects.requireNonNull(asyncExceptionHandler);
        Objects.requireNonNull(sinkFactory);
        
        class Split implements Belt.Sink<T> {
            @Override
            public boolean drainFromSource(Belt.StepSource<? extends T> source) throws Exception {
                try (var scope = new FailureHandlingScope("split-drainFromSource",
                                                          Thread.ofVirtual().name("thread-", 0).factory(),
                                                          asyncExceptionHandler)) {
                    Belt.StepSink<? super T> subSink = e -> true;
                    var exec = scopeExecutor(scope);
                    try {
                        boolean drained = true;
                        boolean split = true;
                        for (T val; (val = source.poll()) != null; ) {
                            if ((!splitAfter && predicate.test(val)) || split) {
                                subSink.complete();
                                scope.join();
                                subSink = sinkFactory.apply(val);
                                subSink.run(exec);
                            }
                            split = splitAfter && predicate.test(val);
                            if (!subSink.offer(val)) {
                                if (eagerCancel) {
                                    drained = false;
                                    break;
                                }
                                split = true;
                            }
                        }
                        subSink.complete();
                        scope.join();
                        return drained;
                    } catch (Throwable e) {
                        if (e instanceof InterruptedException) { Thread.currentThread().interrupt(); }
                        try { subSink.completeAbruptly(e); }
                        catch (Throwable t) { if (t instanceof InterruptedException) { Thread.currentThread().interrupt(); } e.addSuppressed(t); }
                        if (e instanceof InterruptedException) { Thread.interrupted(); }
                        throw e;
                    }
                }
            }
        }
        
        return new Split();
    }
    
    /**
     * Returns a sink that offers input elements to the inner sink associated with the key that the {@code classifier}
     * computes for the element. When the {@code classifier} computes a previously-unseen key for an element, the
     * {@code sinkFactory} is called with the key and element to create a new inner sink, which is then run. That first
     * element, and subsequent elements that map to the same key, are offered to that sink until the sink cancels.
     *
     * <p>If {@code eagerCancel} is {@code true}, and any inner sink cancels, all inner sinks will be completed and
     * running silos awaited, before the outer sink cancels. If {@code eagerCancel} is {@code false}, and any inner sink
     * cancels, it will be completed and its running silos awaited, before the outer sink resumes. Subsequent elements
     * that map to the canceled sink's key will be discarded.
     *
     * <p>Example:
     * {@snippet :
     * try (var scope = new StructuredTaskScope<>()) {
     *     // In this example, the concurrent inner sinks offer to a shared sink, effectively "merging"
     *     List<String> list = new ArrayList<>();
     *     Belt.StepSink<String> sink = list::add;
     *     Belt.StepSink<String> noCompleteSink = sink::offer;
     *
     *     Belts.iteratorSource(List.of("now", "or", "never").iterator())
     *         .andThen(Belts
     *             .groupBy(
     *                 (String s) -> s.substring(0, 1),
     *                 false,
     *                 Throwable::printStackTrace,
     *                 (k, first) -> noCompleteSink.compose(Belts.flatMap(s -> Belts.streamSource(Stream.of(k, s, first)),
     *                                                                    Throwable::printStackTrace))
     *             )
     *             .compose(Belts.alsoComplete(sink))
     *         )
     *         .run(Belts.scopeExecutor(scope));
     *
     *     scope.join();
     *     System.out.println(list);
     *     // Prints: [n, now, now, o, or, or, n, never, now]
     * }
     * }
     *
     * @param classifier a classifier function mapping input elements to keys
     * @param eagerCancel if {@code true}, cancels draining when the first inner sink cancels; else never cancels
     * @param asyncExceptionHandler a function that consumes any exceptions thrown when asynchronously running silos
     *                              encapsulated by created sinks
     * @param sinkFactory a function that creates a sink, using a key and the first element that will be offered to that
     *                    sink
     * @return a sink that offers input elements to the inner sink associated with the key computed for the element
     * @param <T> the element type
     * @param <K> the key type
     */
    public static <T, K> Belt.Sink<T> groupBy(Function<? super T, ? extends K> classifier,
                                              boolean eagerCancel,
                                              Consumer<? super Throwable> asyncExceptionHandler,
                                              BiFunction<? super K, ? super T, ? extends Belt.StepSink<? super T>> sinkFactory) {
        Objects.requireNonNull(classifier);
        Objects.requireNonNull(asyncExceptionHandler);
        Objects.requireNonNull(sinkFactory);
        
        record ScopedSink<T>(SubScope scope, Belt.StepSink<? super T> sink) { }
        
        class GroupBy implements Belt.Sink<T> {
            static final Object TOMBSTONE = new Object();
            
            private static Stream<Belt.Sink<?>> sinks(Map<?, Object> scopedSinkByKey) {
                return scopedSinkByKey.values().stream()
                    .mapMulti((o, downstream) -> {
                        if (o instanceof ScopedSink<?> ss) {
                            downstream.accept(ss.sink);
                        }
                    });
            }
            
            @Override
            public boolean drainFromSource(Belt.StepSource<? extends T> source) throws Exception {
                Map<K, Object> scopedSinkByKey = new HashMap<>();
                try (var scope = new FailureHandlingScope("groupBy-drainFromSource",
                                                          Thread.ofVirtual().name("thread-", 0).factory(),
                                                          asyncExceptionHandler)) {
                    try {
                        boolean drained = true;
                        for (T val; (val = source.poll()) != null; ) {
                            K key = Objects.requireNonNull(classifier.apply(val));
                            var s = scopedSinkByKey.get(key);
                            if (s == TOMBSTONE) {
                                continue;
                            }
                            @SuppressWarnings("unchecked")
                            var scopedSink = (ScopedSink<T>) s;
                            if (s == null) {
                                var subSink = Objects.requireNonNull(sinkFactory.apply(key, val));
                                var subScope = new SubScope(scope);
                                scopedSink = new ScopedSink<>(subScope, subSink);
                                scopedSinkByKey.put(key, scopedSink);
                                // Note that running a sink per key could produce unbounded threads.
                                // We leave this to the sinkFactory to resolve if necessary, eg by tracking
                                // incomplete sinks and returning a no-op Sink if maxed (thus dropping elements).
                                subSink.run(subScope);
                            }
                            if (!scopedSink.sink.offer(val)) {
                                if (eagerCancel) {
                                    drained = false;
                                    break;
                                }
                                scopedSink.sink.complete();
                                scopedSink.scope.join();
                                scopedSinkByKey.put(key, TOMBSTONE);
                            }
                        }
                        composedComplete(sinks(scopedSinkByKey));
                        scope.join();
                        return drained;
                    } catch (Throwable e) {
                        if (e instanceof InterruptedException) { Thread.currentThread().interrupt(); }
                        try { composedCompleteAbruptly(sinks(scopedSinkByKey), e); }
                        catch (Throwable t) { if (t instanceof InterruptedException) { Thread.currentThread().interrupt(); } e.addSuppressed(t); }
                        if (e instanceof InterruptedException) { Thread.interrupted(); }
                        throw e;
                    }
                }
            }
        }
        
        return new GroupBy();
    }
    
    /**
     * Returns an operator that replaces each element with the contents of a mapped source before it reaches a
     * downstream sink. The resultant upstream sink will apply the {@code mapper} to each element to produce a mapped
     * source, run the source, offer its contents downstream, then close it and await its running silos. (If a mapped
     * source is {@code null}, it is discarded.)
     *
     * <p>If the mapped sources are known to never encapsulate silos / cross asynchronous boundaries, it may be possible
     * to replace usage of this operator with the {@link #gather gather} operator and a flat-mapping
     * {@link Gatherer Gatherer}.
     *
     * <p>Example:
     * {@snippet :
     * try (var scope = new StructuredTaskScope<>()) {
     *     List<String> list = new ArrayList<>();
     *
     *     Belts.iteratorSource(List.of("red", "blue", "green").iterator())
     *         .andThen(Belts
     *             .flatMap(
     *                 (String color) -> Belts.streamSource(Stream.of("color", color))
     *                     .andThen(Belts.buffer(256)),
     *                 Throwable::printStackTrace
     *             )
     *             .andThen((Belt.StepSink<String>) list::add)
     *         )
     *         .run(Belts.scopeExecutor(scope));
     *
     *     scope.join();
     *     System.out.println(list);
     *     // Prints: [color, red, color, blue, color, green]
     * }
     * }
     *
     * @param mapper a function to be applied to the upstream elements, producing a mapped source
     * @param asyncExceptionHandler a function that consumes any exceptions thrown when asynchronously running silos
     *                              encapsulated by created sources
     * @return an operator that replaces each element with the contents of a mapped source
     * @param <T> the upstream element type
     * @param <U> the downstream element type
     */
    public static <T, U> Belt.StepSinkOperator<T, U> flatMap(Function<? super T, ? extends Belt.Source<? extends U>> mapper,
                                                             Consumer<? super Throwable> asyncExceptionHandler) {
        Objects.requireNonNull(mapper);
        Objects.requireNonNull(asyncExceptionHandler);
        
        class FlatMap extends ProxySink<T> implements Belt.StepSink<T> {
            final Belt.StepSink<? super U> sink;
            boolean draining = true;
            
            FlatMap(Belt.StepSink<? super U> sink) {
                this.sink = Objects.requireNonNull(sink);
            }
            
            @Override
            public boolean offer(T input) throws Exception {
                Objects.requireNonNull(input);
                if (!draining) {
                    return false;
                }
                var subSource = mapper.apply(input);
                if (subSource == null) {
                    return true;
                }
                try (var scope = new FailureHandlingScope("flatMap-offer",
                                                          Thread.ofVirtual().name("thread-", 0).factory(),
                                                          asyncExceptionHandler)) {
                    subSource.run(scopeExecutor(scope));
                    try (subSource) {
                        draining = subSource.drainToSink(sink);
                    }
                    scope.join();
                    return draining;
                }
            }
            
            @Override
            protected Stream<? extends Belt.Sink<?>> sinks() {
                return Stream.of(sink);
            }
        }
        
        return FlatMap::new;
    }
    
    /**
     * Returns an operator that connects the given {@code sourceMapper} after any source that a downstream sink drains
     * from. The resultant upstream sink will wrap any upstream source it drains from, to discard close signals, and
     * then apply the {@code sourceMapper} to create a downstream source. The downstream source is then run, drained
     * from, and closed, and its running silos are awaited.
     *
     * <p>The effect of this operator is similar, but not equivalent to, connecting the {@code sourceMapper} after the
     * upstream source, and then connecting the downstream sink after that. However, since close signals are expected to
     * be handled externally from draining, any close signals that reach the upstream source during draining are
     * discarded.
     *
     * <p>This operator is intended for use cases where either the caller does not have access to the source (eg, an API
     * that must return a sink), or the sink is part of a fan-out, and does not intend to change the behavior of the
     * source for other sinks. For best performance, it is recommended to chain subsequent operations onto the
     * {@code sourceMapper} passed to this operator, rather than chaining calls to this operator.
     *
     * <p>Example:
     * {@snippet :
     * try (var scope = new StructuredTaskScope<>()) {
     *     List<Integer> list1 = new ArrayList<>();
     *     List<Integer> list2 = new ArrayList<>();
     *
     *     Belts.iteratorSource(List.of(1, 2, 3).iterator())
     *         .andThen(Belts.synchronizeStepSource())
     *         .andThen(Belts.balance(List.of(
     *             (Belt.StepSink<Integer>) list1::add,
     *             ((Belt.StepSink<Integer>) list2::add)
     *                 .compose(Belts.adaptSourceOfSink(
     *                     Belts.filterMap(i -> 10 - i),
     *                     Throwable::printStackTrace
     *                 ))
     *         )))
     *         .run(Belts.scopeExecutor(scope));
     *
     *     scope.join();
     *
     *     String result = Stream.concat(list1.stream(), list2.stream()).map(String::valueOf).collect(Collectors.joining());
     *     System.out.println(result);
     *     // Possible outputs:
     *     // 187; 817; 871; 297; 927; 972; 398; 938; 983; 987;
     *     // 923; 293; 239; 813; 183; 138; 712; 172; 127; 123
     * }
     * }
     *
     * @param sourceMapper an operator that connects after an upstream source to produce a downstream source
     * @param asyncExceptionHandler a function that consumes any exceptions thrown when asynchronously running silos
     *                              encapsulated by the created downstream source
     * @return an operator that connects the given {@code sourceMapper} after any source that a downstream sink drains
     * from
     * @param <T> the upstream element type
     * @param <U> the downstream element type
     */
    public static <T, U> Belt.SinkOperator<T, U> adaptSourceOfSink(Belt.StepSourceOperator<T, U> sourceMapper,
                                                                   Consumer<? super Throwable> asyncExceptionHandler) {
        Objects.requireNonNull(sourceMapper);
        Objects.requireNonNull(asyncExceptionHandler);
        
        class SourceAdaptedSink extends ProxySink<T> {
            final Belt.Sink<? super U> sink;
            
            SourceAdaptedSink(Belt.Sink<? super U> sink) {
                this.sink = Objects.requireNonNull(sink);
            }
            
            @Override
            public boolean drainFromSource(Belt.StepSource<? extends T> source) throws Exception {
                class SignalSource implements Belt.StepSource<T> {
                    volatile boolean drained = false;
                    
                    @Override
                    public T poll() throws Exception {
                        var result = source.poll();
                        if (result == null) {
                            drained = true;
                        }
                        return result;
                    }
                }
                
                var signalSource = new SignalSource();
                var newSource = sourceMapper.compose(signalSource);
                
                try (var scope = new FailureHandlingScope("adaptSourceOfSink-drainFromSource",
                                                          Thread.ofVirtual().name("thread-", 0).factory(),
                                                          asyncExceptionHandler)) {
                    newSource.run(scopeExecutor(scope));
                    try (newSource) {
                        sink.drainFromSource(newSource);
                    }
                    scope.join();
                }
                
                return signalSource.drained;
            }
            
            @Override
            protected Stream<? extends Belt.Sink<?>> sinks() {
                return Stream.of(sink);
            }
        }
        
        return SourceAdaptedSink::new;
    }
    
    /**
     * Returns an operator that connects the given {@code sinkMapper} before any sink that an upstream source drains to.
     * The resultant downstream source will wrap any downstream sink it drains to, to discard completion signals, and
     * then apply the {@code sinkMapper} to create an upstream sink. The upstream sink is then run, drained to, and
     * completed, and its running silos are awaited.
     *
     * <p>The effect of this operator is similar, but not equivalent to, connecting the {@code sinkMapper} before the
     * downstream sink, and then connecting the upstream source before that. However, since completion signals are
     * expected to be handled externally from draining, any completion signals that reach the downstream sink during
     * draining are discarded.
     *
     * <p>This operator is intended for use cases where either the caller does not have access to the sink (eg, an API
     * that must return a source), or the source is part of a fan-in, and does not intend to change the behavior of the
     * sink for other sources. For best performance, it is recommended to chain subsequent operations onto the
     * {@code sinkMapper} passed to this operator, rather than chaining calls to this operator.
     *
     * <p>Example:
     * {@snippet :
     * try (var scope = new StructuredTaskScope<>()) {
     *     List<Integer> list = new ArrayList<>();
     *     Belt.StepSink<Integer> sink = ((Belt.StepSink<Integer>) list::add).compose(Belts.synchronizeStepSink());
     *
     *     Belts
     *         .merge(List.of(
     *             Belts.streamSource(Stream.of(9)),
     *             Belts.streamSource(Stream.of(2))
     *                 .andThen(Belts.adaptSinkOfSource(
     *                     Belts.flatMap(i -> Belts.streamSource(Stream.of(i, i+1, i+2)),
     *                                   Throwable::printStackTrace),
     *                     Throwable::printStackTrace
     *                 ))
     *         ))
     *         .andThen(sink)
     *         .run(Belts.scopeExecutor(scope));
     *
     *     scope.join();
     *
     *     String result = list.stream().map(String::valueOf).collect(Collectors.joining());
     *     System.out.println(result);
     *     // Possible outputs:
     *     // 9234; 2934; 2394; 2349
     * }
     * }
     *
     * @param sinkMapper an operator that connects before a downstream sink to produce an upstream sink
     * @param asyncExceptionHandler a function that consumes any exceptions thrown when asynchronously running silos
     *                              encapsulated by the created upstream sink
     * @return an operator that connects the given {@code sinkMapper} before any sink that an upstream source drains to
     * @param <T> the upstream element type
     * @param <U> the downstream element type
     */
    public static <T, U> Belt.SourceOperator<T, U> adaptSinkOfSource(Belt.StepSinkOperator<T, U> sinkMapper,
                                                                     Consumer<? super Throwable> asyncExceptionHandler) {
        Objects.requireNonNull(sinkMapper);
        Objects.requireNonNull(asyncExceptionHandler);
        
        class SinkAdaptedSource extends ProxySource<U> {
            final Belt.Source<? extends T> source;
            
            SinkAdaptedSource(Belt.Source<? extends T> source) {
                this.source = Objects.requireNonNull(source);
            }
            
            @Override
            public boolean drainToSink(Belt.StepSink<? super U> sink) throws Exception {
                class SignalSink implements Belt.StepSink<U> {
                    volatile boolean drained = true;
                    
                    @Override
                    public boolean offer(U input) throws Exception {
                        if (!sink.offer(input)) {
                            return drained = false;
                        }
                        return true;
                    }
                }
                
                var signalSink = new SignalSink();
                var newSink = sinkMapper.andThen(signalSink);
                
                try (var scope = new FailureHandlingScope("adaptSinkOfSource-drainToSink",
                                                          Thread.ofVirtual().name("thread-", 0).factory(),
                                                          asyncExceptionHandler)) {
                    newSink.run(scopeExecutor(scope));
                    try {
                        source.drainToSink(newSink);
                        newSink.complete();
                        scope.join();
                    } catch (Throwable e) {
                        if (e instanceof InterruptedException) { Thread.currentThread().interrupt(); }
                        try { newSink.completeAbruptly(e); }
                        catch (Throwable t) { if (t instanceof InterruptedException) { Thread.currentThread().interrupt(); } e.addSuppressed(t); }
                        if (e instanceof InterruptedException) { Thread.interrupted(); }
                        throw e;
                    }
                }
                
                return signalSink.drained;
            }
            
            @Override
            protected Stream<? extends Belt.Source<?>> sources() {
                return Stream.of(source);
            }
        }
        
        return SinkAdaptedSource::new;
    }
    
    /**
     * Returns an operator that transforms elements according to the given {@code gatherer} before they reach a
     * downstream sink. The resultant upstream sink will call the gatherer's initializer upon first offer to initialize
     * state, call the integrator on each offer to push elements downstream, and call the finisher on (normal)
     * completion. (The gatherer's combiner is not used.)
     *
     * <p>Example:
     * {@snippet :
     * try (var scope = new StructuredTaskScope<>()) {
     *     List<Integer> list = new ArrayList<>();
     *
     *     Belts.streamSource(Stream.of(1, 2, 3))
     *         .andThen(
     *             Belts.gather(Gatherers.map((Integer i) -> i * 2))
     *                 .andThen((Belt.StepSink<Integer>) list::add)
     *         )
     *         .run(Belts.scopeExecutor(scope));
     *
     *     scope.join();
     *     System.out.println(list);
     *     // Prints: [2, 4, 6]
     * }
     * }
     *
     * @param gatherer the gatherer
     * @return an operator that transforms elements according to the given {@code gatherer}
     * @param <T> the upstream element type
     * @param <A> the gatherer's accumulation type
     * @param <R> the downstream element type
     */
    public static <T, A, R> Belt.StepSinkOperator<T, R> gather(Gatherer<? super T, A, R> gatherer) {
        var supplier = gatherer.initializer();
        var integrator = gatherer.integrator();
        var finisher = gatherer.finisher();
        
        class Gather implements Belt.StepSink<T> {
            final Belt.StepSink<? super R> sink;
            final Gatherer.Downstream<R> downstream;
            A acc = null;
            int state = NEW;
            
            static final int NEW       = 0;
            static final int RUNNING   = 1;
            static final int COMPLETED = 2;
            static final int CLOSED    = 3;
            
            Gather(Belt.StepSink<? super R> sink) {
                this.sink = Objects.requireNonNull(sink);
                this.downstream = el -> {
                    try {
                        return sink.offer(el);
                    } catch (Error | RuntimeException e) {
                        throw e;
                    } catch (Exception e) {
                        // We are not allowed to throw checked exceptions in this context;
                        // wrap them so that we might rediscover them farther up the stack.
                        // (They might still be dropped or re-wrapped between here and there.)
                        if (e instanceof InterruptedException) {
                            Thread.currentThread().interrupt();
                        }
                        throw new WrappingException(e);
                    }
                };
            }
            
            void initIfNew() {
                if (state == NEW) {
                    acc = supplier.get();
                    state = RUNNING;
                }
            }
            
            @Override
            public boolean offer(T input) throws Exception {
                try {
                    if (state >= COMPLETED) {
                        return false;
                    }
                    initIfNew();
                    if (!integrator.integrate(acc, input, downstream)) {
                        state = COMPLETED;
                        return false;
                    }
                    return true;
                } catch (WrappingException e) {
                    if (e.getCause() instanceof InterruptedException) {
                        Thread.interrupted();
                    }
                    throw e.getCause();
                }
            }
            
            @Override
            public void complete() throws Exception {
                try {
                    if (state >= COMPLETED) {
                        return;
                    }
                    initIfNew();
                    finisher.accept(acc, downstream);
                    sink.complete();
                    state = COMPLETED;
                } catch (WrappingException e) {
                    if (e.getCause() instanceof InterruptedException) {
                        Thread.interrupted();
                    }
                    throw e.getCause();
                }
            }
            
            @Override
            public void completeAbruptly(Throwable cause) throws Exception {
                if (state == CLOSED) {
                    return;
                }
                state = CLOSED;
                sink.completeAbruptly(cause);
            }
            
            @Override
            public void run(Executor executor) {
                sink.run(executor);
            }
        }
        
        return Gather::new;
    }
    
    private abstract static class MapBalance<T, U> implements Belt.SinkStepSource<T, U> {
        final int concurrency;
        final int bufferLimit;
        final Deque<Item<T>> completionBuffer;
        final ReentrantLock sourceLock = new ReentrantLock();
        final ReentrantLock lock = new ReentrantLock();
        final Condition completionNotFull = lock.newCondition();
        final Condition outputReady = lock.newCondition();
        int state = RUNNING;
        Throwable exception = null;
        
        static final int RUNNING    = 0;
        static final int COMPLETING = 1;
        static final int CLOSED     = 2;
        
        MapBalance(int concurrency, int bufferLimit) {
            this.concurrency = concurrency;
            this.bufferLimit = bufferLimit;
            this.completionBuffer = new ArrayDeque<>(bufferLimit);
        }
        
        abstract Belt.Sink<T> newWorker();
        
        static class Item<T> {
            // Value of out is initially partition key (or null if not partitioned)
            // When output is computed, output replaces partition key, and null replaces input
            Object out;
            T in;
            
            Item(Object out, T in) {
                this.out = out;
                this.in = in;
            }
        }
        
        class Sink implements Belt.Sink<T> {
            @Override
            public boolean drainFromSource(Belt.StepSource<? extends T> source) throws Exception {
                try (var scope = new StructuredTaskScope.ShutdownOnFailure("mapBalancePartitioned-drainFromSource",
                                                                           Thread.ofVirtual().name("thread-", 0).factory())) {
                    var tasks = IntStream.range(0, concurrency)
                        .mapToObj(i -> newWorker())
                        .map(sink -> scope.fork(() -> sink.drainFromSource(source)))
                        .toList();
                    scope.join().throwIfFailed();
                    return tasks.stream().anyMatch(StructuredTaskScope.Subtask::get);
                } catch (Throwable e) {
                    // Anything after the first unprocessed item is now unreachable, meaning we would deadlock if we
                    // tried to recover this sink. To make recovery safe, we remove unreachable items. This includes
                    // processed items that were behind unprocessed items, to avoid violating order.
                    lock.lock();
                    try {
                        var reachable = new LinkedList<Item<T>>();
                        for (Item<T> i; (i = completionBuffer.poll()) != null && i.in == null; ) {
                            reachable.offer(i);
                        }
                        completionBuffer.clear();
                        completionBuffer.addAll(reachable);
                    } finally {
                        lock.unlock();
                    }
                    throw e;
                }
            }
            
            @Override
            public void complete() throws Exception {
                lock.lockInterruptibly();
                try {
                    if (state >= COMPLETING) {
                        return;
                    }
                    state = COMPLETING;
                    if (completionBuffer.isEmpty()) {
                        outputReady.signalAll();
                    }
                } finally {
                    lock.unlock();
                }
            }
            
            @Override
            public void completeAbruptly(Throwable cause) {
                lock.lock();
                try {
                    if (state == CLOSED) {
                        return;
                    }
                    state = CLOSED;
                    exception = cause == null ? NULL_EXCEPTION : cause;
                    completionNotFull.signalAll();
                    outputReady.signalAll();
                } finally {
                    lock.unlock();
                }
            }
        }
        
        class Source implements Belt.StepSource<U> {
            @Override
            public U poll() throws Exception {
                for (;;) {
                    lock.lockInterruptibly();
                    try {
                        Item<T> item;
                        for (;;) {
                            item = completionBuffer.peek();
                            if (state >= COMPLETING) {
                                if (exception != null) {
                                    throw new UpstreamException(exception == NULL_EXCEPTION ? null : exception);
                                } else if (state == CLOSED || item == null) {
                                    return null;
                                }
                            }
                            if (item != null && item.in == null) {
                                break;
                            }
                            outputReady.await();
                        }
                        completionBuffer.poll();
                        completionNotFull.signal();
                        if (item.out == null) { // Skip nulls
                            continue;
                        }
                        Item<T> nextItem = completionBuffer.peek();
                        if (nextItem != null && nextItem.in == null) {
                            outputReady.signal();
                        }
                        @SuppressWarnings("unchecked")
                        U out = (U) item.out;
                        return out;
                    } finally {
                        lock.unlock();
                    }
                }
            }
            
            @Override
            public void close() {
                lock.lock();
                try {
                    if (state == CLOSED) {
                        return;
                    }
                    state = CLOSED;
                    completionNotFull.signalAll();
                    outputReady.signalAll();
                } finally {
                    lock.unlock();
                }
            }
        }
        
        @Override public Belt.Sink<T> sink() { return new Sink(); }
        @Override public Belt.StepSource<U> source() { return new Source(); }
    }
    
    /**
     * Returns a segue that balances computation of outputs across a limited number of concurrent workers, further
     * limiting concurrency per partition key, and orders outputs based on input arrival.
     *
     * <p>The number of concurrent workers created when draining is controlled by the given {@code concurrency}. The
     * number of workers allowed to work on the same partition at once is controlled by the given
     * {@code permitsPerPartition}. If {@code permitsPerPartition} is greater than or equal to {@code concurrency}, this
     * segue gracefully degrades to {@link #mapBalanceOrdered mapBalanceOrdered}.
     *
     * <p>When a worker polls an element, it first invokes the {@code classifier} on the element to determine a
     * partition key. If the partition key is {@code null}, the worker will throw a {@link NullPointerException}.
     * Otherwise, the worker will add the element to a pending output buffer, first waiting until the buffer is not full
     * (size is below {@code bufferLimit}). Then, if the worker can acquire a partition permit, it will begin work on
     * the element, calling the {@code mapper} and then the resultant callable, before signaling a completed output and
     * releasing its permit. Otherwise, the worker will arrange for the element to be picked up when a permit becomes
     * available, and resume polling.
     *
     * <p>The {@code classifier} is invoked on elements in the order they arrived. The {@code mapper} is invoked on
     * elements with the same partition key in the order they arrived, but no particular order is guaranteed for
     * elements with different partition keys. Output elements are polled from the buffer in the order that their
     * associated input elements arrived (so completed outputs may be held up by earlier incomplete outputs).
     *
     * <p>If a callable returns {@code null}, its output is discarded. If a worker throws an exception for any reason -
     * including because the {@code classifier}, {@code mapper}, or callable threw an exception - the remaining workers
     * are interrupted and awaited, and all elements starting with the first incomplete output are cleared from the
     * buffer.
     *
     * <p>The segue's source can be safely polled and closed concurrently.
     *
     * <p>Example:
     * {@snippet :
     * TODO
     * }
     *
     * @param concurrency the (positive) number of worker threads
     * @param permitsPerPartition the (positive) maximum number of workers that can concurrently work on the same
     *                            partition
     * @param bufferLimit the (positive) maximum number of buffered elements
     * @param classifier a classifier function mapping input elements to keys
     * @param mapper a function to be applied to each input element and its partition key, returning an output-producing
     *               callable
     * @return a segue that balances computation of outputs across workers, also limiting concurrency per partition key,
     * while retaining element order.
     * @param <T> the upstream element type
     * @param <K> the key type
     * @param <U> the downstream element type
     */
    public static <T, K, U> Belt.SinkStepSource<T, U> mapBalancePartitioned(int concurrency,
                                                                            int permitsPerPartition,
                                                                            int bufferLimit,
                                                                            Function<? super T, ? extends K> classifier,
                                                                            BiFunction<? super T, ? super K, ? extends Callable<? extends U>> mapper) {
        if (concurrency < 1 || permitsPerPartition < 1 || bufferLimit < 1) {
            throw new IllegalArgumentException("concurrency, permitsPerPartition, and bufferLimit must be positive");
        }
        Objects.requireNonNull(classifier);
        Objects.requireNonNull(mapper);
        
        if (permitsPerPartition >= concurrency) {
            return mapBalanceOrdered(concurrency, bufferLimit, t -> mapper.apply(t, classifier.apply(t)));
        }
        
        class Partition {
            // Only use buffer if we have no permits left
            final Deque<MapBalance.Item<T>> buffer = new LinkedList<>();
            int permits = permitsPerPartition;
        }
        
        class MapBalancePartitioned extends MapBalance<T, U> {
            final Map<K, Partition> partitionByKey = new HashMap<>();
            
            MapBalancePartitioned(int concurrency, int bufferLimit) {
                super(concurrency, bufferLimit);
            }
            
            class Worker implements Belt.Sink<T> {
                @Override
                public boolean drainFromSource(Belt.StepSource<? extends T> source) throws Exception {
                    K key = null;
                    Item<T> item = null;
                    Callable<? extends U> callable = null;
                    Throwable exception = null;
                    
                    for (;;) {
                        try {
                            if (item == null) {
                                sourceLock.lockInterruptibly();
                                try {
                                    T in = source.poll();
                                    if (in == null) {
                                        return true;
                                    }
                                    key = Objects.requireNonNull(classifier.apply(in));
                                    item = new Item<>(key, in);
                                    
                                    lock.lockInterruptibly();
                                    try {
                                        if (state >= COMPLETING) {
                                            return false;
                                        }
                                        while (completionBuffer.size() == bufferLimit) {
                                            completionNotFull.await();
                                            if (state >= COMPLETING) {
                                                return false;
                                            }
                                        }
                                        completionBuffer.offer(item);
                                        Partition partition = partitionByKey.computeIfAbsent(key, k -> new Partition());
                                        if (partition.permits > 0) {
                                            partition.permits--;
                                        } else {
                                            partition.buffer.offer(item);
                                            key = null;
                                            item = null;
                                            continue;
                                        }
                                        callable = mapper.apply(in, key);
                                    } finally {
                                        lock.unlock();
                                    }
                                } finally {
                                    sourceLock.unlock();
                                }
                            }
                            item.out = callable.call();
                        } catch (Throwable e) {
                            exception = e;
                        } finally {
                            if (item != null) {
                                lock.lock();
                                try {
                                    for (;;) {
                                        item.in = null;
                                        if (item == completionBuffer.peek()) {
                                            outputReady.signal();
                                        }
                                        Partition partition = partitionByKey.get(key);
                                        key = null;
                                        item = null;
                                        callable = null;
                                        if (exception == null && (item = partition.buffer.poll()) != null) {
                                            @SuppressWarnings("unchecked")
                                            K k = key = (K) item.out;
                                            try {
                                                callable = mapper.apply(item.in, key);
                                            } catch (Throwable e) {
                                                exception = e;
                                                continue;
                                            }
                                        } else if (++partition.permits == permitsPerPartition) {
                                            partitionByKey.remove(key);
                                        }
                                        break;
                                    }
                                } finally {
                                    lock.unlock();
                                }
                            }
                            throwAsException(exception);
                        }
                    }
                }
            }
            
            @Override
            Belt.Sink<T> newWorker() {
                return new Worker();
            }
        }
        
        return new MapBalancePartitioned(concurrency, bufferLimit);
    }
    
    /**
     * Returns a segue that balances computation of outputs across a limited number of concurrent workers, and orders
     * outputs based on input arrival. The number of concurrent workers created when draining is controlled by the given
     * {@code concurrency}.
     *
     * <p>When a worker polls an element, it adds the element to a pending output buffer, first waiting until the buffer
     * is not full (size is below {@code bufferLimit}). Then, the worker will begin work on the element, calling the
     * {@code mapper} and then the resultant callable, before signaling a completed output.
     *
     * <p>The {@code mapper} is invoked on elements in the order they arrived. Output elements are polled from the
     * buffer in the order that their associated input elements arrived (so completed outputs may be held up by earlier
     * incomplete outputs).
     *
     * <p>If a callable returns {@code null}, its output is discarded. If a worker throws an exception for any reason -
     * including because the {@code mapper} or callable threw an exception - the remaining workers are interrupted and
     * awaited, and all elements starting with the first incomplete output are cleared from the buffer.
     *
     * <p>The segue's source can be safely polled and closed concurrently.
     *
     * <p>Example:
     * {@snippet :
     * TODO
     * }
     *
     * @param concurrency the (positive) number of worker threads
     * @param bufferLimit the (positive) maximum number of buffered elements
     * @param mapper a function to be applied to each input element, returning an output-producing callable
     * @return a segue that balances computation of outputs across workers, while retaining element order
     * @param <T> the upstream element type
     * @param <U> the downstream element type
     */
    public static <T, U> Belt.SinkStepSource<T, U> mapBalanceOrdered(int concurrency,
                                                                     int bufferLimit,
                                                                     Function<? super T, ? extends Callable<? extends U>> mapper) {
        if (concurrency < 1 || bufferLimit < 1) {
            throw new IllegalArgumentException("concurrency and bufferLimit must be positive");
        }
        Objects.requireNonNull(mapper);
        
        class MapBalanceOrdered extends MapBalance<T, U> {
            MapBalanceOrdered(int concurrency, int bufferLimit) {
                super(concurrency, bufferLimit);
            }
            
            class Worker implements Belt.Sink<T> {
                @Override
                public boolean drainFromSource(Belt.StepSource<? extends T> source) throws Exception {
                    for (;;) {
                        Item<T> item = null;
                        Callable<? extends U> callable;
                        
                        try {
                            sourceLock.lockInterruptibly();
                            try {
                                T in = source.poll();
                                if (in == null) {
                                    return true;
                                }
                                item = new Item<>(null, in);
                                
                                lock.lockInterruptibly();
                                try {
                                    if (state >= COMPLETING) {
                                        return false;
                                    }
                                    while (completionBuffer.size() == bufferLimit) {
                                        completionNotFull.await();
                                        if (state >= COMPLETING) {
                                            return false;
                                        }
                                    }
                                    completionBuffer.offer(item);
                                    callable = mapper.apply(in);
                                } finally {
                                    lock.unlock();
                                }
                            } finally {
                                sourceLock.unlock();
                            }
                            item.out = callable.call();
                        } finally {
                            if (item != null) {
                                lock.lock();
                                try {
                                    item.in = null;
                                    if (item == completionBuffer.peek()) {
                                        outputReady.signal();
                                    }
                                } finally {
                                    lock.unlock();
                                }
                            }
                        }
                    }
                }
            }
            
            @Override
            Belt.Sink<T> newWorker() {
                return new Worker();
            }
        }
        
        return new MapBalanceOrdered(concurrency, bufferLimit);
    }
    
    /**
     * Returns an operator that balances computation of outputs across a limited number of concurrent workers. The
     * resultant downstream source yields output elements as they complete, which may not match the order that inputs
     * arrived. Any {@code null} outputs are discarded.
     *
     * <p>This operator assumes that the upstream source supports concurrent polling, and any downstream sink supports
     * concurrent offering. If necessary, this support can be patched-in using the {@link #synchronizeStepSource} and
     * {@link #synchronizeStepSink} operators.
     *
     * <p>Example:
     * {@snippet :
     * TODO
     * }
     *
     * @param concurrency the (positive) number of worker threads
     * @return an operator that balances computation of outputs across workers
     * @param <T> the output element type
     */
    public static <T> Belt.StepToSourceOperator<Callable<T>, T> balanceMergeSource(int concurrency) {
        if (concurrency < 1) {
            throw new IllegalArgumentException("concurrency must be positive");
        }
        
        return source -> {
            Objects.requireNonNull(source);
            Belt.StepSource<T> worker = () -> {
                for (Callable<T> c; (c = source.poll()) != null; ) {
                    var t = c.call();
                    if (t != null) { // Skip nulls
                        return t;
                    }
                }
                return null;
            };
            return merge(IntStream.range(0, concurrency).mapToObj(i -> worker).toList())
                .andThen(alsoClose(source));
        };
    }
    
    /**
     * Returns an operator that balances computation of outputs across a limited number of concurrent workers. The
     * resultant upstream sink offers output elements downstream as they complete, which may not match the order that
     * inputs arrived. Any {@code null} outputs are discarded.
     *
     * <p>This operator assumes that the downstream sink supports concurrent offering, and any upstream source supports
     * concurrent polling. If necessary, this support can be patched-in using the {@link #synchronizeStepSink} and
     * {@link #synchronizeStepSource} operators.
     *
     * <p>Example:
     * {@snippet :
     * TODO
     * }
     *
     * @param concurrency the (positive) number of worker threads
     * @return an operator that balances computation of outputs across workers
     * @param <T> the output element type
     */
    public static <T> Belt.SinkToStepOperator<Callable<T>, T> balanceMergeSink(int concurrency) {
        if (concurrency < 1) {
            throw new IllegalArgumentException("concurrency must be positive");
        }
        
        return sink -> {
            Objects.requireNonNull(sink);
            Belt.StepSink<Callable<T>> worker = c -> {
                var t = c.call();
                return t == null || sink.offer(t); // Skip nulls
            };
            return balance(IntStream.range(0, concurrency).mapToObj(i -> worker).toList())
                .compose(alsoComplete(sink));
        };
    }
    
    /**
     * Returns a fan-out sink that forwards each input element to the first available sink among the given
     * {@code sinks}.
     *
     * <p>This sink assumes that any upstream source supports concurrent polling. If necessary, this support can be
     * patched-in using the {@link #synchronizeStepSource} operator.
     *
     * <p>Example:
     * {@snippet :
     * TODO
     * }
     *
     * @param sinks the sinks
     * @return a fan-out sink that forwards each input element to the first available sink among the given {@code sinks}
     * @param <T> the element type
     */
    public static <T> Belt.Sink<T> balance(Collection<? extends Belt.Sink<? super T>> sinks) {
        var theSinks = List.copyOf(sinks);
        
        class Balance extends ProxySink<T> {
            @Override
            public boolean drainFromSource(Belt.StepSource<? extends T> source) throws InterruptedException, ExecutionException {
                try (var scope = new StructuredTaskScope.ShutdownOnFailure("balance-drainFromSource",
                                                                           Thread.ofVirtual().name("thread-", 0).factory())) {
                    var tasks = theSinks.stream()
                        .map(sink -> scope.fork(() -> sink.drainFromSource(source)))
                        .toList();
                    scope.join().throwIfFailed();
                    return tasks.stream().anyMatch(StructuredTaskScope.Subtask::get);
                }
            }
            
            @Override
            protected Stream<? extends Belt.Sink<?>> sinks() {
                return theSinks.stream();
            }
        }
        
        return new Balance();
    }
    
    /**
     * Returns a fan-out sink that forwards each input element to all the given {@code sinks}, until any sink cancels.
     *
     * <p>Example:
     * {@snippet :
     * TODO
     * }
     *
     * @param sinks the sinks
     * @return a fan-out sink that forwards each input element to all the given {@code sinks}, until any sink cancels
     * @param <T> the element type
     */
    public static <T> Belt.StepSink<T> broadcast(Collection<? extends Belt.StepSink<? super T>> sinks) {
        var theSinks = List.copyOf(sinks);
        
        class Broadcast extends ProxySink<T> implements Belt.StepSink<T> {
            boolean draining = true;
            
            @Override
            public boolean offer(T input) throws Exception {
                if (!draining) {
                    return false;
                }
                for (var sink : theSinks) {
                    draining &= sink.offer(input);
                }
                return draining;
            }
            
            @Override
            protected Stream<? extends Belt.Sink<?>> sinks() {
                return theSinks.stream();
            }
        }
        
        return new Broadcast();
    }
    
    /**
     * Returns a fan-out sink that selectively offers zero or more replacement elements to the given {@code sinks}, per
     * input element. The {@code router} is applied to each input element in conjunction with a
     * {@link BiConsumer biConsumer} that accepts the index of a sink in the given {@code sinks}, and a replacement
     * element. The {@code router} calls the biConsumer zero or more times to provide the replacement elements.
     *
     * <p>Example:
     * {@snippet :
     * TODO
     * }
     *
     * @param router a function that offers replacement elements to selected sinks
     * @param eagerCancel if {@code true}, cancels when any sink cancels; else cancels when all sinks cancel
     * @param sinks the sinks
     * @return a fan-out sink that selectively offers zero or more replacement elements to each of the given
     * {@code sinks}
     * @param <T> the upstream element type
     * @param <U> the downstream element type
     */
    public static <T, U> Belt.StepSink<T> route(BiConsumer<? super T, ? super BiConsumer<Integer, U>> router,
                                                boolean eagerCancel,
                                                List<? extends Belt.StepSink<? super U>> sinks) {
        Objects.requireNonNull(router);
        var theSinks = List.copyOf(sinks);
        BitSet active = new BitSet(theSinks.size());
        active.set(0, theSinks.size(), true);
        BiConsumer<Integer, U> pusher = (i, u) -> {
            try {
                var sink = theSinks.get(i); // Note: Can throw IOOBE
                if (active.get(i) && !sink.offer(u)) {
                    active.clear(i);
                }
            } catch (Error | RuntimeException e) {
                throw e;
            } catch (Exception e) {
                // We are not allowed to throw checked exceptions in this context;
                // wrap them so that we might rediscover them farther up the stack.
                // (They might still be dropped or re-wrapped between here and there.)
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                throw new WrappingException(e);
            }
        };
        
        class Route extends ProxySink<T> implements Belt.StepSink<T> {
            boolean done = false;
            
            @Override
            public boolean offer(T input) throws Exception {
                if (done) {
                    return false;
                }
                try {
                    router.accept(input, pusher); // Note: User-defined callback can throw exception
                } catch (WrappingException e) {
                    if (e.getCause() instanceof InterruptedException) {
                        Thread.interrupted();
                    }
                    throw e.getCause();
                } finally {
                    done = eagerCancel ? active.cardinality() < theSinks.size() : active.isEmpty();
                }
                return !done;
            }
            
            @Override
            protected Stream<? extends Belt.Sink<?>> sinks() {
                return theSinks.stream();
            }
        }
        
        return new Route();
    }
    
    /**
     * Returns a fan-in source that yields all elements from each of the given {@code sources}, in turn.
     *
     * <p>Example:
     * {@snippet :
     * TODO
     * }
     *
     * @param sources the sources
     * @return a fan-in source that yields all elements from each of the given {@code sources}, in turn
     * @param <T> the element type
     */
    public static <T> Belt.StepSource<T> concatStep(Collection<? extends Belt.StepSource<? extends T>> sources) {
        var theSources = List.copyOf(sources);
        
        class ConcatStep extends ProxySource<T> implements Belt.StepSource<T> {
            int i = 0;
            
            @Override
            public T poll() throws Exception {
                for (; i < theSources.size(); i++) {
                    T t = theSources.get(i).poll();
                    if (t != null) {
                        return t;
                    }
                }
                return null;
            }
            
            @Override
            protected Stream<? extends Belt.Source<?>> sources() {
                return theSources.stream();
            }
        }
        
        return new ConcatStep();
    }
    
    /**
     * Returns a fan-in source that yields all elements from each of the given {@code sources}, in turn.
     *
     * <p>Example:
     * {@snippet :
     * TODO
     * }
     *
     * @param sources the sources
     * @return a fan-in source that yields all elements from each of the given {@code sources}, in turn
     * @param <T> the element type
     */
    public static <T> Belt.Source<T> concat(Collection<? extends Belt.Source<? extends T>> sources) {
        var theSources = List.copyOf(sources);
        
        class Concat extends ProxySource<T> {
            @Override
            public boolean drainToSink(Belt.StepSink<? super T> sink) throws Exception {
                for (var source : theSources) {
                    if (!source.drainToSink(sink)) {
                        return false;
                    }
                }
                return true;
            }
            
            @Override
            protected Stream<? extends Belt.Source<?>> sources() {
                return theSources.stream();
            }
        }
        
        return new Concat();
    }
    
    /**
     * Returns a fan-in source that yields each output element from the first available source among the given
     * {@code sources}.
     *
     * <p>This source assumes that any downstream sink supports concurrent offering. If necessary, this support can be
     * patched-in using the {@link #synchronizeStepSink} operator.
     *
     * <p>Example:
     * {@snippet :
     * TODO
     * }
     *
     * @param sources the sources
     * @return a fan-in source that yields each output element from the first available source among the given
     * {@code sources}
     * @param <T> the element type
     */
    public static <T> Belt.Source<T> merge(Collection<? extends Belt.Source<? extends T>> sources) {
        var theSources = List.copyOf(sources);
        
        class Merge extends ProxySource<T> {
            @Override
            public boolean drainToSink(Belt.StepSink<? super T> sink) throws InterruptedException, ExecutionException {
                try (var scope = new StructuredTaskScope.ShutdownOnFailure("merge-drainToSink",
                                                                           Thread.ofVirtual().name("thread-", 0).factory())) {
                    var tasks = theSources.stream()
                        .map(source -> scope.fork(() -> source.drainToSink(sink)))
                        .toList();
                    scope.join().throwIfFailed();
                    return tasks.stream().allMatch(StructuredTaskScope.Subtask::get);
                }
            }
            
            @Override
            protected Stream<? extends Belt.Source<?>> sources() {
                return theSources.stream();
            }
        }
        
        return new Merge();
    }
    
    /**
     * Returns a fan-in source that yields the minimum of the latest elements from each of the given {@code sources}.
     * The minimum element is determined by the {@code comparator}.
     *
     * <p>Example:
     * {@snippet :
     * TODO
     * }
     *
     * @param comparator the comparator used to sort elements
     * @param sources the sources
     * @return a fan-in source that yields the minimum of the latest elements from each of the given {@code sources}
     * @param <T> the element type
     */
    public static <T> Belt.StepSource<T> mergeSorted(Comparator<? super T> comparator,
                                                     Collection<? extends Belt.StepSource<? extends T>> sources) {
        Objects.requireNonNull(comparator);
        var theSources = List.copyOf(sources);
        
        class MergeSorted extends ProxySource<T> implements Belt.StepSource<T> {
            final PriorityQueue<Indexed<T>> latest = new PriorityQueue<>(theSources.size(), Comparator.comparing(i -> i.element, comparator));
            int lastIndex = NEW;
            
            static final int NEW       = -1;
            static final int COMPLETED = -2;
            
            @Override
            public T poll() throws Exception {
                if (lastIndex <= NEW) {
                    if (lastIndex == COMPLETED) {
                        return null;
                    }
                    // First poll - poll all sources to bootstrap the queue
                    for (int i = 0; i < theSources.size(); i++) {
                        var t = theSources.get(i).poll();
                        if (t != null) {
                            latest.offer(new Indexed<>(t, i));
                        }
                    }
                } else {
                    // Subsequent poll - poll from the source that last emitted
                    var t = theSources.get(lastIndex).poll();
                    if (t != null) {
                        latest.offer(new Indexed<>(t, lastIndex));
                    }
                }
                Indexed<T> min = latest.poll();
                if (min != null) {
                    lastIndex = min.index;
                    return min.element;
                }
                lastIndex = COMPLETED;
                return null;
            }
            
            @Override
            protected Stream<? extends Belt.Source<?>> sources() {
                return theSources.stream();
            }
        }
        
        return new MergeSorted();
    }
    
    /**
     * Returns a fan-in source that combines the latest elements from both sources once both sources yield a new
     * element. The latest elements are combined using the given {@code combiner}, and the result is yielded.
     *
     * <p>Example:
     * {@snippet :
     * TODO
     * }
     *
     * @param source1 the first source
     * @param source2 the second source
     * @param combiner a function used to combine elements from each source
     * @return a fan-in source that combines the latest elements from both sources once both sources yield a new element
     * @param <T1> the first source's element type
     * @param <T2> the second source's element type
     * @param <T> the combined element type
     */
    public static <T1, T2, T> Belt.StepSource<T> zip(Belt.StepSource<? extends T1> source1,
                                                     Belt.StepSource<? extends T2> source2,
                                                     BiFunction<? super T1, ? super T2, ? extends T> combiner) {
        Objects.requireNonNull(source1);
        Objects.requireNonNull(source2);
        Objects.requireNonNull(combiner);
        
        class Zip extends ProxySource<T> implements Belt.StepSource<T> {
            boolean done = false;
            
            @Override
            public T poll() throws Exception {
                if (done) {
                    return null;
                }
                T1 e1 = source1.poll();
                T2 e2 = source2.poll();
                if (e1 == null || e2 == null) {
                    done = true;
                    return null;
                }
                return Objects.requireNonNull(combiner.apply(e1, e2));
            }
            
            @Override
            protected Stream<? extends Belt.Source<?>> sources() {
                return Stream.of(source1, source2);
            }
        }
        
        return new Zip();
    }
    
    /**
     * Returns a fan-in source that combines the latest elements from both sources once either source yields a new
     * element. The latest elements are combined using the given {@code combiner}, and the result is yielded.
     *
     * <p>Example:
     * {@snippet :
     * TODO
     * }
     *
     * @param source1 the first source
     * @param source2 the second source
     * @param combiner a function used to combine elements from each source
     * @return a fan-in source that combines the latest elements from both sources once either source yields a new
     * element
     * @param <T1> the first source's element type
     * @param <T2> the second source's element type
     * @param <T> the combined element type
     */
    public static <T1, T2, T> Belt.Source<T> zipLatest(Belt.Source<? extends T1> source1,
                                                       Belt.Source<? extends T2> source2,
                                                       BiFunction<? super T1, ? super T2, ? extends T> combiner) {
        Objects.requireNonNull(source1);
        Objects.requireNonNull(source2);
        Objects.requireNonNull(combiner);
        
        class ZipLatest extends ProxySource<T> {
            T1 latest1 = null;
            T2 latest2 = null;
            
            @Override
            public boolean drainToSink(Belt.StepSink<? super T> sink) throws InterruptedException, ExecutionException {
                ReentrantLock lock = new ReentrantLock();
                Condition ready = lock.newCondition();
                
                abstract class HelperSink<X, Y> implements Belt.StepSink<X> {
                    @Override
                    public boolean offer(X e) throws Exception {
                        Objects.requireNonNull(e);
                        lock.lockInterruptibly();
                        try {
                            if (setLatest1(e) == null) {
                                if (getLatest2() == null) {
                                    // Wait until we have the first element from both sources
                                    do {
                                        ready.await();
                                    } while (getLatest2() == null);
                                    return true; // First emission handled by other thread
                                }
                                ready.signal();
                            }
                            T t = combiner.apply(latest1, latest2);
                            return sink.offer(t);
                        } finally {
                            lock.unlock();
                        }
                    }
                    
                    abstract X setLatest1(X x);
                    abstract Y getLatest2();
                }
                class HelperSink1 extends HelperSink<T1, T2> {
                    @Override T1 setLatest1(T1 t) { var r = latest1; latest1 = t; return r; }
                    @Override T2 getLatest2() { return latest2; }
                }
                class HelperSink2 extends HelperSink<T2, T1> {
                    @Override T2 setLatest1(T2 t) { var r = latest2; latest2 = t; return r; }
                    @Override T1 getLatest2() { return latest1; }
                }
                
                try (var scope = new StructuredTaskScope.ShutdownOnFailure("zipLatest-drainToSink",
                                                                           Thread.ofVirtual().name("thread-", 0).factory())) {
                    var task1 = scope.fork(() -> source1.drainToSink(new HelperSink1()));
                    var task2 = scope.fork(() -> source2.drainToSink(new HelperSink2()));
                    scope.join().throwIfFailed();
                    return task1.get() && task2.get();
                }
            }
            
            @Override
            protected Stream<? extends Belt.Source<?>> sources() {
                return Stream.of(source1, source2);
            }
        }
        
        return new ZipLatest();
    }
    
    /**
     * Returns a segue in which each offer to the sink must wait for a corresponding poll from the source, and vice
     * versa. In many cases, the {@link #buffer buffer} method is preferable to this method, as buffering provides
     * leeway for an upstream and downstream to temporarily proceed at different rates.
     *
     * <p>The segue's sink can be safely offered to and completed concurrently. Its source can be safely polled and
     * closed concurrently.
     *
     * <p>Example:
     * {@snippet :
     * TODO
     * }
     *
     * @return a segue in which each offer to the sink must wait for a corresponding poll from the source, and vice
     * versa
     * @param <T> the element type
     */
    public static <T> Belt.StepSegue<T, T> rendezvous() {
        class Rendezvous implements Belt.StepSegue<T, T> {
            final ReentrantLock sinkLock = new ReentrantLock();
            final ReentrantLock lock = new ReentrantLock();
            final Condition given = lock.newCondition();
            final Condition taken = lock.newCondition();
            Throwable exception = null;
            T element = null;
            int state = RUNNING;
            
            private static final int RUNNING    = 0;
            private static final int COMPLETING = 1;
            private static final int CLOSED     = 2;
            
            class Sink implements Belt.StepSink<T> {
                @Override
                public boolean offer(T input) throws Exception {
                    Objects.requireNonNull(input);
                    sinkLock.lockInterruptibly(); // Prevent overwriting offers
                    try {
                        lock.lockInterruptibly();
                        try {
                            if (state >= COMPLETING) {
                                return false;
                            }
                            element = input;
                            given.signal();
                            do {
                                taken.await();
                                if (state >= COMPLETING) {
                                    return element == null;
                                }
                            } while (element != null);
                            return true;
                        } finally {
                            lock.unlock();
                        }
                    } finally {
                        sinkLock.unlock();
                    }
                }
                
                @Override
                public void complete() throws Exception {
                    lock.lockInterruptibly();
                    try {
                        if (state >= COMPLETING) {
                            return;
                        }
                        state = COMPLETING;
                        taken.signalAll();
                    } finally {
                        lock.unlock();
                    }
                }
                
                @Override
                public void completeAbruptly(Throwable cause) {
                    lock.lock();
                    try {
                        if (state == CLOSED) {
                            return;
                        }
                        state = CLOSED;
                        exception = cause == null ? NULL_EXCEPTION : cause;
                        taken.signalAll();
                        given.signalAll();
                    } finally {
                        lock.unlock();
                    }
                }
            }
            
            class Source implements Belt.StepSource<T> {
                @Override
                public T poll() throws Exception {
                    lock.lockInterruptibly();
                    try {
                        if (state == CLOSED) {
                            if (exception != null) {
                                throw new UpstreamException(exception == NULL_EXCEPTION ? null : exception);
                            }
                            return null;
                        }
                        while (element == null) {
                            given.await();
                            if (state == CLOSED) {
                                if (exception != null) {
                                    throw new UpstreamException(exception == NULL_EXCEPTION ? null : exception);
                                }
                                return null;
                            }
                        }
                        T ret = element;
                        element = null;
                        taken.signal();
                        return ret;
                    } finally {
                        lock.unlock();
                    }
                }
                
                @Override
                public void close() {
                    lock.lock();
                    try {
                        if (state == CLOSED) {
                            return;
                        }
                        state = CLOSED;
                        taken.signalAll();
                        given.signalAll();
                    } finally {
                        lock.unlock();
                    }
                }
            }
            
            @Override
            public Belt.StepSink<T> sink() {
                return new Sink();
            }
            
            @Override
            public Belt.StepSource<T> source() {
                return new Source();
            }
        }
        
        return new Rendezvous();
    }
    
    /**
     * Returns a segue that buffers offered elements to respond to later polls. If the buffer is empty, polls will
     * block. If the buffer is full (size equals {@code bufferLimit}), offers will block. Buffering provides leeway for
     * an upstream and downstream to temporarily proceed at different rates.
     *
     * <p>The segue's sink can be safely offered to and completed concurrently. Its source can be safely polled and
     * closed concurrently.
     *
     * <p>Example:
     * {@snippet :
     * TODO
     * }
     *
     * @param bufferLimit the (positive) maximum number of buffered elements
     * @return a segue that buffers offered elements to respond to later polls
     * @param <T> the element type
     */
    public static <T> Belt.StepSegue<T ,T> buffer(int bufferLimit) {
        if (bufferLimit < 1) {
            throw new IllegalArgumentException("bufferLimit must be positive");
        }
        
        class Buffer extends DeadlineSegue<T, T> {
            Deque<T> queue = null;
            boolean done = false;
            
            @Override
            protected void onInit(DeadlineSegue.SinkController ctl) {
                queue = new ArrayDeque<>(bufferLimit);
            }
            
            @Override
            protected void onOffer(DeadlineSegue.SinkController ctl, T input) {
                queue.offer(input);
                ctl.latchPollDeadline(Instant.MIN);
                if (queue.size() >= bufferLimit) {
                    ctl.latchOfferDeadline(Instant.MAX);
                }
            }
            
            @Override
            protected void onPoll(DeadlineSegue.SourceController<T> ctl) {
                T head = queue.poll();
                if (head != null) {
                    ctl.latchOfferDeadline(Instant.MIN);
                    ctl.latchOutput(head);
                    if (queue.peek() != null) {
                        return;
                    } else if (!done) {
                        ctl.latchPollDeadline(Instant.MAX);
                        return;
                    } // else fall-through
                }
                ctl.latchClose();
            }
            
            @Override
            protected void onComplete(DeadlineSegue.SinkController ctl) {
                done = true;
                ctl.latchPollDeadline(Instant.MIN);
            }
        }
        
        return new Buffer();
    }
    
    /**
     * Returns a segue that injects additional elements - derived from the last buffered element - to respond to a poll
     * if no elements are buffered. Injection stops once all injected elements are yielded, or a subsequent element is
     * buffered.
     *
     * <p>The segue's sink can be safely offered to and completed concurrently. Its source can be safely polled and
     * closed concurrently.
     *
     * <p>Example:
     * {@snippet :
     * TODO
     * }
     *
     * @param initial an initial element to seed the buffer; ignored if {@code null}
     * @param mapper a function that expands an element to an iterator of zero or more elements to inject
     * @param bufferLimit the (positive) maximum number of buffered elements
     * @return a segue that injects additional elements - derived from the last buffered element - to respond to a poll
     * if no elements are buffered
     * @param <T> the element type
     */
    public static <T> Belt.StepSegue<T, T> extrapolate(T initial,
                                                       Function<? super T, ? extends Iterator<? extends T>> mapper,
                                                       int bufferLimit) {
        Objects.requireNonNull(mapper);
        if (bufferLimit < 1) {
            throw new IllegalArgumentException("bufferLimit must be positive");
        }
        
        class Extrapolate extends DeadlineSegue<T, T> {
            T prev = null;
            Deque<T> queue = null;
            Iterator<? extends T> iter = Collections.emptyIterator();
            boolean done = false;
            
            @Override
            protected void onInit(DeadlineSegue.SinkController ctl) {
                queue = new ArrayDeque<>(bufferLimit);
                if (initial != null) {
                    queue.offer(initial);
                    ctl.latchPollDeadline(Instant.MIN);
                } else {
                    ctl.latchPollDeadline(Instant.MAX);
                }
            }
            
            @Override
            protected void onOffer(DeadlineSegue.SinkController ctl, T input) {
                prev = null;
                iter = Collections.emptyIterator();
                queue.offer(input);
                ctl.latchPollDeadline(Instant.MIN);
                if (queue.size() >= bufferLimit) {
                    ctl.latchOfferDeadline(Instant.MAX);
                }
            }
            
            @Override
            protected void onPoll(DeadlineSegue.SourceController<T> ctl) {
                T head = queue.poll();
                if (head != null) {
                    ctl.latchOfferDeadline(Instant.MIN);
                    ctl.latchOutput(head);
                    if (!done) {
                        prev = head;
                    } else {
                        ctl.latchClose();
                    }
                } else if (!done) {
                    if ((head = prev) != null) {
                        prev = null;
                        iter = Objects.requireNonNull(mapper.apply(head));
                    }
                    if (iter.hasNext()) {
                        ctl.latchOutput(iter.next());
                    } else {
                        ctl.latchPollDeadline(Instant.MAX);
                    }
                } else {
                    ctl.latchClose();
                }
            }
            
            @Override
            protected void onComplete(DeadlineSegue.SinkController ctl) {
                done = true;
                ctl.latchPollDeadline(Instant.MIN);
            }
        }
        
        return new Extrapolate();
    }
    
    /**
     * Returns a segue that accumulates offered elements into a batch, permitting the batch to be yielded once a
     * deadline expires. The deadline is recalculated from the current batch after each accumulation.
     *
     * <p>If the calculated deadline is an empty {@code Optional}, the previous deadline is retained (initially
     * {@link Instant#MAX}). If the calculated deadline is a present {@code Optional} containing {@link Instant#MIN},
     * subsequent offers will block until the current batch is yielded.
     *
     * <p>The segue's sink can be safely offered to and completed concurrently. Its source can be safely polled and
     * closed concurrently.
     *
     * <p>Example:
     * {@snippet :
     * TODO
     * }
     *
     * @param batchSupplier a function that creates an initial (non-{@code null}) batch, called on initial offer and on
     *                      offers following a poll
     * @param accumulator a function that folds an offered element into the current batch
     * @param deadlineMapper a function that optionally calculates a deadline for the current batch after accumulation
     * @return a segue that accumulates offered elements into a batch, permitting the batch to be yielded once a
     * deadline expires
     * @param <T> the upstream element type
     * @param <A> the batch type
     */
    public static <T, A> Belt.StepSegue<T, A> batch(Supplier<? extends A> batchSupplier,
                                                    BiConsumer<? super A, ? super T> accumulator,
                                                    Function<? super A, Optional<Instant>> deadlineMapper) {
        Objects.requireNonNull(batchSupplier);
        Objects.requireNonNull(accumulator);
        Objects.requireNonNull(deadlineMapper);
        
        class Batch extends DeadlineSegue<T, A> {
            A batch = null;
            boolean done = false;
            
            @Override
            protected void onInit(DeadlineSegue.SinkController ctl) { }
            
            @Override
            protected void onOffer(DeadlineSegue.SinkController ctl, T input) {
                A b = batch;
                if (b == null) {
                    b = Objects.requireNonNull(batchSupplier.get());
                }
                accumulator.accept(b, input);
                Instant deadline = deadlineMapper.apply(b).orElse(null);
                batch = b; // No more exception risk -- assign batch
                if (deadline != null) {
                    ctl.latchPollDeadline(deadline);
                    if (deadline == Instant.MIN) {
                        // Alternative implementations might adjust or reset the buffer instead of blocking
                        ctl.latchOfferDeadline(Instant.MAX);
                    }
                }
            }
            
            @Override
            protected void onPoll(DeadlineSegue.SourceController<A> ctl) {
                if (done) {
                    ctl.latchClose();
                    if (batch == null) {
                        return;
                    }
                }
                ctl.latchOutput(batch);
                batch = null;
                ctl.latchPollDeadline(Instant.MAX);
                ctl.latchOfferDeadline(Instant.MIN);
            }
            
            @Override
            protected void onComplete(DeadlineSegue.SinkController ctl) {
                done = true;
                ctl.latchPollDeadline(Instant.MIN);
            }
        }
        
        return new Batch();
    }
    
    /**
     * Returns a segue that uses a token-bucketing scheme to limit the rate at which offered elements can be yielded.
     * In this scheme, each offered element calculates a cost, in tokens, to yield that element. Tokens accrue at a
     * fixed rate, given by {@code tokenInterval}. The initial and maximum number of unused tokens is given by
     * {@code tokenLimit} - tokens that accrue beyond this maximum are discarded. Polls return when there are buffered
     * elements and sufficient unused tokens to yield the element at the front of the buffer, using up those tokens.
     * Elements that cost more than the {@code tokenLimit} temporarily increase the limit to match their cost when they
     * reach the front of the buffer.
     *
     * <p>If an offered element calculates a negative cost, an {@link IllegalStateException} is thrown from the offer.
     * If an offered element would cause the total cost of buffered elements to exceed {@link Long#MAX_VALUE}, an
     * {@link ArithmeticException} is thrown from the offer.
     *
     * <p>Note that when insufficient tokens are available to yield the next element, polls enter timed waits that may
     * have limited resolution, typically in the range of milliseconds. This can lead to an observed emission rate
     * significantly slower than what the function arguments allow.
     *
     * <p>The segue's sink can be safely offered to and completed concurrently. Its source can be safely polled and
     * closed concurrently.
     *
     * <p>Example:
     * {@snippet :
     * TODO
     * }
     *
     * @param tokenInterval a (positive) interval at which tokens accrue
     * @param costMapper a function that calculates the (non-negative) cost, in tokens, to yield an offered element
     * @param tokenLimit a (positive) initial and maximum number of unused tokens
     * @param bufferLimit a (positive) maximum number of buffered elements
     * @return a segue that uses a token-bucketing scheme to limit the rate at which offered elements can be yielded
     * @param <T> the element type
     */
    public static <T> Belt.StepSegue<T, T> throttle(Duration tokenInterval,
                                                    ToLongFunction<? super T> costMapper,
                                                    long tokenLimit,
                                                    long bufferLimit) {
        Objects.requireNonNull(costMapper);
        if (tokenLimit < 0) {
            throw new IllegalArgumentException("tokenLimit must be non-negative");
        }
        if (bufferLimit < 1) {
            throw new IllegalArgumentException("bufferLimit must be positive");
        }
        if (!tokenInterval.isPositive()) {
            throw new IllegalArgumentException("tokenInterval must be positive");
        }
        
        long tmpTokenInterval;
        try {
            tmpTokenInterval = tokenInterval.toNanos();
        } catch (ArithmeticException e) {
            tmpTokenInterval = Long.MAX_VALUE; // Unreasonable but correct
        }
        long tokenIntervalNanos = tmpTokenInterval;
        
        class Throttle extends DeadlineSegue<T, T> {
            Deque<Weighted<T>> queue = null;
            long tempTokenLimit = 0;
            long tokens = tokenLimit;
            long totalCost = 0;
            Instant lastObservedAccrual;
            boolean done = false;
            
            @Override
            protected void onInit(DeadlineSegue.SinkController ctl) {
                queue = new ArrayDeque<>();
                lastObservedAccrual = clock().instant();
            }
            
            @Override
            protected void onOffer(DeadlineSegue.SinkController ctl, T input) {
                long elementCost = costMapper.applyAsLong(input);
                if (elementCost < 0) {
                    throw new IllegalStateException("Element cost cannot be negative");
                }
                totalCost = Math.addExact(totalCost, elementCost);
                var w = new Weighted<>(input, elementCost);
                queue.offer(w);
                if (queue.peek() == w) {
                    ctl.latchPollDeadline(Instant.MIN); // Let source-side do token math
                }
                if (queue.size() == bufferLimit) {
                    ctl.latchOfferDeadline(Instant.MAX);
                }
            }
            
            @Override
            protected void onPoll(DeadlineSegue.SourceController<T> ctl) {
                Weighted<T> head = queue.peek();
                if (head == null) {
                    ctl.latchClose();
                    return;
                }
                // Increase tokens based on actual amount of time that passed
                Instant now = clock().instant();
                long nanosSinceLastObservedAccrual = ChronoUnit.NANOS.between(lastObservedAccrual, now);
                long nanosSinceLastAccrual = nanosSinceLastObservedAccrual % tokenIntervalNanos;
                long newTokens = nanosSinceLastObservedAccrual / tokenIntervalNanos;
                if (newTokens > 0) {
                    lastObservedAccrual = now.minusNanos(nanosSinceLastAccrual);
                    tokens = Math.min(tokens + newTokens, Math.max(tokenLimit, tempTokenLimit));
                }
                // Emit if we can, then schedule next emission
                if (tokens >= head.cost) {
                    tempTokenLimit = 0;
                    tokens -= head.cost;
                    totalCost -= head.cost;
                    queue.poll();
                    ctl.latchOfferDeadline(Instant.MIN);
                    ctl.latchOutput(head.element);
                    head = queue.peek();
                    if (head == null) {
                        if (done) {
                            ctl.latchClose();
                        } else {
                            ctl.latchPollDeadline(Instant.MAX);
                        }
                        return;
                    } else if (tokens >= head.cost) {
                        ctl.latchPollDeadline(Instant.MIN);
                        return;
                    }
                    // else tokens < head.cost; Fall-through to scheduling
                }
                // Schedule to wake up when we have enough tokens for next emission
                tempTokenLimit = head.cost;
                long tokensNeeded = head.cost - tokens;
                ctl.latchPollDeadline(now.plusNanos(tokenIntervalNanos * tokensNeeded - nanosSinceLastAccrual));
            }
            
            @Override
            protected void onComplete(DeadlineSegue.SinkController ctl) {
                done = true;
                if (queue.isEmpty()) {
                    ctl.latchPollDeadline(Instant.MIN);
                }
            }
        }
        
        return new Throttle();
    }
    
    /**
     * Returns a segue that calculates a deadline for each offered element, buffering elements at least until their
     * deadline expires. Elements with nearer deadlines may jump ahead of elements that arrived earlier.
     *
     * <p>The segue's sink can be safely offered to and completed concurrently. Its source can be safely polled and
     * closed concurrently.
     *
     * <p>Example:
     * {@snippet :
     * TODO
     * }
     *
     * @param deadlineMapper a function that calculates a deadline for an element
     * @param bufferLimit the (positive) maximum number of buffered elements
     * @return a segue that calculates a deadline for each offered element, buffering elements at least until their
     * deadline expires
     * @param <T> the element type
     */
    public static <T> Belt.StepSegue<T, T> delay(Function<? super T, Instant> deadlineMapper,
                                                 int bufferLimit) {
        Objects.requireNonNull(deadlineMapper);
        if (bufferLimit < 1) {
            throw new IllegalArgumentException("bufferLimit must be positive");
        }
        
        class Delay extends DeadlineSegue<T, T> {
            PriorityQueue<Expiring<T>> pq = null;
            boolean done = false;
            
            @Override
            protected void onInit(DeadlineSegue.SinkController ctl) {
                pq = new PriorityQueue<>(bufferLimit);
            }
            
            @Override
            protected void onOffer(DeadlineSegue.SinkController ctl, T input) {
                Instant deadline = Objects.requireNonNull(deadlineMapper.apply(input));
                Expiring<T> e = new Expiring<>(input, deadline);
                pq.offer(e);
                if (pq.peek() == e) {
                    ctl.latchPollDeadline(deadline);
                }
                if (pq.size() >= bufferLimit) {
                    ctl.latchOfferDeadline(Instant.MAX);
                }
            }
            
            @Override
            protected void onPoll(DeadlineSegue.SourceController<T> ctl) {
                Expiring<T> head = pq.poll();
                if (head == null) {
                    ctl.latchClose();
                    return;
                }
                ctl.latchOfferDeadline(Instant.MIN);
                ctl.latchOutput(head.element);
                head = pq.peek();
                if (head != null) {
                    ctl.latchPollDeadline(head.deadline);
                } else if (!done) {
                    ctl.latchPollDeadline(Instant.MAX);
                } else {
                    ctl.latchClose();
                }
            }
            
            @Override
            protected void onComplete(DeadlineSegue.SinkController ctl) {
                done = true;
                if (pq.isEmpty()) {
                    ctl.latchPollDeadline(Instant.MIN);
                }
            }
        }
       
        return new Delay();
    }
    
    /**
     * Returns a segue that injects an additional element to respond to a poll if no elements are buffered and a timeout
     * expired since the last poll. The first timeout begins when a poll meets an empty buffer.
     *
     * <p>The segue's sink can be safely offered to and completed concurrently. Its source can be safely polled and
     * closed concurrently.
     *
     * <p>Example:
     * {@snippet :
     * TODO
     * }
     *
     * @param timeout the (positive) timeout duration
     * @param extraSupplier a supplier used to generate injected elements
     * @param bufferLimit the (positive) maximum number of buffered elements
     * @return a segue that injects an additional element to respond to a poll if no elements are buffered within a
     * timeout since the last poll
     * @param <T> the element type
     */
    public static <T> Belt.StepSegue<T, T> keepAlive(Duration timeout,
                                                     Supplier<? extends T> extraSupplier,
                                                     int bufferLimit) {
        Objects.requireNonNull(timeout);
        Objects.requireNonNull(extraSupplier);
        if (bufferLimit < 1) {
            throw new IllegalArgumentException("bufferLimit must be positive");
        }
        if (!timeout.isPositive()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
        
        class KeepAlive extends DeadlineSegue<T, T> {
            Deque<T> queue = null;
            boolean done = false;
            
            @Override
            protected void onInit(DeadlineSegue.SinkController ctl) {
                queue = new ArrayDeque<>(bufferLimit);
                ctl.latchPollDeadline(clock().instant().plus(timeout));
            }
            
            @Override
            protected void onOffer(DeadlineSegue.SinkController ctl, T input) {
                queue.offer(input);
                ctl.latchPollDeadline(Instant.MIN);
                if (queue.size() >= bufferLimit) {
                    ctl.latchOfferDeadline(Instant.MAX);
                }
            }
            
            @Override
            protected void onPoll(DeadlineSegue.SourceController<T> ctl) {
                T head = queue.poll();
                if (head != null) {
                    ctl.latchOfferDeadline(Instant.MIN);
                    ctl.latchOutput(head);
                    if (queue.isEmpty() && !done) {
                        ctl.latchPollDeadline(clock().instant().plus(timeout));
                    }
                } else if (!done) {
                    ctl.latchOutput(extraSupplier.get());
                    ctl.latchPollDeadline(clock().instant().plus(timeout));
                } else {
                    ctl.latchClose();
                }
            }
            
            @Override
            protected void onComplete(DeadlineSegue.SinkController ctl) {
                done = true;
                ctl.latchPollDeadline(Instant.MIN);
            }
        }
        
        return new KeepAlive();
    }
    
    /**
     * Returns a source that yields the elements from the given {@code stream}, and closes the stream when closed.
     * When the source is first drained, it will push as many elements as possible from the stream to the sink.
     * Subsequent attempts to drain the source will short-circuit. If the stream ever yields {@code null}, the source
     * will throw a {@link NullPointerException}.
     *
     * <p>The source does not adjust the parallelism setting of the stream. In particular, if the stream
     * {@link Stream#isParallel() isParallel}, draining the source may issue concurrent offers to the sink.
     *
     * @param stream the stream to yield from
     * @return a source that yields the elements from the given {@code stream}
     * @param <T> the element type
     */
    public static <T> Belt.Source<T> streamSource(Stream<? extends T> stream) {
        Objects.requireNonNull(stream);
        
        class StreamSource implements Belt.Source<T> {
            boolean called = false;
            
            @Override
            public boolean drainToSink(Belt.StepSink<? super T> sink) throws Exception {
                if (called) {
                    return true;
                }
                called = true;
                
                try {
                    return stream.allMatch(el -> {
                        Objects.requireNonNull(el);
                        try {
                            return sink.offer(el);
                        } catch (Error | RuntimeException e) {
                            throw e;
                        } catch (Exception e) {
                            if (e instanceof InterruptedException) {
                                Thread.currentThread().interrupt();
                            }
                            throw new WrappingException(e);
                        }
                    });
                } catch (WrappingException e) {
                    if (e.getCause() instanceof InterruptedException) {
                        Thread.interrupted();
                    }
                    throw e.getCause();
                }
            }
            
            @Override
            public void close() {
                stream.close();
            }
        }
        
        return new StreamSource();
    }
    
    /**
     * Returns a source that yields the elements from the given {@code iterator}. Each time the source is polled, it
     * will advance the iterator, until the iterator is depleted. If the iterator ever yields {@code null}, the source
     * will throw a {@link NullPointerException}.
     *
     * @param iterator the iterator to yield from
     * @return a source that yields the elements from the given {@code iterator}
     * @param <T> the element type
     */
    public static <T> Belt.StepSource<T> iteratorSource(Iterator<T> iterator) {
        return iteratorSource(iterator, () -> { });
    }
    
    /**
     * Returns a source that yields the elements from the given {@code iterator}, and performs the given {@code onClose}
     * operation when closed. Each time the source is polled, it will advance the iterator, until the iterator is
     * depleted. If the iterator ever yields {@code null}, the source will throw a {@link NullPointerException}.
     *
     * @param iterator the iterator to yield from
     * @param onClose the close operation
     * @return a source that yields the elements from the given {@code iterator}
     * @param <T> the element type
     */
    public static <T> Belt.StepSource<T> iteratorSource(Iterator<T> iterator, AutoCloseable onClose) {
        Objects.requireNonNull(iterator);
        Objects.requireNonNull(onClose);
        
        class IteratorSource implements Belt.StepSource<T> {
            @Override
            public T poll() {
                return iterator.hasNext() ? Objects.requireNonNull(iterator.next()) : null;
            }
            
            @Override
            public void close() throws Exception {
                onClose.close();
            }
        }
        
        return new IteratorSource();
    }
    
    /**
     * Returns an {@link Executor Executor} that delegates execution to the given {@code scope}.
     *
     * <p>The resultant executor behaves as if defined by:
     * {@snippet :
     * Executor executor = runnable -> scope.fork(Executors.callable(runnable, null));
     * }
     *
     * @param scope the given scope
     * @return an Executor that delegates to the given scope
     * @throws NullPointerException if the scope is null
     */
    public static Executor scopeExecutor(StructuredTaskScope<?> scope) {
        Objects.requireNonNull(scope);
        return runnable -> scope.fork(Executors.callable(runnable, null));
    }
    
    /**
     * Closes each source, in encounter order. If closing any source throws an exception, the first such exception will
     * be caught and re-thrown before this method returns, with any subsequent exceptions suppressed onto it.
     *
     * @param sources the sources
     * @throws Exception if closing any source throws an exception
     */
    public static void composedClose(Stream<? extends Belt.Source<?>> sources) throws Exception {
        Throwable[] ex = { null };
        sources.sequential().forEach(source -> {
            try {
                source.close();
            } catch (Throwable e) {
                if (ex[0] == null) {
                    ex[0] = e;
                } else {
                    ex[0].addSuppressed(e);
                }
            }
        });
        throwAsException(ex[0]);
    }
    
    /**
     * Completes each sink normally, in encounter order. If completing any sink throws an exception, the first such
     * exception will be caught and re-thrown before this method returns, with any subsequent exceptions suppressed onto
     * it. If completing any sink throws an {@link InterruptedException}, the thread interrupt status will be set before
     * completing remaining sinks.
     *
     * @param sinks the sinks
     * @throws Exception if completing any sink throws an exception
     */
    public static void composedComplete(Stream<? extends Belt.Sink<?>> sinks) throws Exception {
        Throwable[] ex = { null };
        sinks.sequential().forEach(sink -> {
            try {
                sink.complete();
            } catch (Throwable e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                if (ex[0] == null) {
                    ex[0] = e;
                } else {
                    ex[0].addSuppressed(e);
                }
            }
        });
        throwAsException(ex[0]);
    }
    
    /**
     * Completes each sink abruptly, passing the given {@code cause}, in encounter order. If completing any sink throws
     * an exception, the first such exception will be caught and re-thrown before this method returns, with any
     * subsequent exceptions suppressed onto it. If completing any sink throws an {@link InterruptedException}, the
     * thread interrupt status will be set before completing remaining sinks.
     *
     * @param sinks the sinks
     * @param cause the cause of the abrupt completion
     * @throws Exception if completing any sink throws an exception
     */
    public static void composedCompleteAbruptly(Stream<? extends Belt.Sink<?>> sinks, Throwable cause) throws Exception {
        Throwable[] ex = { null };
        sinks.sequential().forEach(sink -> {
            try {
                sink.completeAbruptly(cause);
            } catch (Throwable e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                if (ex[0] == null) {
                    ex[0] = e;
                } else {
                    ex[0].addSuppressed(e);
                }
            }
        });
        throwAsException(ex[0]);
    }
    
    private static void throwAsException(Throwable ex) throws Exception {
        switch (ex) {
            case null -> { }
            case InterruptedException e -> { Thread.interrupted(); throw e; }
            case Exception e -> throw e;
            case Error e -> throw e;
            case Throwable e -> throw new IllegalArgumentException("Unexpected Throwable", e);
        }
    }
    
    private record Indexed<T>(T element, int index) { }
    
    private record Weighted<T>(T element, long cost) { }
    
    private record Expiring<T>(T element, Instant deadline) implements Comparable<Expiring<T>> {
        public int compareTo(Expiring other) {
            return deadline.compareTo(other.deadline);
        }
    }
    
    private static class WrappingException extends RuntimeException {
        WrappingException(Exception e) {
            super(e);
        }
        
        @Override
        public synchronized Exception getCause() {
            return (Exception) super.getCause();
        }
    }
    
    // Used by groupBy (when eagerCancel=false) to wait for run() tasks to finish after completing a Sink
    private static class SubScope implements Executor {
        final StructuredTaskScope<?> scope;
        final Phaser phaser;
        
        SubScope(StructuredTaskScope<?> scope) {
            this.scope = scope;
            this.phaser = new Phaser(1);
        }
        
        @Override
        public void execute(Runnable task) {
            if (phaser.register() < 0) {
                // In case someone holds on to the Executor reference when they shouldn't
                throw new IllegalStateException("SubScope is closed");
            }
            try {
                scope.fork(() -> {
                    try {
                        task.run();
                        return null;
                    } finally {
                        phaser.arriveAndDeregister();
                    }
                });
            } catch (Throwable e) {
                // Task was not forked
                phaser.arriveAndDeregister();
                throw e;
            }
        }
        
        public void join() throws InterruptedException {
            int phase = phaser.arrive();
            phaser.awaitAdvanceInterruptibly(phase);
        }
    }
    
    static final class ClosedSilo<T> implements Belt.Silo {
        final Belt.Source<? extends T> source;
        final Belt.Sink<? super T> sink;
        boolean ran = false;
        
        static final VarHandle RAN;
        static {
            try {
                RAN = MethodHandles.lookup().findVarHandle(ClosedSilo.class, "ran", boolean.class);
            } catch (ReflectiveOperationException e) {
                throw new ExceptionInInitializerError(e);
            }
        }
        
        ClosedSilo(Belt.Source<? extends T> source, Belt.Sink<? super T> sink) {
            this.source = Objects.requireNonNull(source);
            this.sink = Objects.requireNonNull(sink);
        }
        
        @Override
        public void run(Executor executor) {
            source.run(executor);
            sink.run(executor);
            executor.execute(() -> {
                if (!RAN.compareAndSet(this, false, true)) {
                    return;
                }
                try (source) {
                    if (sink instanceof Belt.StepSink<? super T> ss) {
                        source.drainToSink(ss);
                    } else if (source instanceof Belt.StepSource<? extends T> ss) {
                        sink.drainFromSource(ss);
                    }
                    sink.complete();
                } catch (Throwable ex) {
                    if (ex instanceof InterruptedException) { Thread.currentThread().interrupt(); }
                    try { sink.completeAbruptly(ex); }
                    catch (Throwable t) { if (t instanceof InterruptedException) Thread.currentThread().interrupt(); ex.addSuppressed(t); }
                    throw new CompletionException(ex);
                }
            });
        }
    }
    
    private abstract static class Chain {
        final Belt.Stage left;
        final Belt.Stage right;
        
        Chain(Belt.Stage left, Belt.Stage right) {
            this.left = Objects.requireNonNull(left);
            this.right = Objects.requireNonNull(right);
        }
        
        public void run(Executor executor) {
            left.run(executor);
            right.run(executor);
        }
    }
    
    static final class ChainSilo extends Chain implements Belt.Silo {
        ChainSilo(Belt.Silo left, Belt.Silo right) {
            super(left, right);
        }
    }
    
    static sealed class ChainSink<In> extends Chain implements Belt.Sink<In> {
        final Belt.Sink<? super In> sink;
        
        ChainSink(Belt.Sink<? super In> left, Belt.Silo right) {
            super(left, right);
            this.sink = left instanceof ChainSink<? super In> cs ? cs.sink : left;
        }
        
        @Override
        public boolean drainFromSource(Belt.StepSource<? extends In> source) throws Exception {
            return sink.drainFromSource(source);
        }
        
        @Override
        public void complete() throws Exception {
            sink.complete();
        }
        
        @Override
        public void completeAbruptly(Throwable cause) throws Exception {
            sink.completeAbruptly(cause);
        }
    }
    
    static sealed class ChainSource<Out> extends Chain implements Belt.Source<Out> {
        final Belt.Source<? extends Out> source;
        
        ChainSource(Belt.Silo left, Belt.Source<? extends Out> right) {
            super(left, right);
            this.source = right instanceof ChainSource<? extends Out> cs ? cs.source : right;
        }
        
        @Override
        public boolean drainToSink(Belt.StepSink<? super Out> sink) throws Exception {
            return source.drainToSink(sink);
        }
        
        @Override
        public void close() throws Exception {
            source.close();
        }
    }
    
    static final class ChainStepSink<In> extends ChainSink<In> implements Belt.StepSink<In> {
        ChainStepSink(Belt.StepSink<? super In> left, Belt.Silo right) {
            super(left, right);
        }
        
        @Override
        public boolean offer(In input) throws Exception {
            return ((Belt.StepSink<? super In>) sink).offer(input);
        }
    }
    
    static final class ChainStepSource<Out> extends ChainSource<Out> implements Belt.StepSource<Out> {
        ChainStepSource(Belt.Silo left, Belt.StepSource<? extends Out> right) {
            super(left, right);
        }
        
        @Override
        public Out poll() throws Exception {
            return ((Belt.StepSource<? extends Out>) source).poll();
        }
    }
    
    static class ChainSegue<In, Out> implements Belt.Segue<In, Out> {
        final Belt.Sink<? super In> sink;
        final Belt.Source<? extends Out> source;
        
        ChainSegue(Belt.Sink<? super In> sink, Belt.Source<? extends Out> source) {
            this.sink = Objects.requireNonNull(sink);
            this.source = Objects.requireNonNull(source);
        }
        
        @Override
        @SuppressWarnings("unchecked")
        public Belt.Sink<In> sink() {
            return (Belt.Sink<In>) sink;
        }
        
        @Override
        @SuppressWarnings("unchecked")
        public Belt.Source<Out> source() {
            return (Belt.Source<Out>) source;
        }
    }
    
    static class ChainStepSinkSource<In, Out> extends ChainSegue<In, Out> implements Belt.StepSinkSource<In, Out> {
        ChainStepSinkSource(Belt.StepSink<? super In> sink, Belt.Source<? extends Out> source) {
            super(sink, source);
        }
        
        @Override
        @SuppressWarnings("unchecked")
        public Belt.StepSink<In> sink() {
            return (Belt.StepSink<In>) sink;
        }
    }
    
    static class ChainSinkStepSource<In, Out> extends ChainSegue<In, Out> implements Belt.SinkStepSource<In, Out> {
        ChainSinkStepSource(Belt.Sink<? super In> sink, Belt.StepSource<? extends Out> source) {
            super(sink, source);
        }
        
        @Override
        @SuppressWarnings("unchecked")
        public Belt.StepSource<Out> source() {
            return (Belt.StepSource<Out>) source;
        }
    }
    
    static class ChainStepSegue<In, Out> extends ChainSegue<In, Out> implements Belt.StepSegue<In, Out> {
        ChainStepSegue(Belt.StepSink<? super In> sink, Belt.StepSource<? extends Out> source) {
            super(sink, source);
        }
        
        @Override
        @SuppressWarnings("unchecked")
        public Belt.StepSink<In> sink() {
            return (Belt.StepSink<In>) sink;
        }
        
        @Override
        @SuppressWarnings("unchecked")
        public Belt.StepSource<Out> source() {
            return (Belt.StepSource<Out>) source;
        }
    }
}
