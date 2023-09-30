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

public class Belts {
    private Belts() {} // Utility
    
    static final Throwable NULL_EXCEPTION = new Throwable();
    
    // TODO: Some rules:
    //  - drainFromSource/drainToSink (+poll/offer) must never call close/complete on arguments or 'this'
    //    - for the passed-in sources/sinks, they may be reused externally, eg by concat/spill
    //    - for this source/sink, it may be reused externally, eg if it's a StepSink/Source
    //    - fine to call close/complete on internally-created sources/sinks
    //  - complete() may throw for any reason
    //    - it should not return normally if it failed to complete a downstream
    //    - it should not short subsequent complete() if it failed to complete a downstream
    //  - complete(x) MUST complete each downstream before returning (normally or abruptly)
    //  - close() MUST close each upstream before returning (normally or abruptly)
    //  - no violating structured concurrency - if a method forks threads, it must wait for them to finish
    
    public static <T> Belt.StepSourceOperator<T, T> synchronizeStepSource() {
        class SynchronizedStepSource implements Belt.StepSource<T> {
            final Belt.StepSource<T> source;
            final ReentrantLock lock = new ReentrantLock();
            
            SynchronizedStepSource(Belt.StepSource<T> source) {
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
    
    public static <T> Belt.StepSinkOperator<T, T> synchronizeStepSink() {
        class SynchronizedStepSink implements Belt.StepSink<T> {
            final Belt.StepSink<T> sink;
            final ReentrantLock lock = new ReentrantLock();
            
            SynchronizedStepSink(Belt.StepSink<T> sink) {
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
    
    public static <T> Belt.StepSinkOperator<T, T> recoverStep(Function<? super Throwable, ? extends Belt.Source<? extends T>> mapper,
                                                              Consumer<? super Throwable> asyncExceptionHandler) {
        Objects.requireNonNull(mapper);
        Objects.requireNonNull(asyncExceptionHandler);
        
        class RecoverStep implements Belt.StepSink<T> {
            final Belt.StepSink<T> sink;
            
            RecoverStep(Belt.StepSink<T> sink) {
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
                            sink.complete(); // Note: This may be the second time calling...
                        }
                        scope.join();
                    }
                } catch (Error | Exception e) {
                    var exception = running ? e : cause;
                    callSuppressed(e, () -> { sink.completeAbruptly(exception); return null; });
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
    
    public static <T> Belt.SinkOperator<T, T> recover(Function<? super Throwable, ? extends Belt.StepSource<? extends T>> mapper,
                                                      Consumer<? super Throwable> asyncExceptionHandler) {
        Objects.requireNonNull(mapper);
        Objects.requireNonNull(asyncExceptionHandler);
        
        class Recover implements Belt.Sink<T> {
            final Belt.Sink<T> sink;
            
            Recover(Belt.Sink<T> sink) {
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
                            sink.complete(); // Note: This may be the second time calling...
                        }
                        scope.join();
                    }
                } catch (Error | Exception e) {
                    var exception = running ? e : cause;
                    callSuppressed(e, () -> { sink.completeAbruptly(exception); return null; });
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
    
    public static <T> Belt.SourceOperator<T, T> alsoClose(Belt.Source<?> sourceToClose) {
        Objects.requireNonNull(sourceToClose);
        
        class AlsoClose extends ProxySource<T> {
            final Belt.Source<T> source;
            
            AlsoClose(Belt.Source<T> source) {
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
    
    public static <T> Belt.SinkOperator<T, T> alsoComplete(Belt.Sink<?> sinkToComplete) {
        Objects.requireNonNull(sinkToComplete);
        
        class AlsoComplete extends ProxySink<T> {
            final Belt.Sink<T> sink;
            
            AlsoComplete(Belt.Sink<T> sink) {
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
                    Belt.StepSink<? super T> subSink = Belts.stepSink(e -> true);
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
                    } catch (Error | Exception e) {
                        var s = subSink;
                        callSuppressed(e, () -> { s.completeAbruptly(e); return null; });
                        throw e;
                    }
                }
            }
        }
        
        return new Split();
    }
    
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
                    } catch (Error | Exception e) {
                        callSuppressed(e, () -> { composedCompleteAbruptly(sinks(scopedSinkByKey), e); return null; });
                        throw e;
                    }
                }
            }
        }
        
        return new GroupBy();
    }
    
    public static <T, U> Belt.StepSinkOperator<T, U> flatMap(Function<? super T, ? extends Belt.Source<? extends U>> mapper,
                                                             Consumer<? super Throwable> asyncExceptionHandler) {
        Objects.requireNonNull(mapper);
        Objects.requireNonNull(asyncExceptionHandler);
        
        class FlatMap extends ProxySink<T> implements Belt.StepSink<T> {
            final Belt.StepSink<U> sink;
            boolean draining = true;
            
            FlatMap(Belt.StepSink<U> sink) {
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
    
    public static <T, U> Belt.SinkOperator<T, U> adaptSourceOfSink(Belt.StepSourceOperator<T, U> sourceMapper,
                                                                   Consumer<? super Throwable> asyncExceptionHandler) {
        Objects.requireNonNull(sourceMapper);
        Objects.requireNonNull(asyncExceptionHandler);
        
        class SourceAdaptedSink extends ProxySink<T> {
            final Belt.Sink<U> sink;
            
            SourceAdaptedSink(Belt.Sink<U> sink) {
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
                    // Basically, we are trying to make
                    //   `source.andThen(adaptSourceOfSink(sourceMapper).andThen(sink))`
                    // behave as if it was
                    //   `source.andThen(sourceMapper).andThen(sink)`
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
    
    // TODO: Do these compose well? eg, for A:t->a, B:a->b, C:b->c, compare:
    //  A-B
    //    .andThen(adaptSinkOfSource(A.andThen(B)))
    //    .andThen(adaptSinkOfSource(A).andThen(adaptSinkOfSource(B)))
    //    .andThen(adaptSinkOfSource(A)).andThen(adaptSinkOfSource(B))
    //  A-B-C
    //    .andThen(adaptSinkOfSource(A.andThen(B.andThen(C))))
    //    .andThen(adaptSinkOfSource(A.andThen(B).andThen(C)))
    //    .andThen(adaptSinkOfSource(A.andThen(B)).andThen(adaptSinkOfSource(C)))
    //    .andThen(adaptSinkOfSource(A.andThen(B))).andThen(adaptSinkOfSource(C))
    //    .andThen(adaptSinkOfSource(A).andThen(adaptSinkOfSource(B.andThen(C))))
    //    .andThen(adaptSinkOfSource(A).andThen(adaptSinkOfSource(B).andThen(adaptSinkOfSource(C))))
    //    .andThen(adaptSinkOfSource(A).andThen(adaptSinkOfSource(B)).andThen(adaptSinkOfSource(C)))
    //    .andThen(adaptSinkOfSource(A).andThen(adaptSinkOfSource(B))).andThen(adaptSinkOfSource(C))
    //    .andThen(adaptSinkOfSource(A)).andThen(adaptSinkOfSource(B.andThen(C)))
    //    .andThen(adaptSinkOfSource(A)).andThen(adaptSinkOfSource(B).andThen(adaptSinkOfSource(C)))
    //    .andThen(adaptSinkOfSource(A)).andThen(adaptSinkOfSource(B)).andThen(adaptSinkOfSource(C))
    
    public static <T, U> Belt.SourceOperator<T, U> adaptSinkOfSource(Belt.StepSinkOperator<T, U> sinkMapper,
                                                                     Consumer<? super Throwable> asyncExceptionHandler) {
        Objects.requireNonNull(sinkMapper);
        Objects.requireNonNull(asyncExceptionHandler);
        
        class SinkAdaptedSource extends ProxySource<U> {
            final Belt.Source<T> source;
            
            SinkAdaptedSource(Belt.Source<T> source) {
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
                    // Basically, we are trying to make
                    //   `source.andThen(adaptSinkOfSource(sinkMapper)).andThen(sink)`
                    // behave as if it was
                    //   `source.andThen(sinkMapper.andThen(sink))`
                    //
                    // A potential difference is in exception handling. We process newSink in this thread, and throw if
                    // the 'silo' below throws. This means that exception may be passed to sink.completeAbruptly
                    // sometime after this method exits. If there is an async boundary between newSink and sink, sink
                    // would see a different exception than it would have in the `sinkMapper.andThen(sink)` version. No
                    // heroics here can perfectly match the behavior of that version, so instead of trying and failing
                    // in subtle ways, we go with the more obvious implementation.
                    newSink.run(scopeExecutor(scope));
                    try {
                        source.drainToSink(newSink);
                        newSink.complete();
                        scope.join();
                    } catch (Error | Exception e) {
                        callSuppressed(e, () -> { newSink.completeAbruptly(e); return null; });
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
    
    public static <T, A, R> Belt.StepSinkOperator<T, R> gather(Gatherer<? super T, A, R> gatherer) {
        var supplier = gatherer.supplier();
        var integrator = gatherer.integrator();
        var finisher = gatherer.finisher();
        
        class Gather implements Belt.StepSink<T> {
            final Belt.StepSink<R> sink;
            final Gatherer.Sink<R> gsink;
            A acc = null;
            int state = NEW;
            
            static final int NEW       = 0;
            static final int RUNNING   = 1;
            static final int COMPLETED = 2;
            static final int CLOSED    = 3;
            
            Gather(Belt.StepSink<R> sink) {
                this.sink = Objects.requireNonNull(sink);
                this.gsink = el -> {
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
                    if (!integrator.integrate(acc, input, gsink)) {
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
                    finisher.accept(acc, gsink);
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
    
    public static <T, K, U> Belt.SinkStepSource<T, U> mapBalancePartitioned(int parallelism,
                                                                            int permitsPerPartition,
                                                                            int bufferLimit,
                                                                            Function<? super T, ? extends K> classifier,
                                                                            BiFunction<? super T, ? super K, ? extends Callable<? extends U>> mapper) {
        // POLL:
        // Worker polls element from source
        // If completion buffer is full, worker waits until not full
        // Worker offers element to completion buffer
        // If element partition has permits, worker takes one and begins work (END)
        // Worker offers element to partition buffer, goes to step 1
        
        // TODO^: If partition has no permits, then MAX other workers are already working the partition, so leave that be
        //  But if those workers fail (and we recover), might we forget about the partition?
        
        // OFFER:
        // Worker polls partition buffer, continues if not empty (END)
        // Worker gives permit back to partition
        // If partition has max permits, worker removes partition
        
        if (parallelism < 1 || permitsPerPartition < 1 || bufferLimit < 1) {
            throw new IllegalArgumentException("parallelism, permitsPerPartition, and bufferLimit must be positive");
        }
        Objects.requireNonNull(classifier);
        Objects.requireNonNull(mapper);
        
        if (permitsPerPartition >= parallelism) {
            return mapBalanceOrdered(parallelism, bufferLimit, t -> mapper.apply(t, classifier.apply(t)));
        }
        
        class Item {
            // Value of out is initially partition key
            // When output is computed, output replaces partition key, and null replaces input
            Object out;
            T in;
            
            Item(K key, T in) {
                this.out = key;
                this.in = in;
            }
        }
        
        class Partition {
            // Only use buffer if we have no permits left
            final Deque<Item> buffer = new LinkedList<>();
            int permits = permitsPerPartition;
        }
        
        class MapBalancePartitioned implements Belt.SinkStepSource<T, U> {
            final ReentrantLock sourceLock = new ReentrantLock();
            final ReentrantLock lock = new ReentrantLock();
            final Condition completionNotFull = lock.newCondition();
            final Condition outputReady = lock.newCondition();
            final Deque<Item> completionBuffer = new ArrayDeque<>(bufferLimit);
            final Map<K, Partition> partitionByKey = new HashMap<>();
            int state = RUNNING;
            Throwable exception = null;
            
            static final int RUNNING    = 0;
            static final int COMPLETING = 1;
            static final int CLOSED     = 2;
            
            class Worker implements Belt.Sink<T> {
                @Override
                public boolean drainFromSource(Belt.StepSource<? extends T> source) throws Exception {
                    K key = null;
                    Item item = null;
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
                                    item = new Item(key, in);
                                    
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
                        } catch (Error | Exception e) {
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
                                            } catch (Error | Exception e) {
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
            
            class Sink implements Belt.Sink<T> {
                @Override
                public boolean drainFromSource(Belt.StepSource<? extends T> source) throws Exception {
                    try (var scope = new StructuredTaskScope.ShutdownOnFailure("mapBalancePartitioned-drainFromSource",
                                                                               Thread.ofVirtual().name("thread-", 0).factory())) {
                        var tasks = IntStream.range(0, parallelism)
                            .mapToObj(i -> new Worker())
                            .map(sink -> scope.fork(() -> sink.drainFromSource(source)))
                            .toList();
                        scope.join().throwIfFailed();
                        return tasks.stream().anyMatch(StructuredTaskScope.Subtask::get);
                    } catch (Error | Exception e) {
                        // Anything after the first unprocessed item is now unreachable, meaning we would deadlock if we
                        // tried to recover this sink. To make recovery safe, we remove unreachable items. This includes
                        // processed items that were behind unprocessed items, to avoid violating order.
                        lock.lock();
                        try {
                            var reachable = new LinkedList<Item>();
                            for (Item i; (i = completionBuffer.poll()) != null && i.in == null; ) {
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
                            Item item;
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
                            Item nextItem = completionBuffer.peek();
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
        
        return new MapBalancePartitioned();
    }
    
    public static <T, U> Belt.SinkStepSource<T, U> mapBalanceOrdered(int parallelism,
                                                                     int bufferLimit,
                                                                     Function<? super T, ? extends Callable<? extends U>> mapper) {
        if (parallelism < 1 || bufferLimit < 1) {
            throw new IllegalArgumentException("parallelism and bufferLimit must be positive");
        }
        Objects.requireNonNull(mapper);

        class Item {
            // Value of out is initially null
            // When output is computed, output replaces null, and null replaces input
            U out = null;
            T in;
            
            Item(T in) {
                this.in = in;
            }
        }
        
        class MapBalanceOrdered implements Belt.SinkStepSource<T, U> {
            final ReentrantLock sourceLock = new ReentrantLock();
            final ReentrantLock lock = new ReentrantLock();
            final Condition completionNotFull = lock.newCondition();
            final Condition outputReady = lock.newCondition();
            final Deque<Item> completionBuffer = new ArrayDeque<>(bufferLimit);
            int state = RUNNING;
            Throwable exception = null;
            
            static final int RUNNING    = 0;
            static final int COMPLETING = 1;
            static final int CLOSED     = 2;
            
            class Worker implements Belt.Sink<T> {
                @Override
                public boolean drainFromSource(Belt.StepSource<? extends T> source) throws Exception {
                    for (;;) {
                        Item item = null;
                        Callable<? extends U> callable;
                        
                        try {
                            sourceLock.lockInterruptibly();
                            try {
                                T in = source.poll();
                                if (in == null) {
                                    return true;
                                }
                                item = new Item(in);
                                
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
            
            class Sink implements Belt.Sink<T> {
                @Override
                public boolean drainFromSource(Belt.StepSource<? extends T> source) throws Exception {
                    try (var scope = new StructuredTaskScope.ShutdownOnFailure("mapBalanceOrdered-drainFromSource",
                                                                               Thread.ofVirtual().name("thread-", 0).factory())) {
                        var tasks = IntStream.range(0, parallelism)
                            .mapToObj(i -> new Worker())
                            .map(sink -> scope.fork(() -> sink.drainFromSource(source)))
                            .toList();
                        scope.join().throwIfFailed();
                        return tasks.stream().anyMatch(StructuredTaskScope.Subtask::get);
                    } catch (Error | Exception e) {
                        // Anything after the first unprocessed item is now unreachable, meaning we would deadlock if we
                        // tried to recover this sink. To make recovery safe, we remove unreachable items. This includes
                        // processed items that were behind unprocessed items, to avoid violating order.
                        lock.lock();
                        try {
                            var reachable = new LinkedList<Item>();
                            for (Item i; (i = completionBuffer.poll()) != null && i.in == null; ) {
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
                            Item item;
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
                            Item nextItem = completionBuffer.peek();
                            if (nextItem != null && nextItem.in == null) {
                                outputReady.signal();
                            }
                            return item.out;
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
        
        return new MapBalanceOrdered();
    }
    
    public static <T, C extends Callable<T>> Belt.StepToSourceOperator<C, T> balanceMergeSource(int parallelism) {
        if (parallelism < 1) {
            throw new IllegalArgumentException("parallelism must be positive");
        }
        
        return source -> {
            Objects.requireNonNull(source);
            Belt.StepSource<T> worker = () -> {
                for (C c; (c = source.poll()) != null; ) {
                    var t = c.call();
                    if (t != null) { // Skip nulls
                        return t;
                    }
                }
                return null;
            };
            return merge(IntStream.range(0, parallelism).mapToObj(i -> worker).toList())
                .andThen(alsoClose(source));
        };
    }
    
    public static <T, C extends Callable<T>> Belt.SinkToStepOperator<C, T> balanceMergeSink(int parallelism) {
        if (parallelism < 1) {
            throw new IllegalArgumentException("parallelism must be positive");
        }
        
        return sink -> {
            Objects.requireNonNull(sink);
            Belt.StepSink<C> worker = c -> {
                var t = c.call();
                return t == null || sink.offer(t); // Skip nulls
            };
            return balance(IntStream.range(0, parallelism).mapToObj(i -> worker).toList())
                .compose(alsoComplete(sink));
        };
    }
    
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
    
    public static <T> Belt.StepSink<T> broadcast(Collection<? extends Belt.StepSink<? super T>> sinks) {
        var theSinks = List.copyOf(sinks);
        
        class Broadcast extends ProxySink<T> implements Belt.StepSink<T> {
            @Override
            public boolean offer(T input) throws Exception {
                for (var sink : theSinks) {
                    if (!sink.offer(input)) {
                        return false;
                    }
                }
                return true;
            }
            
            @Override
            protected Stream<? extends Belt.Sink<?>> sinks() {
                return theSinks.stream();
            }
        }
        
        return new Broadcast();
    }
    
    public static <T, U> Belt.StepSink<T> route(BiConsumer<? super T, ? super BiConsumer<Integer, U>> router,
                                                boolean eagerCancel,
                                                Collection<? extends Belt.StepSink<? super U>> sinks) {
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
    
    public static <T> Belt.StepSink<T> spillStep(Collection<? extends Belt.StepSink<? super T>> sinks) {
        var theSinks = List.copyOf(sinks);
        
        class SpillStep extends ProxySink<T> implements Belt.StepSink<T> {
            int i = 0;
            
            @Override
            public boolean offer(T input) throws Exception {
                for (; i < theSinks.size(); i++) {
                    if (theSinks.get(i).offer(input)) {
                        return true;
                    }
                }
                return false;
            }
            
            @Override
            protected Stream<? extends Belt.Sink<?>> sinks() {
                return theSinks.stream();
            }
        }
        
        return new SpillStep();
    }
    
    public static <T> Belt.Sink<T> spill(Collection<? extends Belt.Sink<? super T>> sinks) {
        var theSinks = List.copyOf(sinks);
        
        class Spill extends ProxySink<T> {
            @Override
            public boolean drainFromSource(Belt.StepSource<? extends T> source) throws Exception {
                for (var sink : theSinks) {
                    if (sink.drainFromSource(source)) {
                        return true;
                    }
                }
                return false;
            }
            
            @Override
            protected Stream<? extends Belt.Sink<?>> sinks() {
                return theSinks.stream();
            }
        }
        
        return new Spill();
    }
    
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
    
    public static <T> Belt.StepSource<T> mergeSorted(Collection<? extends Belt.StepSource<? extends T>> sources,
                                                     Comparator<? super T> comparator) {
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
                T1 e1;
                T2 e2;
                if ((e1 = source1.poll()) == null || (e2 = source2.poll()) == null) {
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
    
    // --- Segues ---
    
    public static <T, A> Belt.StepSegue<T, A> batch(Supplier<? extends A> batchSupplier,
                                                    BiConsumer<? super A, ? super T> accumulator,
                                                    Function<? super A, Optional<Instant>> deadlineMapper) {
        Objects.requireNonNull(batchSupplier);
        Objects.requireNonNull(accumulator);
        Objects.requireNonNull(deadlineMapper);
        
        class Batch implements TimedSegue.Core<T, A> {
            A batch = null;
            boolean done = false;
            
            @Override
            public void onInit(TimedSegue.SinkController ctl) { }
            
            @Override
            public void onOffer(TimedSegue.SinkController ctl, T input) {
                A b = batch;
                if (b == null) {
                    b = Objects.requireNonNull(batchSupplier.get());
                }
                accumulator.accept(b, input);
                Instant deadline = deadlineMapper.apply(b).orElse(null);
                batch = b; // No more exception risk -- assign batch
                if (deadline != null) {
                    ctl.latchSourceDeadline(deadline);
                    if (deadline == Instant.MIN) {
                        // Alternative implementations might adjust or reset the buffer instead of blocking
                        ctl.latchSinkDeadline(Instant.MAX);
                    }
                }
            }
            
            @Override
            public void onPoll(TimedSegue.SourceController<A> ctl) {
                if (done) {
                    ctl.latchClose();
                    if (batch == null) {
                        return;
                    }
                }
                ctl.latchOutput(batch);
                batch = null;
                ctl.latchSourceDeadline(Instant.MAX);
                ctl.latchSinkDeadline(Instant.MIN);
            }
            
            @Override
            public void onComplete(TimedSegue.SinkController ctl) {
                done = true;
                ctl.latchSourceDeadline(Instant.MIN);
            }
        }
        
        var core = new Batch();
        return new TimedSegue<>(core);
    }
    
    public static <T> Belt.StepSegue<T, T> tokenBucket(Duration tokenInterval,
                                                       ToLongFunction<? super T> costMapper,
                                                       long tokenLimit,
                                                       long costLimit) { // TODO: Obviate costLimit?
        Objects.requireNonNull(tokenInterval);
        Objects.requireNonNull(costMapper);
        if ((tokenLimit | costLimit) < 0) {
            throw new IllegalArgumentException("tokenLimit and costLimit must be non-negative");
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
        
        class Throttle implements TimedSegue.Core<T, T> {
            Deque<Weighted<T>> queue = null;
            long tempTokenLimit = 0;
            long tokens = 0;
            long cost = 0;
            Instant lastObservedAccrual;
            boolean done = false;
            
            @Override
            public void onInit(TimedSegue.SinkController ctl) {
                queue = new ArrayDeque<>();
                lastObservedAccrual = clock().instant();
            }
            
            @Override
            public void onOffer(TimedSegue.SinkController ctl, T input) {
                long elementCost = costMapper.applyAsLong(input);
                if (elementCost < 0) {
                    throw new IllegalStateException("Element cost cannot be negative");
                }
                if ((cost = Math.addExact(cost, elementCost)) >= costLimit) {
                    // Optional blocking for boundedness, here based on cost rather than queue size
                    ctl.latchSinkDeadline(Instant.MAX);
                }
                var w = new Weighted<>(input, elementCost);
                queue.offer(w);
                if (queue.peek() == w) {
                    ctl.latchSourceDeadline(Instant.MIN); // Let source-side do token math
                }
            }
            
            @Override
            public void onPoll(TimedSegue.SourceController<T> ctl) {
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
                    cost -= head.cost;
                    queue.poll();
                    ctl.latchSinkDeadline(Instant.MIN); // TODO: Might not be below costLimit yet
                    ctl.latchOutput(head.element);
                    head = queue.peek();
                    if (head == null) {
                        if (done) {
                            ctl.latchClose();
                        } else {
                            ctl.latchSourceDeadline(Instant.MAX);
                        }
                        return;
                    } else if (tokens >= head.cost) {
                        ctl.latchSourceDeadline(Instant.MIN);
                        return;
                    }
                    // else tokens < head.cost; Fall-through to scheduling
                }
                // Schedule to wake up when we have enough tokens for next emission
                tempTokenLimit = head.cost;
                long tokensNeeded = head.cost - tokens;
                ctl.latchSourceDeadline(now.plusNanos(tokenIntervalNanos * tokensNeeded - nanosSinceLastAccrual));
            }
            
            @Override
            public void onComplete(TimedSegue.SinkController ctl) {
                done = true;
                if (queue.isEmpty()) {
                    ctl.latchSourceDeadline(Instant.MIN);
                }
            }
        }
        
        var core = new Throttle();
        return new TimedSegue<>(core);
    }
    
    public static <T> Belt.StepSegue<T, T> delay(Function<? super T, Instant> deadlineMapper,
                                                 int bufferLimit) {
        Objects.requireNonNull(deadlineMapper);
        if (bufferLimit < 1) {
            throw new IllegalArgumentException("bufferLimit must be positive");
        }
        
        class Delay implements TimedSegue.Core<T, T> {
            PriorityQueue<Expiring<T>> pq = null;
            boolean done = false;
            
            @Override
            public void onInit(TimedSegue.SinkController ctl) {
                pq = new PriorityQueue<>(bufferLimit);
            }
            
            @Override
            public void onOffer(TimedSegue.SinkController ctl, T input) {
                Instant deadline = Objects.requireNonNull(deadlineMapper.apply(input));
                Expiring<T> e = new Expiring<>(input, deadline);
                pq.offer(e);
                if (pq.peek() == e) {
                    ctl.latchSourceDeadline(deadline);
                }
                if (pq.size() >= bufferLimit) {
                    ctl.latchSinkDeadline(Instant.MAX);
                }
            }
            
            @Override
            public void onPoll(TimedSegue.SourceController<T> ctl) {
                Expiring<T> head = pq.poll();
                if (head == null) {
                    ctl.latchClose();
                    return;
                }
                ctl.latchSinkDeadline(Instant.MIN);
                ctl.latchOutput(head.element);
                head = pq.peek();
                if (head != null) {
                    ctl.latchSourceDeadline(head.deadline);
                } else if (!done) {
                    ctl.latchSourceDeadline(Instant.MAX);
                } else {
                    ctl.latchClose();
                }
            }
            
            @Override
            public void onComplete(TimedSegue.SinkController ctl) {
                done = true;
                if (pq.isEmpty()) {
                    ctl.latchSourceDeadline(Instant.MIN);
                }
            }
        }
       
        var core = new Delay();
        return new TimedSegue<>(core);
    }
    
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
        
        class KeepAlive implements TimedSegue.Core<T, T> {
            Deque<T> queue = null;
            boolean done = false;
            
            @Override
            public void onInit(TimedSegue.SinkController ctl) {
                queue = new ArrayDeque<>(bufferLimit);
                ctl.latchSourceDeadline(clock().instant().plus(timeout));
            }
            
            @Override
            public void onOffer(TimedSegue.SinkController ctl, T input) {
                queue.offer(input);
                ctl.latchSourceDeadline(Instant.MIN);
                if (queue.size() >= bufferLimit) {
                    ctl.latchSinkDeadline(Instant.MAX);
                }
            }
            
            @Override
            public void onPoll(TimedSegue.SourceController<T> ctl) {
                T head = queue.poll();
                if (head != null) {
                    ctl.latchSinkDeadline(Instant.MIN);
                    ctl.latchOutput(head);
                    if (queue.isEmpty() && !done) {
                        ctl.latchSourceDeadline(clock().instant().plus(timeout));
                    }
                } else if (!done) {
                    ctl.latchOutput(extraSupplier.get());
                    ctl.latchSourceDeadline(clock().instant().plus(timeout));
                } else {
                    ctl.latchClose();
                }
            }
            
            @Override
            public void onComplete(TimedSegue.SinkController ctl) {
                done = true;
                ctl.latchSourceDeadline(Instant.MIN);
            }
        }
        
        var core = new KeepAlive();
        return new TimedSegue<>(core);
    }
    
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
    
    public static <T> Belt.StepSegue<T ,T> buffer(int bufferLimit) {
        if (bufferLimit < 1) {
            throw new IllegalArgumentException("bufferLimit must be positive");
        }
        
        class Buffer implements TimedSegue.Core<T, T> {
            Deque<T> queue = null;
            boolean done = false;
            
            @Override
            public void onInit(TimedSegue.SinkController ctl) {
                queue = new ArrayDeque<>(bufferLimit);
            }
            
            @Override
            public void onOffer(TimedSegue.SinkController ctl, T input) {
                queue.offer(input);
                ctl.latchSourceDeadline(Instant.MIN);
                if (queue.size() >= bufferLimit) {
                    ctl.latchSinkDeadline(Instant.MAX);
                }
            }
            
            @Override
            public void onPoll(TimedSegue.SourceController<T> ctl) {
                T head = queue.poll();
                if (head != null) {
                    ctl.latchSinkDeadline(Instant.MIN);
                    ctl.latchOutput(head);
                    if (queue.peek() != null) {
                        return;
                    } else if (!done) {
                        ctl.latchSourceDeadline(Instant.MAX);
                        return;
                    } // else fall-through
                }
                ctl.latchClose();
            }
            
            @Override
            public void onComplete(TimedSegue.SinkController ctl) {
                done = true;
                ctl.latchSourceDeadline(Instant.MIN);
            }
        }
        
        var core = new Buffer();
        return new TimedSegue<>(core);
    }

    public static <T> Belt.StepSegue<T, T> extrapolate(T initial,
                                                       Function<? super T, ? extends Iterator<? extends T>> mapper,
                                                       int bufferLimit) {
        Objects.requireNonNull(mapper);
        if (bufferLimit < 1) {
            throw new IllegalArgumentException("bufferLimit must be positive");
        }
        
        class Extrapolate implements TimedSegue.Core<T, T> {
            T prev = null;
            Deque<T> queue = null;
            Iterator<? extends T> iter = Collections.emptyIterator();
            boolean done = false;
            
            @Override
            public void onInit(TimedSegue.SinkController ctl) {
                queue = new ArrayDeque<>(bufferLimit);
                if (initial != null) {
                    queue.offer(initial);
                    ctl.latchSourceDeadline(Instant.MIN);
                } else {
                    ctl.latchSourceDeadline(Instant.MAX);
                }
            }
            
            @Override
            public void onOffer(TimedSegue.SinkController ctl, T input) {
                prev = null;
                iter = Collections.emptyIterator();
                queue.offer(input);
                ctl.latchSourceDeadline(Instant.MIN);
                if (queue.size() >= bufferLimit) {
                    ctl.latchSinkDeadline(Instant.MAX);
                }
            }
            
            @Override
            public void onPoll(TimedSegue.SourceController<T> ctl) {
                T head = queue.poll();
                if (head != null) {
                    ctl.latchSinkDeadline(Instant.MIN);
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
                        ctl.latchSourceDeadline(Instant.MAX);
                    }
                } else {
                    ctl.latchClose();
                }
            }
            
            @Override
            public void onComplete(TimedSegue.SinkController ctl) {
                done = true;
                ctl.latchSourceDeadline(Instant.MIN);
            }
        }
        
        var core = new Extrapolate();
        return new TimedSegue<>(core);
    }
    
    public static <T> Belt.Source<T> source(Stream<? extends T> stream) {
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
                        } catch (Error | RuntimeException ex) {
                            throw ex;
                        } catch (Exception ex) {
                            if (ex instanceof InterruptedException) {
                                Thread.currentThread().interrupt();
                            }
                            throw new WrappingException(ex);
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
    
    public static <T> Belt.StepSource<T> stepSource(Iterator<T> iterator) {
        return stepSource(iterator, () -> { });
    }
    
    public static <T> Belt.StepSource<T> stepSource(Iterator<T> iterator, AutoCloseable onClose) {
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
    
    public static <T> Belt.Source<T> source(Belt.Source<T> source) {
        return Objects.requireNonNull(source);
    }
    
    public static <T> Belt.Sink<T> sink(Belt.Sink<T> sink) {
        return Objects.requireNonNull(sink);
    }
    
    public static <T> Belt.StepSource<T> stepSource(Belt.StepSource<T> source) {
        return Objects.requireNonNull(source);
    }
    
    public static <T> Belt.StepSink<T> stepSink(Belt.StepSink<T> sink) {
        return Objects.requireNonNull(sink);
    }
    
    public static Executor scopeExecutor(StructuredTaskScope<?> scope) {
        Objects.requireNonNull(scope);
        return runnable -> scope.fork(Executors.callable(runnable, null));
    }
    
    public static void composedClose(Stream<? extends Belt.Source<?>> sources) throws Exception {
        Throwable[] ex = { null };
        sources.sequential().forEach(source -> {
            try {
                source.close();
            } catch (Error | Exception e) {
                if (ex[0] == null) {
                    ex[0] = e;
                } else {
                    ex[0].addSuppressed(e);
                }
            }
        });
        throwAsException(ex[0]);
    }
    
    public static void composedComplete(Stream<? extends Belt.Sink<?>> sinks) throws Exception {
        Throwable[] ex = { null };
        sinks.sequential().forEach(sink -> {
            try {
                sink.complete();
            } catch (Error | Exception e) {
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
    
    public static void composedCompleteAbruptly(Stream<? extends Belt.Sink<?>> sinks, Throwable cause) throws Exception {
        Throwable[] ex = { null };
        sinks.sequential().forEach(sink -> {
            try {
                sink.completeAbruptly(cause);
            } catch (Error | Exception e) {
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
    
    private static void callSuppressed(Throwable exception, Callable<?> callable) {
        if (exception instanceof InterruptedException) {
            Thread.currentThread().interrupt();
            try {
                callable.call();
            } catch (Throwable t) {
                exception.addSuppressed(t);
            }
            Thread.interrupted();
        } else {
            try {
                callable.call();
            } catch (Throwable t) {
                if (t instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                exception.addSuppressed(t);
            }
        }
    }
    
    private static class Indexed<T> {
        final T element;
        final int index;
        
        Indexed(T element, int index) {
            this.element = element;
            this.index = index;
        }
    }
    
    private static class Weighted<T> {
        final T element;
        final long cost;
        
        Weighted(T element, long cost) {
            this.element = element;
            this.cost = cost;
        }
    }
    
    private static class Expiring<T> implements Comparable<Expiring<T>> {
        final T element;
        final Instant deadline;
        
        Expiring(T element, Instant deadline) {
            this.element = element;
            this.deadline = deadline;
        }
        
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
            } catch (Error | RuntimeException e) {
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
                    callSuppressed(ex, () -> { sink.completeAbruptly(ex); return null; });
                    if (ex instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
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
        final Belt.Sink<In> sink;
        
        ChainSink(Belt.Sink<In> left, Belt.Silo right) {
            super(left, right);
            this.sink = left instanceof ChainSink<In> cs ? cs.sink : left;
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
        final Belt.Source<Out> source;
        
        ChainSource(Belt.Silo left, Belt.Source<Out> right) {
            super(left, right);
            this.source = right instanceof ChainSource<Out> cs ? cs.source : right;
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
        ChainStepSink(Belt.StepSink<In> left, Belt.Silo right) {
            super(left, right);
        }
        
        @Override
        public boolean offer(In input) throws Exception {
            return ((Belt.StepSink<In>) sink).offer(input);
        }
    }
    
    static final class ChainStepSource<Out> extends ChainSource<Out> implements Belt.StepSource<Out> {
        ChainStepSource(Belt.Silo left, Belt.StepSource<Out> right) {
            super(left, right);
        }
        
        @Override
        public Out poll() throws Exception {
            return ((Belt.StepSource<Out>) source).poll();
        }
    }
    
    record ChainSegue<In, Out>(Belt.Sink<In> sink, Belt.Source<Out> source) implements Belt.Segue<In, Out> {
        public ChainSegue {
            Objects.requireNonNull(sink);
            Objects.requireNonNull(source);
        }
    }
    
    record ChainStepSinkSource<In, Out>(Belt.StepSink<In> sink, Belt.Source<Out> source) implements Belt.StepSinkSource<In, Out> {
        public ChainStepSinkSource {
            Objects.requireNonNull(sink);
            Objects.requireNonNull(source);
        }
    }
    
    record ChainSinkStepSource<In, Out>(Belt.Sink<In> sink, Belt.StepSource<Out> source) implements Belt.SinkStepSource<In, Out> {
        public ChainSinkStepSource {
            Objects.requireNonNull(sink);
            Objects.requireNonNull(source);
        }
    }
    
    record ChainStepSegue<In, Out>(Belt.StepSink<In> sink, Belt.StepSource<Out> source) implements Belt.StepSegue<In, Out> {
        public ChainStepSegue {
            Objects.requireNonNull(sink);
            Objects.requireNonNull(source);
        }
    }
}