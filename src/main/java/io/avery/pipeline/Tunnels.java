package io.avery.pipeline;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.*;
import java.util.stream.IntStream;

public class Tunnels {
    private Tunnels() {} // Utility
    
    public static void main(String[] args) throws Exception {
        Gatherer<String, Void, String> flatMap3 = new Gatherer<>() {
            @Override
            public Supplier<Void> supplier() {
                return () -> (Void) null;
            }
            
            @Override
            public Integrator<Void, String, String> integrator() {
                return (state, element, downstream) ->
                    IntStream.range(0, 3).allMatch(i -> downstream.flush(element));
            }
            
            @Override
            public BinaryOperator<Void> combiner() {
                return (l, r) -> l;
            }
            
            @Override
            public BiConsumer<Void, Sink<? super String>> finisher() {
                return (state, downstream) -> {};
            }
            
            @Override
            public Set<Characteristics> characteristics() {
                return Set.of(Characteristics.GREEDY, Characteristics.STATELESS);
            }
        };
        
        try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
            var stage = Tunnels.chain(
                flatMap3,
                Tunnels.tokenBucket(
                    Duration.ofSeconds(1),
                    String::length,
                    10,
                    100
                )
//                Tunnels.batch(
//                    () -> new ArrayList<>(10),
//                    Collection::add,
//                    list ->
//                          list.size() ==  1 ? Optional.of(Instant.now().plusSeconds(5))
//                        : list.size() >= 10 ? Optional.of(Instant.MIN)
//                        : Optional.empty()
//                )
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
    
    // TODO: In forEachUntilCancel(), is exception because of source or sink?
    //  Matters for deciding what becomes of source + other sinks
    //  Maybe non-gated Sources shouldn't be allowed at all?
    //   - Because, if any / all sinks cancel, source can't recover well (snapshot its state) for new sinks
    //   - Instead, have combinators that take a Gate to build a Source
    
    <T> Tunnel.Source<T> merge(List<Tunnel.Source<T>> sources) {
        class Merge implements Tunnel.Source<T> {
            @Override
            public void drainToSink(Tunnel.GatedSink<? super T> sink) throws InterruptedException, ExecutionException {
                try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
                    for (var source : sources) {
                        scope.fork(() -> {
                            try (source) { // TODO: Should we not (in case source is read by other sinks)?
                                source.drainToSink(sink);
                            } catch (Throwable error) {
                                sink.complete(error);
                            }
                            return null;
                        });
                    }
                    scope.join().throwIfFailed();
                }
            }
            
            @Override
            public void close() throws Exception {
                composedClose(0);
            }
            
            private void composedClose(int i) throws Exception {
                if (i == sources.size()) {
                    return;
                }
                try {
                    composedClose(i+1);
                } finally {
                    sources.get(i).close();
                }
            }
        }
        
        return new Merge();
    }
    
    <T> Tunnel.Sink<T> balance(List<Tunnel.GatedSink<T>> sinks) {
        class Balance implements Tunnel.Sink<T> {
            @Override
            public void drainFromSource(Tunnel.GatedSource<? extends T> source) throws InterruptedException, ExecutionException {
                try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
                    for (var sink : sinks) {
                        scope.fork(() -> {
                            try {
                                source.drainToSink(sink);
                                sink.complete(null); // TODO: Should we not (in case sink wants more from another source)?
                            } catch (Throwable error) {
                                sink.complete(error);
                            }
                            return null;
                        });
                    }
                    scope.join().throwIfFailed();
                }
            }
            
            @Override
            public void complete(Throwable error) throws InterruptedException, ExecutionException {
                try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
                    for (var sink : sinks) {
                        scope.fork(() -> {
                            sink.complete(error);
                            return null;
                        });
                    }
                    scope.join().throwIfFailed();
                }
            }
        }
        
        return new Balance();
    }
    
    <T> Tunnel.GatedSink<T> broadcast(List<Tunnel.GatedSink<T>> sinks) {
        class Broadcast implements Tunnel.GatedSink<T> {
            @Override
            public boolean offer(T input) throws InterruptedException, ExecutionException {
                try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
                    for (var sink : sinks) {
                        scope.fork(() -> {
                            try {
                                // TODO: Check return - handle cancel
                                sink.offer(input);
                            } catch (Throwable error) {
                                // TODO: Remove sinks that have already completed
                                sink.complete(error);
                            }
                            return null;
                        });
                    }
                    scope.join().throwIfFailed();
                }
                return true; // TODO: Unless any/all sinks have cancelled
            }
            
            @Override
            public void complete(Throwable error) throws InterruptedException, ExecutionException {
                try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
                    for (var sink : sinks) {
                        scope.fork(() -> {
                            sink.complete(error);
                            return null;
                        });
                    }
                    scope.join().throwIfFailed();
                }
            }
        }
        
        return new Broadcast();
    }
    
    // Is there a problem with 'init-on-first-use'? Vs having/requiring an explicit init?
    // Uses of 'init' (onSubscribe()) in Rx:
    //  - Set up Publisher state for the new Subscriber
    //  - Send initial elements (or even error/completion), eg in a Replay
    
    // init() as a way to initialize threads in a chained tunnel / pipeline?
    // Problem: Can't create the STScope inside init(), cuz scope - execution would need to finish inside init()
    // Could pass in a scope - init(scope) - that feels opinionated, and unnecessary for single-stage
    
    
    private static class WrappingException extends RuntimeException {
        WrappingException(Exception e) {
            super(e);
        }
        
        @Override
        public synchronized Exception getCause() {
            return (Exception) super.getCause();
        }
    }
    
    private static class PrependedSink<In, T, A> implements Tunnel.GatedSink<In> {
        final ReentrantLock upstreamLock = new ReentrantLock();
        final Supplier<A> supplier;
        final Gatherer.Integrator<A, In, T> integrator;
        final BiConsumer<A, Gatherer.Sink<? super T>> finisher;
        final Tunnel.GatedSink<T> tunnel;
        final Gatherer.Sink<T> gsink;
        A acc = null;
        int state = NEW;
        
        static final int NEW = 0;
        static final int RUNNING = 1;
        static final int CLOSED = 2;
        
        PrependedSink(Gatherer<In, A, T> gatherer, Tunnel.GatedSink<T> sink) {
            this.supplier = gatherer.supplier();
            this.integrator = gatherer.integrator();
            this.finisher = gatherer.finisher();
            this.tunnel = sink;
            this.gsink = el -> {
                try {
                    return tunnel().offer(el);
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
        
        Tunnel.GatedSink<T> tunnel() {
            return tunnel;
        }
        
        void initIfNew() {
            //assert upstreamLock.isHeldByCurrentThread();
            if (state == NEW) {
                acc = supplier.get();
                state = RUNNING;
            }
        }
        
        @Override
        public boolean offer(In input) throws Exception {
            upstreamLock.lockInterruptibly();
            try {
                if (state == CLOSED) {
                    return false;
                }
                initIfNew();
                if (!integrator.integrate(acc, input, gsink)) {
                    state = CLOSED;
                    return false;
                }
                return true;
            } catch (WrappingException e) {
                state = CLOSED;
                if (e.getCause() instanceof InterruptedException) {
                    Thread.interrupted();
                }
                throw e.getCause();
            } catch (Error | Exception e) {
                state = CLOSED;
                throw e;
            } finally {
                upstreamLock.unlock();
            }
        }
        
        @Override
        public void complete(Throwable error) throws Exception {
            upstreamLock.lockInterruptibly();
            try {
                if (state == CLOSED) {
                    return;
                }
                initIfNew();
                if (error == null) {
                    finisher.accept(acc, gsink);
                }
                tunnel().complete(error);
            } catch (WrappingException e) {
                if (e.getCause() instanceof InterruptedException) {
                    Thread.interrupted();
                }
                throw e.getCause();
            } finally {
                state = CLOSED;
                upstreamLock.unlock();
            }
        }
    }
    
    private static class PrependedGate<In, T, A, Out> extends PrependedSink<In, T, A> implements Tunnel.Gate<In, Out> {
        PrependedGate(Gatherer<In, A, T> gatherer, Tunnel.Gate<T, Out> gate) {
            super(gatherer, gate);
        }
        
        @Override
        Tunnel.Gate<T, Out> tunnel() {
            return (Tunnel.Gate<T, Out>) tunnel;
        }
        
        @Override
        public Out poll() throws Exception {
            return tunnel().poll();
        }
        
        @Override
        public void close() throws Exception {
            tunnel().close();
        }
    }
    
    public static <In, T, A> Tunnel.GatedSink<In> chain(Gatherer<In, A, ? extends T> gatherer, Tunnel.GatedSink<T> sink) {
        @SuppressWarnings("unchecked")
        Gatherer<In, A, T> g = (Gatherer<In, A, T>) gatherer;
        return new PrependedSink<>(g, sink);
    }
    
    public static <In, T, A, Out> Tunnel.Gate<In, Out> chain(Gatherer<In, A, ? extends T> gatherer, Tunnel.Gate<T, Out> gate) {
        @SuppressWarnings("unchecked")
        Gatherer<In, A, T> g = (Gatherer<In, A, T>) gatherer;
        return new PrependedGate<>(g, gate);
    }
    
    private interface Accessor<X, Y> {
        void setLatest1(X x);
        Y latest2();
    }
    
    private static class Comm {
        boolean cancelled = false;
    }
    
    public static <T1, T2, T> Tunnel.Source<T> zip(Tunnel.GatedSource<T1> source1,
                                                   Tunnel.GatedSource<T2> source2,
                                                   BiFunction<? super T1, ? super T2, T> merger) {
        Objects.requireNonNull(source1);
        Objects.requireNonNull(source2);
        Objects.requireNonNull(merger);
        
        class Zip implements Tunnel.Source<T> {
            final ReentrantLock lock = new ReentrantLock();
            final Condition ready = lock.newCondition();
            T1 latest1 = null;
            T2 latest2 = null;
            
            <X, Y> Void runSource(Tunnel.GatedSource<X> source,
                                  Accessor<X, Y> access,
                                  Comm comm,
                                  Tunnel.GatedSink<? super T> sink) throws Exception {
                for (X e; (e = source.poll()) != null; ) {
                    lock.lockInterruptibly();
                    try {
                        if (comm.cancelled) {
                            return null;
                        }
                        access.setLatest1(e);
                        if (access.latest2() == null) {
                            do {
                                ready.await();
                                if (comm.cancelled) {
                                    return null;
                                }
                            } while (access.latest2() == null);
                        } else {
                            ready.signal();
                            T t = merger.apply(latest1, latest2);
                            latest1 = null;
                            latest2 = null;
                            if (!sink.offer(t)) {
                                comm.cancelled = true;
                                return null;
                            }
                        }
                    } finally {
                        lock.unlock();
                    }
                }
                return null;
            }
            
            @Override
            public void drainToSink(Tunnel.GatedSink<? super T> sink) throws Exception {
                try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
                    var comm = new Comm();
                    var accessor1 = new Accessor<T1, T2>() {
                        public void setLatest1(T1 t1) { latest1 = t1; }
                        public T2 latest2() { return latest2; }
                    };
                    var accessor2 = new Accessor<T2, T1>() {
                        public void setLatest1(T2 t1) { latest2 = t1; }
                        public T1 latest2() { return latest1; }
                    };
                    scope.fork(() -> runSource(source1, accessor1, comm, sink));
                    scope.fork(() -> runSource(source2, accessor2, comm, sink));
                    scope.join().throwIfFailed();
                }
            }
            
            @Override
            public void close() throws Exception {
                try (source1; source2) {}
            }
        }
        
        return new Zip();
    }
    
    // TODO: The problem of reuse for non-gated Sources/Sinks
    //  eg, in combineLatest: if I attach more sinks, later or concurrently, it should behave naturally
    //  eg, in balance: if I attach more sources, later or concurrently, it should behave naturally
    //
    // Options:
    //  1. Document that non-gated Sources/Sinks may throw if connected to multiple Sinks/Sources
    //  2. Model differently, separating connection from execution
    
    // There is no way to know that a source is empty without polling it.
    // And once we poll it and get something, there is no way to put it back.
    // So if we need to poll 2 sources to produce 1 output, and 1 source is empty,
    // we will end up discarding the element from the other source.
    
    // What happens when 2 threads run combineLatest.forEachUntilCancel?
    // If they share the same state, the algorithm ceases to work, because it
    // assumes only one competing thread, eg when polling, going to sleep,
    // waking up.
    // TODO: Does separate state even mitigate this? Still contending to poll, etc...
    
    public static <T1, T2, T> Tunnel.Source<T> combineLatest(Tunnel.GatedSource<T1> source1,
                                                             Tunnel.GatedSource<T2> source2,
                                                             BiFunction<? super T1, ? super T2, T> merger) {
        Objects.requireNonNull(source1);
        Objects.requireNonNull(source2);
        Objects.requireNonNull(merger);
        
        class CombineLatest implements Tunnel.Source<T> {
            // TODO: Bring-Your-Own-State?
            final ReentrantLock lock = new ReentrantLock();
            final Condition ready = lock.newCondition();
            T1 latest1 = null;
            T2 latest2 = null;
            
            <X, Y> Void runSource(Tunnel.GatedSource<X> source,
                                  Accessor<X, Y> access,
                                  Comm comm,
                                  Tunnel.GatedSink<? super T> sink) throws Exception {
                X e = source.poll();
                lock.lockInterruptibly();
                try {
                    if (comm.cancelled) {
                        return null;
                    }
                    if (e == null) {
                        // If either source is empty, we will never emit
                        comm.cancelled = true; // TODO
                        ready.signal();
                        return null;
                    }
                    // Wait until we have the first element from both sources
                    access.setLatest1(e);
                    if (access.latest2() == null) {
                        do {
                            ready.await();
                            if (comm.cancelled) {
                                return null;
                            }
                        }
                        while (access.latest2() == null);
                    } else {
                        ready.signal();
                        T t = merger.apply(latest1, latest2);
                        if (!sink.offer(t)) {
                            comm.cancelled = true;
                            return null;
                        }
                    }
                } finally {
                    lock.unlock();
                }
                // Normal mode
                while ((e = source.poll()) != null) {
                    lock.lockInterruptibly();
                    try {
                        if (comm.cancelled) {
                            return null;
                        }
                        access.setLatest1(e);
                        T t = merger.apply(latest1, latest2);
                        if (!sink.offer(t)) {
                            // TODO: Wake up other thread from poll()?
                            //  But if we interrupt during poll(), we close the source when we might not have wanted to
                            comm.cancelled = true;
                            return null;
                        }
                    } finally {
                        lock.unlock();
                    }
                }
                return null;
            }
            
            @Override
            public void drainToSink(Tunnel.GatedSink<? super T> sink) throws InterruptedException, ExecutionException {
                try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
                    var comm = new Comm();
                    var accessor1 = new Accessor<T1, T2>() {
                        public void setLatest1(T1 t1) { latest1 = t1; }
                        public T2 latest2() { return latest2; }
                    };
                    var accessor2 = new Accessor<T2, T1>() {
                        public void setLatest1(T2 t1) { latest2 = t1; }
                        public T1 latest2() { return latest1; }
                    };
                    scope.fork(() -> runSource(source1, accessor1, comm, sink));
                    scope.fork(() -> runSource(source2, accessor2, comm, sink));
                    scope.join().throwIfFailed();
                }
            }
            
            @Override
            public void close() throws Exception {
                try (source1; source2) {}
            }
        }
        
        return new CombineLatest();
    }
    
    public static <T, A> Tunnel.Gate<T, A> batch(Supplier<? extends A> batchSupplier,
                                                 BiConsumer<? super A, ? super T> accumulator,
                                                 Function<? super A, Optional<Instant>> deadlineMapper) {
        Objects.requireNonNull(batchSupplier);
        Objects.requireNonNull(accumulator);
        Objects.requireNonNull(deadlineMapper);
        
        class Batch implements TimedGate.Core<T, A> {
            A batch = null;
            Instant currentDeadline = null;
            boolean done = false;
            Throwable err = null;
            
            @Override
            public Instant onInit() {
                return Instant.MAX;
            }
            
            @Override
            public void onOffer(TimedGate.Upstream ctl, T input) throws InterruptedException {
                // Alternative implementations might adjust or reset the buffer instead of blocking
                while (batch != null && currentDeadline == Instant.MIN) {
                    if (!ctl.awaitDownstream()) {
                        return;
                    }
                }
                if (batch == null) {
                    batch = Objects.requireNonNull(batchSupplier.get());
                }
                accumulator.accept(batch, input);
                currentDeadline = deadlineMapper.apply(batch).orElse(null);
                if (currentDeadline != null) {
                    ctl.latchDeadline(currentDeadline);
                }
            }
            
            @Override
            public void onPoll(TimedGate.Downstream<A> ctl) throws ExecutionException {
                if (done) {
                    if (err != null) {
                        throw new ExecutionException(err);
                    }
                    ctl.latchClose();
                    if (batch == null) {
                        return;
                    }
                }
                ctl.latchOutput(batch);
                batch = null;
                currentDeadline = null;
                ctl.latchDeadline(Instant.MAX);
                ctl.signalUpstream();
            }
            
            @Override
            public void onComplete(TimedGate.Upstream ctl, Throwable error) {
                err = error;
                done = true;
                ctl.latchDeadline(Instant.MIN);
            }
        }
        
        var core = new Batch();
        return new TimedGate<>(core);
    }
    
    public static <T> Tunnel.Gate<T, T> tokenBucket(Duration tokenInterval,
                                                    ToLongFunction<T> costMapper,
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
        
        class Throttle implements TimedGate.Core<T, T> {
            Deque<Weighted<T>> queue = null;
            long tempTokenLimit = 0;
            long tokens = 0;
            long cost = 0;
            Instant lastObservedAccrual;
            boolean done = false;
            Throwable err = null;
            
            @Override
            public Instant onInit() {
                queue = new ArrayDeque<>();
                lastObservedAccrual = clock().instant();
                return Instant.MAX;
            }
            
            @Override
            public void onOffer(TimedGate.Upstream ctl, T input) throws InterruptedException {
                // Optional blocking for boundedness, here based on cost rather than queue size
                while (cost >= costLimit) {
                    if (!ctl.awaitDownstream()) {
                        return;
                    }
                }
                long elementCost = costMapper.applyAsLong(input);
                if (elementCost < 0) {
                    throw new IllegalStateException("Element cost cannot be negative");
                }
                cost = Math.addExact(cost, elementCost);
                var w = new Weighted<>(input, elementCost);
                queue.offer(w);
                if (queue.peek() == w) {
                    ctl.latchDeadline(Instant.MIN); // Let downstream do token math
                }
            }
            
            @Override
            public void onPoll(TimedGate.Downstream<T> ctl) throws ExecutionException {
                if (err != null) {
                    throw new ExecutionException(err);
                }
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
                    ctl.signalUpstream();
                    ctl.latchOutput(head.element);
                    head = queue.peek();
                    if (head != null) {
                        if (tokens >= head.cost) {
                            ctl.latchDeadline(Instant.MIN);
                            return;
                        }
                        // Fall-through to scheduling!
                    } else if (!done) {
                        ctl.latchDeadline(Instant.MAX);
                        return;
                    } else {
                        ctl.latchClose();
                        return;
                    }
                }
                // Schedule to wake up when we have enough tokens for next emission
                tempTokenLimit = head.cost;
                long tokensNeeded = head.cost - tokens;
                ctl.latchDeadline(now.plusNanos(tokenIntervalNanos * tokensNeeded - nanosSinceLastAccrual));
            }
            
            @Override
            public void onComplete(TimedGate.Upstream ctl, Throwable error) {
                err = error;
                done = true;
                if (error != null || queue.isEmpty()) {
                    ctl.latchDeadline(Instant.MIN);
                }
            }
        }
        
        var core = new Throttle();
        return new TimedGate<>(core);
    }
    
    public static <T> Tunnel.Gate<T, T> delay(Function<? super T, Instant> deadlineMapper,
                                              int bufferLimit) {
        Objects.requireNonNull(deadlineMapper);
        if (bufferLimit < 1) {
            throw new IllegalArgumentException("bufferLimit must be positive");
        }
        
        class Delay implements TimedGate.Core<T, T> {
            PriorityQueue<Expiring<T>> pq = null;
            boolean done = false;
            Throwable err = null;
            
            @Override
            public Instant onInit() {
                pq = new PriorityQueue<>(bufferLimit);
                return Instant.MAX;
            }
            
            @Override
            public void onOffer(TimedGate.Upstream ctl, T input) throws InterruptedException {
                while (pq.size() >= bufferLimit) {
                    if (!ctl.awaitDownstream()) {
                        return;
                    }
                }
                Instant deadline = Objects.requireNonNull(deadlineMapper.apply(input));
                Expiring<T> e = new Expiring<>(input, deadline);
                pq.offer(e);
                if (pq.peek() == e) {
                    ctl.latchDeadline(deadline);
                }
            }
            
            @Override
            public void onPoll(TimedGate.Downstream<T> ctl) throws ExecutionException {
                if (err != null) {
                    throw new ExecutionException(err);
                }
                Expiring<T> head = pq.poll();
                if (head == null) {
                    ctl.latchClose();
                    return;
                }
                ctl.latchOutput(head.element);
                head = pq.peek();
                if (head != null) {
                    ctl.latchDeadline(head.deadline);
                } else if (!done) {
                    ctl.latchDeadline(Instant.MAX);
                } else {
                    ctl.latchClose();
                }
           }
            
            @Override
            public void onComplete(TimedGate.Upstream ctl, Throwable error) {
                err = error;
                done = true;
                if (error != null || pq.isEmpty()) {
                    ctl.latchDeadline(Instant.MIN);
                }
            }
        }
       
        var core = new Delay();
        return new TimedGate<>(core);
    }
    
    public static <T> Tunnel.Gate<T, T> keepAlive(Duration timeout,
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
        
        class KeepAlive implements TimedGate.Core<T, T> {
            Deque<T> queue = null;
            boolean done = false;
            Throwable err = null;
            
            @Override
            public Instant onInit() {
                queue = new ArrayDeque<>(bufferLimit);
                return clock().instant().plus(timeout);
            }
            
            @Override
            public void onOffer(TimedGate.Upstream ctl, T input) throws InterruptedException {
                while (queue.size() >= bufferLimit) {
                    if (!ctl.awaitDownstream()) {
                        return;
                    }
                }
                queue.offer(input);
                ctl.latchDeadline(Instant.MIN);
            }
            
            @Override
            public void onPoll(TimedGate.Downstream<T> ctl) throws ExecutionException {
                if (err != null) {
                    throw new ExecutionException(err);
                }
                T head = queue.poll();
                if (head != null) {
                    ctl.latchOutput(head);
                    ctl.latchDeadline((!queue.isEmpty() || done) ? Instant.MIN : clock().instant().plus(timeout));
                } else if (!done) {
                    ctl.latchOutput(extraSupplier.get());
                    ctl.latchDeadline(clock().instant().plus(timeout));
                } else {
                    ctl.latchClose();
                }
            }
            
            @Override
            public void onComplete(TimedGate.Upstream ctl, Throwable error) {
                err = error;
                done = true;
                ctl.latchDeadline(Instant.MIN);
            }
        }
        
        var core = new KeepAlive();
        return new TimedGate<>(core);
    }

    public static <T> Tunnel.Gate<T, T> extrapolate(Function<? super T, ? extends Iterator<? extends T>> mapper,
                                                    int bufferLimit) {
        Objects.requireNonNull(mapper);
        if (bufferLimit < 1) {
            throw new IllegalArgumentException("bufferLimit must be positive");
        }
        
        class Extrapolate implements TimedGate.Core<T, T> {
            Deque<T> queue = null;
            Iterator<? extends T> iter = null;
            boolean done = false;
            Throwable err = null;
            
            @Override
            public Instant onInit() {
                queue = new ArrayDeque<>(bufferLimit);
                return Instant.MAX;
            }
            
            @Override
            public void onOffer(TimedGate.Upstream ctl, T input) throws InterruptedException {
                while (queue.size() >= bufferLimit) {
                    if (!ctl.awaitDownstream()) {
                        return;
                    }
                }
                queue.offer(input);
                iter = null;
                ctl.latchDeadline(Instant.MIN);
            }
            
            @Override
            public void onPoll(TimedGate.Downstream<T> ctl) throws ExecutionException {
                if (err != null) {
                    throw new ExecutionException(err);
                }
                T head = queue.poll();
                if (head != null) {
                    ctl.latchOutput(head);
                    if (queue.peek() != null) {
                        ctl.latchDeadline(Instant.MIN);
                    } else if (!done) {
                        iter = Objects.requireNonNull(mapper.apply(head));
                        ctl.latchDeadline(iter.hasNext() ? Instant.MIN : Instant.MAX);
                    } else {
                        ctl.latchClose();
                    }
                } else if (!done) {
                    ctl.latchOutput(iter.next());
                    ctl.latchDeadline(iter.hasNext() ? Instant.MIN : Instant.MAX);
                } else {
                    ctl.latchClose();
                }
            }
            
            @Override
            public void onComplete(TimedGate.Upstream ctl, Throwable error) {
                err = error;
                done = true;
                ctl.latchDeadline(Instant.MIN);
            }
        }
        
        var core = new Extrapolate();
        return new TimedGate<>(core);
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
}
