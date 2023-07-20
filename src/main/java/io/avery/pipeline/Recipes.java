package io.avery.pipeline;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.*;
import java.util.stream.Collector;
import java.util.stream.Stream;

public class Recipes {
    private Recipes() {} // Utility
    
    public static void main(String[] args) throws ExecutionException, InterruptedException {
       try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
//            var tunnel = Recipes.throttle(
//                Duration.ofSeconds(1),
//                String::length,
//                10,
//                100
//            );
            var tunnel = Recipes.throttle(
               Duration.ofNanos(1),
               (String s) -> s.length() * 1_000_000_000L,
               0,
               Long.MAX_VALUE
           );
            
            // Producer
            scope.fork(() -> {
                BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
                for (String line; !"stop".equalsIgnoreCase(line = reader.readLine());) {
                    tunnel.offer(line);
                }
                tunnel.complete();
                return null;
            });
            
            // Consumer
            scope.fork(() -> {
//                tunnel.forEach(System.out::println);
                for (String line; (line = tunnel.poll()) != null; ) {
                    System.out.println(line + " " + line.length() + " " + Instant.now());
                }
                return null;
            });
            
            scope.join().throwIfFailed();
        }
    }
    
    // ✅
    public static void backpressureTimeout() {
        // Throws if a timeout elapses between upstream emission and downstream consumption.
        // Probably does not block upstream. Unbounded buffer?
        
        // Inherently async (addresses speed mismatch between upstream/downstream).
    }
    
    // -------
    
    // ✅
    public static void batchWeighted() {
        // Like collectWithin, but downstream does not wait for an open window to be 'full'.
        // Downstream will consume an open window as soon as it sees it.
        // No waiting = no timeout.
        // Upstream blocks if the open window is 'full'.
        
        // Inherently async (addresses speed mismatch between upstream/downstream).
    }
    
    // ✅
    public static void buffer() {
        // Like batchWeighted, but is not limited to blocking upstream when the open window is 'full'.
        // It can alternatively adjust (DropHead, DropTail) or reset (DropBuffer) the window, or throw (Fail).
        // (Blocking or dropping would essentially be a form of throttling, esp. if downstream is fixed-rate.)
        // Might be obviated by a general impl of batchWeighted.
        
        // Inherently async (addresses speed mismatch between upstream/downstream).
    }
    
    // ✅
    public static void conflateWithSeed() {
        // Like batchWeighted, but accumulates instead of blocking upstream when the open window is 'full'.
        // Would be obviated by a general impl of buffer.
        
        // Inherently async (addresses speed mismatch between upstream/downstream).
    }
    
    // -------
    
    // ✅
    public static void delayWith() {
        // Shifts element emission in time by a specified amount.
        // Implement by buffering each element with a deadline.
        // Maybe change to allow reordering if new elements have a sooner deadline than older elements?
        
        // Could be async or sync (sync would of course be constrained to run on arrivals).
    }
    
    // ✅
    public static void extrapolate() {
        // Flatmaps the most recent element from upstream until upstream emits again.
        
        // Inherently async (addresses speed mismatch between upstream/downstream).
    }
    
    // ✅
    public static void keepAlive() {
        // Injects additional elements if upstream does not emit for a configured amount of time.
        
        // Inherently async (addresses speed mismatch between upstream/downstream).
    }
    
    // ✅
    public static void throttle() {
        // Limits upstream emission rate, while allowing for some 'burstiness'.
        
        // Could be async or sync (sync would of course be constrained to run on arrivals).
    }
    
    // ✅
    public static void throttleLast() {
        // Partitions into sequential fixed-duration time windows, and emits the last element of each window.
        
        // Could be async or sync (sync would of course be constrained to run on arrivals).
    }
    
    // ✅
    public static void debounce() {
        // aka throttleWithTimeout
        // Drops elements if another (different?) element arrives before a deadline.
        // Deadline may be computed per-element.
        // Elements will be emitted no sooner than their deadline expires.
        
        // Could be async or sync (sync would of course be constrained to run on arrivals).
    }
    
    // TODO: Version of collectWithin that saturates timeout until minWeight is reached?
    //   ie: [ cannot emit | can emit if timed-out | can emit asap ]
    //                     ^ minWeight             ^ maxWeight
    //   A non-blocking overflow strategy might allow 'overweight' windows to go back to 'underweight'/'in-weight'
    
    // Could return multiple sides of a channel, ie Producer & Consumer (like Rust mpsc)
    // But this feels like layering that needn't exist?
    // What if there are more than two sides? eg, need a third party to poll
    
    // What if we derived a timeout from the window, rather than a 'weight' or 'readiness'?
    //  Inf timeout -> Not Ready; <=0 timeout -> Ready
    //  In that case, should upstream block when Ready? Or allow transitioning back to Not Ready?
    
    // What if we could still adjust a Ready window in response to new elements?
    //  'readiness' is already user-defined - user could store this on the window itself
    //  It would be the (user-defined) accumulator's problem to block on a Ready window if desired
    
    // Throttling - provideToken() for user-controlled token rate? Or baked-in rate controls?
    
    // collectWithin -> expiringBatch ?
    
    // Types of timeout:
    //  1. Sequential fixed-duration time windows
    //    - ex: throttleLast
    //  2. Sequential expiring batches from time-of-creation (or first element)
    //    - ex: groupedWithin
    //    - riff: Updating batch updates expiration
    //  3. Overlapping expiring emissions
    //    - ex: delayWith
    
    // debounce could be done with sequential expiring batches, if deadline could be adjusted
    //  - batch: { deadline, element }
    //  - if newElement != element, set batch = { newDeadline, newElement }
    
    // upstream:   () -> state
    // upstream:   (state, item) -> deadline
    // downstream: (state, sink) -> deadline
    // TODO: downstream emit should be lockless
    
    // Seems like a good idea to 'start' in the consumer (ie create initial state, deadline, etc),
    // Maybe have a method that creates the state + consumer (+ producer?)
    
    // What if state was created outside the handlers?
    // state;
    // (producerControl) -> deadline
    // (consumerControl) -> deadline
    
    // ctl.deadline() isn't very useful if the deadline may have been Instant.MIN / Instant.MAX
    // But, used carefully, it is more accurate than relying on Instant.now() (since downstream may be late)
    // May be better to schedule using Instant.now() instead of Instant.MIN
    
    // Terminal conditions:
    //  1. Producer or consumer throws an uncaught exception
    //    - If run with ShutdownOnFailure, this will commence cleanup, by interrupting other threads
    //    - TODO: Should this also set some 'done' state, etc, in case cleanup tries to call methods again?
    //  2. Downstream indicates cancellation to producer
    //    - TODO: If consumer decides to stop processing, how does it notify producer? Interrupt?
    //    - cancel() ??
    //  3. Producer indicates completion to consumer
    //    - complete()
    //    - TODO: What do later calls to producer (completion or otherwise) do?
    //    - When/how is the consumer informed of completion?
    //      - A: Consumer's poll() returns false
    
    // TODO: What happens if producer tries to awaitConsumer() that has been cancelled? Does anything wake it?
    //  - Maybe awaitConsumer() can return a boolean - false iff the consumer is cancelled
    //    - Still does not address any unmanaged blocking the producer might do (and only interrupt() can help there)
    
    public interface Tunnel<In, Out> extends AutoCloseable {
        boolean offer(In input) throws InterruptedException;
        void complete() throws InterruptedException;
        
        Out poll() throws InterruptedException;
        void close();
        
        default void forEach(Consumer<? super Out> action) throws InterruptedException {
            for (Out e; (e = poll()) !=null; ) {
                action.accept(e);
            }
        }
        
        default <A,R> R collect(Collector<? super Out, A, R> collector) throws InterruptedException {
            BiConsumer<A, ? super Out> accumulator = collector.accumulator();
            Function<A, R> finisher = collector.finisher();
            A acc = collector.supplier().get();
            for (Out e; (e = poll()) != null; ) {
                accumulator.accept(acc, e);
            }
            return finisher.apply(acc);
        }
    }
    
    public interface TimedTunnelConfig<In, Out> {
        default Clock clock() { return Clock.systemUTC(); }
        Instant init() throws InterruptedException;
        void produce(ProducerControl<In> ctl) throws InterruptedException;
        void consume(ConsumerControl<Out> ctl) throws InterruptedException;
        void complete(CompleterControl ctl) throws InterruptedException;
        
        interface CompleterControl {
            void latchDeadline(Instant deadline);
            boolean awaitConsumer() throws InterruptedException;
        }
        
        interface ProducerControl<T> {
            T input();
            void latchDeadline(Instant deadline);
            boolean awaitConsumer() throws InterruptedException;
        }
        
        interface ConsumerControl<T> {
            void latchClose();
            void latchOutput(T output);
            void latchDeadline(Instant deadline);
            void signalProducer();
        }
    }
    
    private static class TimedTunnel<In, Out> implements Tunnel<In, Out> {
        final TimedTunnelConfig<In, Out> config;
        final ReentrantLock lock = new ReentrantLock();
        final Condition produced = lock.newCondition();
        final Condition consumed = lock.newCondition();
        final Condition noProducers = lock.newCondition();
        final Control control = new Control();
        Instant deadline = null;
        Instant latchedDeadline = null;
        In latchedInput = null;
        Out latchedOutput = null;
        int ctl = 0; // We encode the remaining properties in 7 bits
        
        //int state = NEW;
        //int access = NONE;
        //boolean isProducerWaiting = false;
        //boolean isProducerRunning = false;
        //boolean latchedClose = false;
        
        private int state() { return ctl & 0x3; }
        private int access() { return ctl & 0xC; }
        private boolean isProducerWaiting() { return (ctl & 0x10) != 0; }
        private boolean isProducerRunning() { return (ctl & 0x20) != 0; }
        private boolean latchedClose() { return (ctl & 0x40) != 0; }
        
        private void setState(int state) { ctl = (ctl & ~0x3) | state; }
        private void setAccess(int access) { ctl = (ctl & ~0xC) | access; }
        private void setIsProducerWaiting(boolean b) { ctl = b ? (ctl | 0x10) : (ctl & ~0x10); }
        private void setIsProducerRunning(boolean b) { ctl = b ? (ctl | 0x20) : (ctl & ~0x20); }
        private void setLatchedClose() { ctl = (ctl | 0x40); }
        
        // Possible state transitions:
        // NEW -> RUNNING -> COMPLETING -> CLOSED
        // NEW -> RUNNING -> CLOSED
        // NEW -> CLOSED
        private static final int NEW        = 0;
        private static final int RUNNING    = 1;
        private static final int COMPLETING = 2;
        private static final int CLOSED     = 3;
        
        // Control access modes, to verify that calls to the shared Control instance are legal, regardless of casting.
        private static final int NONE      = 0 << 2;
        private static final int CONSUMER  = 1 << 2;
        private static final int PRODUCER  = 2 << 2;
        private static final int COMPLETER = 3 << 2;
        
        TimedTunnel(TimedTunnelConfig<In, Out> config) {
            this.config = config;
        }
        
        private class Control implements TimedTunnelConfig.CompleterControl,
                                         TimedTunnelConfig.ProducerControl<In>,
                                         TimedTunnelConfig.ConsumerControl<Out> {
            @Override
            public void latchDeadline(Instant deadline) {
                if (access() < CONSUMER) {
                    throw new IllegalStateException();
                }
                latchedDeadline = Objects.requireNonNull(deadline);
            }
            
            @Override
            public void latchOutput(Out output) {
                if (access() != CONSUMER) {
                    throw new IllegalStateException();
                }
                latchedOutput = Objects.requireNonNull(output);
            }
            
            @Override
            public void latchClose() {
                if (access() != CONSUMER) {
                    throw new IllegalStateException();
                }
                setLatchedClose();
            }
            
            @Override
            public void signalProducer() {
                if (access() != CONSUMER) {
                    throw new IllegalStateException();
                }
                setIsProducerWaiting(false);
                consumed.signal();
            }
            
            @Override
            public boolean awaitConsumer() throws InterruptedException {
                if (access() < PRODUCER) {
                    throw new IllegalStateException();
                }
                if (state() == CLOSED) {
                    return false;
                }
                // These may be overwritten while we wait, so save them to stack and restore after.
                int savedAccess = access();
                Instant savedDeadline = latchedDeadline;
                
                setIsProducerWaiting(true);
                try {
                    do {
                        consumed.await();
                        if (state() == CLOSED) {
                            return false;
                        }
                    }
                    while (isProducerWaiting());
                    return true;
                } finally {
                    latchedDeadline = savedDeadline;
                    setAccess(savedAccess);
                }
            }
            
            @Override
            public In input() {
                if (access() != PRODUCER) {
                    throw new IllegalStateException();
                }
                return latchedInput;
            }
        }
        
        private void initIfNew() throws InterruptedException{
            //assert lock.isHeldByCurrentThread();
            if (state() == NEW) {
                deadline = Objects.requireNonNull(config.init());
                setState(RUNNING);
            }
        }
        
        private void updateDeadline(Instant nextDeadline) {
            //assert lock.isHeldByCurrentThread();
            if (nextDeadline != null) {
                if (nextDeadline.isBefore(deadline)) {
                    produced.signalAll();
                }
                deadline = nextDeadline;
            }
        }
        
        private boolean awaitDeadline() throws InterruptedException {
            //assert lock.isHeldByCurrentThread();
            Instant savedDeadline = null;
            long nanosRemaining = 0;
            do {
                if (savedDeadline != (savedDeadline = deadline)) {
                    // Check for Instant.MIN/MAX to preempt common causes of ArithmeticException below
                    if (savedDeadline == Instant.MIN) {
                        return true;
                    } else if (savedDeadline == Instant.MAX) {
                        nanosRemaining = Long.MAX_VALUE;
                    } else {
                        Instant now = config.clock().instant();
                        try {
                            nanosRemaining = ChronoUnit.NANOS.between(now, savedDeadline);
                        } catch (ArithmeticException e) {
                            nanosRemaining = now.isBefore(savedDeadline) ? Long.MAX_VALUE : 0;
                        }
                    }
                }
                if (nanosRemaining <= 0) {
                    return true;
                } else if (nanosRemaining == Long.MAX_VALUE) {
                    produced.await();
                } else {
                    nanosRemaining = produced.awaitNanos(nanosRemaining);
                }
                if (state() == CLOSED) {
                    return false;
                }
            } while (true);
        }
        
        // We use an extra Condition and flag to effectively prevent other producers
        // from acquiring the lock when a running producer calls awaitConsumer().
        // This is particularly important for ensuring that complete() cannot proceed
        // while another producer is blocked on awaitConsumer().
        
        private boolean acquireExclusiveProducer() throws InterruptedException {
            //assert lock.isHeldByCurrentThread();
            while (isProducerRunning()) {
                noProducers.await();
                if (state() >= COMPLETING) {
                    return false;
                }
            }
            return true;
        }
        
        @Override
        public boolean offer(In input) throws InterruptedException {
            Objects.requireNonNull(input);
            lock.lockInterruptibly();
            try {
                if (state() >= COMPLETING || !acquireExclusiveProducer()) {
                    return false;
                }
                setIsProducerRunning(true);
                try {
                    initIfNew();
                    setAccess(PRODUCER);
                    latchedInput = input;
                    
                    config.produce(control);
                    
                    if (state() == CLOSED) { // Possible if produce() called awaitConsumer()
                        return false;
                    }
                    updateDeadline(latchedDeadline);
                    noProducers.signal();
                    return true;
                } finally {
                    latchedDeadline = null;
                    latchedInput = null;
                    setAccess(NONE);
                    setIsProducerRunning(false);
                }
            } catch (Error | RuntimeException | InterruptedException e) {
                close();
                throw e;
            } finally {
                lock.unlock();
            }
        }
        
        @Override
        public void complete() throws InterruptedException {
            lock.lockInterruptibly();
            try {
                if (state() >= COMPLETING || !acquireExclusiveProducer()) {
                    return;
                }
                setIsProducerRunning(true);
                try {
                    initIfNew();
                    setAccess(COMPLETER);
                    
                    config.complete(control);
                    
                    if (state() == CLOSED) { // Possible if complete() called awaitConsumer()
                        return;
                    }
                    updateDeadline(latchedDeadline);
                    setState(COMPLETING);
                    noProducers.signalAll();
                } finally {
                    latchedDeadline = null;
                    setAccess(NONE);
                    setIsProducerRunning(false);
                }
            } catch (Error | RuntimeException | InterruptedException e) {
                close();
                throw e;
            } finally {
                lock.unlock();
            }
        }
        
        @Override
        public Out poll() throws InterruptedException {
            for (;;) {
                lock.lockInterruptibly();
                try {
                    if (state() == CLOSED) {
                        return null;
                    }
                    initIfNew();
                    if (!awaitDeadline()) {
                        return null;
                    }
                    setAccess(CONSUMER);
                    
                    config.consume(control);
                    
                    updateDeadline(latchedDeadline);
                    if (latchedClose()) {
                        close();
                    }
                    if (latchedOutput != null) {
                        return latchedOutput;
                    }
                } catch (Error | RuntimeException | InterruptedException e) {
                    close();
                    throw e;
                } finally {
                    latchedOutput = null;
                    latchedDeadline = null;
                    setAccess(NONE);
                    lock.unlock();
                }
            }
        }
        
        @Override
        public void close() {
            lock.lock();
            try {
                if (state() == CLOSED) {
                    return;
                }
                setState(CLOSED);
                consumed.signal(); // Wake producer blocked in awaitConsumer()
                produced.signalAll(); // Wake consumers blocked in poll()
                noProducers.signalAll(); // Wake producers blocked in offer()
            } finally {
                lock.unlock();
            }
        }
    }
    
    public static <T, A> Tunnel<T, A> batch(Supplier<? extends A> batchSupplier,
                                            BiConsumer<? super A, ? super T> accumulator,
                                            Predicate<? super A> batchFull,
                                            Duration batchTimeout) {
        Objects.requireNonNull(batchSupplier);
        Objects.requireNonNull(accumulator);
        Objects.requireNonNull(batchFull);
        Objects.requireNonNull(batchTimeout);
        
        class BatchConfig implements TimedTunnelConfig<T, A> {
            boolean done = false;
            A batch = null;
            
            @Override
            public Instant init() {
                return Instant.MAX;
            }
            
            @Override
            public void produce(ProducerControl<T> ctl) throws InterruptedException {
                // Alternative implementations might adjust or reset the buffer instead of blocking
                while (batch != null && batchFull.test(batch)) {
                    if (!ctl.awaitConsumer()) {
                        return;
                    }
                }
                boolean open = batch != null;
                if (!open) {
                    batch = Objects.requireNonNull(batchSupplier.get());
                }
                accumulator.accept(batch, ctl.input());
                if (batchFull.test(batch)) {
                    ctl.latchDeadline(Instant.MIN);
                } else if (!open) {
                    ctl.latchDeadline(clock().instant().plus(batchTimeout));
                }
            }
            
            @Override
            public void consume(ConsumerControl<A> ctl) {
                if (done) {
                    ctl.latchClose();
                    if (batch == null) {
                        return;
                    }
                }
                ctl.latchOutput(batch);
                batch = null;
                ctl.latchDeadline(Instant.MAX);
                ctl.signalProducer();
            }
            
            @Override
            public void complete(CompleterControl ctl) {
                done = true;
                ctl.latchDeadline(Instant.MIN);
            }
        }
        
        var config = new BatchConfig();
        return new TimedTunnel<>(config);
    }
    
    public static <T> Tunnel<T, T> throttle(Duration tokenInterval,
                                            ToLongFunction<T> costMapper,
                                            long tokenLimit,
                                            long costLimit) {
        Objects.requireNonNull(tokenInterval);
        Objects.requireNonNull(costMapper);
        if ((tokenLimit | costLimit) < 0 || !tokenInterval.isPositive()) {
            throw new IllegalArgumentException();
        }
        
        long tmpTokenInterval;
        try {
            tmpTokenInterval = tokenInterval.toNanos();
        } catch (ArithmeticException e) {
            tmpTokenInterval = Long.MAX_VALUE; // Unreasonable but correct
        }
        long tokenIntervalNanos = tmpTokenInterval;
        
        class ThrottleConfig implements TimedTunnelConfig<T, T> {
            final Deque<Weighted<T>> queue = new ArrayDeque<>();
            long tempTokenLimit = 0;
            long tokens = 0;
            long cost = 0;
            Instant lastObservedAccrual;
            
            @Override
            public Instant init() {
                lastObservedAccrual = clock().instant();
                return Instant.MAX;
            }
            
            @Override
            public void produce(ProducerControl<T> ctl) throws InterruptedException {
                // Optional blocking for boundedness, here based on cost rather than queue size
                while (cost >= costLimit) {
                    if (!ctl.awaitConsumer()) {
                        return;
                    }
                }
                T element = ctl.input();
                long elementCost = costMapper.applyAsLong(element);
                if (elementCost < 0) {
                    throw new IllegalStateException("Element cost cannot be negative");
                }
                cost = Math.addExact(cost, elementCost);
                queue.offer(new Weighted<>(element, elementCost));
                if (queue.size() == 1) {
                    ctl.latchDeadline(Instant.MIN); // Let consumer do token math
                }
            }
            
            @Override
            public void consume(ConsumerControl<T> ctl) {
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
                Weighted<T> head = queue.peek();
                if (tokens >= head.cost) {
                    if (head.element == null) {
                        ctl.latchClose();
                        return;
                    }
                    tempTokenLimit = 0;
                    tokens -= head.cost;
                    cost -= head.cost;
                    queue.poll();
                    ctl.signalProducer();
                    ctl.latchOutput(head.element);
                    head = queue.peek();
                    if (head == null) {
                        ctl.latchDeadline(Instant.MAX);
                        return;
                    }
                    if (tokens >= head.cost) {
                        ctl.latchDeadline(Instant.MIN);
                        return;
                    }
                }
                // Schedule to wake up when we have enough tokens for next emission
                tempTokenLimit = head.cost;
                long tokensNeeded = head.cost - tokens;
                ctl.latchDeadline(now.plusNanos(tokenIntervalNanos * tokensNeeded - nanosSinceLastAccrual));
            }
            
            @Override
            public void complete(CompleterControl ctl) {
                queue.offer(new Weighted<>(null, 0));
                if (queue.size() == 1) {
                    ctl.latchDeadline(Instant.MIN);
                }
            }
        }
        
        var config = new ThrottleConfig();
        return new TimedTunnel<>(config);
    }
    
    public interface Sink<T> {
        void accept(T in) throws Exception;
    }
    
    public static <T,U> void boundary(Iterable<? extends T> source,
                                      Instant initialDeadline,
                                      Sink<? super TimedTunnelConfig.ProducerControl<T>> producer,
                                      Sink<? super TimedTunnelConfig.ConsumerControl<U>> consumer) {
        // initial
        //   - downstream waits for initial deadline
        // when producer is called:
        //   - can accumulate element onto state
        //   - can store weight on state to tell if we should block, drop tail, etc
        //   - return an instant to indicate when consumer should be called next
        // when consumer is called: (deadline expires)
        //   - can flush output to consumer
        //   - can update state to reset window or remove element(s)
        //   - return an instant to indicate when consumer should be called next
        
        // Problem?: upstream can block before downstream has a deadline
        // Problem: feeder/flusher can throw checked exceptions
        
        // Upstream blocks waiting for downstream to consume state
        // Downstream blocks waiting for upstream to prepare state
        
    }
    
    public static void main() {
        // throttleFirst
        var state2 = new Object(){
            int element;
            boolean present;
            Instant coolDownDeadline = Instant.MIN;
        };
        boundary(
            List.of(1),
            Instant.MAX,
            ctl -> {
                if (state2.present || Instant.now().isBefore(state2.coolDownDeadline)) {
                    return;
                }
                state2.present = true;
                state2.element = ctl.input();
                ctl.latchDeadline(Instant.MIN);
            },
            ctl -> {
                state2.coolDownDeadline = Instant.now().plusSeconds(5);
                state2.present = false;
                ctl.latchOutput(state2.element);
                ctl.latchDeadline(Instant.MAX);
            }
        );
        
        // throttleLast
        var state3 = new Object(){
            int element;
            boolean present;
            Instant lastDeadline = Instant.now().plusSeconds(5);
        };
        boundary(
            List.of(1),
            state3.lastDeadline,
            ctl -> {
                state3.present = true;
                state3.element = ctl.input();
            },
            ctl -> {
                if (state3.present) {
                    state3.present = false;
                    ctl.latchOutput(state3.element);
                }
                ctl.latchDeadline(state3.lastDeadline = state3.lastDeadline.plusSeconds(5));
            }
        );
        
        // throttleLatest
        var state4 = new Object(){
            int curr;
            int next;
            boolean currPresent;
            boolean nextPresent;
            Instant coolDownDeadline = Instant.MIN;
        };
        boundary(
            List.of(1),
            Instant.MAX,
            ctl -> {
                if (!state4.currPresent) {
                    state4.curr = ctl.input();
                    state4.currPresent = true;
                    ctl.latchDeadline(state4.coolDownDeadline);
                } else {
                    state4.next = ctl.input();
                    state4.nextPresent = true;
                }
            },
            ctl -> {
                state4.coolDownDeadline = Instant.now().plusSeconds(5);
                ctl.latchOutput(state4.curr);
                if (state4.currPresent = state4.nextPresent) {
                    state4.curr = state4.next;
                    state4.nextPresent = false;
                    ctl.latchDeadline(state4.coolDownDeadline);
                } else {
                    ctl.latchDeadline(Instant.MAX);
                }
            }
        );
        
        // delayWith
        var state7 = new Object(){
            final PriorityQueue<Expiring<Integer>> pq = new PriorityQueue<>();
        };
        boundary(
            List.of(1),
            Instant.MAX,
            ctl -> {
                Instant deadline = Instant.now().plusSeconds(ctl.input());
                Expiring<Integer> e = new Expiring<>(ctl.input(), deadline);
                state7.pq.offer(e);
                if (state7.pq.peek() == e) {
                    ctl.latchDeadline(deadline);
                }
            },
            ctl -> {
                ctl.latchOutput(state7.pq.poll().element);
                Expiring<Integer> head = state7.pq.peek();
                if (head != null) {
                    ctl.latchDeadline(head.deadline);
                } else {
                    ctl.latchDeadline(Instant.MAX);
                }
            }
        );
        
        // extrapolate
        var state5 = new Object(){
            final Deque<Integer> queue = new ArrayDeque<>(10);
            Iterator<Integer> iter;
        };
        boundary(
            List.of(1),
            Instant.MAX,
            ctl -> {
                // Optional blocking for boundedness
                while (state5.queue.size() >= 10) {
                    ctl.awaitConsumer();
                }
                state5.queue.offer(ctl.input());
                state5.iter = null;
                ctl.latchDeadline(Instant.MIN);
            },
            ctl -> {
                if (state5.queue.size() > 1) {
                    ctl.latchOutput(state5.queue.poll());
                    ctl.latchDeadline(Instant.MIN);
                } else {
                    if (state5.iter == null) {
                        int out = state5.queue.poll();
                        state5.iter = Stream.iterate(out + 1,
                                                     i -> i < out + 10,
                                                     i -> i + 1).iterator();
                        ctl.latchOutput(out);
                    } else {
                        ctl.latchOutput(state5.iter.next());
                    }
                    ctl.latchDeadline(state5.iter.hasNext() ? Instant.MIN : Instant.MAX);
                }
            }
        );
        
        // backpressureTimeout
        var state6 = new Object(){
            final Deque<Expiring<Integer>> queue = new ArrayDeque<>();
        };
        boundary(
            List.of(1),
            Instant.MAX,
            ctl -> {
                Instant now = Instant.now();
                Expiring<Integer> head = state6.queue.peek();
                if (head != null && head.deadline.isBefore(now)) {
                    throw new IllegalStateException();
                }
                Expiring<Integer> e = new Expiring<>(ctl.input(), now.plusSeconds(5));
                state6.queue.offer(e);
                ctl.latchDeadline(Instant.MIN);
            },
            ctl -> {
                Instant now = Instant.now();
                Expiring<Integer> head = state6.queue.poll();
                if (head.deadline.isBefore(now)) {
                    throw new IllegalStateException();
                }
                ctl.latchOutput(head.element);
                ctl.latchDeadline(state6.queue.peek() != null ? Instant.MIN : Instant.MAX);
            }
        );
        
        // debounce
        var state8 = new Object(){
            int element;
        };
        boundary(
            List.of(1),
            Instant.MAX,
            ctl -> {
                state8.element = ctl.input();
                ctl.latchDeadline(Instant.now().plusSeconds(5));
            },
            ctl -> {
                ctl.latchOutput(state8.element);
                ctl.latchDeadline(Instant.MAX);
            }
        );
        
        // keepAlive
        var state0 = new Object(){
            final Deque<Integer> queue = new ArrayDeque<>();
        };
        boundary(
            List.of(1),
            Instant.now().plusSeconds(5),
            ctl -> {
                // Optional blocking for boundedness
                while (state0.queue.size() >= 10) {
                    ctl.awaitConsumer();
                }
                state0.queue.offer(ctl.input());
                ctl.latchDeadline(Instant.MIN);
            },
            ctl -> {
                Integer next = state0.queue.poll();
                if (next != null) {
                    ctl.latchOutput(next);
                    ctl.latchDeadline(state0.queue.isEmpty() ? Instant.now().plusSeconds(5) : Instant.MIN);
                } else {
                    ctl.latchOutput(22);
                    ctl.latchDeadline(Instant.now().plusSeconds(5));
                }
            }
        );
        
        // throttleLeakyBucket ("as a queue")
        var state10 = new Object(){
            final Deque<Integer> queue = new ArrayDeque<>();
            Instant coolDownDeadline = Instant.MIN;
        };
        boundary(
            List.of(1),
            Instant.MAX,
            ctl -> {
                if (state10.queue.size() >= 10) {
                    return;
                }
                state10.queue.offer(ctl.input());
                ctl.latchDeadline(state10.coolDownDeadline);
            },
            ctl -> {
                state10.coolDownDeadline = Instant.now().plusSeconds(1);
                ctl.latchOutput(state10.queue.poll());
                ctl.latchDeadline(state10.queue.isEmpty() ? Instant.MAX : state10.coolDownDeadline);
            }
        );
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
