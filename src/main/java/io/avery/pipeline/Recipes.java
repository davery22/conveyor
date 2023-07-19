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
            var tunnel = Recipes.throttle(
                Duration.ofSeconds(1),
                String::length,
                10,
                100
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
                tunnel.forEach(System.out::println);
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
        default Clock clock() {
            return Clock.systemUTC();
        }
        Instant init();
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
        final TimedProducerControl pctl = new TimedProducerControl();
        final TimedConsumerControl cctl = new TimedConsumerControl();
        Instant deadline = null;
        boolean isProducerWaiting = false;
        boolean isProducerRunning = false;
        int state = NEW;
        
        // Possible state transitions:
        // NEW -> RUNNING -> COMPLETING -> CLOSED
        // NEW -> RUNNING -> CLOSED
        // NEW -> CLOSED
        private static final int NEW = 0;
        private static final int RUNNING = 1;
        private static final int COMPLETING = 2;
        private static final int CLOSED = 3;
        
        TimedTunnel(TimedTunnelConfig<In, Out> config) {
            this.config = config;
        }
        
        class TimedProducerControl implements TimedTunnelConfig.ProducerControl<In>, TimedTunnelConfig.CompleterControl {
            boolean accessible = false;
            In input = null;
            Instant latchedDeadline = null;
            
            @Override
            public void latchDeadline(Instant deadline) {
                if (!accessible) {
                    throw new IllegalStateException();
                }
                latchedDeadline = Objects.requireNonNull(deadline);
            }
            
            @Override
            public boolean awaitConsumer() throws InterruptedException {
                if (!accessible) {
                    throw new IllegalStateException();
                }
                isProducerWaiting = true;
                do {
                    consumed.await();
                    if (state == CLOSED) {
                        return false;
                    }
                } while (isProducerWaiting);
                return true;
            }
            
            @Override
            public In input() {
                if (!accessible) {
                    throw new IllegalStateException();
                }
                return input;
            }
        }
        
        class TimedConsumerControl implements TimedTunnelConfig.ConsumerControl<Out> {
            boolean accessible = false;
            boolean close = false;
            Out latchedOutput = null;
            Instant latchedDeadline = null;
            
            @Override
            public void latchDeadline(Instant deadline) {
                if (!accessible) {
                    throw new IllegalStateException();
                }
                latchedDeadline = Objects.requireNonNull(deadline);
            }
            
            @Override
            public void latchOutput(Out output) {
                if (!accessible) {
                    throw new IllegalStateException();
                }
                latchedOutput = Objects.requireNonNull(output);
            }
            
            @Override
            public void latchClose() {
                if (!accessible) {
                    throw new IllegalStateException();
                }
                close = true;
            }
            
            @Override
            public void signalProducer() {
                if (!accessible) {
                    throw new IllegalStateException();
                }
                isProducerWaiting = false;
                consumed.signal();
            }
        }
        
        private void initIfNew() {
            //assert lock.isHeldByCurrentThread();
            if (state == NEW) {
                deadline = Objects.requireNonNull(config.init());
                state = RUNNING;
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
        
        // We use an extra Condition and flag to prevent other producers from
        // grabbing the lock when a producer calls awaitConsumer().
        // This is particularly important for ensuring that complete() cannot
        // proceed while another producer is sleeping in awaitConsumer().
        
        @Override
        public boolean offer(In input) throws InterruptedException {
            Objects.requireNonNull(input);
            boolean acquired = false;
            lock.lockInterruptibly();
            try {
                if (state >= COMPLETING) {
                    return false;
                }
                while (isProducerRunning) {
                    noProducers.await();
                    if (state >= COMPLETING) {
                        return false;
                    }
                }
                isProducerRunning = acquired = true;
                initIfNew();
                pctl.input = input;
                
                config.produce(pctl);
                
                updateDeadline(pctl.latchedDeadline);
                return true;
            } catch (Error | RuntimeException | InterruptedException e) {
                close();
                throw e;
            } finally {
                if (acquired) {
                    pctl.latchedDeadline = null;
                    pctl.input = null;
                    isProducerRunning = false;
                    noProducers.signal();
                }
                lock.unlock();
            }
        }
        
        @Override
        public void complete() throws InterruptedException {
            boolean acquired = false;
            lock.lockInterruptibly();
            try {
                if (state >= COMPLETING) {
                    return;
                }
                while (isProducerRunning) {
                    noProducers.await();
                    if (state >= COMPLETING) {
                        return;
                    }
                }
                isProducerRunning = acquired = true;
                initIfNew();
                pctl.accessible = true;
                
                config.complete(pctl);
                
                updateDeadline(pctl.latchedDeadline);
                state = COMPLETING;
            } catch (Error | RuntimeException | InterruptedException e) {
                close();
                throw e;
            } finally {
                if (acquired) {
                    pctl.latchedDeadline = null;
                    pctl.accessible = false;
                    isProducerRunning = false;
                    noProducers.signal();
                }
                lock.unlock();
            }
        }
        
        @Override
        public Out poll() throws InterruptedException {
            for (;;) {
                lock.lockInterruptibly();
                try {
                    if (state == CLOSED) {
                        return null;
                    }
                    initIfNew();
                    
                    // Await deadline, but try to minimize work
                    Clock clock = config.clock();
                    Instant savedDeadline = null;
                    long nanosRemaining = 0;
                    do {
                        if (savedDeadline != (savedDeadline = deadline)) {
                            // Check for Instant.MIN/MAX to preempt common causes of ArithmeticException below
                            if (savedDeadline == Instant.MIN) {
                                break;
                            } else if (savedDeadline == Instant.MAX) {
                                nanosRemaining = Long.MAX_VALUE;
                            } else {
                                Instant now = clock.instant();
                                try {
                                    nanosRemaining = ChronoUnit.NANOS.between(now, savedDeadline);
                                } catch (ArithmeticException e) {
                                    nanosRemaining = now.isBefore(savedDeadline) ? Long.MAX_VALUE : 0;
                                }
                            }
                        }
                        if (nanosRemaining <= 0) {
                            break;
                        } else if (nanosRemaining == Long.MAX_VALUE) {
                            produced.await();
                        } else {
                            nanosRemaining = produced.awaitNanos(nanosRemaining);
                        }
                        if (state == CLOSED) {
                            return null;
                        }
                    } while (true);
                    
                    cctl.accessible = true;
                    
                    config.consume(cctl);
                    
                    updateDeadline(cctl.latchedDeadline);
                    if (cctl.close) {
                        close();
                    }
                    if (cctl.latchedOutput != null) {
                        return cctl.latchedOutput;
                    }
                } catch (Error | RuntimeException | InterruptedException e) {
                    close();
                    throw e;
                } finally {
                    cctl.latchedOutput = null;
                    cctl.latchedDeadline = null;
                    cctl.accessible = false;
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
                consumed.signalAll(); // Wake producers blocked in awaitConsumer()
                produced.signalAll(); // Wake consumers blocked in poll()
            } finally {
                lock.unlock();
            }
        }
    }
    
    public static <T,A> Tunnel<T,A> collectWithin(Supplier<? extends A> supplier,
                                                  BiConsumer<? super A, ? super T> accumulator,
                                                  Predicate<? super A> windowReady,
                                                  Duration windowTimeout) {
        Objects.requireNonNull(supplier);
        Objects.requireNonNull(accumulator);
        Objects.requireNonNull(windowReady);
        Objects.requireNonNull(windowTimeout);
        
        class CollectWithinConfig implements TimedTunnelConfig<T,A> {
            boolean done = false;
            A window = null;
            
            @Override
            public Instant init() {
                return Instant.MAX;
            }
            
            @Override
            public void produce(ProducerControl<T> ctl) throws InterruptedException {
                while (window != null && windowReady.test(window)) {
                    ctl.awaitConsumer();
                }
                boolean open = window != null;
                if (!open) {
                    window = Objects.requireNonNull(supplier.get());
                }
                accumulator.accept(window, ctl.input());
                if (windowReady.test(window)) {
                    ctl.latchDeadline(Instant.MIN);
                } else if (!open) {
                    ctl.latchDeadline(clock().instant().plus(windowTimeout));
                }
            }
            
            @Override
            public void consume(ConsumerControl<A> ctl) {
                if (done) {
                    ctl.latchClose();
                    if (window == null) {
                        return;
                    }
                }
                ctl.latchOutput(window);
                window = null;
                ctl.latchDeadline(Instant.MAX); // Wait indefinitely for next window to open
                ctl.signalProducer();
            }
            
            @Override
            public void complete(CompleterControl ctl) {
                done = true;
                ctl.latchDeadline(Instant.MIN); // Emit whatever is left right away
            }
        }
        
        var config = new CollectWithinConfig();
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
            tmpTokenInterval = Long.MAX_VALUE; // Correct, though unreasonable
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
                    ctl.awaitConsumer();
                }
                T element = ctl.input();
                long elementCost = costMapper.applyAsLong(element);
                if (elementCost < 0) {
                    throw new IllegalStateException("Element cost cannot be negative");
                }
                cost = Math.addExact(cost, elementCost);
                queue.offer(new Weighted<>(element, elementCost));
                if (queue.size() == 1) {
                    ctl.latchDeadline(Instant.MIN);
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
                } else {
                    // This will temporarily allow a higher token limit if head.cost > tokenLimit
                    tempTokenLimit = head.cost;
                }
                // Schedule to wake up when we have enough tokens for next emission
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
        // batchWeighted (+ buffer/conflate)
        var state9 = new Object(){
            List<Integer> window = null;
        };
        boundary(
            List.of(1),
            Instant.MAX,
            ctl -> {
                // For buffer/conflate, adjust or reset the window, or throw, instead of waiting
                while (state9.window != null && state9.window.size() >= 10) {
                    ctl.awaitConsumer();
                }
                if (state9.window == null) {
                    state9.window = new ArrayList<>(10);
                }
                state9.window.add(ctl.input());
                ctl.latchDeadline(Instant.MIN);
            },
            ctl -> {
                ctl.latchOutput(state9.window);
                state9.window = null;
                ctl.latchDeadline(Instant.MAX);
            }
        );
        
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
