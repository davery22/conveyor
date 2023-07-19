package io.avery.pipeline;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.*;
import java.util.stream.Collector;
import java.util.stream.Stream;

public class Recipes {
    private Recipes() {} // Utility
    
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var in = new Iterator<String>() {
            final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            boolean done = false;
            String next = null;
            
            @Override
            public boolean hasNext() {
                if (done) {
                    return false;
                }
                if (next != null) {
                    return true;
                }
                try {
                    next = reader.readLine();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                if ("stop".equalsIgnoreCase(next)) {
                    next = null;
                    done = true;
                    return false;
                }
                return true;
            }
            
            @Override
            public String next() {
                if (done) {
                    throw new NoSuchElementException();
                }
                String s = next;
                next = null;
                return s;
            }
        };
        
//        Recipes.groupedWithin(() -> in, 10, Duration.ofSeconds(5), System.out::println);
        Recipes.collectWithin(
            () -> in,
            () -> new ArrayList<>(10),
            Collection::add,
            window -> window.size() >= 10,
            Duration.ofSeconds(5),
            System.out::println
        );
    }
    
    // Emits a window when any of the following occurs:
    //  - The window is filled
    //  - The upstream completes and the window is non-empty
    //  - A timeout elapses since the first element of the window arrived
    
    // Alternatives:
    //  1. A timeout elapses since the last window was emitted
    //    - But this means the new window may be empty when the timeout elapses!
    //    - Two options:
    //      1. Reset the timeout when it elapses
    //        - This means that the next actual window may emit at an arbitrary time
    //        - Surprising, and arguably a violation of contract (says 'last window', not 'last timeout')
    //      2. Saturate the timeout, emit when the first element of the next window arrives
    //        - Prioritizes responding to the downstream over batching
    //        - Unclear from contract
    //  2. Periodic timeout + allow empty windows
    //    - Prioritizes responding to downstream, even if just noise
    
    // Augments:
    //  1. Control emission rate / set a maximum rate (throttle)
    //    - Maybe this can be done separately, with a downstream operator?
    
    // Notes:
    //  - Multiple open windows would require timeouts for each one (based on when its first element arrived)
    //    - In that case, multiple producers implies multiple consumers, ie the whole operator is duplicated
    //  - Multiple producers can accumulate onto the same window, but the locking prevents any parallelism there
    
    public static <T> void groupedWithin(Iterable<T> source,
                                         int windowSize,
                                         Duration windowTimeout,
                                         Consumer<? super List<T>> downstream) throws InterruptedException, ExecutionException {
        collectWithin(
            source,
            () -> new ArrayList<>(windowSize),
            Collection::add,
            window -> window.size() >= windowSize,
            windowTimeout,
            downstream
        );
    }
    
    // Note this version of collectWithin uses an async boundary, for more accurate timing.
    // A synchronous version could be written, but could only check timing when elements arrive.
    
    public static <T,A> void collectWithin(Iterable<? extends T> source,
                                           Supplier<? extends A> supplier,
                                           BiConsumer<? super A, ? super T> accumulator,
                                           Predicate<? super A> windowReady,
                                           Duration windowTimeout,
                                           Consumer<? super A> downstream) throws InterruptedException, ExecutionException {
        if (windowTimeout.isNegative()) {
            throw new IllegalArgumentException("windowTimeout must be positive");
        }
        
        long tempTimeout;
        try {
            tempTimeout = windowTimeout.toNanos();
        } catch (ArithmeticException e) {
            tempTimeout = Long.MAX_VALUE;
        }
        long timeoutNanos = tempTimeout;
        
        class Organizer {
            final Lock lock = new ReentrantLock();
            final Condition filled = lock.newCondition();
            final Condition flushed = lock.newCondition();
            long nextDeadline = System.nanoTime() + Long.MAX_VALUE; // 'Infinite' timeout
            boolean done = false;
            boolean open = false;
            A openWindow = null;
            
            void accumulate(T item) throws InterruptedException {
                lock.lockInterruptibly();
                try {
                    // 1. Wait for consumer to flush
                    // 2. Fill the window
                    // 3. Wake up consumer to flush
                    while (open && windowReady.test(openWindow)) {
                        flushed.await();
                    }
                    if (!open) {
                        openWindow = supplier.get();
                    }
                    accumulator.accept(openWindow, item);
                    if (!open) {
                        nextDeadline = System.nanoTime() + timeoutNanos;
                        open = true;
                        filled.signal(); // Wake up consumer to update its deadline
                    }
                    if (windowReady.test(openWindow)) {
                        filled.signal();
                    }
                } finally {
                    lock.unlock();
                }
            }
            
            void finish() throws InterruptedException {
                lock.lockInterruptibly();
                try {
                    done = true;
                    filled.signal();
                } finally {
                    lock.unlock();
                }
            }
            
            void consume() throws InterruptedException {
                while (true) {
                    A closedWindow;
                    lock.lockInterruptibly();
                    try {
                        // 1. Wait for producer(s) to fill the window, finish, or timeout
                        // 2. Drain the window
                        // 3. Wake up producer(s) to refill
                        if ((!open || !windowReady.test(openWindow)) && !done) {
                            do {
                                long nanosRemaining = nextDeadline - System.nanoTime();
                                if (nanosRemaining <= 0L) {
                                    break;
                                }
                                filled.awaitNanos(nanosRemaining);
                            } while ((!open || !windowReady.test(openWindow)) && !done);
                        }
                        if (!open) { // Implies done
                            break;
                        }
                        closedWindow = openWindow;
                        openWindow = null;
                        open = false;
                        nextDeadline = System.nanoTime() + Long.MAX_VALUE; // 'Infinite' timeout
                        flushed.signalAll();
                    } finally {
                        lock.unlock();
                    }
                    downstream.accept(closedWindow);
                }
            }
        }
        
        var organizer = new Organizer();
        
        try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
            // Producer
            scope.fork(() -> {
                for (var item : source) {
                    organizer.accumulate(item);
                }
                organizer.finish();
                return null;
            });
            
            // Consumer
            scope.fork(() -> {
                organizer.consume();
                return null;
            });
            
            scope.join().throwIfFailed();
        }
    }
    
    public static <T,A> void collectWithinSync(Iterable<? extends T> source,
                                               Supplier<? extends A> supplier,
                                               BiConsumer<? super A, ? super T> accumulator,
                                               Predicate<? super A> windowReady,
                                               Duration windowTimeout,
                                               Consumer<? super A> downstream) throws InterruptedException, ExecutionException {
        if (windowTimeout.isNegative()) {
            throw new IllegalArgumentException("windowTimeout must be positive");
        }
        
        long tempTimeout;
        try {
            tempTimeout = windowTimeout.toNanos();
        } catch (ArithmeticException e) {
            tempTimeout = Long.MAX_VALUE;
        }
        long timeoutNanos = tempTimeout;
        
        class Organizer {
            boolean open = false;
            long nextDeadline = 0L;
            A openWindow = null;
            
            void accumulate(T item) {
                if (!open) {
                    openWindow = supplier.get();
                }
                accumulator.accept(openWindow, item);
                if (!open) {
                    nextDeadline = System.nanoTime() + timeoutNanos;
                    open = true;
                }
                if (System.nanoTime() - nextDeadline >= 0 || windowReady.test(openWindow)) {
                    emit();
                }
            }
            
            void finish() {
                if (open) {
                    emit();
                }
            }
            
            void emit() {
                var closedWindow = openWindow;
                openWindow = null;
                open = false;
                downstream.accept(closedWindow);
            }
        }
        
        var organizer = new Organizer();
        
        for (var item : source) {
            organizer.accumulate(item);
        }
        organizer.finish();
    }
    
    public static <T> void delay(Iterable<? extends T> source,
                                 Function<? super T, Instant> deadline,
                                 Consumer<? super T> downstream) throws InterruptedException, ExecutionException {
        class Organizer {
            final DelayedT<T> SENTINEL = null;
            final DelayQueue<DelayedT<T>> queue = new DelayQueue<>();
            
            void accumulate(T item) {
                queue.put(new DelayedT<>(item, deadline.apply(item)));
            }
            
            void finish() {
                // TODO: A sentinel won't work here, because it would need to be a Delayed whose deadline is
                //  *just after* the last normal element's deadline.
                //  What we really want is to tell the consumer that no more elements are coming (so don't bother waiting).
                queue.put(SENTINEL);
            }
            
            void consume() throws InterruptedException {
                for (DelayedT<T> d; (d = queue.take()) != SENTINEL; ) {
                    downstream.accept(d.el);
                }
            }
        }
        
        var organizer = new Organizer();
        
        try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
            scope.fork(() -> {
                for (var item : source) {
                    organizer.accumulate(item);
                }
                organizer.finish();
                return null;
            });
            
            scope.fork(() -> {
                organizer.consume();
                return null;
            });
            
            scope.join().throwIfFailed();
        }
    }
    
    private static class DelayedT<T> implements Delayed {
        T el;
        Instant deadline;
        
        DelayedT(T el, Instant deadline) {
            this.el = el;
            this.deadline = deadline;
        }
        
        @Override
        public long getDelay(TimeUnit unit) {
            return unit.toChronoUnit().between(Instant.now(), deadline);
        }
        
        @Override
        public int compareTo(Delayed o) {
            if (o instanceof DelayedT<?> dt) {
                return this.deadline.compareTo(dt.deadline);
            }
            return this.deadline.compareTo(Instant.now().plusNanos(o.getDelay(TimeUnit.NANOSECONDS)));
        }
    }
    
    // TODO: Should we prevent accumulating after finish()? (Guaranteed? Even with concurrent producers?)
    //       Should we prevent consuming after finish()?
    //       Should we control the consume loop, or only consume one item per invocation?
    //       Can downstream cancel? Does emission return boolean?
    
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
        void close() throws InterruptedException;
        
        default void forEach(Consumer<? super Out> action) throws InterruptedException {
            Out e;
            while ((e = poll()) != null) {
                action.accept(e);
            }
        }
        
        default <A,R> R collect(Collector<? super Out, A, R> collector) throws InterruptedException {
            BiConsumer<A, ? super Out> accumulator = collector.accumulator();
            Function<A, R> finisher = collector.finisher();
            A acc = collector.supplier().get();
            Out e;
            while ((e = poll()) != null) {
                accumulator.accept(acc, e);
            }
            return finisher.apply(acc);
        }
    }
    
    public interface TimedTunnelConfig<In, Out> {
        default Clock clock() {
            return Clock.systemUTC();
        }
        Instant initialDeadline();
        void produce(ProducerControl<In> ctl) throws InterruptedException;
        void consume(ConsumerControl<Out> ctl) throws InterruptedException;
        void complete(CompleterControl ctl) throws InterruptedException;
    }
    
    public interface CompleterControl {
        void latchDeadline(Instant deadline);
        boolean awaitConsumer() throws InterruptedException;
    }
    
    public interface ProducerControl<T> {
        void latchDeadline(Instant deadline);
        boolean awaitConsumer() throws InterruptedException;
        T input();
    }
    
    public interface ConsumerControl<T> {
        void latchDeadline(Instant deadline);
        void latchOutput(T output);
    }
    
    private static class TimedTunnel<In, Out> implements Tunnel<In, Out> {
        final TimedTunnelConfig<In, Out> config;
        final ReentrantLock lock = new ReentrantLock();
        final Condition produced = lock.newCondition();
        final Condition consumed = lock.newCondition();
        final TimedProducerControl pctl = new TimedProducerControl();
        final TimedConsumerControl cctl = new TimedConsumerControl();
        Instant deadline = null;
        int state = NOT_STARTED;
        int consumerCallId = 0;
        
        private static final int NOT_STARTED = 0;
        private static final int STARTED = 1;
        private static final int COMPLETED = 2;
        private static final int CLOSED = 3;
        
        TimedTunnel(TimedTunnelConfig<In, Out> config) {
            this.config = config;
        }
        
        class TimedProducerControl implements ProducerControl<In>, CompleterControl {
            In input = null;
            Instant latchedDeadline = null;
            
            @Override
            public void latchDeadline(Instant deadline) {
                if (input == null) {
                    throw new IllegalStateException();
                }
                latchedDeadline = Objects.requireNonNull(deadline);
            }
            
            @Override
            public boolean awaitConsumer() throws InterruptedException {
                // Other producers may run and overwrite this shared control while we wait, so save/restore its fields.
                In savedInput = input;
                Instant savedDeadline = latchedDeadline;
                int savedConsumerCallId = consumerCallId;
                try {
                    while (savedConsumerCallId == consumerCallId) {
                        consumed.await(); // Throws IMSE if lock is not held
                        if (state == CLOSED) {
                            return false;
                        }
                    }
                    return true;
                } finally {
                    input = savedInput;
                    latchedDeadline = savedDeadline;
                }
            }
            
            @Override
            public In input() {
                if (input == null) {
                    throw new IllegalStateException();
                }
                return input;
            }
        }
        
        class TimedConsumerControl implements ConsumerControl<Out> {
            boolean accessible = false;
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
        }
        
        protected void init() throws InterruptedException {
            //assert lock.isHeldByCurrentThread();
            deadline = config.initialDeadline();
        }
        
        private void initIfNeeded() throws InterruptedException {
            //assert lock.isHeldByCurrentThread();
            if (state == NOT_STARTED) {
                init();
                Objects.requireNonNull(deadline);
                state = STARTED;
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
        
        @Override
        public boolean offer(In input) throws InterruptedException {
            Objects.requireNonNull(input);
            lock.lockInterruptibly();
            try {
                if (state >= COMPLETED) {
                    return false;
                }
                initIfNeeded();
                pctl.input = input;
                config.produce(pctl);
                updateDeadline(pctl.latchedDeadline);
                return true;
            } catch (Error | RuntimeException | InterruptedException e) {
                close();
                throw e;
            } finally {
                pctl.latchedDeadline = null;
                pctl.input = null;
                lock.unlock();
            }
        }
        
        @Override
        public void complete() throws InterruptedException {
            lock.lockInterruptibly();
            try {
                if (state >= COMPLETED) {
                    return;
                }
                initIfNeeded();
                // This cast is incorrect, but CompleterControl only uses the value for null-checking.
                // A callback that casts the CompleterControl to ProducerControl and calls input() could see the fake value.
                @SuppressWarnings("unchecked")
                In fake = (In) new Object();
                pctl.input = fake;
                config.complete(pctl);
                updateDeadline(pctl.latchedDeadline);
                state = COMPLETED;
            } catch (Error | RuntimeException | InterruptedException e) {
                close();
                throw e;
            } finally {
                pctl.latchedDeadline = null;
                pctl.input = null;
                lock.unlock();
            }
        }
        
        @Override
        public Out poll() throws InterruptedException {
            lock.lockInterruptibly();
            try {
                if (state == CLOSED) {
                    return null;
                }
                initIfNeeded();
                
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
                return cctl.latchedOutput;
            } catch (Error | RuntimeException | InterruptedException e) {
                close();
                throw e;
            } finally {
                cctl.latchedOutput = null;
                cctl.latchedDeadline = null;
                cctl.accessible = false;
                consumerCallId++;
                consumed.signalAll();
                lock.unlock();
            }
        }
        
        @Override
        public void close() throws InterruptedException {
            lock.lockInterruptibly();
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
    
    public interface Sink<T> {
        void accept(T in) throws Exception;
    }
    
    public static <T,U> void boundary(Iterable<? extends T> source,
                                      Instant initialDeadline,
                                      Sink<? super ProducerControl<T>> producer,
                                      Sink<? super ConsumerControl<U>> consumer) {
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
        // collectWithin
        var state1 = new Object(){
            List<Integer> window = null;
        };
        boundary(
            List.of(1),
            Instant.MAX,
            ctl -> {
                while (state1.window != null && state1.window.size() >= 10) {
                    ctl.awaitConsumer();
                }
                boolean open = state1.window != null;
                if (!open) {
                    state1.window = new ArrayList<>(10);
                }
                state1.window.add(ctl.input());
                if (!open) {
                    ctl.latchDeadline(Instant.now().plusSeconds(5));
                }
            },
            ctl -> {
                ctl.latchOutput(state1.window);
                state1.window = null;
                ctl.latchDeadline(Instant.MAX); // Wait indefinitely for next window
            }
        );
        
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
        class Expiring<T> implements Comparable<Expiring<T>> {
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
        
        // throttleTokenBucket
        class Weighted<T> {
            final T element;
            final long cost;
            
            Weighted(T element, long cost) {
                this.element = element;
                this.cost = cost;
            }
        }
        var state11 = new Object(){
            final Deque<Weighted<Integer>> queue = new ArrayDeque<>();
            final ToLongFunction<Integer> costFn = el -> el;
            final long accrualRateNanos = Duration.ofSeconds(1).toNanos();
            final long tokenLimit = 5;
            final long costLimit = 10;
            long tempTokenLimit = 0;
            long tokens = 0;
            long cost = 0;
            Instant lastObservedAccrual = Instant.now();
        };
        boundary(
            List.of(1),
            Instant.MAX,
            ctl -> {
                // Optional blocking for boundedness, here based on cost rather than queue size
                while (state11.cost >= state11.costLimit) {
                    ctl.awaitConsumer();
                }
                int element = ctl.input();
                long cost = state11.costFn.applyAsLong(element);
                if (cost < 0) {
                    throw new IllegalStateException("Cost cannot be negative");
                }
                state11.cost += cost;
                state11.queue.offer(new Weighted<>(element, cost));
                if (state11.queue.size() == 1) {
                    ctl.latchDeadline(Instant.MIN);
                }
            },
            ctl -> {
                // Increase tokens based on actual amount of time that passed
                Instant now = Instant.now();
                long nanosSinceLastObservedAccrual = ChronoUnit.NANOS.between(state11.lastObservedAccrual, now);
                long nanosSinceLastAccrual = nanosSinceLastObservedAccrual % state11.accrualRateNanos;
                long newTokens = nanosSinceLastObservedAccrual / state11.accrualRateNanos;
                if (newTokens > 0) {
                    state11.lastObservedAccrual = now.minusNanos(nanosSinceLastAccrual);
                    state11.tokens = Math.min(state11.tokens + newTokens, Math.max(state11.tokenLimit, state11.tempTokenLimit));
                }
                // Emit if we can, then schedule next emission
                Weighted<Integer> head = state11.queue.peek();
                if (state11.tokens >= head.cost) {
                    ctl.latchOutput(head.element);
                    state11.tempTokenLimit = 0;
                    state11.tokens -= head.cost;
                    state11.cost -= head.cost;
                    state11.queue.poll();
                    head = state11.queue.peek();
                    if (head == null) {
                        ctl.latchDeadline(Instant.MAX);
                        return;
                    }
                    if (state11.tokens >= head.cost) {
                        ctl.latchDeadline(Instant.MIN);
                        return;
                    }
                } else {
                    // This will temporarily allow a higher token limit if head.cost > tokenLimit
                    state11.tempTokenLimit = head.cost;
                }
                // Schedule to wake up when we have enough tokens for next emission
                long tokensNeeded = head.cost - state11.tokens;
                ctl.latchDeadline(now.plusNanos(state11.accrualRateNanos * tokensNeeded - nanosSinceLastAccrual));
            }
        );
    }
}
