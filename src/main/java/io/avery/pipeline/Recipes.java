package io.avery.pipeline;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.*;

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
            final Condition opened = lock.newCondition();
            final Condition filled = lock.newCondition();
            final Condition drained = lock.newCondition();
            boolean done = false;
            boolean open = false;
            long nextDeadline = 0L;
            A openWindow = null;
            
            void accumulate(T item) throws InterruptedException {
                lock.lockInterruptibly();
                try {
                    // 1. Wait for consumer to flush
                    // 2. Fill the window
                    // 3. Wake up consumer to flush
                    while (open && windowReady.test(openWindow)) {
                        drained.await();
                    }
                    if (!open) {
                        openWindow = supplier.get();
                    }
                    accumulator.accept(openWindow, item);
                    if (!open) {
                        nextDeadline = System.nanoTime() + timeoutNanos;
                        open = true;
                        opened.signal();
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
                    opened.signal();
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
                        // Wait if no window is open
                        while (!open && !done) {
                            opened.await();
                        }
                        if (!done) { // Implies open
                            // 1. Wait for producer(s) to fill the window, finish, or timeout
                            // 2. Drain the window
                            // 3. Wake up producer(s) to refill
                            if (!windowReady.test(openWindow)) {
                                do {
                                    long nanosRemaining = nextDeadline - System.nanoTime();
                                    if (nanosRemaining <= 0L) {
                                        break;
                                    }
                                    filled.awaitNanos(nanosRemaining);
                                } while (!windowReady.test(openWindow) && !done);
                            }
                        }
                        if (!open) { // Implies done
                            break;
                        }
                        closedWindow = openWindow;
                        openWindow = null;
                        open = false;
                        drained.signalAll();
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
    
    public static void backpressureTimeout() {
        // Throws if a timeout elapses between upstream emission and downstream consumption.
        // Probably does not block upstream. Unbounded buffer?
        
        // Inherently async (addresses speed mismatch between upstream/downstream).
    }
    
    // -------
    
    public static void batchWeighted() {
        // Like collectWithin, but downstream does not wait for an open window to be 'full'.
        // Downstream will consume an open window as soon as it sees it.
        // No waiting = no timeout.
        // Upstream blocks if the open window is 'full'.
        
        // Inherently async (addresses speed mismatch between upstream/downstream).
    }
    
    public static void buffer() {
        // Like batchWeighted, but is not limited to blocking upstream when the open window is 'full'.
        // It can alternatively adjust (DropHead, DropTail) or reset (DropBuffer) the window, or throw (Fail).
        // (Blocking or dropping would essentially be a form of throttling, esp. if downstream is fixed-rate.)
        // Might be obviated by a general impl of batchWeighted.
        
        // Inherently async (addresses speed mismatch between upstream/downstream).
    }
    
    public static void conflateWithSeed() {
        // Like batchWeighted, but accumulates instead of blocking upstream when the open window is 'full'.
        // Would be obviated by a general impl of buffer.
        
        // Inherently async (addresses speed mismatch between upstream/downstream).
    }
    
    // -------
    
    public static void delayWith() {
        // Shifts element emission in time by a specified amount.
        // Implement by buffering each element with a deadline.
        // Maybe change to allow reordering if new elements have a sooner deadline than older elements?
        
        // Could be async or sync (sync would of course be constrained to run on arrivals).
    }
    
    public static void extrapolate() {
        // Continually flatmaps the most recent element from upstream until upstream emits again.
        
        // Inherently async (addresses speed mismatch between upstream/downstream).
    }
    
    public static void keepAlive() {
        // Injects additional elements if upstream does not emit for a configured amount of time.
        
        // Inherently async (addresses speed mismatch between upstream/downstream).
    }
    
    public static void throttle() {
        // Limits upstream emission rate, while allowing for some 'burstiness'.
        
        // Could be async or sync (sync would of course be constrained to run on arrivals).
    }
    
    public static void throttleFirst() {
        // Drops elements that arrive within a timeout since the last emission.
        // (Close to throttleLatest, but that buffers the last element for emission in the next window.)
        // OR
        // Partitions into sequential fixed-duration time windows, and emits the first element of each window.
        
        // This can actually be fully synchronous!
    }
    
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
    //    - ex: throttleFirst
    //  2. Sequential expiring batches from time-of-creation (or first element)
    //    - ex: groupedWithin
    //    - riff: Updating batch updates expiration
    //  3. Overlapping expiring emissions / delay-queue
    //    - ex: delayWith
    
    // debounce could be done with sequential expiring batches, if deadline could be adjusted
    //  - batch: { deadline, element }
    //  - if newElement != element, set batch = { newDeadline, newElement }
    
    // upstream:   () -> state
    // upstream:   (state, item) -> deadline
    // downstream: (state, sink) -> deadline
    // TODO: downstream emit should be lockless
    
    public interface Waiter {
        void await();
    }
    
    public static <T,A,R> void boundary(Iterable<? extends T> source,
                                        Function<? super Waiter, ? extends A> factory,
                                        BiFunction<? super A, ? super T, Instant> feeder,
                                        BiFunction<? super A, Consumer<? super R>, Instant> flusher) {
        // 0. initial
        //   - downstream is waiting on upstream to start
        // 1. factory is initially called with Blocker to produce initial state
        //   - can store Blocker on state
        // 2. feeder is called with state and element
        //   - can accumulate element onto state
        //   - can store weight on state to tell if we should block, drop tail, etc
        //   - return an instant to indicate when flushing should resume
        // 3. flusher is called with state
        //   - can emit output to consumer
        //   - can update state to reset window or remove element(s)
        //   - return an instant to indicate when feeding should resume
        
        // Problem?: upstream can block before downstream has a deadline
        // Problem: feeder/flusher can throw checked exceptions
        
        // Upstream blocks waiting for downstream to consume state
        // Downstream blocks waiting for upstream to prepare state
        
        Waiter waiter = () -> {};
    }
    
    public static void main() {
        boundary(List.of(1),
                 b -> new Object(){
                     final Waiter waiter = b;
                     List<Integer> window = null;
                     Instant nextDeadline = Instant.MAX;
                 },
                 (state, el) -> {
                     while (state.window != null && state.window.size() >= 10) {
                         state.waiter.await();
                     }
                     boolean open = state.window != null;
                     if (!open) {
                         state.window = new ArrayList<>(10);
                     }
                     state.window.add(el);
                     if (!open) {
                         state.nextDeadline = Instant.now().plusSeconds(5);
                     }
                     return state.nextDeadline;
                 },
                 (state, sink) -> {
                     state.waiter.await();
                     sink.accept(state.window);
                     state.window = null;
                     return Instant.MIN; // Do not block upstream
                 }
        );
        
        // Is this worth it, over just implementing accumulate(), finish(), consume()?
    }
}
