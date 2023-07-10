package io.avery.pipeline;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

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
        Recipes.collectWithinSync(
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
                lock.lock();
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
            
            void finish() {
                lock.lock();
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
                    lock.lock();
                    try {
                        // Wait if no window is open
                        while (!open && !done) {
                            opened.await();
                        }
                        if (!done) {
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
                        if (!open) {
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
}
