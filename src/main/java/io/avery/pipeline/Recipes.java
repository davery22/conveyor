package io.avery.pipeline;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

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
        
        Recipes.groupedWithin(() -> in, 10, Duration.ofSeconds(0), System.out::println);
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
    
    public static <T> void groupedWithin(Iterable<T> tasks,
                                         int windowSize,
                                         Duration windowTimeout,
                                         Consumer<? super List<T>> downstream) throws InterruptedException, ExecutionException {
        if (windowSize < 1) {
            throw new IllegalArgumentException("windowSize must be positive");
        }
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
    
        try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
            var openWindow = new ArrayList<T>(windowSize);      // Supplier
            var lock = new ReentrantLock();
            var opened = lock.newCondition();
            var filled = lock.newCondition();
            var drained = lock.newCondition();
            var state = new Object() {
                boolean done = false;
                boolean open = false;
                long nextDeadline = 0L;
            };
    
            // Producer
            scope.fork(() -> {
                for (var task : tasks) {
                    lock.lock();
                    try {
                        // 1. Wait for consumer to flush
                        // 2. Fill the window
                        // 3. Wake up consumer to flush
                        while (openWindow.size() >= windowSize) {  // Weight function
                            drained.await();
                        }
                        boolean first = openWindow.isEmpty();
                        first &= openWindow.add(task);             // Accumulator
                        if (first) {
                            state.nextDeadline = System.nanoTime() + timeoutNanos;
                            state.open = true;
                            opened.signal();
                        }
                        if (openWindow.size() >= windowSize) {     // Weight function
                            filled.signal();
                        }
                    } finally {
                        lock.unlock();
                    }
                }
        
                // If there were multiple producers, this would come after ALL of them.
                lock.lock();
                try {
                    state.done = state.open = true;
                    opened.signal();
                    filled.signal();
                } finally {
                    lock.unlock();
                }
        
                return null;
            });
    
            // Consumer
            scope.fork(() -> {
                while (true) {
                    List<T> closedWindow;
                    lock.lock();
                    try {
                        // Wait if no window is open
                        while (!state.open && !state.done) {
                            opened.await();
                        }
                        if (!state.done) {
                            // 1. Wait for producer(s) to fill the window, finish, or timeout
                            // 2. Drain the window
                            // 3. Wake up producer(s) to refill
                            if (openWindow.size() < windowSize) {                        // Negated weight function
                                do {
                                    long nanosRemaining = state.nextDeadline - System.nanoTime();
                                    if (nanosRemaining <= 0L) {
                                        break;
                                    }
                                    filled.awaitNanos(nanosRemaining);
                                } while (openWindow.size() < windowSize && !state.done); // Negated weight function
                            }
                            closedWindow = List.copyOf(openWindow);                      //
                            openWindow.clear();                                          // 'Supplier' (reset)
                            state.open = false;
                            drained.signalAll();
                        } else if (!openWindow.isEmpty()) {                              // ???
                            closedWindow = List.copyOf(openWindow);                      //
                            openWindow.clear();                                          // 'Supplier' (reset)
                        } else {
                            break;
                        }
                    } finally {
                        lock.unlock();
                    }
                    if (!closedWindow.isEmpty()) {                                       // ???
                        downstream.accept(closedWindow);                                 // (Finisher)
                    }
                }
                return null;
            });
    
            scope.join().throwIfFailed();
        }
    }
}
