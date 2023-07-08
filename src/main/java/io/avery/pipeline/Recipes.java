package io.avery.pipeline;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

public class Recipes {
    private Recipes() {} // Utility
    
    public <T> void groupedWithin(Iterable<T> tasks,
                                  int windowSize,
                                  Duration windowTimeout,
                                  Consumer<? super List<T>> downstream) throws InterruptedException, ExecutionException {
        var timeoutNanos = windowTimeout.toNanos();
    
        try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
            var openWindow = new ArrayList<T>(windowSize);      // Supplier
            var lock = new ReentrantLock();
            var filled = lock.newCondition();
            var drained = lock.newCondition();
            var done = new Object(){ boolean yet = false; };
    
            // Atomically:
            // If the queue is full AND there is not a waiting flush, then submit a flush
            
            // Better: Timeout starts when the first item is added to a new window
    
            // Producer
            scope.fork(() -> {
                for (var task : tasks) {
                    lock.lock();
                    try {
                        // 1. Wait for consumer to flush
                        // 2. Fill the queue
                        // 3. Wake up consumer to flush
                        while (openWindow.size() >= windowSize) {  // Weight function
                            drained.await();
                        }
                        openWindow.add(task);                      // Accumulator
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
                    done.yet = true;
                    filled.signal();
                } finally {
                    lock.unlock();
                }
        
                return null;
            });
    
            // Consumer
            scope.fork(() -> {
                long nextDeadline = System.nanoTime() + timeoutNanos;
                while (true) {
                    List<T> closedWindow;
                    lock.lock();
                    try {
                        if (!done.yet) {
                            // 1. Wait for producer(s) to fill the queue, or timeout
                            // 2. Drain the queue
                            // 3. Wake up producer(s) to refill
                            long nanosRemaining = nextDeadline - System.nanoTime();
                            while (openWindow.size() < windowSize) {      // Negated weight function
                                if (nanosRemaining <= 0L) {
                                    break;
                                }
                                nanosRemaining = filled.awaitNanos(nanosRemaining);
                            }
                            nextDeadline = System.nanoTime() + timeoutNanos;
                            closedWindow = List.copyOf(openWindow);       //
                            openWindow.clear();                           // 'Supplier' (reset)
                            drained.signalAll();
                        } else if (!openWindow.isEmpty()) {               // ???
                            closedWindow = List.copyOf(openWindow);       //
                            openWindow.clear();                           // 'Supplier' (reset)
                        } else {
                            break;
                        }
                    } finally {
                        lock.unlock();
                    }
                    if (!closedWindow.isEmpty()) {                        // ???
                        downstream.accept(closedWindow);                  // (Finisher)
                    }
                }
                return null;
            });
    
            scope.join().throwIfFailed();
        }
    }
}
