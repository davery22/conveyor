package io.avery.pipeline;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Phaser;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A 'StructuredTaskScope-lite', whose only job is to join after (perhaps rounds of) forking. The actual forking will
 * delegate to something else (eg a real STS that knows how to handle completions).
 */
class SubScope implements Executor, AutoCloseable {
    final Executor executor;
    final Phaser phaser;
    final Thread owner;
    final Map<Thread, AtomicBoolean> threads = new ConcurrentHashMap<>();
    volatile boolean isShutdown = false;
    
    public SubScope(Executor executor) {
        this.executor = executor;
        this.phaser = new Phaser(1);
        this.owner = Thread.currentThread();
    }
    
    @Override
    public void execute(Runnable task) {
        if (phaser.register() < 0) {
            throw new IllegalStateException("SubScope is closed");
        }
        try {
            // TODO: We assume that whatever we submit to the executor will run eventually... else close() will deadlock
            executor.execute(() -> {
                var thread = Thread.currentThread();
                var lock = new AtomicBoolean(false);
                threads.put(thread, lock);
                try {
                    // If owner called execute, and executor ran task on the same thread, then we would deadlock if
                    // task calls join() or close(). (In practical usage, a same-thread executor would likely
                    // deadlock anyway, eg trying to run() one side of a Segue.)
                    ensureNotOwner();
                    if (!isShutdown) {
                        task.run();
                    }
                } finally {
                    while (!lock.compareAndSet(false, true)) {
                        Thread.onSpinWait();
                    }
                    threads.remove(thread);
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
        ensureOwner();
        int phase = phaser.arrive();
        if (phase < 0) {
            throw new IllegalStateException("SubScope is closed");
        }
        phaser.awaitAdvanceInterruptibly(phase);
    }
    
    @Override
    public void close() {
        ensureOwner();
        int phase = phaser.arriveAndDeregister();
        if (phase < 0) {
            return;
        }
        isShutdown = true;
        // Interrupt running threads.
        // Note that unlike STS we do not 'own' these threads, and if the executor pools them, our interrupt may
        // race with the thread picking up another task. To avoid this, we associate a small 'lock' with each
        // thread, to momentarily block the thread if it is about to exit the task while we are interrupting it.
        threads.forEach((thread, lock) -> {
            if (lock.compareAndSet(false, true)) {
                try {
                    thread.interrupt();
                } catch (Throwable ignore) { }
                lock.set(false);
            }
        });
        phaser.awaitAdvance(phase); // Phaser is terminated upon return
    }
    
    private void ensureOwner() {
        if (Thread.currentThread() != owner) {
            throw new WrongThreadException("Current thread not owner");
        }
    }
    
    private void ensureNotOwner() {
        if (Thread.currentThread() == owner) {
            throw new WrongThreadException("Current thread is owner");
        }
    }
}
