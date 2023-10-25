package io.avery.conveyor;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A {@link Belt.StepSegue StepSegue} that uses deadlines to manage timed waits for pushes and pulls, and provides
 * several lifecycle hooks to simplify state management. Each lifecycle hook executes under synchronization, ensuring
 * exclusive access to state. Lifecycle hooks receive controller objects with methods to "latch" deadlines or outputs.
 * Latched deadlines and outputs are applied after the hook returns, or discarded if the hook throws an exception.
 *
 * @param <In> the sink's input element type
 * @param <Out> the source's output element type
 */
public abstract class DeadlineSegue<In, Out> implements Belt.StepSegue<In, Out> {
    /**
     * Returns the clock that this segue uses for timed waits. Deadlines are compared to the clock's current
     * {@code Instant} to calculate how long to wait.
     *
     * <p>The default clock is {@code Clock.systemUTC()}, which provides the current instant as if by calling
     * {@code Instant.now()}.
     *
     * @return the clock that this segue uses for timed waits.
     */
    protected Clock clock() { return Clock.systemUTC(); }
    
    /**
     * Runs synchronized when the first {@code pull}, {@code push}, or {@code complete} reaches this segue's source or
     * sink, before awaiting any deadlines. The given controller can be used to latch initial push or pull deadlines.
     * Otherwise, the initial push deadline is {@code Instant.MIN}, and the initial pull deadline is
     * {@code Instant.MAX}. This method can also be used to late-initialize state for this segue.
     *
     * <p>Latched values are discarded if this method throws an exception.
     *
     * @param ctl the controller
     * @throws Exception if unable to initialize
     */
    protected abstract void onInit(SinkController ctl) throws Exception;
    
    /**
     * Runs synchronized each time an input element is pushed to this segue's sink, after awaiting the current push
     * deadline. The given controller can be used to latch subsequent push or pull deadlines. The input element can be
     * accumulated onto this segue's state, in preparation for later pushes or pulls.
     *
     * <p>Latched values are discarded if this method throws an exception.
     *
     * @param ctl the controller
     * @param input the pushed input element
     * @throws Exception if unable to push
     */
    protected abstract void onPush(SinkController ctl, In input) throws Exception;
    
    /**
     * Runs synchronized each time an output element is pulled from this segue's source, after awaiting the current
     * pull deadline. The given controller can be used to latch subsequent push or pull deadlines, as well as to latch
     * an output element or {@code close} signal. If no output or {@code close} is latched, the {@code pull} will
     * restart after applying any latched deadlines.
     *
     * <p>Latched values are discarded if this method throws an exception.
     *
     * @param ctl the controller
     * @throws Exception if unable to pull
     */
    protected abstract void onPull(SourceController<Out> ctl) throws Exception;
    
    /**
     * Runs synchronized when this segue's source is completed (normally). The given controller can be used to latch a
     * subsequent pull deadline. The push deadline will be ignored, since the sink will no longer accept pushes after
     * this method returns normally.
     *
     * <p>Latched values are discarded if this method throws an exception.
     *
     * @param ctl the controller
     * @throws Exception if unable to complete
     */
    protected abstract void onComplete(SinkController ctl) throws Exception;
    
    /**
     * A controller for sink-like operations on a {@code DeadlineSegue}.
     */
    public sealed interface SinkController {
        /**
         * Latches a new {@code deadline} for subsequent pushes to await before running.
         *
         * <p>Subsequent calls to this method in the same hook invocation will replace the latched deadline.
         *
         * @param deadline the deadline
         * @throws NullPointerException if deadline is null
         */
        void latchPushDeadline(Instant deadline);
        
        /**
         * Latches a new {@code deadline} for subsequent pulls to await before running.
         *
         * <p>Subsequent calls to this method in the same hook invocation will replace the latched deadline.
         *
         * @param deadline the deadline
         * @throws NullPointerException if deadline is null
         */
        void latchPullDeadline(Instant deadline);
    }
    
    /**
     * A controller for source-like operations on a {@code DeadlineSegue}.
     *
     * @param <Out> the output element type
     */
    public sealed interface SourceController<Out> {
        /**
         * Latches a new {@code deadline} for subsequent pushes to await before running.
         *
         * <p>Subsequent calls to this method in the same hook invocation will replace the latched deadline.
         *
         * @param deadline the deadline
         * @throws NullPointerException if deadline is null
         */
        void latchPushDeadline(Instant deadline);
        
        /**
         * Latches a new {@code deadline} for subsequent pulls to await before running.
         *
         * <p>Subsequent calls to this method in the same hook invocation will replace the latched deadline.
         *
         * @param deadline the deadline
         * @throws NullPointerException if deadline is null
         */
        void latchPullDeadline(Instant deadline);
        
        /**
         * Latches an {@code output} element for the enclosing {@code pull} to return.
         *
         * <p>Subsequent calls to this method in the same hook invocation will replace the latched output.
         *
         * @param output an output element
         * @throws NullPointerException if output is null
         */
        void latchOutput(Out output);
        
        /**
         * Latches a {@code close} operation, to be applied before the enclosing {@code pull} returns. After closing the
         * segue's source, subsequent pulls on the source will return {@code null} without waiting, and subsequent
         * pushes to the sink will return {@code false} without waiting.
         */
        void latchClose();
    }
    
    /**
     * Constructs a new {@code DeadlineSegue}.
     */
    protected DeadlineSegue() {
    }
    
    /**
     * Returns the {@link Belt.StepSink sink} side of this segue.
     *
     * <p>The sink can be safely pushed to and completed concurrently.
     *
     * @return the sink side of this segue
     */
    @Override
    public Belt.StepSink<In> sink() {
        return new Sink();
    }
    
    /**
     * Returns the {@link Belt.StepSource source} side of this segue.
     *
     * <p>The source can be safely pulled and closed concurrently.
     *
     * @return the source side of this segue
     */
    @Override
    public Belt.StepSource<Out> source() {
        return new Source();
    }
    
    final ReentrantLock lock = new ReentrantLock();
    final Condition readyForSource = lock.newCondition();
    final Condition readyForSink = lock.newCondition();
    final Controller controller = new Controller();
    Instant pushDeadline = Instant.MIN;
    Instant pullDeadline = Instant.MAX;
    Instant latchedPushDeadline = null;
    Instant latchedPullDeadline = null;
    Out latchedOutput = null;
    Throwable exception = null;
    int ctl = 0; // We encode the remaining properties in 5 bits
    
    //int state = NEW;
    //int access = NONE;
    //boolean latchedClose = false;
    
    private int state() { return ctl & 0x3; }
    private int access() { return ctl & 0xC; }
    private boolean latchedClose() { return (ctl & 0x10) != 0; }
    
    private void setState(int state) { ctl = (ctl & ~0x3) | state; }
    private void setAccess(int access) { ctl = (ctl & ~0xC) | access; }
    private void setLatchedClose(boolean val) { ctl = val ? (ctl | 0x10) : (ctl & ~0x10); }
    
    // Possible state transitions:
    // NEW -> RUNNING -> COMPLETING -> CLOSED
    // NEW -> RUNNING -> CLOSED
    // NEW -> CLOSED
    private static final int NEW        = 0;
    private static final int RUNNING    = 1;
    private static final int COMPLETING = 2;
    private static final int CLOSED     = 3;
    
    // Access modes, used to verify that calls to the shared Controller instance are legal, regardless of casting.
    private static final int NONE   = 0 << 2;
    private static final int SOURCE = 1 << 2;
    private static final int SINK   = 2 << 2;
    
    // One instance per DeadlineSegue.
    // Methods protect against some kinds of misuse:
    //  1. Casting to another interface and calling its methods - protected by checking access()
    //  2. Capturing the instance and calling from outside its scope - protected by checking lock ownership
    private final class Controller implements SinkController, SourceController<Out> {
        @Override
        public void latchPushDeadline(Instant deadline) {
            if (access() < SOURCE || !lock.isHeldByCurrentThread()) {
                throw new IllegalStateException();
            }
            latchedPushDeadline = Objects.requireNonNull(deadline);
        }
        
        @Override
        public void latchPullDeadline(Instant deadline) {
            if (access() < SOURCE || !lock.isHeldByCurrentThread()) {
                throw new IllegalStateException();
            }
            latchedPullDeadline = Objects.requireNonNull(deadline);
        }
        
        @Override
        public void latchOutput(Out output) {
            if (access() != SOURCE || !lock.isHeldByCurrentThread()) {
                throw new IllegalStateException();
            }
            latchedOutput = Objects.requireNonNull(output);
        }
        
        @Override
        public void latchClose() {
            if (access() != SOURCE || !lock.isHeldByCurrentThread()) {
                throw new IllegalStateException();
            }
            setLatchedClose(true);
        }
    }
    
    private void initIfNew() throws Exception {
        //assert lock.isHeldByCurrentThread();
        if (state() == NEW) {
            setAccess(SINK);
            onInit(controller);
            updatePushDeadline();
            updatePullDeadline();
            setState(RUNNING);
        }
    }
    
    private void updatePushDeadline() {
        //assert lock.isHeldByCurrentThread();
        Instant nextDeadline = latchedPushDeadline;
        if (nextDeadline != null) {
            if (nextDeadline.isBefore(pushDeadline)) {
                readyForSink.signalAll();
            }
            pushDeadline = nextDeadline;
        }
    }
    
    private void updatePullDeadline() {
        //assert lock.isHeldByCurrentThread();
        Instant nextDeadline = latchedPullDeadline;
        if (nextDeadline != null) {
            if (nextDeadline.isBefore(pullDeadline)) {
                readyForSource.signalAll();
            }
            pullDeadline = nextDeadline;
        }
    }
    
    private boolean awaitPushDeadline() throws InterruptedException {
        //assert lock.isHeldByCurrentThread();
        Instant savedDeadline = null;
        long nanosRemaining = 0;
        for (;;) {
            if (savedDeadline != (savedDeadline = pushDeadline)) {
                // Check for Instant.MIN/MAX to preempt common causes of ArithmeticException below
                if (savedDeadline == Instant.MIN) {
                    return true;
                } else if (savedDeadline == Instant.MAX) {
                    nanosRemaining = Long.MAX_VALUE;
                } else {
                    Instant now = clock().instant();
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
                readyForSink.await();
            } else {
                nanosRemaining = readyForSink.awaitNanos(nanosRemaining);
            }
            if (state() >= COMPLETING) {
                return false;
            }
        }
    }
    
    private boolean awaitPullDeadline() throws InterruptedException {
        //assert lock.isHeldByCurrentThread();
        Instant savedDeadline = null;
        long nanosRemaining = 0;
        for (;;) {
            if (savedDeadline != (savedDeadline = pullDeadline)) {
                // Check for Instant.MIN/MAX to preempt common causes of ArithmeticException below
                if (savedDeadline == Instant.MIN) {
                    return true;
                } else if (savedDeadline == Instant.MAX) {
                    nanosRemaining = Long.MAX_VALUE;
                } else {
                    Instant now = clock().instant();
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
                readyForSource.await();
            } else {
                nanosRemaining = readyForSource.awaitNanos(nanosRemaining);
            }
            if (state() == CLOSED) {
                return false;
            }
        }
    }
    
    class Sink implements Belt.StepSink<In> {
        @Override
        public boolean push(In input) throws Exception {
            Objects.requireNonNull(input);
            lock.lockInterruptibly();
            try {
                if (state() >= COMPLETING) {
                    return false;
                }
                initIfNew();
                if (!awaitPushDeadline()) {
                    return false;
                }
                setAccess(SINK);
                
                onPush(controller, input);
                
                updatePushDeadline();
                updatePullDeadline();
                return true;
            } finally {
                latchedPushDeadline = null;
                latchedPullDeadline = null;
                setAccess(NONE);
                lock.unlock();
            }
        }
        
        @Override
        public void complete() throws Exception {
            lock.lockInterruptibly();
            try {
                if (state() >= COMPLETING) {
                    return;
                }
                initIfNew();
                setAccess(SINK);
                
                onComplete(controller);
                
                setState(COMPLETING);
                updatePullDeadline();
                readyForSink.signalAll();
            } finally {
                latchedPushDeadline = null;
                latchedPullDeadline = null;
                setAccess(NONE);
                lock.unlock();
            }
        }
        
        @Override
        public void completeAbruptly(Throwable cause) {
            lock.lock();
            try {
                if (state() == CLOSED) {
                    return;
                }
                setState(CLOSED);
                exception = cause == null ? Belts.NULL_EXCEPTION : cause;
                readyForSink.signalAll();
                readyForSource.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }
    
    class Source implements Belt.StepSource<Out> {
        @Override
        public Out pull() throws Exception {
            for (;;) {
                lock.lockInterruptibly();
                try {
                    if (state() == CLOSED) {
                        if (exception != null) {
                            throw new UpstreamException(exception == Belts.NULL_EXCEPTION ? null : exception);
                        }
                        return null;
                    }
                    initIfNew();
                    if (!awaitPullDeadline()) {
                        if (exception != null) {
                            throw new UpstreamException(exception == Belts.NULL_EXCEPTION ? null : exception);
                        }
                        return null;
                    }
                    setAccess(SOURCE);
                    
                    onPull(controller);
                    
                    updatePushDeadline();
                    updatePullDeadline();
                    if (latchedClose()) {
                        close();
                    }
                    if (latchedOutput != null) {
                        return latchedOutput;
                    }
                } finally {
                    latchedPushDeadline = null;
                    latchedPullDeadline = null;
                    latchedOutput = null;
                    setLatchedClose(false);
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
                readyForSink.signalAll();
                readyForSource.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }
}