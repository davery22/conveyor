package io.avery.pipeline;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class TimedGate<In, Out> implements Tunnel.FullGate<In, Out> {
    public interface Core<In, Out> {
        default Clock clock() { return Clock.systemUTC(); }
        Instant onInit() throws Exception;
        void onOffer(SinkController ctl, In input) throws Exception;
        void onPoll(SourceController<Out> ctl) throws Exception;
        void onComplete(SinkController ctl, Throwable error) throws Exception;
    }
    
    public sealed interface SinkController {
        void latchDeadline(Instant deadline);
        boolean awaitSource() throws InterruptedException;
    }
    
    public sealed interface SourceController<T> {
        void latchClose();
        void latchOutput(T output);
        void latchDeadline(Instant deadline);
        void signalSink();
    }
    
    final Core<In, Out> core;
    final ReentrantLock sinkLock = new ReentrantLock();
    final ReentrantLock sourceLock = new ReentrantLock();
    final Condition offered = sourceLock.newCondition();
    final Condition polled = sourceLock.newCondition();
    final Controller controller = new Controller();
    Instant deadline = null;
    Instant latchedDeadline = null;
    Out latchedOutput = null;
    int ctl = 0; // We encode the remaining properties in 6 bits
    
    //int state = NEW;
    //int access = NONE;
    //boolean isSinkWaiting = false;
    //boolean latchedClose = false;
    
    private int state() { return ctl & 0x3; }
    private int access() { return ctl & 0xC; }
    private boolean isSinkWaiting() { return (ctl & 0x10) != 0; }
    private boolean latchedClose() { return (ctl & 0x20) != 0; }
    
    private void setState(int state) { ctl = (ctl & ~0x3) | state; }
    private void setAccess(int access) { ctl = (ctl & ~0xC) | access; }
    private void setIsSinkWaiting(boolean b) { ctl = b ? (ctl | 0x10) : (ctl & ~0x10); }
    private void setLatchedClose() { ctl = (ctl | 0x20); }
    
    // Possible state transitions:
    // NEW -> RUNNING -> COMPLETING -> CLOSED
    // NEW -> RUNNING -> CLOSED
    // NEW -> CLOSED
    private static final int NEW        = 0;
    private static final int RUNNING    = 1;
    private static final int COMPLETING = 2;
    private static final int CLOSED     = 3;
    
    // Access modes, used to verify that calls to the shared Control instance are legal, regardless of casting.
    private static final int NONE   = 0 << 2;
    private static final int SOURCE = 1 << 2;
    private static final int SINK   = 2 << 2;
    
    TimedGate(Core<In, Out> core) {
        this.core = core;
    }
    
    // One instance per TimedStage.
    // Methods protect against some kinds of misuse:
    //  1. Casting to another interface and calling its methods - protected by checking access()
    //  2. Capturing the instance and calling from outside its scope - protected by checking lock ownership
    private final class Controller implements SinkController, SourceController<Out> {
        @Override
        public void latchDeadline(Instant deadline) {
            if (access() < SOURCE || !sourceLock.isHeldByCurrentThread()) {
                throw new IllegalStateException();
            }
            latchedDeadline = Objects.requireNonNull(deadline);
        }
        
        @Override
        public void latchOutput(Out output) {
            if (access() != SOURCE || !sourceLock.isHeldByCurrentThread()) {
                throw new IllegalStateException();
            }
            latchedOutput = Objects.requireNonNull(output);
        }
        
        @Override
        public void latchClose() {
            if (access() != SOURCE || !sourceLock.isHeldByCurrentThread()) {
                throw new IllegalStateException();
            }
            setLatchedClose();
        }
        
        @Override
        public void signalSink() {
            if (access() != SOURCE || !sourceLock.isHeldByCurrentThread()) {
                throw new IllegalStateException();
            }
            setIsSinkWaiting(false);
            polled.signal();
        }
        
        @Override
        public boolean awaitSource() throws InterruptedException {
            if (access() != SINK || !sourceLock.isHeldByCurrentThread()) {
                throw new IllegalStateException();
            }
            if (state() == CLOSED) {
                return false;
            }
            // This may be overwritten while we wait, so save it to stack and restore after.
            Instant savedDeadline = latchedDeadline;
            
            setIsSinkWaiting(true);
            try {
                do {
                    polled.await();
                    if (state() == CLOSED) {
                        return false;
                    }
                }
                while (isSinkWaiting());
                return true;
            } finally {
                latchedDeadline = savedDeadline;
                setAccess(SINK);
            }
        }
    }
    
    private void initIfNew() throws Exception {
        //assert sourceLock.isHeldByCurrentThread();
        if (state() == NEW) {
            deadline = Objects.requireNonNull(core.onInit());
            setState(RUNNING);
        }
    }
    
    private void updateDeadline() {
        //assert sourceLock.isHeldByCurrentThread();
        Instant nextDeadline = latchedDeadline;
        if (nextDeadline != null) {
            if (nextDeadline.isBefore(deadline)) {
                offered.signalAll();
            }
            deadline = nextDeadline;
        }
    }
    
    private boolean awaitDeadline() throws InterruptedException {
        //assert sourceLock.isHeldByCurrentThread();
        Instant savedDeadline = null;
        long nanosRemaining = 0;
        for (;;) {
            if (savedDeadline != (savedDeadline = deadline)) {
                // Check for Instant.MIN/MAX to preempt common causes of ArithmeticException below
                if (savedDeadline == Instant.MIN) {
                    return true;
                } else if (savedDeadline == Instant.MAX) {
                    nanosRemaining = Long.MAX_VALUE;
                } else {
                    Instant now = core.clock().instant();
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
                offered.await();
            } else {
                nanosRemaining = offered.awaitNanos(nanosRemaining);
            }
            if (state() == CLOSED) {
                return false;
            }
        }
    }
    
    @Override
    public boolean offer(In input) throws Exception {
        Objects.requireNonNull(input);
        sinkLock.lockInterruptibly();
        try {
            sourceLock.lockInterruptibly();
            try {
                if (state() >= COMPLETING) {
                    return false;
                }
                initIfNew();
                setAccess(SINK);
                
                core.onOffer(controller, input);
                
                if (state() == CLOSED) { // Possible if onOffer() called awaitSource()
                    return false;
                }
                updateDeadline();
                return true;
            } finally {
                latchedDeadline = null;
                setAccess(NONE);
                sourceLock.unlock();
            }
        } finally {
            sinkLock.unlock();
        }
    }
    
    @Override
    public void complete(Throwable error) throws Exception {
        sinkLock.lockInterruptibly();
        try {
            sourceLock.lockInterruptibly();
            try {
                if (state() >= COMPLETING) {
                    return;
                }
                initIfNew();
                setAccess(SINK);
                
                core.onComplete(controller, error);
                
                if (state() == CLOSED) { // Possible if onComplete() called awaitSource()
                    return;
                }
                updateDeadline();
                setState(COMPLETING);
            } finally {
                latchedDeadline = null;
                setAccess(NONE);
                sourceLock.unlock();
            }
        } finally {
            sinkLock.unlock();
        }
    }
    
    @Override
    public Out poll() throws Exception {
        for (;;) {
            sourceLock.lockInterruptibly();
            try {
                if (state() == CLOSED) {
                    return null;
                }
                initIfNew();
                if (!awaitDeadline()) {
                    return null;
                }
                setAccess(SOURCE);
                
                core.onPoll(controller);
                
                updateDeadline();
                if (latchedClose()) {
                    close();
                }
                if (latchedOutput != null) {
                    return latchedOutput;
                }
            } finally {
                latchedOutput = null;
                latchedDeadline = null;
                setAccess(NONE);
                sourceLock.unlock();
            }
        }
    }
    
    @Override
    public void close() {
        sourceLock.lock();
        try {
            if (state() == CLOSED) {
                return;
            }
            setState(CLOSED);
            polled.signal(); // Wake sink blocked in awaitSource()
            offered.signalAll(); // Wake sources blocked in awaitDeadline()
        } finally {
            sourceLock.unlock();
        }
    }
}