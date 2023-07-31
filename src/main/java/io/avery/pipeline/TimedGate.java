package io.avery.pipeline;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class TimedGate<In, Out> implements Tunnel.Gate<In, Out> {
    public interface Core<In, Out> {
        default Clock clock() { return Clock.systemUTC(); }
        Instant onInit() throws Exception;
        void onOffer(Upstream ctl, In input) throws Exception;
        void onPoll(Downstream<Out> ctl) throws Exception;
        void onComplete(Upstream ctl, Throwable error) throws Exception;
    }
    
    public sealed interface Upstream {
        void latchDeadline(Instant deadline);
        boolean awaitDownstream() throws InterruptedException;
    }
    
    public sealed interface Downstream<T> {
        void latchClose();
        void latchOutput(T output);
        void latchDeadline(Instant deadline);
        void signalUpstream();
    }
    
    final Core<In, Out> core;
    final ReentrantLock upstreamLock = new ReentrantLock();
    final ReentrantLock downstreamLock = new ReentrantLock();
    final Condition offered = downstreamLock.newCondition();
    final Condition polled = downstreamLock.newCondition();
    final Control control = new Control();
    Instant deadline = null;
    Instant latchedDeadline = null;
    Out latchedOutput = null;
    int ctl = 0; // We encode the remaining properties in 6 bits
    
    //int state = NEW;
    //int access = NONE;
    //boolean isUpstreamWaiting = false;
    //boolean latchedClose = false;
    
    private int state() { return ctl & 0x3; }
    private int access() { return ctl & 0xC; }
    private boolean isUpstreamWaiting() { return (ctl & 0x10) != 0; }
    private boolean latchedClose() { return (ctl & 0x20) != 0; }
    
    private void setState(int state) { ctl = (ctl & ~0x3) | state; }
    private void setAccess(int access) { ctl = (ctl & ~0xC) | access; }
    private void setIsUpstreamWaiting(boolean b) { ctl = b ? (ctl | 0x10) : (ctl & ~0x10); }
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
    private static final int NONE       = 0 << 2;
    private static final int DOWNSTREAM = 1 << 2;
    private static final int UPSTREAM   = 2 << 2;
    
    TimedGate(Core<In, Out> core) {
        this.core = core;
    }
    
    // One instance per TimedStage.
    // Methods protect against some kinds of misuse:
    //  1. Casting to another interface and calling its methods - protected by checking access()
    //  2. Capturing the instance and calling from outside its scope - protected by checking lock ownership
    private final class Control implements Upstream, Downstream<Out> {
        @Override
        public void latchDeadline(Instant deadline) {
            if (access() < DOWNSTREAM || !downstreamLock.isHeldByCurrentThread()) {
                throw new IllegalStateException();
            }
            latchedDeadline = Objects.requireNonNull(deadline);
        }
        
        @Override
        public void latchOutput(Out output) {
            if (access() != DOWNSTREAM || !downstreamLock.isHeldByCurrentThread()) {
                throw new IllegalStateException();
            }
            latchedOutput = Objects.requireNonNull(output);
        }
        
        @Override
        public void latchClose() {
            if (access() != DOWNSTREAM || !downstreamLock.isHeldByCurrentThread()) {
                throw new IllegalStateException();
            }
            setLatchedClose();
        }
        
        @Override
        public void signalUpstream() {
            if (access() != DOWNSTREAM || !downstreamLock.isHeldByCurrentThread()) {
                throw new IllegalStateException();
            }
            setIsUpstreamWaiting(false);
            polled.signal();
        }
        
        @Override
        public boolean awaitDownstream() throws InterruptedException {
            if (access() != UPSTREAM || !downstreamLock.isHeldByCurrentThread()) {
                throw new IllegalStateException();
            }
            if (state() == CLOSED) {
                return false;
            }
            // This may be overwritten while we wait, so save it to stack and restore after.
            Instant savedDeadline = latchedDeadline;
            
            setIsUpstreamWaiting(true);
            try {
                do {
                    polled.await();
                    if (state() == CLOSED) {
                        return false;
                    }
                }
                while (isUpstreamWaiting());
                return true;
            } finally {
                latchedDeadline = savedDeadline;
                setAccess(UPSTREAM);
            }
        }
    }
    
    private void initIfNew() throws Exception {
        //assert downstreamLock.isHeldByCurrentThread();
        if (state() == NEW) {
            deadline = Objects.requireNonNull(core.onInit());
            setState(RUNNING);
        }
    }
    
    private void updateDeadline() {
        //assert downstreamLock.isHeldByCurrentThread();
        Instant nextDeadline = latchedDeadline;
        if (nextDeadline != null) {
            if (nextDeadline.isBefore(deadline)) {
                offered.signalAll();
            }
            deadline = nextDeadline;
        }
    }
    
    private boolean awaitDeadline() throws InterruptedException {
        //assert downstreamLock.isHeldByCurrentThread();
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
        upstreamLock.lockInterruptibly();
        try {
            downstreamLock.lockInterruptibly();
            try {
                if (state() >= COMPLETING) {
                    return false;
                }
                initIfNew();
                setAccess(UPSTREAM);
                
                core.onOffer(control, input);
                
                if (state() == CLOSED) { // Possible if onOffer() called awaitDownstream()
                    return false;
                }
                updateDeadline();
                return true;
            } finally {
                latchedDeadline = null;
                setAccess(NONE);
                downstreamLock.unlock();
            }
        } finally {
            upstreamLock.unlock();
        }
    }
    
    @Override
    public void complete(Throwable error) throws Exception {
        upstreamLock.lockInterruptibly();
        try {
            downstreamLock.lockInterruptibly();
            try {
                if (state() >= COMPLETING) {
                    return;
                }
                initIfNew();
                setAccess(UPSTREAM);
                
                core.onComplete(control, error);
                
                if (state() == CLOSED) { // Possible if onComplete() called awaitDownstream()
                    return;
                }
                updateDeadline();
                setState(COMPLETING);
            } finally {
                latchedDeadline = null;
                setAccess(NONE);
                downstreamLock.unlock();
            }
        } finally {
            upstreamLock.unlock();
        }
    }
    
    @Override
    public Out poll() throws Exception {
        for (;;) {
            downstreamLock.lockInterruptibly();
            try {
                if (state() == CLOSED) {
                    return null;
                }
                initIfNew();
                if (!awaitDeadline()) {
                    return null;
                }
                setAccess(DOWNSTREAM);
                
                core.onPoll(control);
                
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
                downstreamLock.unlock();
            }
        }
    }
    
    @Override
    public void close() {
        downstreamLock.lock();
        try {
            if (state() == CLOSED) {
                return;
            }
            setState(CLOSED);
            polled.signal(); // Wake upstream blocked in awaitDownstream()
            offered.signalAll(); // Wake downstreams blocked in awaitDeadline()
        } finally {
            downstreamLock.unlock();
        }
    }
}