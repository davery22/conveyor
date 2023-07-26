package io.avery.pipeline;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class TimedTunnel<In, Out> implements Tunnel<In, Out> {
    public interface Core<In, Out> {
        default Clock clock() { return Clock.systemUTC(); }
        Instant init() throws Exception;
        void consume(Consumer<Out> ctl) throws Exception;
        void produce(Producer ctl, In input) throws Exception;
        void complete(Producer ctl, Throwable error) throws Exception;
    }
    
    public interface Producer {
        void latchDeadline(Instant deadline);
        boolean awaitConsumer() throws InterruptedException;
    }
    
    public interface Consumer<T> {
        void latchClose();
        void latchOutput(T output);
        void latchDeadline(Instant deadline);
        void signalProducer();
    }
    
    final Core<In, Out> core;
    final ReentrantLock producerLock = new ReentrantLock();
    final ReentrantLock consumerLock = new ReentrantLock();
    final Condition produced = consumerLock.newCondition();
    final Condition consumed = consumerLock.newCondition();
    final Control control = new Control();
    Instant deadline = null;
    Instant latchedDeadline = null;
    Out latchedOutput = null;
    int ctl = 0; // We encode the remaining properties in 6 bits
    
    //int state = NEW;
    //int access = NONE;
    //boolean isProducerWaiting = false;
    //boolean latchedClose = false;
    
    private int state() { return ctl & 0x3; }
    private int access() { return ctl & 0xC; }
    private boolean isProducerWaiting() { return (ctl & 0x10) != 0; }
    private boolean latchedClose() { return (ctl & 0x20) != 0; }
    
    private void setState(int state) { ctl = (ctl & ~0x3) | state; }
    private void setAccess(int access) { ctl = (ctl & ~0xC) | access; }
    private void setIsProducerWaiting(boolean b) { ctl = b ? (ctl | 0x10) : (ctl & ~0x10); }
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
    private static final int NONE      = 0 << 2;
    private static final int CONSUMER  = 1 << 2;
    private static final int PRODUCER  = 2 << 2;
    
    TimedTunnel(Core<In, Out> core) {
        this.core = core;
    }
    
    // One instance per TimedTunnel.
    // Methods protect against some kinds of misuse:
    //  1. Casting to another interface and calling its methods - protected by checking access()
    //  2. Capturing the instance and calling from outside its scope - protected by checking lock ownership
    private class Control implements Producer, Consumer<Out> {
        @Override
        public void latchDeadline(Instant deadline) {
            if (access() < CONSUMER || !consumerLock.isHeldByCurrentThread()) {
                throw new IllegalStateException();
            }
            latchedDeadline = Objects.requireNonNull(deadline);
        }
        
        @Override
        public void latchOutput(Out output) {
            if (access() != CONSUMER || !consumerLock.isHeldByCurrentThread()) {
                throw new IllegalStateException();
            }
            latchedOutput = Objects.requireNonNull(output);
        }
        
        @Override
        public void latchClose() {
            if (access() != CONSUMER || !consumerLock.isHeldByCurrentThread()) {
                throw new IllegalStateException();
            }
            setLatchedClose();
        }
        
        @Override
        public void signalProducer() {
            if (access() != CONSUMER || !consumerLock.isHeldByCurrentThread()) {
                throw new IllegalStateException();
            }
            setIsProducerWaiting(false);
            consumed.signal();
        }
        
        @Override
        public boolean awaitConsumer() throws InterruptedException {
            if (access() != PRODUCER || !consumerLock.isHeldByCurrentThread()) {
                throw new IllegalStateException();
            }
            if (state() == CLOSED) {
                return false;
            }
            // This may be overwritten while we wait, so save it to stack and restore after.
            Instant savedDeadline = latchedDeadline;
            
            setIsProducerWaiting(true);
            try {
                do {
                    consumed.await();
                    if (state() == CLOSED) {
                        return false;
                    }
                }
                while (isProducerWaiting());
                return true;
            } finally {
                latchedDeadline = savedDeadline;
                setAccess(PRODUCER);
            }
        }
    }
    
    private void initIfNew() throws Exception {
        //assert consumerLock.isHeldByCurrentThread();
        if (state() == NEW) {
            deadline = Objects.requireNonNull(core.init());
            setState(RUNNING);
        }
    }
    
    private void updateDeadline() {
        //assert consumerLock.isHeldByCurrentThread();
        Instant nextDeadline = latchedDeadline;
        if (nextDeadline != null) {
            if (nextDeadline.isBefore(deadline)) {
                produced.signalAll();
            }
            deadline = nextDeadline;
        }
    }
    
    private boolean awaitDeadline() throws InterruptedException {
        //assert consumerLock.isHeldByCurrentThread();
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
                produced.await();
            } else {
                nanosRemaining = produced.awaitNanos(nanosRemaining);
            }
            if (state() == CLOSED) {
                return false;
            }
        }
    }
    
    @Override
    public boolean offer(In input) throws Exception {
        Objects.requireNonNull(input);
        producerLock.lockInterruptibly();
        try {
            consumerLock.lockInterruptibly();
            try {
                if (state() >= COMPLETING) {
                    return false;
                }
                initIfNew();
                setAccess(PRODUCER);
                
                core.produce(control, input);
                
                if (state() == CLOSED) { // Possible if produce() called awaitConsumer()
                    return false;
                }
                updateDeadline();
                return true;
            } catch (Error | Exception e) {
                close();
                throw e;
            } finally {
                latchedDeadline = null;
                setAccess(NONE);
                consumerLock.unlock();
            }
        } finally {
            producerLock.unlock();
        }
    }
    
    @Override
    public void complete(Throwable error) throws Exception {
        producerLock.lockInterruptibly();
        try {
            consumerLock.lockInterruptibly();
            try {
                if (state() >= COMPLETING) {
                    return;
                }
                initIfNew();
                setAccess(PRODUCER);
                
                core.complete(control, error);
                
                if (state() == CLOSED) { // Possible if complete() called awaitConsumer()
                    return;
                }
                updateDeadline();
                setState(COMPLETING);
            } catch (Error | Exception e) {
                close();
                throw e;
            } finally {
                latchedDeadline = null;
                setAccess(NONE);
                consumerLock.unlock();
            }
        } finally {
            producerLock.unlock();
        }
    }
    
    @Override
    public Out poll() throws Exception {
        for (;;) {
            consumerLock.lockInterruptibly();
            try {
                if (state() == CLOSED) {
                    return null;
                }
                initIfNew();
                if (!awaitDeadline()) {
                    return null;
                }
                setAccess(CONSUMER);
                
                core.consume(control);
                
                updateDeadline();
                if (latchedClose()) {
                    close();
                }
                if (latchedOutput != null) {
                    return latchedOutput;
                }
            } catch (Error | Exception e) {
                close();
                throw e;
            } finally {
                latchedOutput = null;
                latchedDeadline = null;
                setAccess(NONE);
                consumerLock.unlock();
            }
        }
    }
    
    @Override
    public void close() {
        consumerLock.lock();
        try {
            if (state() == CLOSED) {
                return;
            }
            setState(CLOSED);
            consumed.signal(); // Wake producer blocked in awaitConsumer()
            produced.signalAll(); // Wake consumers blocked in awaitDeadline()
        } finally {
            consumerLock.unlock();
        }
    }
}