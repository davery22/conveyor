package io.avery.pipeline;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class TimedSegue<In, Out> implements Conduit.Segue<In, Out> {
    public interface Core<In, Out> {
        default Clock clock() { return Clock.systemUTC(); }
        void onInit(SinkController ctl) throws Exception;
        void onOffer(SinkController ctl, In input) throws Exception;
        void onPoll(SourceController<Out> ctl) throws Exception;
        void onComplete(SinkController ctl, Throwable error) throws Exception;
    }
    
    public sealed interface SinkController {
        void latchSinkDeadline(Instant deadline);
        void latchSourceDeadline(Instant deadline);
    }
    
    public sealed interface SourceController<T> {
        void latchSinkDeadline(Instant deadline);
        void latchSourceDeadline(Instant deadline);
        void latchOutput(T output);
        void latchClose();
    }
    
    final Core<In, Out> core;
    final ReentrantLock lock = new ReentrantLock();
    final Condition readyForSource = lock.newCondition();
    final Condition readyForSink = lock.newCondition();
    final Controller controller = new Controller();
    Instant sinkDeadline = Instant.MIN;
    Instant sourceDeadline = Instant.MAX;
    Instant latchedSinkDeadline = null;
    Instant latchedSourceDeadline = null;
    Out latchedOutput = null;
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
    
    TimedSegue(Core<In, Out> core) {
        this.core = core;
    }
    
    // One instance per TimedSegue.
    // Methods protect against some kinds of misuse:
    //  1. Casting to another interface and calling its methods - protected by checking access()
    //  2. Capturing the instance and calling from outside its scope - protected by checking lock ownership
    private final class Controller implements SinkController, SourceController<Out> {
        @Override
        public void latchSinkDeadline(Instant deadline) {
            if (access() < SOURCE || !lock.isHeldByCurrentThread()) {
                throw new IllegalStateException();
            }
            latchedSinkDeadline = Objects.requireNonNull(deadline);
        }
        
        @Override
        public void latchSourceDeadline(Instant deadline) {
            if (access() < SOURCE || !lock.isHeldByCurrentThread()) {
                throw new IllegalStateException();
            }
            latchedSourceDeadline = Objects.requireNonNull(deadline);
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
            core.onInit(controller);
            updateSinkDeadline();
            updateSourceDeadline();
            setState(RUNNING);
        }
    }
    
    private void updateSinkDeadline() {
        //assert lock.isHeldByCurrentThread();
        Instant nextDeadline = latchedSinkDeadline;
        if (nextDeadline != null) {
            if (nextDeadline.isBefore(sinkDeadline)) {
                readyForSink.signalAll();
            }
            sinkDeadline = nextDeadline;
        }
    }
    
    private void updateSourceDeadline() {
        //assert lock.isHeldByCurrentThread();
        Instant nextDeadline = latchedSourceDeadline;
        if (nextDeadline != null) {
            if (nextDeadline.isBefore(sourceDeadline)) {
                readyForSource.signalAll();
            }
            sourceDeadline = nextDeadline;
        }
    }
    
    private boolean awaitSinkDeadline() throws InterruptedException {
        //assert lock.isHeldByCurrentThread();
        Instant savedDeadline = null;
        long nanosRemaining = 0;
        for (;;) {
            if (savedDeadline != (savedDeadline = sinkDeadline)) {
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
                readyForSink.await();
            } else {
                nanosRemaining = readyForSink.awaitNanos(nanosRemaining);
            }
            if (state() >= COMPLETING) {
                return false;
            }
        }
    }
    
    private boolean awaitSourceDeadline() throws InterruptedException {
        //assert lock.isHeldByCurrentThread();
        Instant savedDeadline = null;
        long nanosRemaining = 0;
        for (;;) {
            if (savedDeadline != (savedDeadline = sourceDeadline)) {
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
                readyForSource.await();
            } else {
                nanosRemaining = readyForSource.awaitNanos(nanosRemaining);
            }
            if (state() == CLOSED) {
                return false;
            }
        }
    }
    
    class Sink implements Conduit.StepSink<In> {
        @Override
        public boolean offer(In input) throws Exception {
            Objects.requireNonNull(input);
            lock.lockInterruptibly();
            try {
                if (state() >= COMPLETING) {
                    return false;
                }
                initIfNew();
                if (!awaitSinkDeadline()) {
                    return false;
                }
                setAccess(SINK);
                
                core.onOffer(controller, input);
                
                updateSinkDeadline();
                updateSourceDeadline();
                // TODO: latchComplete()?
                return true;
            } finally {
                latchedSinkDeadline = null;
                latchedSourceDeadline = null;
                setAccess(NONE);
                lock.unlock();
            }
        }
        
        @Override
        public void complete(Throwable error) throws Exception {
            lock.lockInterruptibly();
            try {
                if (state() >= COMPLETING) {
                    return;
                }
                initIfNew();
                setAccess(SINK);
                
                core.onComplete(controller, error);
                
                updateSourceDeadline();
                setState(COMPLETING);
                readyForSink.signalAll();
            } finally {
                latchedSinkDeadline = null;
                latchedSourceDeadline = null;
                setAccess(NONE);
                lock.unlock();
            }
        }
    }
    
    class Source implements Conduit.StepSource<Out> {
        @Override
        public Out poll() throws Exception {
            for (;;) {
                lock.lockInterruptibly();
                try {
                    if (state() == CLOSED) {
                        return null;
                    }
                    initIfNew();
                    if (!awaitSourceDeadline()) {
                        return null;
                    }
                    setAccess(SOURCE);
                    
                    core.onPoll(controller);
                    
                    updateSinkDeadline();
                    updateSourceDeadline();
                    if (latchedClose()) {
                        close();
                    }
                    if (latchedOutput != null) {
                        return latchedOutput;
                    }
                } finally {
                    latchedSinkDeadline = null;
                    latchedSourceDeadline = null;
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
    
    @Override
    public Conduit.StepSink<In> sink() {
        return new Sink();
    }
    
    @Override
    public Conduit.StepSource<Out> source() {
        return new Source();
    }
}