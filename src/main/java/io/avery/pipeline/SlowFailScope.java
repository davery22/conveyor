package io.avery.pipeline;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.ThreadFactory;

// TODO: Polish this

public class SlowFailScope extends StructuredTaskScope<Object> {
    volatile Throwable error;
    
    static final VarHandle ERROR;
    static {
        try {
            ERROR = MethodHandles.lookup().findVarHandle(SlowFailScope.class, "error", Throwable.class);
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }
    
    SlowFailScope(String name, ThreadFactory factory) {
        super(name, factory);
    }
    
    SlowFailScope() {
        super();
    }
    
    @Override
    protected void handleComplete(StructuredTaskScope.Subtask<?> subtask) {
        if (subtask.state() == StructuredTaskScope.Subtask.State.FAILED) {
            Throwable err = subtask.exception();
            if (!ERROR.compareAndSet(this, null, err)) {
                error.addSuppressed(err);
            }
        }
    }
    
    @Override
    public SlowFailScope join() throws InterruptedException {
        super.join();
        return this;
    }
    
    public void throwIfFailed() throws ExecutionException {
        ensureOwnerAndJoined();
        Throwable err = error;
        if (err != null) {
            throw new ExecutionException(err);
        }
    }
}
