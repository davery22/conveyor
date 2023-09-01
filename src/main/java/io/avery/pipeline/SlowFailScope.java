package io.avery.pipeline;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

public final class SlowFailScope extends StructuredTaskScope<Object> {
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
    
    @Override
    public SlowFailScope joinUntil(Instant deadline) throws InterruptedException, TimeoutException {
        super.joinUntil(deadline);
        return this;
    }
    
    public Optional<Throwable> exception() {
        ensureOwnerAndJoined();
        return Optional.ofNullable(error);
    }
    
    public void throwIfFailed() throws ExecutionException {
        throwIfFailed(ExecutionException::new);
    }
    
    public <X extends Throwable> void throwIfFailed(Function<Throwable, ? extends X> esf) throws X {
        ensureOwnerAndJoined();
        Objects.requireNonNull(esf);
        Throwable err = error;
        if (err != null) {
            X ex = esf.apply(err);
            Objects.requireNonNull(ex, "esf returned null");
            throw ex;
        }
    }
}
