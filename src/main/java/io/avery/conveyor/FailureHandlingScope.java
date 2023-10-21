package io.avery.conveyor;

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;

/**
 * A {@code StructuredTaskScope} that handles exceptions from failed tasks by delegating to a handler. The handler may
 * choose to log or aggregate failures, for example. Once the scope is {@link #shutdown() shutdown} (including by
 * closing it), tasks no longer report completion, so the handler will no longer be called.
 */
public final class FailureHandlingScope extends StructuredTaskScope<Object> {
    final Consumer<? super Throwable> exceptionHandler;
    
    /**
     * Constructs a new {@code FailureHandlingScope} with the given name and thread factory, that delegates failure
     * handling to the {@code exceptionHandler}. The handler may be called concurrently as tasks complete.
     *
     * <p>The task scope is optionally named for the purposes of monitoring and management. The thread factory is used
     * to {@link ThreadFactory#newThread(Runnable) create} threads when subtasks are
     * {@linkplain #fork(Callable) forked}. The task scope is owned by the current thread.
     *
     * @param name the name of the task scope, can be null
     * @param factory the thread factory
     * @param exceptionHandler the exception handler
     */
    public FailureHandlingScope(String name, ThreadFactory factory, Consumer<? super Throwable> exceptionHandler) {
        super(name, factory);
        this.exceptionHandler = Objects.requireNonNull(exceptionHandler);
    }
    
    /**
     * Constructs a new unnamed {@code FailureHandlingScope} that creates virtual threads and delegates failure handling
     * to the {@code exceptionHandler}. The handler may be called concurrently as tasks complete.
     *
     * @implSpec This constructor is equivalent to invoking the 3-arg constructor with a name of {@code null} and a
     * thread factory that creates virtual threads.
     *
     * @param exceptionHandler the exception handler
     */
    public FailureHandlingScope(Consumer<? super Throwable> exceptionHandler) {
        this(null, Thread.ofVirtual().factory(), exceptionHandler);
    }
    
    @Override
    protected void handleComplete(Subtask<?> subtask) {
        if (subtask.state() == Subtask.State.FAILED) {
            exceptionHandler.accept(subtask.exception());
        }
    }
}
