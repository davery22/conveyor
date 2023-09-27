package io.avery.conveyor;

import java.util.Objects;
import java.util.concurrent.StructuredTaskScope;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;

class FailureHandlingScope extends StructuredTaskScope<Object> {
    final Consumer<? super Throwable> exceptionHandler;
    
    FailureHandlingScope(Consumer<? super Throwable> exceptionHandler) {
        this(null, Thread.ofVirtual().factory(), exceptionHandler);
    }
    
    FailureHandlingScope(String name, ThreadFactory factory, Consumer<? super Throwable> exceptionHandler) {
        super(name, factory);
        this.exceptionHandler = Objects.requireNonNull(exceptionHandler);
    }
    
    @Override
    protected void handleComplete(Subtask<?> subtask) {
        if (subtask.state() == Subtask.State.FAILED) {
            exceptionHandler.accept(subtask.exception());
        }
    }
}
