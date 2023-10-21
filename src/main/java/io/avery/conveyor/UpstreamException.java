package io.avery.conveyor;

/**
 * Exception thrown when attempting to yield an element from a boundary {@link Belt.Source Source} whose linked
 * boundary {@link Belt.Sink Sink} has {@link Belt.Sink#completeAbruptly completed abruptly}. This exception can be
 * inspected using the {@link Throwable#getCause()} method.
 */
public class UpstreamException extends Exception {
    /**
     * Constructs an {@code UpstreamException} with the specified detail message and cause.
     *
     * @param message the detail message
     * @param cause the cause (which is saved for later retrieval by the {@link Throwable#getCause()} method)
     */
    public UpstreamException(String message, Throwable cause) {
        super(message, cause);
    }
    
    /**
     * Constructs an {@code UpstreamException} with the specified cause. The detail message is set to
     * {@code (cause == null ? null : cause.toString())} (which typically contains the class and detail message of
     * {@code cause}).
     *
     * @param cause the cause (which is saved for later retrieval by the {@link Throwable#getCause()} method)
     */
    public UpstreamException(Throwable cause) {
        super(cause);
    }
}
