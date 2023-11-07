package io.avery.conveyor;

public class AsyncException extends Exception {
    public AsyncException(String message, Throwable cause) {
        super(message, cause);
    }

    public AsyncException(Throwable cause) {
        super(cause);
    }
}
