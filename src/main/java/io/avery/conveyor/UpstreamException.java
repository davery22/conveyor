package io.avery.conveyor;

public class UpstreamException extends Exception {
    public UpstreamException(Throwable cause) {
        super(cause);
    }
    
    public UpstreamException(String message, Throwable cause) {
        super(message, cause);
    }
}