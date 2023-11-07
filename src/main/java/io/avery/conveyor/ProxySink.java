package io.avery.conveyor;

import java.util.concurrent.Callable;
import java.util.stream.Stream;

/**
 * A {@link Belt.Sink Sink} that runs and completes downstream {@link #sinks() sinks} when run or completed (normally or
 * abruptly), respectively.
 *
 * @param <In> the input element type
 */
public abstract class ProxySink<In> implements Belt.Sink<In> {
    /**
     * Constructs a new {@code ProxySink}.
     */
    protected ProxySink() {
    }
    
    /**
     * Returns a stream of downstream sinks. By default, the {@link #run run}, {@link #complete complete}, and
     * {@link #completeAbruptly completeAbruptly} methods on this sink will call the corresponding methods on each sink
     * in the stream.
     *
     * @return a stream of downstream sinks
     */
    protected abstract Stream<? extends Belt.Sink<?>> sinks();
    
    /**
     * Calls {@code complete} on each downstream sink, as if by
     * {@snippet :
     * Belts.composedComplete(this.sinks());
     * }
     */
    @Override
    public void complete() throws Exception {
        Belts.composedComplete(sinks());
    }
    
    /**
     * Calls {@code completeAbruptly} on each downstream sink, as if by
     * {@snippet :
     * Belts.composedCompleteAbruptly(this.sinks(), cause);
     * }
     */
    @Override
    public void completeAbruptly(Throwable cause) throws Exception {
        Belts.composedCompleteAbruptly(sinks(), cause);
    }
    
    @Override
    public Stream<Callable<Void>> tasks() {
        return sinks().flatMap(Belt.Stage::tasks);
    }
}
