package io.avery.conveyor;

import java.util.concurrent.Executor;
import java.util.stream.Stream;

/**
 * A {@link Belt.Source Source} that runs and closes upstream {@link #sources() sources} when run or closed,
 * respectively.
 *
 * @param <Out> the output element type
 */
public abstract class ProxySource<Out> implements Belt.Source<Out> {
    /**
     * Constructs a new {@code ProxySource}.
     */
    protected ProxySource() {
    }
    
    /**
     * Returns a stream of upstream sources. By default, the {@link #run run} and {@link #close close} methods on this
     * source will call the corresponding methods on each source in the stream.
     *
     * @return a stream of upstream sources
     */
    protected abstract Stream<? extends Belt.Source<?>> sources();
    
    /**
     * Calls {@code close} on each upstream source, as if by
     * {@snippet :
     * Belts.composedClose(this.sources());
     * }
     */
    @Override
    public void close() throws Exception {
        Belts.composedClose(sources());
    }
    
    /**
     * Calls {@code run(executor)} on each upstream source, as if by
     * {@snippet :
     * this.sources().sequential().forEach(source -> source.run(executor));
     * }
     * @param executor the executor to submit tasks to
     */
    @Override
    public void run(Executor executor) {
        sources().sequential().forEach(source -> source.run(executor));
    }
}
