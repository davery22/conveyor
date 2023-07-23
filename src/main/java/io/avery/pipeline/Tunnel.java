package io.avery.pipeline;

public interface Tunnel<In, Out> extends TunnelSource<Out>, TunnelSink<In> {
    default <U, A> Tunnel<U, Out> compose(Gatherer<U, A, In> gatherer) {
        return Tunnels.compose(gatherer, this);
    }
}