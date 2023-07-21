package io.avery.pipeline;

public interface Tunnel<In, Out> extends TunnelSource<Out>, TunnelSink<In> {
}