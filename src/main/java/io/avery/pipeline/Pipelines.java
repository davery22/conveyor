package io.avery.pipeline;

public class Pipelines {
    private Pipelines() {} // Utility
    
    // concat() could work by creating a Tunnel.Source that switches between 2 Tunnel.Sources,
    // or by creating a Pipeline.Source that consumes one Tunnel.Source and then another.
    // The latter case reduces to the former case, since Pipeline.Source needs to expose a Tunnel.GatedSource
    
    // ----- create
    
    public static <T, U> Pipeline.Source<U> source(Tunnel.Source<T> source, Tunnel.Gate<? super T, U> gate) {
    
    }
    
    public static <T> Pipeline.Source<T> source(Tunnel.GatedSource<T> source) {
    
    }
    
    public static <T, U> Pipeline.Sink<T> sink(Tunnel.Gate<T, U> gate, Tunnel.Sink<? super U> sink) {
    
    }
    
    public static <T> Pipeline.Sink<T> sink(Tunnel.GatedSink<T> sink) {
    
    }
    
    public static <T, U> Pipeline.Segment<T, U> segment(Tunnel.Gate<T, U> gate) {
    
    }
    
    // ----- connect
    
    public static <T, U> Pipeline.Source<U> connect(Pipeline.Source<T> source, Pipeline.Segment<? super T, U> seg) {
    
    }
    
    public static <T, U> Pipeline.Sink<T> connect(Pipeline.Segment<T, U> seg, Pipeline.Sink<? super U> sink) {
    
    }
    
    public static <T, U, V> Pipeline.Segment<T, V> connect(Pipeline.Segment<T, U> seg1, Pipeline.Segment<? super U, V> seg2) {
    
    }
    
    public static <T> Pipeline.System connect(Pipeline.Source<T> source, Pipeline.Sink<? super T> sink) {
    
    }
}
