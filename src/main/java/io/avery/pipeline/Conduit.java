package io.avery.pipeline;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collector;

public class Conduit {
    private Conduit() {}
    
    public sealed interface Stage permits BaseSink, BaseSource, Silo, Conduits.ChainedStage {
        default <T> void run(BiConsumer<BaseSource<T>, BaseSink<T>> connector) { }
    }
    
    public sealed interface BaseSink<In> extends Stage {
        boolean drainFromSource(BaseStepSource<? extends In> source) throws Exception;
        
        default void complete(Throwable error) throws Exception {
            // Default impl handles the case where the Sink has no async downstream.
            // Implementations that have an async downstream should override this method to propagate error downstream.
            if (error != null) {
                throw new UpstreamException(error);
            }
        }
    }
    
    public sealed interface BaseSource<Out> extends Stage, AutoCloseable {
        boolean drainToSink(BaseStepSink<? super Out> sink) throws Exception;
        
        default void close() throws Exception { }
        
        default void forEach(Consumer<? super Out> action) throws Exception {
            class ConsumerSink implements StepSink<Out> {
                @Override
                public boolean offer(Out input) {
                    action.accept(input);
                    return true;
                }
            }
            
            drainToSink(new ConsumerSink());
        }
        
        default <A, R> R collect(Collector<? super Out, A, R> collector) throws Exception {
            BiConsumer<A, ? super Out> accumulator = collector.accumulator();
            Function<A, R> finisher = collector.finisher();
            A acc = collector.supplier().get();
            
            class CollectorSink implements StepSink<Out> {
                @Override
                public boolean offer(Out input) {
                    accumulator.accept(acc, input);
                    return true;
                }
            }
            
            drainToSink(new CollectorSink());
            return finisher.apply(acc);
        }
    }
    
    public sealed interface BaseStepSink<In> extends BaseSink<In> {
        boolean offer(In input) throws Exception;
        
        @Override
        default boolean drainFromSource(BaseStepSource<? extends In> source) throws Exception {
            for (In e; (e = source.poll()) != null; ) {
                if (!offer(e)) {
                    return false;
                }
            }
            return true;
        }
    }
    
    public sealed interface BaseStepSource<Out> extends BaseSource<Out> {
        Out poll() throws Exception;
        
        @Override
        default boolean drainToSink(BaseStepSink<? super Out> sink) throws Exception {
            for (Out e; (e = poll()) != null; ) {
                if (!sink.offer(e)) {
                    return false;
                }
            }
            return true;
        }
    }
    
    public non-sealed interface Silo extends Stage {
        default Silo compose(Silo before) { }
        default <T> Sink<T> compose(Sink<T> before) { }
        default <T> StepSink<T> compose(StepSink<T> before) { }
        default Silo andThen(Silo after) { }
        default <T> Source<T> andThen(Source<T> after) { }
        default <T> StepSource<T> andThen(StepSource<T> after) { }
    }
    
    @FunctionalInterface
    public non-sealed interface Sink<In> extends BaseSink<In> {
        default Silo compose(StepSource<? extends In> before) { }
        default <T> Sink<T> compose(SinkStepSource<T, ? extends In> before) { }
        default <T> StepSink<T> compose(Segue<T, ? extends In> before) { }
        default Sink<In> andThen(Silo after) { }
        default <T> SinkSource<In, T> andThen(Source<T> after) { }
        default <T> SinkStepSource<In, T> andThen(StepSource<T> after) { }
    }
    
    @FunctionalInterface
    public non-sealed interface Source<Out> extends BaseSource<Out> {
        default Source<Out> compose(Silo before) { }
        default <T> SinkSource<T, Out> compose(Sink<T> before) { }
        default <T> StepSinkSource<T, Out> compose(StepSink<T> before) { }
        default Silo andThen(StepSink<? super Out> after) { }
        default <T> Source<T> andThen(StepSinkSource<? super Out, T> after) { }
        default <T> StepSource<T> andThen(Segue<? super Out, T> after) { }
    }
    
    @FunctionalInterface
    public non-sealed interface StepSink<In> extends Sink<In>, BaseStepSink<In> {
        default Silo compose(Source<? extends In> before) { }
        default <T> Sink<T> compose(SinkSource<T, ? extends In> before) { }
        default <T> StepSink<T> compose(StepSinkSource<T, ? extends In> before) { }
        @Override default StepSink<In> andThen(Silo after) { }
        @Override default <T> StepSinkSource<In, T> andThen(Source<T> after) { }
        @Override default <T> Segue<In, T> andThen(StepSource<T> after) { }
    }
    
    @FunctionalInterface
    public non-sealed interface StepSource<Out> extends Source<Out>, BaseStepSource<Out> {
        @Override default StepSource<Out> compose(Silo before) { }
        @Override default <T> SinkStepSource<T, Out> compose(Sink<T> before) { }
        @Override default <T> Segue<T, Out> compose(StepSink<T> before) { }
        default Silo andThen(Sink<? super Out> after) { }
        default <T> Source<T> andThen(SinkSource<? super Out, T> after) { }
        default <T> StepSource<T> andThen(SinkStepSource<? super Out, T> after) { }
    }
    
    public non-sealed interface SinkSource<In, Out> extends BaseSink<In>, BaseSource<Out> {
        default Sink<In> sink() {
            var self = this;
            class WrapperSink implements Sink<In> {
                @Override public boolean drainFromSource(BaseStepSource<? extends In> source) throws Exception { return self.drainFromSource(source); }
                @Override public void complete(Throwable error) throws Exception { self.complete(error); }
            }
            return new WrapperSink();
        }
        
        default Source<Out> source() {
            var self = this;
            class WrapperSource implements Source<Out> {
                @Override public boolean drainToSink(BaseStepSink<? super Out> sink) throws Exception { return self.drainToSink(sink); }
                @Override public void close() throws Exception { self.close(); }
            }
            return new WrapperSource();
        }
        
        default Source<Out> compose(StepSource<? extends In> before) { }
        default <T> SinkSource<T, Out> compose(SinkStepSource<T, ? extends In> before) { }
        default <T> StepSinkSource<T, Out> compose(Segue<T, ? extends In> before) { }
        default Sink<In> andThen(StepSink<? super Out> after) { }
        default <T> SinkSource<In, T> andThen(StepSinkSource<? super Out, T> after) { }
        default <T> SinkStepSource<In, T> andThen(Segue<? super Out, T> after) { }
    }
    
    public non-sealed interface StepSinkSource<In, Out> extends BaseStepSink<In>, SinkSource<In, Out> {
        @Override
        default StepSink<In> sink() {
            var self = this;
            class WrapperStepSink implements StepSink<In> {
                @Override public boolean offer(In input) throws Exception { return self.offer(input); }
                @Override public void complete(Throwable error) throws Exception { self.complete(error); }
            }
            return new WrapperStepSink();
        }
        
        default Source<Out> compose(Source<? extends In> before) { }
        default <T> SinkSource<T, Out> compose(SinkSource<T, ? extends In> before) { }
        default <T> StepSinkSource<T, Out> compose(StepSinkSource<T, ? extends In> before) { }
        @Override default StepSink<In> andThen(StepSink<? super Out> after) { }
        @Override default <T> StepSinkSource<In, T> andThen(StepSinkSource<? super Out, T> after) { }
        @Override default <T> Segue<In, T> andThen(Segue<? super Out, T> after) { }
    }
    
    public non-sealed interface SinkStepSource<In, Out> extends SinkSource<In, Out>, BaseStepSource<Out> {
        @Override
        default StepSource<Out> source() {
            var self = this;
            class WrapperStepSource implements StepSource<Out> {
                @Override public Out poll() throws Exception { return self.poll(); }
                @Override public void close() throws Exception { self.close(); }
            }
            return new WrapperStepSource();
        }
        
        @Override default StepSource<Out> compose(StepSource<? extends In> before) { }
        @Override default <T> SinkStepSource<T, Out> compose(SinkStepSource<T, ? extends In> before) { }
        @Override default <T> Segue<T, Out> compose(Segue<T, ? extends In> before) { }
        default Sink<In> andThen(Sink<? super Out> after) { }
        default <T> SinkSource<In, T> andThen(SinkSource<? super Out, T> after) { }
        default <T> SinkStepSource<In, T> andThen(SinkStepSource<? super Out, T> after) { }
    }
    
    public interface Segue<In, Out> extends StepSinkSource<In, Out>, SinkStepSource<In, Out> {
        @Override default StepSource<Out> compose(Source<? extends In> before) { }
        @Override default <T> SinkStepSource<T, Out> compose(SinkSource<T, ? extends In> before) { }
        @Override default <T> Segue<T, Out> compose(StepSinkSource<T, ? extends In> before) { }
        @Override default StepSink<In> andThen(Sink<? super Out> after) { }
        @Override default <T> StepSinkSource<In, T> andThen(SinkSource<? super Out, T> after) { }
        @Override default <T> Segue<In, T> andThen(SinkStepSource<? super Out, T> after) { }
    }
}