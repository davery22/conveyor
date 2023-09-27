package io.avery.conveyor;

import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;

interface Gatherer<T,A,R> {
    
    interface Sink<R> {
        boolean flush(R element);
    }
    
    interface Integrator<A,T,R> {
        boolean integrate(A state, T element, Sink<? super R> downstream);
    }
    
    enum Characteristics {
        GREEDY,          // Never short-circuits
        SIZE_PRESERVING, // Emits exactly once per input element
        STATELESS;       // No need to initialize or combine state
    }
    
    Supplier<A> supplier();
    Integrator<A, T, R> integrator();
    BinaryOperator<A> combiner();
    BiConsumer<A, Sink<? super R>> finisher();
    Set<Characteristics> characteristics();
    
//    default <AA, RR> Gatherer<T,?,RR> andThen(Gatherer<R,AA,RR> that) {
//        // Gatherers is analogous to Collectors
//        return Gatherers.Composite.of(this, that);
//    }
//
//    default <RR> Collector<T,?,RR> collect(Collector<R, ?, RR> collector) {
//        // Gatherers is analogous to Collectors
//        return Gatherers.GathererCollector.of(this, collector);
//    }
}