package io.avery.conveyor;

import java.util.EnumSet;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;

/**
 * This is a shim of the "Gatherer" API proposed in
 * <a href="https://cr.openjdk.org/~vklang/Gatherers.html">https://cr.openjdk.org/~vklang/Gatherers.html</a>.
 * This interface is intended to be superseded by the official API once available.
 */
public interface Gatherer<T,A,R> {
    
    interface Downstream<R> {
        boolean push(R element);
        default boolean isRejecting() { return false; }
    }
    
    interface Integrator<A,T,R> {
        boolean integrate(A state, T element, Downstream<? super R> downstream);
    }
    
    enum Characteristics {
        GREEDY,          // Never short-circuits
        SIZE_PRESERVING, // Emits exactly once per input element
        STATELESS;       // No need to initialize or combine state
    }
    
    Supplier<A> initializer();
    Integrator<A, T, R> integrator();
    BiConsumer<A, Downstream<R>> finisher();
    default BinaryOperator<A> combiner() { return null; };
    default Set<Characteristics> characteristics() { return EnumSet.noneOf(Characteristics.class); }
}