package io.avery.conveyor;

import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;

/**
 * This is a crude shim of the "Gatherer" API proposed in <a href="https://openjdk.org/jeps/461">JEP-461</a>.
 *
 * <p>This interface is for prototyping purposes only. It is intended to be superseded by the official API once
 * available.
 */
public interface Gatherer<T,A,R> {
    interface Downstream<R> {
        boolean push(R element);
        default boolean isRejecting() { return false; }
    }
    
    interface Integrator<A,T,R> {
        boolean integrate(A state, T element, Downstream<? super R> downstream);
    }
    
    Supplier<A> initializer();
    Integrator<A, T, R> integrator();
    BiConsumer<A, Downstream<R>> finisher();
    default BinaryOperator<A> combiner() { return null; };
}