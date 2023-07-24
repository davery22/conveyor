package io.avery.pipeline;

public interface TunnelSink<T> {
    boolean offer(T input) throws Exception;
    default void complete() throws Exception {}
    
    // 1. Call when outer offer starts, with outerLock == null (outer reuses inner lock)
    //   - outer offers run inner offers 'atomically' (can interleave inner/outer polls, but NOT inner/outer offers)
    // 2. Call when outer offer calls inner offer, with outerLock == null
    //   - outerLock is NOT released if inner offer blocks
    //   - outer offers/polls are blocked until inner poll unblocks inner offer
    //   - TODO: May want outer poll to reuse inner poll's lock, since outer poll is a pass-through anyway
    // 3. Call when outer offer calls inner offer, with outerLock != null
    //   - outerLock is released if inner offer blocks
    //   - outer offers are blocked until inner/outer poll unblocks inner offer
    
    // L(P1), L(C1), L(P2), L(C2)
    // U(C2), U(C1) -> L(P1), L(P2)
    // L(C1), L(C2) -> L(P1), L(C1), L(P2), L(C2)
    // if someone else acquires C1 or C2 while we only have P1 and P2, and they then try to acquire P1 or P2, deadlock
    // but they messed up if they're doing that
    
    // 'Gatherers' can be used to prepend transformations, creating a new sink
    default <U, A> TunnelSink<U> prepend(Gatherer<U, A, ? extends T> gatherer) {
        return Tunnels.prepend(gatherer, this);
    }
}
