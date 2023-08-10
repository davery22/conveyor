package io.avery.pipeline;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.*;
import java.util.stream.IntStream;

public class Conduits {
    private Conduits() {} // Utility
    
    private static class WrappingException extends RuntimeException {
        WrappingException(Exception e) {
            super(e);
        }
        
        @Override
        public synchronized Exception getCause() {
            return (Exception) super.getCause();
        }
    }
    
    private static class FusedStepSink<In, T, A> implements Conduit.StepSink<In> {
        final ReentrantLock sinkLock = new ReentrantLock();
        final Supplier<A> supplier;
        final Gatherer.Integrator<A, In, T> integrator;
        final BiConsumer<A, Gatherer.Sink<? super T>> finisher;
        final Conduit.StepSink<T> conduit;
        final Gatherer.Sink<T> gsink;
        A acc = null;
        int state = NEW;
        
        static final int NEW       = 0;
        static final int RUNNING   = 1;
        static final int COMPLETED = 2;
        
        FusedStepSink(Gatherer<In, A, T> gatherer, Conduit.StepSink<T> sink) {
            this.supplier = gatherer.supplier();
            this.integrator = gatherer.integrator();
            this.finisher = gatherer.finisher();
            this.conduit = Objects.requireNonNull(sink);
            this.gsink = el -> {
                try {
                    return conduit().offer(el);
                } catch (Error | RuntimeException e) {
                    throw e;
                } catch (Exception e) {
                    // We are not allowed to throw checked exceptions in this context;
                    // wrap them so that we might rediscover them farther up the stack.
                    // (They might still be dropped or re-wrapped between here and there.)
                    if (e instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                    throw new WrappingException(e);
                }
            };
        }
        
        Conduit.StepSink<T> conduit() {
            return conduit;
        }
        
        void initIfNew() {
            //assert sinkLock.isHeldByCurrentThread();
            if (state == NEW) {
                acc = supplier.get();
                state = RUNNING;
            }
        }
        
        @Override
        public boolean offer(In input) throws Exception {
            sinkLock.lockInterruptibly();
            try {
                if (state == COMPLETED) {
                    return false;
                }
                initIfNew();
                if (!integrator.integrate(acc, input, gsink)) {
                    state = COMPLETED;
                    return false;
                }
                return true;
            } catch (WrappingException e) {
                if (e.getCause() instanceof InterruptedException) {
                    Thread.interrupted();
                }
                throw e.getCause();
            } finally {
                sinkLock.unlock();
            }
        }
        
        @Override
        public void complete(Throwable error) throws Exception {
            sinkLock.lockInterruptibly();
            try {
                if (state == COMPLETED) {
                    return;
                }
                initIfNew();
                if (error == null) {
                    finisher.accept(acc, gsink);
                }
                conduit().complete(error);
                state = COMPLETED;
            } catch (WrappingException e) {
                if (e.getCause() instanceof InterruptedException) {
                    Thread.interrupted();
                }
                throw e.getCause();
            } finally {
                sinkLock.unlock();
            }
        }
    }
    
    private static class FusedStepStage<In, T, A, Out> extends FusedStepSink<In, T, A> implements Conduit.Stage<In, Out> {
        FusedStepStage(Gatherer<In, A, T> gatherer, Conduit.Stage<T, Out> stage) {
            super(gatherer, stage);
        }
        
        @Override
        Conduit.Stage<T, Out> conduit() {
            return (Conduit.Stage<T, Out>) conduit;
        }
        
        @Override
        public Out poll() throws Exception {
            return conduit().poll();
        }
        
        @Override
        public void close() throws Exception {
            conduit().close();
        }
    }
    
    public static <In, T, A> Conduit.StepSink<In> stepFuse(Gatherer<In, A, ? extends T> gatherer, Conduit.StepSink<T> sink) {
        @SuppressWarnings("unchecked")
        Gatherer<In, A, T> g = (Gatherer<In, A, T>) gatherer;
        return new FusedStepSink<>(g, sink);
    }
    
    public static <In, T, A, Out> Conduit.Stage<In, Out> stepFuse(Gatherer<In, A, ? extends T> gatherer, Conduit.Stage<T, Out> stage) {
        @SuppressWarnings("unchecked")
        Gatherer<In, A, T> g = (Gatherer<In, A, T>) gatherer;
        return new FusedStepStage<>(g, stage);
    }
    
    // TODO: Probably remove this
    public static <T, A, Out> Conduit.StepSource<Out> stepFuse(Conduit.StepSource<T> source,
                                                               Gatherer<? super T, A, Out> gatherer,
                                                               int bufferLimit) {
        var supplier = gatherer.supplier();
        var integrator = gatherer.integrator();
        var finisher = gatherer.finisher();
        
        class FusedStepSource implements Conduit.StepSource<Out> {
            final Deque<Out> buffer = new ArrayDeque<>();
            final Gatherer.Sink<? super Out> gsink = out -> {
                if (buffer.size() >= bufferLimit) {
                    throw new IllegalStateException();
                }
                return buffer.offer(out);
            };
            A acc = null;
            int state = NEW;
            
            static final int NEW       = 0;
            static final int RUNNING   = 1;
            static final int CANCELLED = 2;
            static final int CLOSED    = 3;
            
            @Override
            public Out poll() throws Exception {
                for (;;) {
                    if (state == CLOSED) {
                        return null;
                    }
                    Out out = buffer.poll();
                    if (out != null || state == CANCELLED) {
                        return out;
                    }
                    if (state == NEW) {
                        acc = supplier.get();
                        state = RUNNING;
                    }
                    T t = source.poll();
                    if (t == null) {
                        state = CANCELLED;
                        finisher.accept(acc, gsink);
                    } else if (!integrator.integrate(acc, t, gsink)) {
                        state = CANCELLED;
                    }
                }
            }
            
            @Override
            public void close() throws Exception {
                if (state == CLOSED) {
                    return;
                }
                source.close();
                state = CLOSED;
            }
        }
        
        return new FusedStepSource();
    }
    
    // TODO: This works, but is it better than putting stepFuse(gatherer, stage) before the sink?
    //
    // 1. A=loop{process+offer} | B=loop{poll+process}
    // 2. B=loop{(poll+fork(a))?+poll+process} | a=loop*{process+offer}
    //
    // 2 is likely less efficient due to forking overhead. Does it buy anything?
    //  - Can fuse directly to a Sink, instead of fusing to a Stage before the Sink
    //    - Possible because the asynchrony is implicit in the Sink itself, rather than explicit between Stage and Sink
    //  - Propagation of completion/error?
    //    - In 1, an error from the fused Stage is propagated down by Sink.complete(error); Sink will propagate or throw wrapped in UpstreamException
    //    - In 2, an error from the fused Stage is propagated out by Sink.drainFromSource(..); Sink will propagate or throw unwrapped
    //
    // Theoretically we could put any Pipeline in 2 if we had a reliable way to insert delimiters at the output side, to
    // know when to poll source again. But since conduits have async boundaries, there is no boundary-state-agnostic way
    // to signal that all offers are fully plumbed through (or conversely, that further polls will block indefinitely -
    // which they may even if offers are not fully plumbed through, eg due to min size for flush...). So this signal is
    // really only possible when the pipeline is fully synchronous - ie a Gatherer.
    
    public static <In, T, A> Conduit.Sink<In> fuse(Gatherer<In, A, ? extends T> gatherer,
                                                   int bufferLimit,
                                                   Conduit.Sink<T> sink) {
        // We take advantage of the fact that Sink.drainFromSource(StepSource)
        // encloses the Sink's usage of the StepSource. We define a delegating
        // StepSource that can launch asynchronous tasks - bounded by the
        // enclosing scope - to transform input polled from the original StepSource.
        
        // Create a gatherer that flushes a special value between inputs,
        // to indicate when all outputs for an input have been flushed.
        var delimiter = new Object();
        
        class DelimitedGatherer implements Gatherer<In, A, Object> {
            @Override public Supplier<A> supplier() { return gatherer.supplier(); }
            @Override public Integrator<A, In, Object> integrator() {
                var integrator = gatherer.integrator();
                return (s, e, d) -> integrator.integrate(s, e, d) && d.flush(delimiter);
            }
            @Override public BinaryOperator<A> combiner() { return gatherer.combiner(); }
            @Override public BiConsumer<A, Sink<? super Object>> finisher() { return gatherer.finisher()::accept; }
            @Override public Set<Characteristics> characteristics() { return gatherer.characteristics(); }
        }
        var stage = stepFuse(
            new DelimitedGatherer(),
            Conduits.extrapolate(delimiter, e -> Collections.emptyIterator(), bufferLimit)
        );
        
        class FusedSink implements Conduit.Sink<In> {
            @Override
            public void drainFromSource(Conduit.StepSource<? extends In> source) throws Exception {
                try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
                    var stepSource = new Conduit.StepSource<T>() {
                        @Override
                        public T poll() throws Exception {
                            for (;;) {
                                Object out = stage.poll();
                                if (out != delimiter) {
                                    return (T) out;
                                }
                                In in = source.poll();
                                scope.fork(() -> {
                                    if (in == null || !stage.offer(in)) {
                                        stage.complete(null);
                                    }
                                    return null;
                                });
                            }
                        }
                    };
                    
                    scope.fork(() -> {
                        // Done in a fork so that an error from other forks will interrupt this
                        sink.drainFromSource(stepSource);
                        scope.shutdown();
                        return null;
                    });
                    
                    scope.join().throwIfFailed();
                }
            }
            
            @Override
            public void complete(Throwable error) throws Exception {
                sink.complete(error);
            }
        }
        
        return new FusedSink();
    }
    
    // concat() could work by creating a Conduit.Source that switches between 2 Conduit.Sources,
    // or by creating a Pipeline.Source that consumes one Conduit.Source and then another.
    // The latter case reduces to the former case, since Pipeline.Source needs to expose a Conduit.StepSource
    
    // TODO: Likely going to have alternates of several methods to account for (non-)step-ness.
    
    // TODO: In forEachUntilCancel(), is exception because of source or sink?
    //  Matters for deciding what becomes of source + other sinks
    //  Maybe non-step Sources shouldn't be allowed at all?
    //   - Because, if any / all sinks cancel, source can't recover well (snapshot its state) for new sinks
    //   - Instead, have combinators that take a Stage to build a Source
    
    // Is there a problem with 'init-on-first-use'? Vs having/requiring an explicit init?
    // Uses of 'init' (onSubscribe()) in Rx:
    //  - Set up Publisher state for the new Subscriber
    //  - ~Send~ Kick off request for initial elements (or even error/completion), eg in a Replay
    //    - Has to happen by calling Subscription.request(n)
    
    // init() as a way to initialize threads in a chained conduit / pipeline?
    // Problem: Can't create the STScope inside init(), cuz scope - execution would need to finish inside init()
    // Could pass in a scope - init(scope) - that feels opinionated, and unnecessary for single-stage
    
    // TODO: The problem of reuse for non-step Sources/Sinks
    //  eg, in combineLatest: if I attach more sinks, later or concurrently, it should behave naturally
    //  eg, in balance: if I attach more sources, later or concurrently, it should behave naturally
    //
    // Options:
    //  1. Document that non-step Sources/Sinks may throw if connected to multiple Sinks/Sources
    //  2. Model differently, separating connection from execution
    
    // There is no way to know that a source is empty without polling it.
    // And once we poll it and get something, there is no way to put it back.
    // So if we need to poll 2 sources to produce 1 output, and 1 source is empty,
    // we will end up discarding the element from the other source.
    
    // What happens when 2 threads run combineLatest.forEachUntilCancel?
    // If they share the same state, the algorithm ceases to work, because it
    // assumes only one competing thread, eg when polling, going to sleep,
    // waking up.
    // TODO: Does separate state even mitigate this? Still contending to poll, etc...
    
    // Sink.drainFromSource(source) should never call source.close(); only source.poll/drainToSink.
    // Source.drainToSink(sink) should never call sink.complete(); only sink.offer/drainFromSource.
    
    // Cannot write a stepBalance, even if it takes StepSinks. Even if we wrap the sinks to know which ones are
    // "currently" blocked in offer(), that would only be aware of offers that we submitted, not other threads using the
    // sink.
    // For the same reason, cannot write a stepMerge - we cannot know which sources may be blocked in poll() by another
    // thread.
    
    // Why can't we write a step operator from non-step operators?
    // Non-step operators can only "run-all". To wrap in step, we would either need to:
    // StepSource: When we poll(), internal StepSink (eventually) returns an element buffered from original Source.
    //  - In other words we've made a Stage!
    //  - If we run-all the original Source on first poll(), we need unbounded buffering to complete within poll()
    //  - Otherwise, the original Source would already need to be running in an unscoped thread, outside our poll()
    // StepSink: When we offer(), internal StepSource buffers so original Sink can see result on poll()
    //  - In other words we've made a Stage!
    //  - If we run-all the original Sink on first offer(), we deadlock because offer() is now waiting for more offers
    //  - Otherwise, the original Sink would already need to be running in an unscoped thread, outside our offer()
    
    // Cannot write a stepZipLatest, because poll() should return as soon as the first interior poll() completes, but
    // that would imply unscoped threads running the other interior polls. Even if we waited for all polls, that still
    // would not work, because correct behavior means that we should re-poll each source as soon as its previous poll
    // finishes, since sources may emit at different rates.
    
    // Non-step from step     - ALWAYS(?) possible
    // Non-step from non-step - ALWAYS(?) possible
    // Step from step         - SOMETIMES possible
    // Step from non-step     - NEVER possible
    
    // Streams tend to be a good approach to parallelism when the source can be split
    //  - Run the whole pipeline on each split of the source, in its own thread
    // Queues tend to be a good approach to parallelism when pipeline stages can be detached
    //  - (and stages progress at similar rates / spend less than 1-1/COUNT of their time blocked on each other)
    //  - Run each stage of the pipeline on the whole source, in its own thread
    
    // balance() needs a step-source that can handle concurrent polls from multiple threads
    // merge() needs a step-sink that can handle concurrent offers from multiple threads
    //  - this is about ensuring correct behavior
    // non-step sources/sinks only need a CAS up-front (and internal thread-safety as needed)
    //  - this is about catching incorrect usage
    // (step-)sources do not need any extra protection if only one thread is calling them
    // (step-)sinks do not need any extra protection if only one thread is calling them
    // stages need protection just to prevent conflict between their own sink/source
    
    // fastFailSource - fail if Source is accessed by multiple threads
    // fastFailSink - fail if Sink is accessed by multiple threads
    // synchronizeStepSource
    // synchronizeStepSink
    // synchronizeStage
    // catchError - log / recover / re-throw
    
    // What of sources returning elements outside the synchronized block?
    //  - In the case of one thread consuming, this is fine
    //  - In the case of multiple threads consuming, they need to determine what synchronization works for them
    //    - eg, are they trying to protect the entire downstream? Just part of it?
    //    - at some point, consumer needs to await preceding consumer, eg before an offer downstream
    //    - at some point, consumer needs to signal next consumer, eg after an offer downstream
    //    - consumer controls when the offer downstream happens
    // Sink works by polling a StepSource - element sequence is the order that poll is called (need sync'd poll)
    // StepSink works by a Source offering to it - element sequence is the order that offer is called (need sync'd offer)
    //
    
    // TODO: Note limitations of relying on ScopedValues, if user code invokes callbacks in an unmanaged thread
    
    // balance() is not quite mapAsyncUnordered()
    //  - The latter has one downstream, but this can be resolved outside of balance() by having all sinks offer to the
    //    same sink.
    //  - The latter has a function that is called on elements in-order before balancing. This can be resolved by fusing
    //    before the balance(), but since balance is a Sink (not StepSink), fusing would require introducing a
    //    Stage/boundary. Instead, we can wrap the StepSource to synchronize polls, and apply the function in the same
    //    synchronized block to get application ordering.
    
    // --- Sinks ---
    
    // TODO: Remove
    public static <T, P, U> Conduit.Sink<T> mapAsyncPartitioned(int parallelism,
                                                                int perPartition,
                                                                Function<? super T, P> classifier,
                                                                BiFunction<? super T, ? super P, ? extends Callable<U>> mapper,
                                                                Conduit.StepSink<U> sink) {
        return mapBalancePartitioned(classifier, mapper, IntStream.range(0, parallelism).mapToObj(i -> sink).toList(), perPartition);
    }
    
    // TODO: Remove
    public static <T, U> Conduit.Sink<T> mapAsyncOrdered(int parallelism,
                                                         Function<? super T, ? extends Callable<U>> mapper,
                                                         Conduit.StepSink<U> sink) {
        return mapBalanceOrdered(mapper, IntStream.range(0, parallelism).mapToObj(i -> sink).toList());
    }
    
    // TODO: How does this behave under adversarial exception-catching?
    // TODO: Upon re-entry after throwing, operators should either recover or fail-fast.
    
    public static <T, P, U> Conduit.Sink<T> mapBalancePartitioned(Function<? super T, P> classifier,
                                                                  BiFunction<? super T, ? super P, ? extends Callable<U>> mapper,
                                                                  List<? extends Conduit.Sink<U>> sinks,
                                                                  int permitsPerPartition) {
        if (permitsPerPartition >= sinks.size()) {
            // TODO: This does not allow the mapper to run concurrently for separate partitions
            //  Is that even intended behavior?
            return mapBalanceOrdered(t -> mapper.apply(t, classifier.apply(t)), sinks);
        }
        if (permitsPerPartition < 1) {
            throw new IllegalArgumentException();
        }
        
        class LatchHolder {
            CountDownLatch latch = new CountDownLatch(1);
        }
        
        class PartitionMutex {
            final Semaphore semaphore = new Semaphore(permitsPerPartition);
            CountDownLatch latch = new CountDownLatch(0);
            int refCount = 0;
        }
        
        class MapBalancedPartitioned implements Conduit.Sink<T> {
            final ScopedValue<LatchHolder> scopedLatch = ScopedValue.newInstance();
            final ReentrantLock lock = new ReentrantLock();
            final ConcurrentHashMap<P, PartitionMutex> mutexByKey = new ConcurrentHashMap<>();
            CountDownLatch currLatch = new CountDownLatch(0);
            
            @Override
            public void drainFromSource(Conduit.StepSource<? extends T> source) throws Exception {
                var stepSource = new Conduit.StepSource<U>() {
                    @Override
                    public U poll() throws Exception {
                        var holder = scopedLatch.get();
                        holder.latch.countDown();
                        for (;;) {
                            T in;
                            U out;
                            P key;
                            Callable<U> callable;
                            PartitionMutex pMutex;
                            CountDownLatch   gCurr,
                                             gNext = holder.latch = new CountDownLatch(1),
                                             pNext = new CountDownLatch(1);
                            CountDownLatch[] pCurr = { null };
                            // At most one thread (globally) can run classifier at a time - sequenced globally
                            lock.lockInterruptibly();
                            try {
                                gCurr = currLatch;
                                currLatch = gNext;
                                in = source.poll();
                                if (in == null) {
                                    return null;
                                }
                                key = classifier.apply(in);
                                pMutex = mutexByKey.compute(key, (k, v) -> {
                                    if (v == null) {
                                        v = new PartitionMutex();
                                    }
                                    ++v.refCount;
                                    pCurr[0] = v.latch;
                                    v.latch = pNext;
                                    return v;
                                });
                            } finally {
                                lock.unlock();
                            }
                            // At most one thread (per partition) can run mapper at a time - sequenced per partition, in poll-order
                            pCurr[0].await();
                            try {
                                pMutex.semaphore.acquire();
                                callable = mapper.apply(in, key);
                            } finally {
                                pNext.countDown();
                            }
                            // At most N callables (per partition) can be pending at a time - bounded per partition
                            try {
                                out = callable.call();
                            } finally {
                                pMutex.semaphore.release();
                                mutexByKey.compute(key, (k, v) -> --v.refCount == 0 ? null : v);
                            }
                            // At most one thread (globally) can offer downstream at a time - sequenced globally, in poll-order
                            if (out != null) {
                                gCurr.await();
                                return out;
                            }
                            gNext.countDown();
                        }
                    }
                };
                
                try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
                    for (var sink : sinks) {
                        var holder = new LatchHolder();
                        scope.fork(() -> ScopedValue.callWhere(scopedLatch, holder, () -> {
                            sink.drainFromSource(stepSource);
                            holder.latch.countDown();
                            return null;
                        }));
                    }
                    scope.join().throwIfFailed();
                }
            }
            
            @Override
            public void complete(Throwable error) throws InterruptedException, ExecutionException {
                parallelComplete(sinks, error);
            }
        }
        
        return new MapBalancedPartitioned();
    }
    
    public static <T, U> Conduit.Sink<T> mapBalanceOrdered(Function<? super T, ? extends Callable<U>> mapper,
                                                           List<? extends Conduit.Sink<U>> sinks) {
        class LatchHolder {
            CountDownLatch latch = new CountDownLatch(1);
        }
        
        class MapBalanceOrdered implements Conduit.Sink<T> {
            final ScopedValue<LatchHolder> scopedLatch = ScopedValue.newInstance();
            final ReentrantLock lock = new ReentrantLock();
            CountDownLatch currLatch = new CountDownLatch(0);
            
            @Override
            public void drainFromSource(Conduit.StepSource<? extends T> source) throws InterruptedException, ExecutionException {
                var stepSource = new Conduit.StepSource<U>() {
                    @Override
                    public U poll() throws Exception {
                        var holder = scopedLatch.get();
                        holder.latch.countDown();
                        for (;;) {
                            Callable<U> callable;
                            CountDownLatch curr, next = holder.latch = new CountDownLatch(1);
                            lock.lockInterruptibly();
                            try {
                                curr = currLatch;
                                currLatch = next;
                                T in = source.poll();
                                if (in == null) {
                                    return null;
                                }
                                callable = mapper.apply(in);
                            } finally {
                                lock.unlock();
                            }
                            U out = callable.call();
                            if (out != null) {
                                curr.await();
                                return out;
                            }
                            next.countDown();
                        }
                    }
                };
                
                try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
                    for (var sink : sinks) {
                        var holder = new LatchHolder();
                        scope.fork(() -> ScopedValue.callWhere(scopedLatch, holder, () -> {
                            sink.drainFromSource(stepSource);
                            holder.latch.countDown();
                            return null;
                        }));
                    }
                    scope.join().throwIfFailed();
                }
            }
            
            @Override
            public void complete(Throwable error) throws InterruptedException, ExecutionException {
                parallelComplete(sinks, error);
            }
        }
        
        return new MapBalanceOrdered();
    }
    
    // This could be separated into a 'map(stepSource, toCallable)' | balance(sinks.map(sink -> fuse(callCallable, sink))
    
    public static <T, U> Conduit.Sink<T> mapBalance(Function<? super T, ? extends Callable<U>> mapper,
                                                    List<? extends Conduit.Sink<U>> sinks) {
        class MapBalance implements Conduit.Sink<T> {
            final ReentrantLock lock = new ReentrantLock();
            
            @Override
            public void drainFromSource(Conduit.StepSource<? extends T> source) throws InterruptedException, ExecutionException {
                var stepSource = new Conduit.StepSource<U>() {
                    @Override
                    public U poll() throws Exception {
                        for (;;) {
                            T in;
                            Callable<U> callable;
                            lock.lockInterruptibly();
                            try {
                                if ((in = source.poll()) == null) {
                                    return null;
                                }
                                callable = mapper.apply(in);
                            } finally {
                                lock.unlock();
                            }
                            U out = callable.call();
                            if (out != null) {
                                return out;
                            }
                        }
                    }
                };
                
                try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
                    for (var sink : sinks) {
                        scope.fork(() -> {
                            sink.drainFromSource(stepSource);
                            return null;
                        });
                    }
                    scope.join().throwIfFailed();
                }
            }
            
            @Override
            public void complete(Throwable error) throws InterruptedException, ExecutionException {
                parallelComplete(sinks, error);
            }
        }
        
        return new MapBalance();
    }
    
    public static <T> Conduit.Sink<T> balance(List<? extends Conduit.Sink<T>> sinks) {
        class Balance implements Conduit.Sink<T> {
            @Override
            public void drainFromSource(Conduit.StepSource<? extends T> source) throws InterruptedException, ExecutionException {
                try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
                    for (var sink : sinks) {
                        scope.fork(() -> {
                            sink.drainFromSource(source);
                            return null;
                        });
                    }
                    scope.join().throwIfFailed();
                }
            }
            
            @Override
            public void complete(Throwable error) throws InterruptedException, ExecutionException {
                parallelComplete(sinks, error);
            }
        }
        
        return new Balance();
    }
    
    public static <T> Conduit.StepSink<T> stepBroadcast(List<? extends Conduit.StepSink<T>> sinks) {
        class StepBroadcast implements Conduit.StepSink<T> {
            @Override
            public boolean offer(T input) throws InterruptedException, ExecutionException {
                try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
                    for (var sink : sinks) {
                        scope.fork(() -> {
                            // TODO: Check return - handle cancel
                            sink.offer(input);
                            return null;
                        });
                    }
                    scope.join().throwIfFailed();
                }
                return true; // TODO: Unless any/all sinks have cancelled
            }
            
            @Override
            public void complete(Throwable error) throws InterruptedException, ExecutionException {
                parallelComplete(sinks, error);
            }
        }
        
        return new StepBroadcast();
    }
    
    public static <T> Conduit.Sink<T> broadcast(List<? extends Conduit.Sink<T>> sinks) {
        class Broadcast implements Conduit.Sink<T> {
            final ScopedValue<Integer> scopedPos = ScopedValue.newInstance();
            final ReentrantLock lock = new ReentrantLock();
            final Condition ready = lock.newCondition();
            final BitSet bitSet = new BitSet(sinks.size());
            int count = sinks.size();
            boolean done = false;
            T current = null;
            
            @Override
            public void drainFromSource(Conduit.StepSource<? extends T> source) throws Exception {
                // Every sink has to call drainFromSource(source) in its own thread
                // The poll() on that source needs to respond differently based on which sink is calling
                //  - If the sink has already consumed this round's element, wait for next round
                //  - First sink in each round polls for the next element
                //  - Last sink in each round wakes the others for the next round
                var stepSource = new Conduit.StepSource<T>() {
                    @Override
                    public T poll() throws Exception {
                        lock.lockInterruptibly();
                        try {
                            if (done) {
                                return null;
                            }
                            int myPos = scopedPos.get();
                            if (bitSet.get(myPos)) {
                                // Already seen - Wait for next round
                                do {
                                    ready.await();
                                    if (done) {
                                        return null;
                                    }
                                } while (bitSet.get(myPos));
                            }
                            bitSet.set(myPos);
                            int seen = bitSet.cardinality();
                            if (seen == 1) {
                                // First-one-in - Poll
                                done = (current = source.poll()) == null;
                            }
                            if (seen == count) {
                                // Last-one-in - Wake up others for next round
                                bitSet.clear();
                                ready.signalAll();
                            }
                            return current;
                        } finally {
                            lock.unlock();
                        }
                    }
                };
                
                try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
                    for (int i = 0; i < sinks.size(); i++) {
                        var ii = i;
                        var sink = sinks.get(i);
                        scope.fork(() -> ScopedValue.callWhere(scopedPos, ii, () -> {
                            try {
                                sink.drainFromSource(stepSource);
                            } finally {
                                lock.lock();
                                try {
                                    bitSet.clear(ii);
                                    if (bitSet.cardinality() == --count) {
                                        bitSet.clear();
                                        ready.signalAll();
                                    }
                                } finally {
                                    lock.unlock();
                                }
                            }
                            return null;
                        }));
                    }
                    scope.join().throwIfFailed();
                }
            }
            
            @Override
            public void complete(Throwable error) throws Exception {
                parallelComplete(sinks, error);
            }
        }
        
        return new Broadcast();
    }
    
    public static <T> Conduit.StepSink<T> stepPartition(List<? extends Conduit.StepSink<T>> sinks,
                                                        BiConsumer<T, IntConsumer> selector) {
        class StepPartition implements Conduit.StepSink<T> {
            @Override
            public boolean offer(T input) throws Exception {
                try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
                    IntConsumer router = i -> {
                        var sink = sinks.get(i); // Note: Can throw IOOBE
                        scope.fork(() -> {
                            // TODO: Check return - handle cancel
                            sink.offer(input);
                            return null;
                        });
                    };
                    selector.accept(input, router); // Note: User-defined callback can throw exception
                    scope.join().throwIfFailed();
                }
                return true; // TODO: Unless any/all sinks have cancelled
            }
            
            @Override
            public void complete(Throwable error) throws Exception {
                parallelComplete(sinks, error);
            }
        }
        
        return new StepPartition();
    }
    
    // TODO: partition
    
    // --- Sources ---
    
    public static <T> Conduit.StepSource<T> stepConcat(List<? extends Conduit.StepSource<T>> sources) {
        class StepConcat implements Conduit.StepSource<T> {
            int i = 0;
            
            @Override
            public T poll() throws Exception {
                for (; i < sources.size(); i++) {
                    T t = sources.get(i).poll();
                    if (t != null) {
                        return t;
                    }
                }
                return null;
            }
            
            @Override
            public void close() throws Exception {
                parallelClose(sources);
            }
        }
        
        return new StepConcat();
    }
    
    public static <T> Conduit.Source<T> concat(List<? extends Conduit.Source<T>> sources) {
        class Concat implements Conduit.Source<T> {
            @Override
            public void drainToSink(Conduit.StepSink<? super T> sink) throws Exception {
                for (var source : sources) {
                    source.drainToSink(sink);
                }
            }
            
            @Override
            public void close() throws Exception {
                parallelClose(sources);
            }
        }
        
        return new Concat();
    }
    
    public static <T> Conduit.Source<T> merge(List<? extends Conduit.Source<T>> sources) {
        class Merge implements Conduit.Source<T> {
            @Override
            public void drainToSink(Conduit.StepSink<? super T> sink) throws InterruptedException, ExecutionException {
                try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
                    for (var source : sources) {
                        scope.fork(() -> {
                            source.drainToSink(sink);
                            return null;
                        });
                    }
                    scope.join().throwIfFailed();
                }
            }
            
            @Override
            public void close() throws Exception {
                parallelClose(sources);
            }
        }
        
        return new Merge();
    }
    
    public static <T> Conduit.StepSource<T> stepMergeSorted(List<? extends Conduit.StepSource<T>> sources,
                                                            Comparator<? super T> comparator) {
        class MergeSorted implements Conduit.StepSource<T> {
            final PriorityQueue<Indexed<T>> latest = new PriorityQueue<>(sources.size(), Comparator.comparing(i -> i.element, comparator));
            int lastIndex = -1;
            
            @Override
            public T poll() throws Exception {
                if (lastIndex == -1) {
                    // First poll - poll all sources to bootstrap the queue
                    try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
                        List<StructuredTaskScope.Subtask<T>> tasks = new ArrayList<>();
                        for (var source : sources) {
                            tasks.add(scope.fork(source::poll));
                        }
                        scope.join().throwIfFailed();
                        for (int i = 0; i < tasks.size(); i++) {
                            var t = tasks.get(i).get();
                            if (t != null) {
                                latest.offer(new Indexed<>(t, i));
                            }
                        }
                    }
                } else {
                    // Subsequent poll - poll from the source that last emitted
                    var t = sources.get(lastIndex).poll();
                    if (t != null) {
                        latest.offer(new Indexed<>(t, lastIndex));
                    }
                }
                Indexed<T> min = latest.poll();
                if (min != null) {
                    lastIndex = min.index;
                    return min.element;
                }
                return null;
            }
            
            @Override
            public void close() throws Exception {
                parallelClose(sources);
            }
        }
        
        return new MergeSorted();
    }
    
    // TODO: mergeSorted
    
    // TODO: interleave (, mergePreferred, mergePrioritized)
    
    private interface Accessor<X, Y> {
        void setLatest1(X x);
        void setLatest2(Y y);
        X latest1();
        Y latest2();
    }
    
    public static <T1, T2, T> Conduit.StepSource<T> stepZip(Conduit.StepSource<T1> source1,
                                                            Conduit.StepSource<T2> source2,
                                                            BiFunction<? super T1, ? super T2, T> merger) {
        Objects.requireNonNull(source1);
        Objects.requireNonNull(source2);
        Objects.requireNonNull(merger);
        
        class StepZip implements Conduit.StepSource<T> {
            final ReentrantLock lock = new ReentrantLock();
            int state = RUNNING;
            
            static final int RUNNING    = 0;
            static final int COMPLETING = 1;
            static final int CLOSED     = 2;
            
            @Override
            public T poll() throws Exception {
                lock.lockInterruptibly();
                if (state >= COMPLETING) {
                    lock.unlock();
                    return null;
                }
                try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
                    var task1 = scope.fork(source1::poll);
                    var task2 = scope.fork(source2::poll);
                    scope.join().throwIfFailed();
                    var result1 = task1.get();
                    var result2 = task2.get();
                    if (result1 == null || result2 == null) {
                        state = COMPLETING;
                        return null;
                    }
                    return Objects.requireNonNull(merger.apply(result1, result2));
                } finally {
                    lock.unlock();
                }
            }
            
            @Override
            public void close() throws Exception {
                lock.lockInterruptibly();
                if (state == CLOSED) {
                    lock.unlock();
                    return;
                }
                try {
                    parallelClose(List.of(source1, source2));
                    state = CLOSED;
                } finally {
                    lock.unlock();
                }
            }
        }
        
        return new StepZip();
    }
    
    public static <T1, T2, T> Conduit.Source<T> zip(Conduit.Source<T1> source1,
                                                    Conduit.Source<T2> source2,
                                                    BiFunction<? super T1, ? super T2, T> merger) {
        Objects.requireNonNull(source1);
        Objects.requireNonNull(source2);
        Objects.requireNonNull(merger);
        
        class Zip implements Conduit.Source<T> {
            final ReentrantLock lock = new ReentrantLock();
            final Condition ready = lock.newCondition();
            T1 latest1 = null;
            T2 latest2 = null;
            int state = NEW;
            
            static final int NEW        = 0;
            static final int RUNNING    = 1;
            static final int COMPLETING = 2;
            static final int CLOSED     = 3;
            
            static final VarHandle STATE;
            static {
                try {
                    STATE = MethodHandles.lookup().findVarHandle(Zip.class, "state", int.class);
                } catch (ReflectiveOperationException e) {
                    throw new ExceptionInInitializerError(e);
                }
            }
            
            <X, Y> Void runSource(Conduit.Source<X> source,
                                  Accessor<X, Y> access,
                                  Conduit.StepSink<? super T> sink) throws Exception {
                source.drainToSink(e -> {
                    lock.lockInterruptibly();
                    try {
                        if (state >= COMPLETING) {
                            return false;
                        }
                        access.setLatest1(e);
                        if (access.latest2() == null) {
                            do {
                                ready.await();
                                if (state >= COMPLETING) {
                                    return false;
                                }
                            } while (access.latest1() != null);
                            return true;
                        }
                        ready.signal();
                        T t = Objects.requireNonNull(merger.apply(latest1, latest2));
                        latest1 = null;
                        latest2 = null;
                        if (!sink.offer(t)) {
                            state = COMPLETING;
                            return false;
                        }
                        return true;
                    } finally {
                        lock.unlock();
                    }
                });
                return null;
            }

            @Override
            public void drainToSink(Conduit.StepSink<? super T> sink) throws Exception {
                if (!STATE.compareAndSet(this, NEW, RUNNING)) {
                    throw new IllegalStateException("source already consumed or closed");
                }
                try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
                    var accessor1 = new Accessor<T1, T2>() {
                        public void setLatest1(T1 t1) { latest1 = t1; }
                        public void setLatest2(T2 t2) { latest2 = t2; }
                        public T1 latest1() { return latest1; }
                        public T2 latest2() { return latest2; }
                    };
                    var accessor2 = new Accessor<T2, T1>() {
                        public void setLatest1(T2 t2) { latest2 = t2; }
                        public void setLatest2(T1 t1) { latest1 = t1; }
                        public T2 latest1() { return latest2; }
                        public T1 latest2() { return latest1; }
                    };
                    scope.fork(() -> runSource(source1, accessor1, sink));
                    scope.fork(() -> runSource(source2, accessor2, sink));
                    scope.join().throwIfFailed();
                }
            }
            
            @Override
            public void close() throws Exception {
                lock.lockInterruptibly();
                if (state == CLOSED) {
                    lock.unlock();
                    return;
                }
                try {
                    parallelClose(List.of(source1, source2));
                    state = CLOSED;
                } finally {
                    lock.unlock();
                }
            }
        }
        
        return new Zip();
    }
    
    public static <T1, T2, T> Conduit.Source<T> zipLatest(Conduit.Source<T1> source1,
                                                          Conduit.Source<T2> source2,
                                                          BiFunction<? super T1, ? super T2, T> merger) {
        Objects.requireNonNull(source1);
        Objects.requireNonNull(source2);
        Objects.requireNonNull(merger);
        
        class ZipLatest implements Conduit.Source<T> {
            final ReentrantLock lock = new ReentrantLock();
            final Condition ready = lock.newCondition();
            T1 latest1 = null;
            T2 latest2 = null;
            int state = NEW;
            
            static final int NEW        = 0;
            static final int RUNNING    = 1;
            static final int COMPLETING = 2;
            static final int CLOSED     = 3;
            
            static final VarHandle STATE;
            static {
                try {
                    STATE = MethodHandles.lookup().findVarHandle(ZipLatest.class, "state", int.class);
                } catch (ReflectiveOperationException e) {
                    throw new ExceptionInInitializerError(e);
                }
            }
            
            <X, Y> Void runSource(Conduit.Source<X> source,
                                  Accessor<X, Y> access,
                                  Conduit.StepSink<? super T> sink) throws Exception {
                source.drainToSink(new Conduit.StepSink<>() {
                    boolean first = true;
                    
                    @Override
                    public boolean offer(X e) throws Exception {
                        lock.lockInterruptibly();
                        try {
                            if (state >= COMPLETING) {
                                return false;
                            }
                            if (first) {
                                first = false;
                                if (e == null) {
                                    // If either source is empty, we will never emit
                                    state = COMPLETING;
                                    ready.signal();
                                    return false;
                                }
                                // Wait until we have the first element from both sources
                                access.setLatest1(e);
                                if (access.latest2() == null) {
                                    do {
                                        ready.await();
                                        if (state >= COMPLETING) {
                                            return false;
                                        }
                                    } while (access.latest2() == null);
                                    return true; // First emission handled by other thread
                                }
                                ready.signal();
                            }
                            access.setLatest1(e);
                            T t = Objects.requireNonNull(merger.apply(latest1, latest2));
                            if (!sink.offer(t)) {
                                state = COMPLETING;
                                return false;
                            }
                            return true;
                        } finally {
                            lock.unlock();
                        }
                    }
                });
                return null;
            }
            
            @Override
            public void drainToSink(Conduit.StepSink<? super T> sink) throws InterruptedException, ExecutionException {
                if (!STATE.compareAndSet(this, NEW, RUNNING)) {
                    throw new IllegalStateException("source already consumed or closed");
                }
                try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
                    var accessor1 = new Accessor<T1, T2>() {
                        public void setLatest1(T1 t1) { latest1 = t1; }
                        public void setLatest2(T2 t2) { latest2 = t2; }
                        public T1 latest1() { return latest1; }
                        public T2 latest2() { return latest2; }
                    };
                    var accessor2 = new Accessor<T2, T1>() {
                        public void setLatest1(T2 t2) { latest2 = t2; }
                        public void setLatest2(T1 t1) { latest1 = t1; }
                        public T2 latest1() { return latest2; }
                        public T1 latest2() { return latest1; }
                    };
                    scope.fork(() -> runSource(source1, accessor1, sink));
                    scope.fork(() -> runSource(source2, accessor2, sink));
                    scope.join().throwIfFailed();
                }
            }
            
            @Override
            public void close() throws Exception {
                lock.lockInterruptibly();
                if (state == CLOSED) {
                    lock.unlock();
                    return;
                }
                try {
                    parallelClose(List.of(source1, source2));
                    state = CLOSED;
                } finally {
                    lock.unlock();
                }
            }
        }
        
        return new ZipLatest();
    }
    
    // --- Stages ---
    
    public static <T, A> Conduit.Stage<T, A> batch(Supplier<? extends A> batchSupplier,
                                                   BiConsumer<? super A, ? super T> accumulator,
                                                   Function<? super A, Optional<Instant>> deadlineMapper) {
        Objects.requireNonNull(batchSupplier);
        Objects.requireNonNull(accumulator);
        Objects.requireNonNull(deadlineMapper);
        
        class Batch implements TimedStage.Core<T, A> {
            A batch = null;
            Instant currentDeadline = null;
            boolean done = false;
            Throwable err = null;
            
            @Override
            public Instant onInit() {
                return Instant.MAX;
            }
            
            @Override
            public void onOffer(TimedStage.SinkController ctl, T input) throws InterruptedException {
                // Alternative implementations might adjust or reset the buffer instead of blocking
                while (batch != null && currentDeadline == Instant.MIN) {
                    if (!ctl.awaitAdvance()) {
                        return;
                    }
                }
                if (batch == null) {
                    batch = Objects.requireNonNull(batchSupplier.get());
                }
                accumulator.accept(batch, input);
                currentDeadline = deadlineMapper.apply(batch).orElse(null);
                if (currentDeadline != null) {
                    ctl.latchDeadline(currentDeadline);
                }
            }
            
            @Override
            public void onPoll(TimedStage.SourceController<A> ctl) throws UpstreamException {
                if (done) {
                    if (err != null) {
                        throw new UpstreamException(err);
                    }
                    ctl.latchClose();
                    if (batch == null) {
                        return;
                    }
                }
                ctl.latchOutput(batch);
                batch = null;
                currentDeadline = null;
                ctl.latchDeadline(Instant.MAX);
                ctl.signalAdvance();
            }
            
            @Override
            public void onComplete(TimedStage.SinkController ctl, Throwable error) {
                err = error;
                done = true;
                ctl.latchDeadline(Instant.MIN);
            }
        }
        
        var core = new Batch();
        return new TimedStage<>(core);
    }
    
    public static <T> Conduit.Stage<T, T> tokenBucket(Duration tokenInterval,
                                                      ToLongFunction<T> costMapper,
                                                      long tokenLimit,
                                                      long costLimit) { // TODO: Obviate costLimit?
        Objects.requireNonNull(tokenInterval);
        Objects.requireNonNull(costMapper);
        if ((tokenLimit | costLimit) < 0) {
            throw new IllegalArgumentException("tokenLimit and costLimit must be non-negative");
        }
        if (!tokenInterval.isPositive()) {
            throw new IllegalArgumentException("tokenInterval must be positive");
        }
        
        long tmpTokenInterval;
        try {
            tmpTokenInterval = tokenInterval.toNanos();
        } catch (ArithmeticException e) {
            tmpTokenInterval = Long.MAX_VALUE; // Unreasonable but correct
        }
        long tokenIntervalNanos = tmpTokenInterval;
        
        class Throttle implements TimedStage.Core<T, T> {
            Deque<Weighted<T>> queue = null;
            long tempTokenLimit = 0;
            long tokens = 0;
            long cost = 0;
            Instant lastObservedAccrual;
            boolean done = false;
            Throwable err = null;
            
            @Override
            public Instant onInit() {
                queue = new ArrayDeque<>();
                lastObservedAccrual = clock().instant();
                return Instant.MAX;
            }
            
            @Override
            public void onOffer(TimedStage.SinkController ctl, T input) throws InterruptedException {
                // Optional blocking for boundedness, here based on cost rather than queue size
                while (cost >= costLimit) {
                    if (!ctl.awaitAdvance()) {
                        return;
                    }
                }
                long elementCost = costMapper.applyAsLong(input);
                if (elementCost < 0) {
                    throw new IllegalStateException("Element cost cannot be negative");
                }
                cost = Math.addExact(cost, elementCost);
                var w = new Weighted<>(input, elementCost);
                queue.offer(w);
                if (queue.peek() == w) {
                    ctl.latchDeadline(Instant.MIN); // Let source-side do token math
                }
            }
            
            @Override
            public void onPoll(TimedStage.SourceController<T> ctl) throws UpstreamException {
                if (err != null) {
                    throw new UpstreamException(err);
                }
                Weighted<T> head = queue.peek();
                if (head == null) {
                    ctl.latchClose();
                    return;
                }
                // Increase tokens based on actual amount of time that passed
                Instant now = clock().instant();
                long nanosSinceLastObservedAccrual = ChronoUnit.NANOS.between(lastObservedAccrual, now);
                long nanosSinceLastAccrual = nanosSinceLastObservedAccrual % tokenIntervalNanos;
                long newTokens = nanosSinceLastObservedAccrual / tokenIntervalNanos;
                if (newTokens > 0) {
                    lastObservedAccrual = now.minusNanos(nanosSinceLastAccrual);
                    tokens = Math.min(tokens + newTokens, Math.max(tokenLimit, tempTokenLimit));
                }
                // Emit if we can, then schedule next emission
                if (tokens >= head.cost) {
                    tempTokenLimit = 0;
                    tokens -= head.cost;
                    cost -= head.cost;
                    queue.poll();
                    ctl.signalAdvance();
                    ctl.latchOutput(head.element);
                    head = queue.peek();
                    if (head != null) {
                        if (tokens >= head.cost) {
                            ctl.latchDeadline(Instant.MIN);
                            return;
                        }
                        // Fall-through to scheduling!
                    } else if (!done) {
                        ctl.latchDeadline(Instant.MAX);
                        return;
                    } else {
                        ctl.latchClose();
                        return;
                    }
                }
                // Schedule to wake up when we have enough tokens for next emission
                tempTokenLimit = head.cost;
                long tokensNeeded = head.cost - tokens;
                ctl.latchDeadline(now.plusNanos(tokenIntervalNanos * tokensNeeded - nanosSinceLastAccrual));
            }
            
            @Override
            public void onComplete(TimedStage.SinkController ctl, Throwable error) {
                err = error;
                done = true;
                if (error != null || queue.isEmpty()) {
                    ctl.latchDeadline(Instant.MIN);
                }
            }
        }
        
        var core = new Throttle();
        return new TimedStage<>(core);
    }
    
    public static <T> Conduit.Stage<T, T> delay(Function<? super T, Instant> deadlineMapper,
                                                int bufferLimit) {
        Objects.requireNonNull(deadlineMapper);
        if (bufferLimit < 1) {
            throw new IllegalArgumentException("bufferLimit must be positive");
        }
        
        class Delay implements TimedStage.Core<T, T> {
            PriorityQueue<Expiring<T>> pq = null;
            boolean done = false;
            Throwable err = null;
            
            @Override
            public Instant onInit() {
                pq = new PriorityQueue<>(bufferLimit);
                return Instant.MAX;
            }
            
            @Override
            public void onOffer(TimedStage.SinkController ctl, T input) throws InterruptedException {
                while (pq.size() >= bufferLimit) {
                    if (!ctl.awaitAdvance()) {
                        return;
                    }
                }
                Instant deadline = Objects.requireNonNull(deadlineMapper.apply(input));
                Expiring<T> e = new Expiring<>(input, deadline);
                pq.offer(e);
                if (pq.peek() == e) {
                    ctl.latchDeadline(deadline);
                }
            }
            
            @Override
            public void onPoll(TimedStage.SourceController<T> ctl) throws UpstreamException {
                if (err != null) {
                    throw new UpstreamException(err);
                }
                Expiring<T> head = pq.poll();
                if (head == null) {
                    ctl.latchClose();
                    return;
                }
                ctl.signalAdvance();
                ctl.latchOutput(head.element);
                head = pq.peek();
                if (head != null) {
                    ctl.latchDeadline(head.deadline);
                } else if (!done) {
                    ctl.latchDeadline(Instant.MAX);
                } else {
                    ctl.latchClose();
                }
           }
            
            @Override
            public void onComplete(TimedStage.SinkController ctl, Throwable error) {
                err = error;
                done = true;
                if (error != null || pq.isEmpty()) {
                    ctl.latchDeadline(Instant.MIN);
                }
            }
        }
       
        var core = new Delay();
        return new TimedStage<>(core);
    }
    
    public static <T> Conduit.Stage<T, T> keepAlive(Duration timeout,
                                                    Supplier<? extends T> extraSupplier,
                                                    int bufferLimit) {
        Objects.requireNonNull(timeout);
        Objects.requireNonNull(extraSupplier);
        if (bufferLimit < 1) {
            throw new IllegalArgumentException("bufferLimit must be positive");
        }
        if (!timeout.isPositive()) {
            throw new IllegalArgumentException("timeout must be positive");
        }
        
        class KeepAlive implements TimedStage.Core<T, T> {
            Deque<T> queue = null;
            boolean done = false;
            Throwable err = null;
            
            @Override
            public Instant onInit() {
                queue = new ArrayDeque<>(bufferLimit);
                return clock().instant().plus(timeout);
            }
            
            @Override
            public void onOffer(TimedStage.SinkController ctl, T input) throws InterruptedException {
                while (queue.size() >= bufferLimit) {
                    if (!ctl.awaitAdvance()) {
                        return;
                    }
                }
                queue.offer(input);
                ctl.latchDeadline(Instant.MIN);
            }
            
            @Override
            public void onPoll(TimedStage.SourceController<T> ctl) throws UpstreamException {
                if (err != null) {
                    throw new UpstreamException(err);
                }
                T head = queue.poll();
                if (head != null) {
                    ctl.signalAdvance();
                    ctl.latchOutput(head);
                    ctl.latchDeadline((!queue.isEmpty() || done) ? Instant.MIN : clock().instant().plus(timeout));
                } else if (!done) {
                    ctl.latchOutput(extraSupplier.get());
                    ctl.latchDeadline(clock().instant().plus(timeout));
                } else {
                    ctl.latchClose();
                }
            }
            
            @Override
            public void onComplete(TimedStage.SinkController ctl, Throwable error) {
                err = error;
                done = true;
                ctl.latchDeadline(Instant.MIN);
            }
        }
        
        var core = new KeepAlive();
        return new TimedStage<>(core);
    }

    public static <T> Conduit.Stage<T, T> extrapolate(T initial,
                                                      Function<? super T, ? extends Iterator<? extends T>> mapper,
                                                      int bufferLimit) {
        Objects.requireNonNull(mapper);
        if (bufferLimit < 1) {
            throw new IllegalArgumentException("bufferLimit must be positive");
        }
        
        class Extrapolate implements TimedStage.Core<T, T> {
            Deque<T> queue = null;
            Iterator<? extends T> iter = null;
            boolean done = false;
            Throwable err = null;
            
            @Override
            public Instant onInit() {
                queue = new ArrayDeque<>(bufferLimit);
                if (initial != null) {
                    queue.offer(initial);
                    return Instant.MIN;
                }
                return Instant.MAX;
            }
            
            @Override
            public void onOffer(TimedStage.SinkController ctl, T input) throws InterruptedException {
                while (queue.size() >= bufferLimit) {
                    if (!ctl.awaitAdvance()) {
                        return;
                    }
                }
                queue.offer(input);
                iter = null;
                ctl.latchDeadline(Instant.MIN);
            }
            
            @Override
            public void onPoll(TimedStage.SourceController<T> ctl) throws UpstreamException {
                if (err != null) {
                    throw new UpstreamException(err);
                }
                T head = queue.poll();
                if (head != null) {
                    ctl.signalAdvance();
                    ctl.latchOutput(head);
                    if (queue.peek() != null) {
                        ctl.latchDeadline(Instant.MIN);
                    } else if (!done) {
                        iter = Objects.requireNonNull(mapper.apply(head));
                        ctl.latchDeadline(iter.hasNext() ? Instant.MIN : Instant.MAX);
                    } else {
                        ctl.latchClose();
                    }
                } else if (!done) {
                    ctl.latchOutput(iter.next());
                    ctl.latchDeadline(iter.hasNext() ? Instant.MIN : Instant.MAX);
                } else {
                    ctl.latchClose();
                }
            }
            
            @Override
            public void onComplete(TimedStage.SinkController ctl, Throwable error) {
                err = error;
                done = true;
                ctl.latchDeadline(Instant.MIN);
            }
        }
        
        var core = new Extrapolate();
        return new TimedStage<>(core);
    }
    
    // TODO: Expose
    private static class AggregateFailure extends StructuredTaskScope<Object> {
        volatile Throwable error;
        
        static final VarHandle ERROR;
        static {
            try {
                ERROR = MethodHandles.lookup().findVarHandle(AggregateFailure.class, "error", Throwable.class);
            } catch (Exception e) {
                throw new ExceptionInInitializerError(e);
            }
        }
        
        @Override
        protected void handleComplete(Subtask<?> subtask) {
            if (subtask.state() == Subtask.State.FAILED) {
                Throwable err = subtask.exception();
                if (!ERROR.compareAndSet(this, null, err)) {
                    error.addSuppressed(err);
                }
            }
        }
        
        @Override
        public AggregateFailure join() throws InterruptedException {
            super.join();
            return this;
        }
        
        public void throwIfFailed() throws ExecutionException {
            ensureOwnerAndJoined();
            Throwable err = error;
            if (err != null) {
                // TODO: Does this handle suppression correctly?
                throw new ExecutionException(err);
            }
        }
    }
    
    public static void parallelClose(Collection<? extends Conduit.Source<?>> sources) throws InterruptedException, ExecutionException {
        try (var scope = new AggregateFailure()) {
            for (var source : sources) {
                scope.fork(() -> {
                    source.close();
                    return null;
                });
            }
            scope.join().throwIfFailed();
        }
    }
    
    public static void parallelComplete(Collection<? extends Conduit.Sink<?>> sinks, Throwable error) throws InterruptedException, ExecutionException {
        try (var scope = new AggregateFailure()) {
            for (var sink : sinks) {
                scope.fork(() -> {
                    sink.complete(error);
                    return null;
                });
            }
            scope.join().throwIfFailed();
        }
    }
    
    private static class Indexed<T> {
        final T element;
        final int index;
        
        Indexed(T element, int index) {
            this.element = element;
            this.index = index;
        }
    }
    
    private static class Weighted<T> {
        final T element;
        final long cost;
        
        Weighted(T element, long cost) {
            this.element = element;
            this.cost = cost;
        }
    }
    
    private static class Expiring<T> implements Comparable<Expiring<T>> {
        final T element;
        final Instant deadline;
        
        Expiring(T element, Instant deadline) {
            this.element = element;
            this.deadline = deadline;
        }
        
        public int compareTo(Expiring other) {
            return deadline.compareTo(other.deadline);
        }
    }
}
