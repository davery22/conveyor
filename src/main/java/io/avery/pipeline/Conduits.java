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
        final Conduit.StepSink<T> stepSink;
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
            this.stepSink = Objects.requireNonNull(sink);
            this.gsink = el -> {
                try {
                    return stepSink.offer(el);
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
                stepSink.complete(error);
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
    
    public static <In, T, A> Conduit.StepSink<In> stepFuse(Gatherer<In, A, ? extends T> gatherer, Conduit.StepSink<T> sink) {
        @SuppressWarnings("unchecked")
        Gatherer<In, A, T> g = (Gatherer<In, A, T>) gatherer;
        return new FusedStepSink<>(g, sink);
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
    
    // TODO: This works, but is it better than putting stepFuse(gatherer, segue) before the sink?
    //
    // 1. A=loop{process+offer} | B=loop{poll+process}
    // 2. B=loop{(poll+fork(a))?+poll+process} | a=loop*{process+offer}
    //
    // 2 is likely less efficient due to forking overhead. Does it buy anything?
    //  - Can fuse directly to a Sink, instead of fusing to a Segue before the Sink
    //    - Possible because the asynchrony is implicit in the Sink itself, rather than explicit between Segue and Sink
    //  - Propagation of completion/error?
    //    - In 1, an error from the fused Segue is propagated down by Sink.complete(error); Sink will propagate or throw wrapped in UpstreamException
    //    - In 2, an error from the fused Segue is propagated out by Sink.drainFromSource(..); Sink will propagate or throw unwrapped
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
        var buffer = Conduits.extrapolate(delimiter, e -> Collections.emptyIterator(), bufferLimit);
        var bufferSource = buffer.source();
        var gatherSink = stepFuse(new DelimitedGatherer(), buffer.sink());
        
        class FusedSink implements Conduit.Sink<In> {
            @Override
            public boolean drainFromSource(Conduit.StepSource<? extends In> source) throws Exception {
                try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
                    class HelperSource implements Conduit.StepSource<T> {
                        @Override
                        @SuppressWarnings("unchecked")
                        public T poll() throws Exception {
                            for (;;) {
                                Object out = bufferSource.poll();
                                if (out != delimiter) {
                                    return (T) out;
                                }
                                In in = source.poll();
                                scope.fork(() -> {
                                    if (in == null || !gatherSink.offer(in)) {
                                        gatherSink.complete(null);
                                    }
                                    return null;
                                });
                            }
                        }
                        
                        @Override
                        public void close() {
                            scope.shutdown();
                        }
                    }
                    
                    var task = scope.fork(() -> {
                        // Done in a fork so that an error from other forks will interrupt this
                        try (var stepSource = new HelperSource()) {
                            return sink.drainFromSource(stepSource);
                        }
                    });
                    
                    scope.join().throwIfFailed();
                    return task.get();
                }
            }
            
            @Override
            public void complete(Throwable error) throws Exception {
                sink.complete(error);
            }
        }
        
        return new FusedSink();
    }
    
    public static <In, T> Conduit.Sink<In> compose(Conduit.Sink<T> sink, Conduit.Segue<In, T> segue) {
        var segueSink = segue.sink();
        var segueSource = segue.source();
        
        class FusedSink implements Conduit.Sink<In> {
            @Override
            public boolean drainFromSource(Conduit.StepSource<? extends In> source) throws InterruptedException, ExecutionException {
                try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
                    scope.fork(() -> {
                        try {
                            source.drainToSink(segueSink);
                            segueSink.complete(null);
                        } catch (Throwable e) {
                            segueSink.complete(e);
                        }
                        return null;
                    });
                    scope.fork(() -> {
                        try (segueSource) {
                            return sink.drainFromSource(segueSource);
                        }
                    });
                    scope.join().throwIfFailed();
                    return false;
                }
            }
            
            @Override
            public void complete(Throwable error) throws Exception {
                sink.complete(error);
            }
        }
        
        return new FusedSink();
    }
    
    public static <T, Out> Conduit.Source<Out> andThen(Conduit.Source<T> source, Conduit.Segue<T, Out> segue) {
        var segueSink = segue.sink();
        var segueSource = segue.source();
        
        class FusedSource implements Conduit.Source<Out> {
            @Override
            public boolean drainToSink(Conduit.StepSink<? super Out> sink) throws InterruptedException, ExecutionException {
                try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
                    scope.fork(() -> {
                        try {
                            source.drainToSink(segueSink);
                            segueSink.complete(null);
                        } catch (Throwable e) {
                            segueSink.complete(e);
                        }
                        return null;
                    });
                    scope.fork(() -> {
                        try (segueSource) {
                            return sink.drainFromSource(segueSource);
                        }
                    });
                    scope.join().throwIfFailed();
                    return false;
                }
            }
            
            @Override
            public void close() throws Exception {
                source.close();
            }
        }
        
        return new FusedSource();
    }
    
    // concat() could work by creating a Conduit.Source that switches between 2 Conduit.Sources,
    // or by creating a Pipeline.Source that consumes one Conduit.Source and then another.
    // The latter case reduces to the former case, since Pipeline.Source needs to expose a Conduit.StepSource
    
    // TODO: Likely going to have alternates of several methods to account for (non-)step-ness.
    
    // TODO: In forEachUntilCancel(), is exception because of source or sink?
    //  Matters for deciding what becomes of source + other sinks
    //  Maybe non-step Sources shouldn't be allowed at all?
    //   - Because, if any / all sinks cancel, source can't recover well (snapshot its state) for new sinks
    //   - Instead, have combinators that take a Segue to build a Source
    
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
    
    // Why can't we write a step stage from non-step stages?
    // Non-step stages can only "run-all". To wrap in step, we would either need to:
    // StepSource: When we poll(), internal StepSink (eventually) returns an element buffered from original Source.
    //  - In other words we've made a Segue!
    //  - If we run-all the original Source on first poll(), we need unbounded buffering to complete within poll()
    //  - Otherwise, the original Source would already need to be running in an unscoped thread, outside our poll()
    // StepSink: When we offer(), internal StepSource buffers so original Sink can see result on poll()
    //  - In other words we've made a Segue!
    //  - If we run-all the original Sink on first offer(), we deadlock because offer() is now waiting for more offers
    //  - Otherwise, the original Sink would already need to be running in an unscoped thread, outside our offer()
    
    // Cannot write a stepZipLatest, because poll() should return as soon as the first interior poll() completes, but
    // that would imply unscoped threads running the other interior polls. Even if we waited for all polls, that still
    // would not work, because correct behavior means that we should re-poll each source as soon as its previous poll
    // finishes, since sources may emit at different rates.
    
    // Non-step from non-step - ALWAYS possible - At worst, can reuse 'Step from step' impl with Segues
    // Non-step from step     - ALWAYS possible - See above
    // Step from non-step     - NEVER possible - Requires external asynchrony to pause draining, or unbounded buffering
    //                                           (possible using other buffer-overflow handling, eg error, drop)
    // Step from step         - SOMETIMES possible - If asynchrony is scoped to the poll/offer, or buffering is bounded
    
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
    // segues need protection just to prevent conflict between their own sink/source
    
    // fastFailSource - fail if Source is accessed by multiple threads
    // fastFailSink - fail if Sink is accessed by multiple threads
    // synchronizeStepSource
    // synchronizeStepSink
    // synchronizeSegue
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
    
    // balance() is not quite mapAsyncUnordered()
    //  - The latter has one downstream, but this can be resolved outside of balance() by having all sinks offer to the
    //    same sink.
    //  - The latter has a function that is called on elements in-order before balancing. This can be resolved by fusing
    //    before the balance(), but since balance is a Sink (not StepSink), fusing would require introducing a
    //    Segue/boundary. Instead, we can wrap the StepSource to synchronize polls, and apply the function in the same
    //    synchronized block to get application ordering.
    
    // TODO: How do things behave under adversarial exception-catching?
    // TODO: Upon re-entry after throwing, stages should either recover or fail-fast.
    
    // TODO: What about eg balance(), where one of several Sinks might be synchronous, causing a throw...
    //  In general, when/how can it be safe for close()/complete(err) to actually throw?
    //  Maybe ShutdownOnFailure is not the right scope for pipelines?
    
    // TODO: Guards
    //  - Nullness (params and function results)
    //  - Bounds / Invariants
    //  - Mutability (Copy lists)
    // TODO: Correctness
    //  - Ensure proper locking, CAS, etc
    //  - Ensure consistent behavior across operator variants (eg [step]broadcast)
    //  - Ensure exceptions are recoverable / avoid partial effects
    // TODO: Performance
    //  - Watch for locks that are held across poll / offer
    //    - Bad alternative: Produce and consume on the same thread
    //    - Good alternative: Use thread confinement, with buffers between producer/consumer threads
    
    // Being able to compose/andThen internally within a Sink/Source mainly enables encapsulation of the boundary
    // asynchrony within drainFromSource/ToSink, at the cost of not being able to step (does not produce StepSink/Source).
    // People will always be able to write Sinks/Sources this way, even if we extract boundary async to Pipelines.
    // (Likewise, people will always be able to extend Sinks/Sources with 'run'-style methods, even if we have
    // Pipelines. This would kind of violate Liskov substitution though.)
    // It's not clear if this encapsulation is worth it for 1:1 operators. It may be more worth it for fan-in/out
    // operators? In those cases, setting up + forking a Pipeline for each input Source/Sink may be a larger ordeal.
    
    // TimedSegue design
    //  - Using deadlines on both sides to avoid the need for direct management of Conditions, timed waits, etc
    //  - Latching deadlines and waiting at the start of offer/poll, to avoid the need for more locks, and make error recovery possible
    //    - This means that some use cases, like 'transfer' (wait after updating state) and 'offer multiple' (update + wait multiple times), are inexpressible
    //    - This is by design - aiming for simplicity for common cases, rather than maximum expressiveness or optimal performance
    
    // About chaining Sink -> StepSource, or StepSink -> Source:
    //  - It's reasonable to describe these as separate pipelines, eg
    //    - SOURCE.andThen(sinkFactory(buffer)).run(...); | buffer.andThen(SINK).run(...)
    //    - TODO: Unless they need to be captured in one object, eg mapAsyncPartitioned
    //      - Do we have other options in that case? Yes:
    //        1. Make the external a Segue
    //           - This is the option that does not imply buffering before Sink or after Source
    //        2. Factor internals into Source(s) and Sink(s)
    //        3. Write a run() method that runs the internals
    //  - Methods like .andThen(sink, buffer)
    //    - The buffer must actually be part of the sink; need a 'extends Sink and StepSource'-builder
    //    - Such types would add complexity that doesn't pull its weight
    //    - Such types still wouldn't be able to model fan-in/out situations
    
    // What does boolean return of drainFromSource/ToSink even mean?
    //  - If it means 'will this sink accept more elements', the answer should always be 'false'
    //  - If it means 'will the original sinks accept more elements', the answer is 'which ones?'
    //    - all of them? any of them?
    //  - true: stopped because of source(s); false: stopped because of sink(s)
    //  Practical use:
    //  - If Sink#drainFromSource(StepSource) returns false, we can poll from the StepSource again
    //  - If Source#drainToSink(StepSink) returns true, we can offer to the StepSink again
    //  - So, it's about what we can do with the param afterward!
    
    // TODO: Add scope + thread names
    
    // Talking points:
    //  - why exception cause chaining
    //  - semantics of the boolean return of drainFromSource/ToSink, and why the default impls can be the same
    //  - using buffers between threads (with polling), vs synchronized handoffs (with pushing)
    
    // TODO: Dropping elements...
    //  - eg in zip, can't know we will use either value until we have both
    //  - eg in balance, if sinks have buffers, one sink may eagerly take elements, then fail and drop its buffer
    //  - eg in stepBroadcast with eagerCancel (actually, in that case some sinks process more than others)
    
    // TODO: We should override run() to run any Sources/Sinks passed to us?
    //  Hrm, but think of a buffer that is passed to a Sink (that runs it), but used as a Source separately - would be run twice
    //   - Same with a buffer that is passed to a Source (that runs it), like zip(), but used as a Sink separately
    //   - This would only be safe if buffer.run() doesn't do anything (or is idempotent)
    //  Maybe: Run things when they must be connected to our internals? (Because we can't avoid it then)
    
    // --- Sinks ---
    
    
    // POLL:
    // Worker polls element from source
    // If completion buffer is full, worker waits until not full
    // Worker offers element to completion buffer
    // If element partition has permits, worker takes one and begins work (END)
    // Worker offers element to partition buffer, goes to step 1
    
    // TODO^: If partition has no permits, then MAX other workers are already working the partition, so leave that be
    //  But if those workers fail (and we recover), might we forget about the partition?
    
    // OFFER:
    // Worker polls partition buffer, continues if not empty (END)
    // Worker gives permit back to partition
    // If partition has max permits, worker removes partition
    
    public static <T, K, U> Conduit.SinkStepSource<T, U> mapAsyncPartitioned(int parallelism,
                                                                             int permitsPerPartition,
                                                                             int bufferLimit,
                                                                             Function<? super T, K> classifier,
                                                                             BiFunction<? super T, ? super K, ? extends Callable<U>> mapper) {
        if (parallelism < 1 || permitsPerPartition < 1 || bufferLimit < 1) {
            throw new IllegalArgumentException("parallelism, permitsPerPartition, and bufferLimit must be positive");
        }
        Objects.requireNonNull(classifier);
        Objects.requireNonNull(mapper);
        
        if (permitsPerPartition >= parallelism) {
            return mapAsyncOrdered(parallelism, bufferLimit, t -> mapper.apply(t, classifier.apply(t)));
        }
        
        class Item {
            // Value of out is initially partition key
            // When output is computed, output replaces partition key, and null replaces input
            Object out;
            T in;
            
            Item(K key, T in) {
                this.out = key;
                this.in = in;
            }
        }
        
        class Partition {
            // Only use buffer if we have no permits left
            final Deque<Item> buffer = new LinkedList<>();
            int permits = permitsPerPartition;
        }
        
        class MapAsyncPartitioned implements Conduit.SinkStepSource<T, U> {
            final ReentrantLock sourceLock = new ReentrantLock();
            final ReentrantLock lock = new ReentrantLock();
            final Condition completionNotFull = lock.newCondition();
            final Condition outputReady = lock.newCondition();
            final Deque<Item> completionBuffer = new ArrayDeque<>(bufferLimit);
            final Map<K, Partition> partitionByKey = new HashMap<>();
            int state = RUNNING;
            Throwable err = null;
            
            static final int RUNNING    = 0;
            static final int COMPLETING = 1;
            static final int CLOSED     = 2;
            
            class Worker implements Conduit.Sink<T> {
                @Override
                public boolean drainFromSource(Conduit.StepSource<? extends T> source) throws Exception {
                    K key = null;
                    Item item = null;
                    Callable<U> callable = null;
                    Exception ex = null;
                    
                    for (;;) {
                        try {
                            if (item == null) {
                                sourceLock.lockInterruptibly();
                                try {
                                    T in = source.poll();
                                    if (in == null) {
                                        return true;
                                    }
                                    key = classifier.apply(in);
                                    item = new Item(key, in);
                                    
                                    lock.lockInterruptibly();
                                    try {
                                        if (state == CLOSED) {
                                            return false;
                                        }
                                        while (completionBuffer.size() == bufferLimit) {
                                            completionNotFull.await();
                                            if (state == CLOSED) {
                                                return false;
                                            }
                                        }
                                        completionBuffer.offer(item);
                                        Partition partition = partitionByKey.computeIfAbsent(key, k -> new Partition());
                                        if (partition.permits > 0) {
                                            partition.permits--;
                                        } else {
                                            partition.buffer.offer(item);
                                            key = null;
                                            item = null;
                                            continue;
                                        }
                                        callable = mapper.apply(in, key);
                                    } finally {
                                        lock.unlock();
                                    }
                                } finally {
                                    sourceLock.unlock();
                                }
                            }
                            item.out = callable.call();
                        } catch (Exception e) {
                            ex = e;
                        } finally {
                            if (item != null) {
                                lock.lock();
                                try {
                                    for (;;) {
                                        item.in = null;
                                        if (item == completionBuffer.peek()) {
                                            outputReady.signal();
                                        }
                                        Partition partition = partitionByKey.get(key);
                                        key = null;
                                        item = null;
                                        callable = null;
                                        if (ex == null && (item = partition.buffer.poll()) != null) {
                                            @SuppressWarnings("unchecked")
                                            K k = key = (K) item.out;
                                            try {
                                                callable = mapper.apply(item.in, key);
                                            } catch (Exception e) {
                                                ex = e;
                                                continue;
                                            }
                                        } else if (++partition.permits == permitsPerPartition) {
                                            partitionByKey.remove(key);
                                        }
                                        break;
                                    }
                                } finally {
                                    lock.unlock();
                                }
                            }
                        }
                        
                        if (ex != null) {
                            throw ex;
                        }
                    }
                }
            }
            
            class Sink implements Conduit.Sink<T> {
                @Override
                public boolean drainFromSource(Conduit.StepSource<? extends T> source) throws Exception {
                    try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
                        var tasks = IntStream.range(0, parallelism)
                            .mapToObj(i -> new Worker())
                            .map(sink -> scope.fork(() -> sink.drainFromSource(source)))
                            .toList();
                        scope.join().throwIfFailed();
                        return tasks.stream().anyMatch(StructuredTaskScope.Subtask::get);
                    }
                }
                
                @Override
                public void complete(Throwable error) throws Exception {
                    lock.lockInterruptibly();
                    try {
                        if (state >= COMPLETING) {
                            return;
                        }
                        state = COMPLETING;
                        if ((err = error) != null || completionBuffer.isEmpty()) {
                            outputReady.signalAll();
                        }
                    } finally {
                        lock.unlock();
                    }
                }
            }
            
            class Source implements Conduit.StepSource<U> {
                @Override
                public U poll() throws Exception {
                    for (;;) {
                        lock.lockInterruptibly();
                        try {
                            Item item;
                            for (;;) {
                                item = completionBuffer.peek();
                                if (state >= COMPLETING) {
                                    if (err != null) {
                                        throw new UpstreamException(err);
                                    } else if (state == CLOSED || item == null) {
                                        return null;
                                    }
                                }
                                if (item != null && item.in == null) {
                                    break;
                                }
                                outputReady.await();
                            }
                            completionBuffer.poll();
                            completionNotFull.signal();
                            if (item.out == null) { // Skip nulls
                                continue;
                            }
                            Item nextItem = completionBuffer.peek();
                            if (nextItem != null && nextItem.in == null) {
                                outputReady.signal();
                            }
                            @SuppressWarnings("unchecked")
                            U out = (U) item.out;
                            return out;
                        } finally {
                            lock.unlock();
                        }
                    }
                }
                
                @Override
                public void close() {
                    lock.lock();
                    try {
                        state = CLOSED;
                        completionNotFull.signalAll();
                        outputReady.signalAll();
                    } finally {
                        lock.unlock();
                    }
                }
            }
            
            @Override public Conduit.Sink<T> sink() { return new Sink(); }
            @Override public Conduit.StepSource<U> source() { return new Source(); }
        }
        
        return new MapAsyncPartitioned();
    }
    
    public static <T, U> Conduit.SinkStepSource<T, U> mapAsyncOrdered(int parallelism,
                                                                      int bufferLimit,
                                                                      Function<? super T, ? extends Callable<U>> mapper) {
        if (parallelism < 1 || bufferLimit < 1) {
            throw new IllegalArgumentException("parallelism and bufferLimit must be positive");
        }
        Objects.requireNonNull(mapper);

        class Item {
            // Value of out is initially null
            // When output is computed, output replaces null, and null replaces input
            U out = null;
            T in;
            
            Item(T in) {
                this.in = in;
            }
        }
        
        class MapAsyncOrdered implements Conduit.SinkStepSource<T, U> {
            final ReentrantLock sourceLock = new ReentrantLock();
            final ReentrantLock lock = new ReentrantLock();
            final Condition completionNotFull = lock.newCondition();
            final Condition outputReady = lock.newCondition();
            final Deque<Item> completionBuffer = new ArrayDeque<>(bufferLimit);
            int state = RUNNING;
            Throwable err = null;
            
            static final int RUNNING    = 0;
            static final int COMPLETING = 1;
            static final int CLOSED     = 2;
            
            class Worker implements Conduit.Sink<T> {
                @Override
                public boolean drainFromSource(Conduit.StepSource<? extends T> source) throws Exception {
                    for (;;) {
                        Item item = null;
                        Callable<U> callable;
                        
                        try {
                            sourceLock.lockInterruptibly();
                            try {
                                T in = source.poll();
                                if (in == null) {
                                    return true;
                                }
                                item = new Item(in);
                                
                                lock.lockInterruptibly();
                                try {
                                    if (state == CLOSED) {
                                        return false;
                                    }
                                    while (completionBuffer.size() == bufferLimit) {
                                        completionNotFull.await();
                                        if (state == CLOSED) {
                                            return false;
                                        }
                                    }
                                    completionBuffer.offer(item);
                                    callable = mapper.apply(in);
                                } finally {
                                    lock.unlock();
                                }
                            } finally {
                                sourceLock.unlock();
                            }
                            item.out = callable.call();
                        } finally {
                            if (item != null) {
                                lock.lock();
                                try {
                                    item.in = null;
                                    if (item == completionBuffer.peek()) {
                                        outputReady.signal();
                                    }
                                } finally {
                                    lock.unlock();
                                }
                            }
                        }
                    }
                }
            }
            
            class Sink implements Conduit.Sink<T> {
                @Override
                public boolean drainFromSource(Conduit.StepSource<? extends T> source) throws Exception {
                    try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
                        var tasks = IntStream.range(0, parallelism)
                            .mapToObj(i -> new Worker())
                            .map(sink -> scope.fork(() -> sink.drainFromSource(source)))
                            .toList();
                        scope.join().throwIfFailed();
                        return tasks.stream().anyMatch(StructuredTaskScope.Subtask::get);
                    }
                }
                
                @Override
                public void complete(Throwable error) throws Exception {
                    lock.lockInterruptibly();
                    try {
                        if (state >= COMPLETING) {
                            return;
                        }
                        state = COMPLETING;
                        if ((err = error) != null || completionBuffer.isEmpty()) {
                            outputReady.signalAll();
                        }
                    } finally {
                        lock.unlock();
                    }
                }
            }
            
            class Source implements Conduit.StepSource<U> {
                @Override
                public U poll() throws Exception {
                    for (;;) {
                        lock.lockInterruptibly();
                        try {
                            Item item;
                            for (;;) {
                                item = completionBuffer.peek();
                                if (state >= COMPLETING) {
                                    if (err != null) {
                                        throw new UpstreamException(err);
                                    } else if (state == CLOSED || item == null) {
                                        return null;
                                    }
                                }
                                if (item != null && item.in == null) {
                                    break;
                                }
                                outputReady.await();
                            }
                            completionBuffer.poll();
                            completionNotFull.signal();
                            if (item.out == null) { // Skip nulls
                                continue;
                            }
                            Item nextItem = completionBuffer.peek();
                            if (nextItem != null && nextItem.in == null) {
                                outputReady.signal();
                            }
                            return item.out;
                        } finally {
                            lock.unlock();
                        }
                    }
                }
                
                @Override
                public void close() {
                    lock.lock();
                    try {
                        state = CLOSED;
                        completionNotFull.signalAll();
                        outputReady.signalAll();
                    } finally {
                        lock.unlock();
                    }
                }
            }
            
            @Override public Conduit.Sink<T> sink() { return new Sink(); }
            @Override public Conduit.StepSource<U> source() { return new Source(); }
        }
        
        return new MapAsyncOrdered();
    }
    
    public static <T, U> Conduit.Sink<T> mapAsync(int parallelism,
                                                  Function<? super T, ? extends Callable<U>> mapper,
                                                  Conduit.StepSink<U> downstream) {
        class MapAsync implements Conduit.Sink<T> {
            final ReentrantLock lock = new ReentrantLock();
            
            class Worker implements Conduit.Sink<T> {
                @Override
                public boolean drainFromSource(Conduit.StepSource<? extends T> source) throws Exception {
                    for (;;) {
                        T in;
                        Callable<U> callable;
                        
                        lock.lockInterruptibly();
                        try {
                            if ((in = source.poll()) == null) {
                                return true;
                            }
                            callable = mapper.apply(in);
                        } finally {
                            lock.unlock();
                        }
                        U out = callable.call();
                        if (out != null && !downstream.offer(out)) {
                            return false;
                        }
                    }
                }
            }
            
            @Override
            public boolean drainFromSource(Conduit.StepSource<? extends T> source) throws Exception {
                try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
                    var tasks = IntStream.range(0, parallelism)
                        .mapToObj(i -> new Worker())
                        .map(sink -> scope.fork(() -> sink.drainFromSource(source)))
                        .toList();
                    scope.join().throwIfFailed();
                    return tasks.stream().anyMatch(StructuredTaskScope.Subtask::get);
                }
            }
            
            @Override
            public void complete(Throwable error) throws Exception {
                downstream.complete(error);
            }
        }
        
        return new MapAsync();
    }
    
    // TODO: Profile vs mapAsyncOrdered
    public static <T, U> Conduit.Sink<T> mapBalanceOrdered(Function<? super T, ? extends Callable<U>> mapper,
                                                           List<? extends Conduit.Sink<U>> sinks) {
        class MapBalanceOrdered implements Conduit.Sink<T> {
            final ReentrantLock lock = new ReentrantLock();
            CountDownLatch currLatch = new CountDownLatch(0);
            
            @Override
            public boolean drainFromSource(Conduit.StepSource<? extends T> source) throws InterruptedException, ExecutionException {
                class HelperSource implements Conduit.StepSource<U> {
                    CountDownLatch latch = new CountDownLatch(1);
                    
                    @Override
                    public U poll() throws Exception {
                        latch.countDown();
                        for (;;) {
                            Callable<U> callable;
                            CountDownLatch curr, next = latch = new CountDownLatch(1);
                            try {
                                lock.lockInterruptibly();
                                try {
                                    curr = currLatch;
                                    currLatch = next;
                                    T in = source.poll(); // Thread cannot progress to more work while another is blocked on upstream
                                    if (in == null) {
                                        return null;
                                    }
                                    callable = mapper.apply(in);
                                } finally {
                                    lock.unlock();
                                }
                                U out = callable.call();
                                if (out != null) {
                                    curr.await(); // Thread cannot progress to more work (buffer completions) while previous is blocked on downstream
                                    return out;
                                }
                                next.countDown();
                            } catch (Error | Exception e) {
                                next.countDown();
                                throw e;
                            }
                        }
                    }
                    
                    @Override
                    public void close() {
                        latch.countDown();
                    }
                }
                
                try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
                    var tasks = sinks.stream()
                        .map(sink -> scope.fork(() -> {
                            try (var stepSource = new HelperSource()) {
                                return sink.drainFromSource(stepSource);
                            }
                        }))
                        .toList();
                    scope.join().throwIfFailed();
                    return tasks.stream().anyMatch(StructuredTaskScope.Subtask::get);
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
            public boolean drainFromSource(Conduit.StepSource<? extends T> source) throws InterruptedException, ExecutionException {
                class HelperSource implements Conduit.StepSource<U> {
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
                }
                var stepSource = new HelperSource();
                
                try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
                    var tasks = sinks.stream()
                        .map(sink -> scope.fork(() -> sink.drainFromSource(stepSource)))
                        .toList();
                    scope.join().throwIfFailed();
                    return tasks.stream().anyMatch(StructuredTaskScope.Subtask::get);
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
            public boolean drainFromSource(Conduit.StepSource<? extends T> source) throws InterruptedException, ExecutionException {
                try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
                    var tasks = sinks.stream()
                        .map(sink -> scope.fork(() -> sink.drainFromSource(source)))
                        .toList();
                    scope.join().throwIfFailed();
                    return tasks.stream().anyMatch(StructuredTaskScope.Subtask::get);
                }
            }
            
            @Override
            public void complete(Throwable error) throws InterruptedException, ExecutionException {
                parallelComplete(sinks, error);
            }
        }
        
        return new Balance();
    }
    
    // TODO: stepBroadcast cancels when ANY sink cancels, but broadcast cancels when ALL sinks cancel
    
    public static <T> Conduit.StepSink<T> stepBroadcast(List<? extends Conduit.StepSink<T>> sinks) {
        class StepBroadcast implements Conduit.StepSink<T> {
            @Override
            public boolean offer(T input) throws InterruptedException, ExecutionException {
                try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
                    var tasks = sinks.stream()
                        .map(sink -> scope.fork(() -> sink.offer(input)))
                        .toList();
                    scope.join().throwIfFailed();
                    return tasks.stream().allMatch(StructuredTaskScope.Subtask::get);
                }
            }
            
            @Override
            public void complete(Throwable error) throws InterruptedException, ExecutionException {
                parallelComplete(sinks, error);
            }
        }
        
        return new StepBroadcast();
    }
    
    // Current broadcast:
    //  - drainFromSource = 1 thread PER sink (sink.drainFromSource(helperSource))
    //  - each sink tries to poll the same source, blocks if it gets ahead of other sinks
    // Alternative broadcast:
    //  - drainFromSource = 1 buffer for each sink;
    //                      1 thread for source.drainToSink(stepBroadcast(buffers)) [+ 1 thread PER buffer PER offer]
    //                    + 1 thread PER sink (sink.drainFromSource(buffer))
    //  - source offers to buffers, sinks poll from buffers
    //  - each sink tries to poll its own buffer, blocks if its buffer empties (indicating other sinks' buffers are full, source cannot offer)
    
    // We can generally do arbitrary things between 1 (or N) source(s) and 1 (or M) sink(s).
    //
    // In Sink.drainFromSource(StepSource), we can run a Segue, then StepSource.poll -> Segue.offer | Sink.drainFromSource(Segue)
    //  - Makes a new Sink that chains before the original Sink, like Sink.compose(Segue) but not a StepSink
    //
    // In Source.drainToSink(StepSink),     we can run a Segue, then Source.drainToSink(Segue) | Segue.poll -> StepSink.offer
    //  - Makes a new Source that chains after the original Source, like Source.andThen(Segue) but not a StepSource
    
    public static <T> Conduit.Sink<T> broadcast(List<? extends Conduit.Sink<T>> sinks) {
        class Broadcast implements Conduit.Sink<T> {
            final ReentrantLock lock = new ReentrantLock();
            final Condition ready = lock.newCondition();
            final BitSet bitSet = new BitSet(sinks.size());
            int count = sinks.size();
            boolean done = false;
            T current = null;
            
            @Override
            public boolean drainFromSource(Conduit.StepSource<? extends T> source) throws Exception {
                // Every sink has to call drainFromSource(source) in its own thread
                // The poll() on that source needs to respond differently based on which sink is calling
                //  - If the sink has already consumed this round's element, wait for next round
                //  - First sink in each round polls for the next element
                //  - Last sink in each round wakes the others for the next round
                
                class HelperSource implements Conduit.StepSource<T> {
                    final int pos;
                    
                    HelperSource(int pos) {
                        this.pos = pos;
                    }
                    
                    @Override
                    public T poll() throws Exception {
                        lock.lockInterruptibly();
                        try {
                            if (done) {
                                return null;
                            }
                            if (bitSet.get(pos)) {
                                // Already seen - Wait for next round
                                do {
                                    ready.await();
                                    if (done) {
                                        return null;
                                    }
                                } while (bitSet.get(pos));
                            }
                            bitSet.set(pos);
                            int seen = bitSet.cardinality();
                            if (seen == 1 && (current = source.poll()) == null) {
                                // First-one-in - Polled
                                done = true;
                                ready.signalAll();
                            } else if (seen == count) {
                                // Last-one-in - Wake up others for next round
                                bitSet.clear();
                                ready.signalAll();
                            }
                            return current;
                        } finally {
                            lock.unlock();
                        }
                    }
                    
                    @Override
                    public void close() {
                        lock.lock();
                        try {
                            bitSet.clear(pos);
                            if (bitSet.cardinality() == --count) {
                                bitSet.clear();
                                ready.signalAll();
                            }
                        } finally {
                            lock.unlock();
                        }
                    }
                }
                
                try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
                    var tasks = IntStream.range(0, sinks.size())
                        .mapToObj(i -> scope.fork(() -> {
                            try (var stepSource = new HelperSource(i)) {
                                return sinks.get(i).drainFromSource(stepSource);
                            }
                        }))
                        .toList();
                    scope.join().throwIfFailed();
                    return tasks.stream().allMatch(StructuredTaskScope.Subtask::get);
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
                    List<StructuredTaskScope.Subtask<Boolean>> tasks = new ArrayList<>();
                    try {
                        IntConsumer router = i -> {
                            var sink = sinks.get(i); // Note: Can throw IOOBE
                            tasks.add(scope.fork(() -> sink.offer(input)));
                        };
                        selector.accept(input, router); // Note: User-defined callback can throw exception
                    } catch (Error | Exception e) {
                        scope.shutdown();
                        throw e;
                    }
                    scope.join().throwIfFailed();
                    return tasks.stream().allMatch(StructuredTaskScope.Subtask::get);
                    // TODO: This returns false when ANY sink cancels,
                    //  but the sink that cancelled may not be selected in subsequent offers,
                    //  causing return true again.
                }
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
            public boolean drainToSink(Conduit.StepSink<? super T> sink) throws Exception {
                for (var source : sources) {
                    if (!source.drainToSink(sink)) {
                        return false;
                    }
                }
                return true;
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
            public boolean drainToSink(Conduit.StepSink<? super T> sink) throws InterruptedException, ExecutionException {
                try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
                    var tasks = sources.stream()
                        .map(source -> scope.fork(() -> source.drainToSink(sink)))
                        .toList();
                    scope.join().throwIfFailed();
                    return tasks.stream().allMatch(StructuredTaskScope.Subtask::get);
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
        class StepMergeSorted implements Conduit.StepSource<T> {
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
        
        return new StepMergeSorted();
    }
    
//    public static <T> Conduit.Source<T> mergeSorted(List<? extends Conduit.BaseSource<T>> sources,
//                                                    Comparator<? super T> comparator) {
//        class MergeSorted implements Conduit.Source<T> {
//            final ReentrantLock lock = new ReentrantLock();
//            final Condition ready = lock.newCondition();
//            final PriorityQueue<Indexed<T>> latest = new PriorityQueue<>(sources.size(), Comparator.comparing(i -> i.element, comparator));
//
//            @Override
//            public boolean drainToSink(Conduit.BaseStepSink<? super T> sink) throws Exception {
//                class HelperSink implements Conduit.StepSink<T> {
//                    final int pos;
//
//                    HelperSink(int pos) {
//                        this.pos = pos;
//                    }
//
//                    // TODO: We could probably be speedier by keeping our own buffer for each source, so that the source
//                    //  has more work to do when we wake it up. But if we did that, we would essentially have StepSources
//                    //  as input, and this would reduce to stepMergeSorted. This would also imply that something is
//                    //  externally driving the original Sources to feed the buffers (Source -> StepSink | StepSource ->)
//                    //  -
//                    //  If I made the buffers (Segues) internally, I could actually keep the signature the same and run
//                    //  everything inside drainToSink:
//                    //   - fork(() -> source.drainToSink(buffer))*
//                    //   - fork(() -> stepMergeSorted(buffers, comp).drainToSink(sink))
//
//                    @Override
//                    public boolean offer(T input) throws Exception {
//                        lock.lockInterruptibly();
//                        try {
//                            // If this is the first round, wait for everyone to offer
//                            // If we are the last to offer, poll latest for min, wake up index to offer downstream
//                            latest.offer(new Indexed<>(input, pos));
//                        } finally {
//                            lock.unlock();
//                        }
//                    }
//                }
//
//                try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
//                    var tasks = IntStream.range(0, sources.size())
//                        .mapToObj(i -> scope.fork(() -> sources.get(i).drainToSink(new HelperSink(i))))
//                        .toList();
//                    scope.join().throwIfFailed();
//                    return tasks.stream().allMatch(StructuredTaskScope.Subtask::get);
//                }
//            }
//
//            @Override
//            public void close() throws Exception {
//                parallelClose(sources);
//            }
//        }
//
//        return new MergeSorted();
//    }
    
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
            
            @Override
            public boolean drainToSink(Conduit.StepSink<? super T> sink) throws Exception {
                if (!STATE.compareAndSet(this, NEW, RUNNING)) {
                    throw new IllegalStateException("source already consumed or closed");
                }
                
                abstract class HelperSink<X, Y> implements Conduit.StepSink<X> {
                    @Override
                    public boolean offer(X e) throws Exception {
                        lock.lockInterruptibly();
                        try {
                            if (state >= COMPLETING) {
                                return false;
                            }
                            setLatest1(e);
                            if (latest2() == null) {
                                do {
                                    ready.await();
                                    if (state >= COMPLETING) {
                                        return false;
                                    }
                                } while (latest1() != null);
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
                    }
                    
                    abstract void setLatest1(X x);
                    abstract X latest1();
                    abstract Y latest2();
                }
                
                try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
                    var task1 = scope.fork(() -> source1.drainToSink(new HelperSink<>() {
                        @Override void setLatest1(T1 t) { latest1 = t; }
                        @Override T1 latest1() { return latest1; }
                        @Override T2 latest2() { return latest2; }
                    }));
                    var task2 = scope.fork(() -> source2.drainToSink(new HelperSink<>() {
                        @Override void setLatest1(T2 t) { latest2 = t; }
                        @Override T2 latest1() { return latest2; }
                        @Override T1 latest2() { return latest1; }
                    }));
                    scope.join().throwIfFailed();
                    return task1.get() && task2.get();
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
            
            @Override
            public boolean drainToSink(Conduit.StepSink<? super T> sink) throws InterruptedException, ExecutionException {
                if (!STATE.compareAndSet(this, NEW, RUNNING)) {
                    throw new IllegalStateException("source already consumed or closed");
                }
                
                abstract class HelperSink<X, Y> implements Conduit.StepSink<X> {
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
                                setLatest1(e);
                                if (latest2() == null) {
                                    do {
                                        ready.await();
                                        if (state >= COMPLETING) {
                                            return false;
                                        }
                                    } while (latest2() == null);
                                    return true; // First emission handled by other thread
                                }
                                ready.signal();
                            }
                            setLatest1(e);
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
                    
                    abstract void setLatest1(X x);
                    abstract Y latest2();
                }
                
                try (var scope = new StructuredTaskScope.ShutdownOnFailure()) {
                    var task1 = scope.fork(() -> source1.drainToSink(new HelperSink<>() {
                        @Override void setLatest1(T1 t) { latest1 = t; }
                        @Override T2 latest2() { return latest2; }
                    }));
                    var task2 = scope.fork(() -> source2.drainToSink(new HelperSink<>() {
                        @Override void setLatest1(T2 t) { latest2 = t; }
                        @Override T1 latest2() { return latest1; }
                    }));
                    scope.join().throwIfFailed();
                    return task1.get() && task2.get();
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
    
    // --- Segues ---
    
    public static <T, A> Conduit.Segue<T, A> batch(Supplier<? extends A> batchSupplier,
                                                   BiConsumer<? super A, ? super T> accumulator,
                                                   Function<? super A, Optional<Instant>> deadlineMapper) {
        Objects.requireNonNull(batchSupplier);
        Objects.requireNonNull(accumulator);
        Objects.requireNonNull(deadlineMapper);
        
        class Batch implements TimedSegue.Core<T, A> {
            A batch = null;
            boolean done = false;
            Throwable err = null;
            
            @Override
            public void onInit(TimedSegue.SinkController ctl) { }
            
            @Override
            public void onOffer(TimedSegue.SinkController ctl, T input) {
                A b = batch;
                if (b == null) {
                    b = Objects.requireNonNull(batchSupplier.get());
                }
                accumulator.accept(b, input);
                Instant deadline = deadlineMapper.apply(b).orElse(null);
                batch = b; // No more exception risk -- assign batch
                if (deadline != null) {
                    ctl.latchSourceDeadline(deadline);
                    if (deadline == Instant.MIN) {
                        // Alternative implementations might adjust or reset the buffer instead of blocking
                        ctl.latchSinkDeadline(Instant.MAX);
                    }
                }
            }
            
            @Override
            public void onPoll(TimedSegue.SourceController<A> ctl) throws UpstreamException {
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
                ctl.latchSourceDeadline(Instant.MAX);
                ctl.latchSinkDeadline(Instant.MIN);
            }
            
            @Override
            public void onComplete(TimedSegue.SinkController ctl, Throwable error) {
                err = error;
                done = true;
                ctl.latchSourceDeadline(Instant.MIN);
            }
        }
        
        var core = new Batch();
        return new TimedSegue<>(core);
    }
    
    public static <T> Conduit.Segue<T, T> tokenBucket(Duration tokenInterval,
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
        
        class Throttle implements TimedSegue.Core<T, T> {
            Deque<Weighted<T>> queue = null;
            long tempTokenLimit = 0;
            long tokens = 0;
            long cost = 0;
            Instant lastObservedAccrual;
            boolean done = false;
            Throwable err = null;
            
            @Override
            public void onInit(TimedSegue.SinkController ctl) {
                queue = new ArrayDeque<>();
                lastObservedAccrual = clock().instant();
            }
            
            @Override
            public void onOffer(TimedSegue.SinkController ctl, T input) {
                long elementCost = costMapper.applyAsLong(input);
                if (elementCost < 0) {
                    throw new IllegalStateException("Element cost cannot be negative");
                }
                if ((cost = Math.addExact(cost, elementCost)) >= costLimit) {
                    // Optional blocking for boundedness, here based on cost rather than queue size
                    ctl.latchSinkDeadline(Instant.MAX);
                }
                var w = new Weighted<>(input, elementCost);
                queue.offer(w);
                if (queue.peek() == w) {
                    ctl.latchSourceDeadline(Instant.MIN); // Let source-side do token math
                }
            }
            
            @Override
            public void onPoll(TimedSegue.SourceController<T> ctl) throws UpstreamException {
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
                    ctl.latchSinkDeadline(Instant.MIN);
                    ctl.latchOutput(head.element);
                    head = queue.peek();
                    if (head == null) {
                        if (done) {
                            ctl.latchClose();
                        } else {
                            ctl.latchSourceDeadline(Instant.MAX);
                        }
                        return;
                    } else if (tokens >= head.cost) {
                        ctl.latchSourceDeadline(Instant.MIN);
                        return;
                    }
                    // else tokens < head.cost; Fall-through to scheduling
                }
                // Schedule to wake up when we have enough tokens for next emission
                tempTokenLimit = head.cost;
                long tokensNeeded = head.cost - tokens;
                ctl.latchSourceDeadline(now.plusNanos(tokenIntervalNanos * tokensNeeded - nanosSinceLastAccrual));
            }
            
            @Override
            public void onComplete(TimedSegue.SinkController ctl, Throwable error) {
                err = error;
                done = true;
                if (error != null || queue.isEmpty()) {
                    ctl.latchSourceDeadline(Instant.MIN);
                }
            }
        }
        
        var core = new Throttle();
        return new TimedSegue<>(core);
    }
    
    public static <T> Conduit.Segue<T, T> delay(Function<? super T, Instant> deadlineMapper,
                                                int bufferLimit) {
        Objects.requireNonNull(deadlineMapper);
        if (bufferLimit < 1) {
            throw new IllegalArgumentException("bufferLimit must be positive");
        }
        
        class Delay implements TimedSegue.Core<T, T> {
            PriorityQueue<Expiring<T>> pq = null;
            boolean done = false;
            Throwable err = null;
            
            @Override
            public void onInit(TimedSegue.SinkController ctl) {
                pq = new PriorityQueue<>(bufferLimit);
            }
            
            @Override
            public void onOffer(TimedSegue.SinkController ctl, T input) {
                Instant deadline = Objects.requireNonNull(deadlineMapper.apply(input));
                Expiring<T> e = new Expiring<>(input, deadline);
                pq.offer(e);
                if (pq.peek() == e) {
                    ctl.latchSourceDeadline(deadline);
                }
                if (pq.size() >= bufferLimit) {
                    ctl.latchSinkDeadline(Instant.MAX);
                }
            }
            
            @Override
            public void onPoll(TimedSegue.SourceController<T> ctl) throws UpstreamException {
                if (err != null) {
                    throw new UpstreamException(err);
                }
                Expiring<T> head = pq.poll();
                if (head == null) {
                    ctl.latchClose();
                    return;
                }
                ctl.latchSinkDeadline(Instant.MIN);
                ctl.latchOutput(head.element);
                head = pq.peek();
                if (head != null) {
                    ctl.latchSourceDeadline(head.deadline);
                } else if (!done) {
                    ctl.latchSourceDeadline(Instant.MAX);
                } else {
                    ctl.latchClose();
                }
           }
            
            @Override
            public void onComplete(TimedSegue.SinkController ctl, Throwable error) {
                err = error;
                done = true;
                if (error != null || pq.isEmpty()) {
                    ctl.latchSourceDeadline(Instant.MIN);
                }
            }
        }
       
        var core = new Delay();
        return new TimedSegue<>(core);
    }
    
    public static <T> Conduit.Segue<T, T> keepAlive(Duration timeout,
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
        
        class KeepAlive implements TimedSegue.Core<T, T> {
            Deque<T> queue = null;
            boolean done = false;
            Throwable err = null;
            
            @Override
            public void onInit(TimedSegue.SinkController ctl) {
                queue = new ArrayDeque<>(bufferLimit);
                ctl.latchSourceDeadline(clock().instant().plus(timeout));
            }
            
            @Override
            public void onOffer(TimedSegue.SinkController ctl, T input) {
                queue.offer(input);
                ctl.latchSourceDeadline(Instant.MIN);
                if (queue.size() >= bufferLimit) {
                    ctl.latchSinkDeadline(Instant.MAX);
                }
            }
            
            @Override
            public void onPoll(TimedSegue.SourceController<T> ctl) throws UpstreamException {
                if (err != null) {
                    throw new UpstreamException(err);
                }
                T head = queue.poll();
                if (head != null) {
                    ctl.latchSinkDeadline(Instant.MIN);
                    ctl.latchOutput(head);
                    ctl.latchSourceDeadline((!queue.isEmpty() || done) ? Instant.MIN : clock().instant().plus(timeout));
                } else if (!done) {
                    ctl.latchOutput(extraSupplier.get()); // TODO: May throw
                    ctl.latchSourceDeadline(clock().instant().plus(timeout));
                } else {
                    ctl.latchClose();
                }
            }
            
            @Override
            public void onComplete(TimedSegue.SinkController ctl, Throwable error) {
                err = error;
                done = true;
                ctl.latchSourceDeadline(Instant.MIN);
            }
        }
        
        var core = new KeepAlive();
        return new TimedSegue<>(core);
    }

    public static <T> Conduit.Segue<T, T> extrapolate(T initial,
                                                      Function<? super T, ? extends Iterator<? extends T>> mapper,
                                                      int bufferLimit) {
        Objects.requireNonNull(mapper);
        if (bufferLimit < 1) {
            throw new IllegalArgumentException("bufferLimit must be positive");
        }
        
        class Extrapolate implements TimedSegue.Core<T, T> {
            Deque<T> queue = null;
            Iterator<? extends T> iter = null;
            boolean done = false;
            Throwable err = null;
            
            @Override
            public void onInit(TimedSegue.SinkController ctl) {
                queue = new ArrayDeque<>(bufferLimit);
                if (initial != null) {
                    queue.offer(initial);
                    ctl.latchSourceDeadline(Instant.MIN);
                } else {
                    ctl.latchSourceDeadline(Instant.MAX);
                }
            }
            
            @Override
            public void onOffer(TimedSegue.SinkController ctl, T input) {
                queue.offer(input);
                iter = null;
                ctl.latchSourceDeadline(Instant.MIN);
                if (queue.size() >= bufferLimit) {
                    ctl.latchSinkDeadline(Instant.MAX);
                }
            }
            
            @Override
            public void onPoll(TimedSegue.SourceController<T> ctl) throws UpstreamException {
                if (err != null) {
                    throw new UpstreamException(err);
                }
                T head = queue.poll();
                if (head != null) {
                    ctl.latchSinkDeadline(Instant.MIN);
                    ctl.latchOutput(head);
                    if (queue.peek() != null) {
                        ctl.latchSourceDeadline(Instant.MIN);
                    } else if (!done) {
                        iter = Objects.requireNonNull(mapper.apply(head)); // TODO: May throw
                        ctl.latchSourceDeadline(iter.hasNext() ? Instant.MIN : Instant.MAX);
                    } else {
                        ctl.latchClose();
                    }
                } else if (!done) {
                    ctl.latchOutput(iter.next());
                    ctl.latchSourceDeadline(iter.hasNext() ? Instant.MIN : Instant.MAX);
                } else {
                    ctl.latchClose();
                }
            }
            
            @Override
            public void onComplete(TimedSegue.SinkController ctl, Throwable error) {
                err = error;
                done = true;
                ctl.latchSourceDeadline(Instant.MIN);
            }
        }
        
        var core = new Extrapolate();
        return new TimedSegue<>(core);
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
    
    public static <T> Conduit.Source<T> source(Conduit.Source<T> source) {
        return source;
    }
    
    public static <T> Conduit.Sink<T> sink(Conduit.Sink<T> sink) {
        return sink;
    }
    
    public static <T> Conduit.StepSource<T> stepSource(Conduit.StepSource<T> source) {
        return source;
    }
    
    public static <T> Conduit.StepSink<T> stepSink(Conduit.StepSink<T> sink) {
        return sink;
    }
    
//    public static <T> BiConsumer<Conduit.Source<T>, Conduit.Sink<T>> drainToCompletion(Consumer<Callable<?>> fork) {
//        Objects.requireNonNull(fork);
//        class DrainToCompletion implements BiConsumer<Conduit.Source<T>, Conduit.Sink<T>> {
//            @Override
//            public void accept(Conduit.Source<T> source, Conduit.Sink<T> sink) {
//                source.run(this);
//                fork.accept(() -> {
//                    try (source) {
//                        if (sink instanceof Conduit.StepSink<T> ss) {
//                            source.drainToSink(ss);
//                        } else if (source instanceof Conduit.StepSource<T> ss) {
//                            sink.drainFromSource(ss);
//                        } else {
//                            throw new IllegalArgumentException("source must be StepSource or sink must be StepSink");
//                        }
//                        sink.complete(null);
//                    } catch (Throwable error) {
//                        sink.complete(error);
//                    }
//                    return null;
//                });
//                sink.run(this);
//            }
//        }
//        return new DrainToCompletion();
//    }
    
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
    
    record ClosedSilo<T>(Conduit.Source<? extends T> source, Conduit.Sink<? super T> sink) implements Conduit.Silo {
        @Override
        public void run(StructuredTaskScope<?> scope) {
            source.run(scope);
            scope.fork(() -> {
                try (source) {
                    if (sink instanceof Conduit.StepSink<? super T> ss) {
                        source.drainToSink(ss);
                    } else if (source instanceof Conduit.StepSource<? extends T> ss) {
                        sink.drainFromSource(ss);
                    } else {
                        throw new AssertionError("source must be StepSource or sink must be StepSink");
                    }
                    sink.complete(null);
                }
                catch (Throwable error) {
                    sink.complete(error);
                }
                return null;
            });
            sink.run(scope);
        }
    }
    
    record ChainSilo(Conduit.Silo left, Conduit.Silo right) implements Conduit.Silo {
        @Override
        public void run(StructuredTaskScope<?> scope) {
            left.run(scope);
            right.run(scope);
        }
    }
    
    abstract static class Chain {
        final Conduit.Stage left;
        final Conduit.Stage right;
        
        Chain(Conduit.Stage left, Conduit.Stage right) {
            this.left = left;
            this.right = right;
        }
        
        public void run(StructuredTaskScope<?> scope) {
            left.run(scope);
            right.run(scope);
        }
    }
    
    static sealed class ChainSink<In> extends Chain implements Conduit.Sink<In> {
        final Conduit.Sink<In> sink;
        
        ChainSink(Conduit.Sink<In> left, Conduit.Silo right) {
            super(left, right);
            this.sink = left instanceof ChainSink<In> cs ? cs.sink : left;
        }
        
        @Override
        public boolean drainFromSource(Conduit.StepSource<? extends In> source) throws Exception {
            return sink.drainFromSource(source);
        }
        
        @Override
        public void complete(Throwable error) throws Exception {
            sink.complete(error);
        }
    }
    
    static sealed class ChainSource<Out> extends Chain implements Conduit.Source<Out> {
        final Conduit.Source<Out> source;
        
        ChainSource(Conduit.Silo left, Conduit.Source<Out> right) {
            super(left, right);
            this.source = right instanceof ChainSource<Out> cs ? cs.source : right;
        }
        
        @Override
        public boolean drainToSink(Conduit.StepSink<? super Out> sink) throws Exception {
            return source.drainToSink(sink);
        }
        
        @Override
        public void close() throws Exception {
            source.close();
        }
    }
    
    static final class ChainStepSink<In> extends ChainSink<In> implements Conduit.StepSink<In> {
        ChainStepSink(Conduit.StepSink<In> left, Conduit.Silo right) {
            super(left, right);
        }
        
        @Override
        public boolean offer(In input) throws Exception {
            return ((Conduit.StepSink<In>) sink).offer(input);
        }
    }
    
    static final class ChainStepSource<Out> extends ChainSource<Out> implements Conduit.StepSource<Out> {
        ChainStepSource(Conduit.Silo left, Conduit.StepSource<Out> right) {
            super(left, right);
        }
        
        @Override
        public Out poll() throws Exception {
            return ((Conduit.StepSource<Out>) source).poll();
        }
    }
    
    record ChainSinkSource<In, Out>(Conduit.Sink<In> sink, Conduit.Source<Out> source) implements Conduit.SinkSource<In, Out> { }
    record ChainStepSinkSource<In, Out>(Conduit.StepSink<In> sink, Conduit.Source<Out> source) implements Conduit.StepSinkSource<In, Out> { }
    record ChainSinkStepSource<In, Out>(Conduit.Sink<In> sink, Conduit.StepSource<Out> source) implements Conduit.SinkStepSource<In, Out> { }
    record ChainSegue<In, Out>(Conduit.StepSink<In> sink, Conduit.StepSource<Out> source) implements Conduit.Segue<In, Out> { }
}
