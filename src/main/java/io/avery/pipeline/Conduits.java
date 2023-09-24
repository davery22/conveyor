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
import java.util.stream.Stream;

public class Conduits {
    private Conduits() {} // Utility
    
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
    //  - Correct variance
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
    
    // Talking points:
    //  - why exception cause chaining
    //  - semantics of the boolean return of drainFromSource/ToSink, and why the default impls can be the same
    //  - using buffers between threads (with polling), vs synchronized handoffs (with pushing)
    //  - (X) why 'SubScope' instead of actual STS?
    //  - why run(Executor) instead of run(STS)?
    
    // TODO: Dropping elements...
    //  - eg in zip, can't know we will use either value until we have both
    //  - eg in balance, if sinks have buffers, one sink may eagerly take elements, then fail and drop its buffer
    //  - eg in stepBroadcast with eagerCancel (actually, in that case some sinks process more than others)
    
    // TODO: We should override run() to run any Sources/Sinks passed to us?
    //  Hrm, but think of a buffer that is passed to a Sink (that runs it), but used as a Source separately - would be run twice
    //   - Same with a buffer that is passed to a Source (that runs it), like zip(), but used as a Sink separately
    //   - This would only be safe if buffer.run() doesn't do anything (or is idempotent)
    //  Maybe: Run things when they must be connected to our internals? (Because we can't avoid it then)
    // TODO: I'm pretty sure we should run() Stages that we pass / use in combinators
    //  - The risk is in reusing the same Stages in a separate / external pipeline, thus run()-ing again
    //    - We can eliminate this risk by making run() idempotent - eg use a CAS in the Chain impls
    //  - Example: stepBroadcast(), where we pass in StepSinks obtained from eg segue.andThen(sink)
    //    - We need to run() the StepSinks for values to actually reach the true Sinks
    //    - Is there any benefit to NOT calling run() in the combinator Stage, and only doing it somewhere else?
    
    // We are going to end up with a tree (each node has left/right)
    // Would we like to keep the tree balanced (to minimize stack usage during traversal)?
    //  - This would require something like a persistent RB tree, which feels excessive, and is log(N) insertion
    // Better: Keep the left-side to depth=1, so that we are a linked list, then loop
    //  - No: Maintaining this property means that either 'andThen' or 'compose' will be O(N)
    // What might matter to some is: For a SinkSource, which side runs the Silos in-between?
    //  - If we created it by compose-ing only Source/Sinks, Source-side would run Silos (right-associative)
    //  - If we created it by andThen-ing only Source/Sinks, Sink-side would run Silos (left-associative)
    
    // Current broadcast:
    //  - drainFromSource = 1 thread PER sink (sink.drainFromSource(helperSource))
    //  - each sink tries to poll the same source, blocks if it gets ahead of other sinks
    // Alternative broadcast:
    //  - drainFromSource = 1 buffer for each sink;
    //                      1 thread for source.drainToSink(stepBroadcast(buffers)) [+ 1 thread PER buffer PER offer]
    //                    + 1 thread PER sink (sink.drainFromSource(buffer))
    //  - source offers to buffers, sinks poll from buffers
    //  - each sink tries to poll its own buffer, blocks if its buffer empties (indicating other sinks' buffers are full, source cannot offer)
    //
    // We can generally do arbitrary things between 1 (or N) source(s) and 1 (or M) sink(s).
    //
    // In Sink.drainFromSource(StepSource), we can run a Segue, then StepSource.poll -> Segue.offer | Sink.drainFromSource(Segue)
    //  - Makes a new Sink that chains before the original Sink, like Sink.compose(Segue) but not a StepSink
    //
    // In Source.drainToSink(StepSink),     we can run a Segue, then Source.drainToSink(Segue) | Segue.poll -> StepSink.offer
    //  - Makes a new Source that chains after the original Source, like Source.andThen(Segue) but not a StepSource
    
    // [step]fuse() is a curious case. It accepts a StepSink and returns a StepSink. It can be implemented Sink -> Sink,
    // but fundamentally needs a buffer to handle backpressure when pushing multiple elements.
    
    // A Sink demands a stronger capability from its [Step]Source - the capability to poll()
    // A Source demands a stronger capability from its [Step]Sink - the capability to offer()
    // A builder can sometimes provide a stronger capability by demanding a stronger capability from its inputs
    //
    // The capability to poll()/offer() implies that the [Step]Source/Sink does not have a long-lived asynchronous scope
    // (or that it is managed by the Sink/Source calling it)
    
    // For mapAsyncPartitioned(), making it a Sink allows us to poll a StepSource
    // By asking for a stronger capability from the Source, we avoid needing to provide it ourselves (no internal poll())
    // Which we certainly would have needed to, because the next step is a fundamental Sink (balance() has a long-lived async scope)
    
    // New theory:
    // Some combinators are 'fundamental' Sources/Sinks, due to needing a long-lived asynchronous scope (or other things?)
    // Others can be written as Step-Sources/Sinks, providing stronger capability by demanding stronger capability from inputs.
    // Inputs can be given stronger capability by chaining the Source/Sink to a Segue, yielding a Step-Source/Sink.
    // If the Segue is a handoff, we get something with similar performance to writing a non-step combinator by hand.
    //  - Assuming that
    // If the Segue has a buffer / more substance, that amortizes the cost of context-switching.
    
    // TODO: The lack of these invariants seems problematic for plain Sinks/Source,
    //       as complete/close could be called concurrently with drainFromSource/ToSink, leading to pain?
    //       - If the shared stage is not lock-protected, complete/close may dangerously race with offer/poll
    //       - But, a shared stage that is not lock-protected implies offers/polls may dangerously race with each other
    //  Sink invariant? complete() will be called after all offer()s (or after drainFromSource())
    //   - counter-ex: Many sinks/sources that offer to the same sink
    //   - common case: passing the same buffer for multiple sinks to offer to, possibly with different async bounds between ('custom merge')
    //  Source invariant? close() will be called after all poll()s (or after drainToSink())
    //   - counter-ex: Many sinks/sources that poll from the same source
    //   - haven't really seen an example of this one - currently no reusing source-side of buffers ('custom balance')
    
    // flatMapMerge():
    // Conduits.gather(...).apply(buffer)
    //  - StepSink; gather can 'flatMap' or 'mapMulti' incoming elements
    // Conduits.balance(IntStream.range(0, 10).mapToObj(i -> Conduits.gather(...).apply(buffer)).toList())
    //  - Sink;
    //
    // flatMapMerge():
    //  - Conduits.balance(IntStream.range(0, 10).mapToObj(i -> Conduits.flapMapSink(..).apply(sink)).toList())
    // flatMapConcat():
    //  - Conduits.flatMapSink(..).apply(sink)
    
    // complete(null) MAY throw before completing downstreams
    // complete(ex) MUST complete downstreams before throwing
    // "complete downstreams" may be: calling complete(*) on a known set of downstreams
    // "complete downstreams" may be: setting state that will cause downstreams to halt on next poll() / pass of drainToSink()
    
    // danger of running provided stages in a local scope:
    // can be forced to wait until everything completes, even though outcome of interest is known quickly
    // eg offering to an intermediary sink, interested if a downstream sink cancelled (!offer) or completed exceptionally
    // (X) if the sink is completed we can wake up - nope
    //  - the sink may be completed exceptionally after being completed normally
    //  - if we wake up we leak threads
    
    // multiple sinks that all offer to the same buffer - like a merge, but on the sink-side
    // multiple sources that all poll from the same buffer - like a balance, but on the source-side
    // TODO: Ref-counted Sink and Source (for sharing), plus onComplete / onClose
    
    // Some rules:
    //  - drainFromSource/drainToSink (+poll/offer) must never call close/complete on arguments or 'this'
    //    - for the passed-in sources/sinks, they may be reused externally, eg by concat/spill
    //    - for this source/sink, it may be reused externally, eg if it's a StepSink/Source
    //    - fine to call close/complete on internally-created sources/sinks
    //  - complete(null) may throw for any reason
    //    - it should not return normally if it failed to complete a downstream
    //    - it should not short subsequent complete(x) if it failed to complete a downstream
    //  - complete(x) MUST complete each downstream before returning (normally or abruptly)
    //  - close() MUST close each upstream before returning (normally or abruptly)
    //  - errors from an inner run() should always be captured on outer scope, never inner scope
    //    - if we don't need to control thread leaks / bound lifetime (eg groupBy?), capture+pass outer scope to inner run
    //    - if we do need to control thread leaks / bound lifetime (eg flatMap/adapt*), capture+pass outer scope to inner scope, so we can re-throw errors in outer scope
    //    - maybe we can devise a more efficient STS that only forks one thread (per join) that waits on threads it forks to outer scope
    //      - that would more strongly control thread leaking, but would lose the inner scope name for threads
    //  - after close(), poll() / drainToSink() should return null / false
    //  - after complete(), offer() / drainFromSource() should return false
    //  - no violating structured concurrency - if a method forks threads, it must wait for them to finish
    
    // Some non-rules:
    //  - it's fine to wait for run() to be called inside drainFromSource/drainToSink
    //    - since we should assume things may deadlock otherwise, eg if either side is a buffer that is not being polled/offered
    
    // Why not run(STS)?
    //  - This would allow run() far too permissive access to the STS - close, shutdown, join...
    //  - STS is overly-restrictive
    //    - an ExecutorService should also work
    //    - makes it harder to 'wrap' behavior in sub-scope situations
    //      - need a whole new (well-behaved?) STS when all we should really need to worry about is fork(task)
    
    // On waiting for inner runs to finish - if there is a situation where an inner Source/Sink runs a Silo that other
    // inner Sources/Sinks depend on, we deadlock. To trigger that, the Silo would have to NOT already be running
    // when the Source/Sink is run().
    //
    // This is only an issue for Silos that are internally connected to multiple Sources, ie the
    // Sources are 'polling from the same buffer' (or Sinks are 'offering to the same buffer'), AND not
    // all Sources can be scheduled. We generally shouldn't run into this in flatMap, since a new
    // Source is not created until the last one closes. We may run into this in groupBy, since multiple
    // Sinks are running concurrently. Except, we should not run into this via nesting, where the outer Stage
    // and inner Stage(s) run the shared piece, because the outer should run it first, and not wait
    // until all inners are finished (if it waits at all).
    
    // Does plain Sink need complete()/completeExceptionally()? (Can they be implicit in drainFromSource()?)
    // Does plain Source need close()? (Can it be implicit in drainToSink()?)
    // If plain Sink's drainFromSource() implicitly completes downstreams, so should StepSink...
    // If plain Source's drainToSink() implicitly closes upstreams, so should StepSource...
    //
    // Why not implicit close/complete/exceptionally for plain Sinks/Sources?
    // 1. Forcing Sinks to replicate the try-catch-complete-suppress dance is obnoxious & error-prone
    // 2. Some operators - like plain flatMap - need Sink.drainFromSource to NOT complete the Sink
    // 3. If a sink throws, its fan-out siblings only see interrupt[edException], not original exception
    // 4. Default impls (ie 'do nothing') are already hands-off - only override if we need propagation
    
    
    // Maybe complete(x) should set exception even if complete(null) returned normally?
    
    // We should never complete the sink internally, as it is not safe to assume the sink is ready-to-complete when this
    // source is done with it. For example: concat(). If Source#drainToSink completed the sink, the StepSink passed to
    // concat#drainToSink would be completed by the first Source, and the StepSink would be unable to receive from
    // subsequent Sources.
    
    // TODO: What about adapting the (non-step)Sink-side of a StepSource?
    //  You can do it... but then the StepSource becomes a Source - ie it is destructive
    //  There's no way non-destructive to do this - the method called by run() (drainFromSource) is on the Sink.
    //  But is there a use case?
    
    // TODO: Throws before completing downstream vs throws after
    // If this throws uncaught, we end up completing sink exceptionally, which may not be correct
    // We would ideally throw out of the fork() within Silo#run(), but currently that is impossible...
    // TODO: We should ONLY throw an exception if the SignalSink received one
    //  - In that case, we're just giving it back to Sink#complete(error)
    //  Otherwise, throw the exception in a fork off the original scope, so scope sees it
    
    // Could we just do completion internally, and let external completion no-op?
    //  - This would be unable to handle external causes for completion
    //    - ...Which don't occur / aren't safe for a plain Sink/Source...?
    //      - But these are StepSinks/Sources, darn
    //    - Does this really matter?
    //      - In Silo'd usage, if newSink.complete(ex) throws, we may race to complete the Sink with that exception instead of ex (or a downstream ex, or nothing if recovered downstream)
    //        - TODO: Okay so... always swallow newSink throws then?
    //          - But if newSink has no async bound, newSink throws may be original Sink throws
    //      - In ad-hoc usage, we could:
    //        a) call complete() before calling drainToSink TODO
    //        b) call complete() concurrently with drainToSink TODO
    //        c) call complete() after calling drainToSink
    //  - We generally don't want to complete the sink internally, in case it is shared with others (eg shared buffer)
    //    - TODO: But then, what DOES complete the shared buffer?? Currently (eg balance) we actually would have each sink completing it...
    //      - Can maybe do some shenanigans like balanceMerge(List<StepSinkOperator>) -> StepToSinkOperator
    //        - Pass buffer to result, internally wraps in a StepSink that tracks completions, passes Sinks to balance
    //        - This is pretty limited, eg 'balance' comes from needing a single StepSink in the end, rather than multiple discreet
    //        - This is like a more powerful mapAsync
    //      - balanceMerge(List<StepSourceOperator>) -> StepToSourceOperator
    //        - Pass buffer to result, internally wraps in a StepSource that tracks closes, passes Sources to merge
    //        - Again like mapAsync, but as a Source
    //      - Or expose the 'buffer-wrapper' types directly:
    //        - MergeSink - completes the underlying StepSink upon normal completions reaching count, or upon any abrupt completion
    //        - BalanceSource - closes the underlying StepSource upon closes reaching count
    //        - counting completions/closes would be thrown off if they happen both internally and externally
    // Could we delegate the completion to be handled correctly externally?
    //  - Sink should complete normally or abruptly if it would complete that way as a result of completing newSink normally or abruptly
    //  - If complete(null) reaches the SignalSink, don't throw; as the Sink itself may throw on complete(null), but we won't know til later
    //    - For that matter, maybe just don't throw after source.drainToSink(newSink)?
    //      - No... Due to possible async bounds, completion only makes sense coming from newSink
    //      - And we can't do anything about what the original Sink is on the outside (no wrapping)
    //  - Not completing the Sink when we normally would can mean that it accepts more offers than it normally would
    
    // TODO: default close() / complete() do not prevent further poll() / offer()...
    
    // Note: concat substreams happens naturally in split, since we join after each sink, and sinks do not overlap
    //       merge substreams happens naturally in groupBy, since we join after each sink, and sinks overlap
    
    // --- Sinks ---
    
    public static <T> Conduit.StepSourceOperator<T, T> synchronizeStepSource() {
        class SynchronizedStepSource implements Conduit.StepSource<T> {
            final Conduit.StepSource<T> source;
            final ReentrantLock lock = new ReentrantLock();
            
            SynchronizedStepSource(Conduit.StepSource<T> source) {
                this.source = source;
            }
            
            @Override
            public T poll() throws Exception {
                lock.lockInterruptibly();
                try {
                    return source.poll();
                } finally {
                    lock.unlock();
                }
            }
            
            @Override
            public void close() throws Exception {
                lock.lock();
                try {
                    source.close();
                } finally {
                    lock.unlock();
                }
            }
            
            @Override
            public void run(Executor executor) {
                source.run(executor);
            }
        }
        
        return SynchronizedStepSource::new;
    }
    
    public static <T> Conduit.StepSinkOperator<T, T> synchronizeStepSink() {
        class SynchronizedStepSink implements Conduit.StepSink<T> {
            final Conduit.StepSink<T> sink;
            final ReentrantLock lock = new ReentrantLock();
            
            SynchronizedStepSink(Conduit.StepSink<T> sink) {
                this.sink = sink;
            }
            
            @Override
            public boolean offer(T input) throws Exception {
                lock.lockInterruptibly();
                try {
                    return sink.offer(input);
                } finally {
                    lock.unlock();
                }
            }
            
            @Override
            public void complete() throws Exception {
                lock.lockInterruptibly();
                try {
                    sink.complete();
                } finally {
                    lock.unlock();
                }
            }
            
            @Override
            public void completeExceptionally(Throwable ex) throws Exception {
                Objects.requireNonNull(ex);
                lock.lock();
                try {
                    sink.completeExceptionally(ex);
                } finally {
                    lock.unlock();
                }
            }
            
            @Override
            public void run(Executor executor) {
                sink.run(executor);
            }
        }
        
        return SynchronizedStepSink::new;
    }
    
    public static <T> Conduit.StepSinkOperator<T, T> recoverStep(Function<? super Throwable, ? extends Conduit.Source<? extends T>> mapper,
                                                                 Consumer<? super Throwable> asyncExceptionHandler) {
        class RecoverStep implements Conduit.StepSink<T> {
            final Conduit.StepSink<T> sink;
            
            RecoverStep(Conduit.StepSink<T> sink) {
                this.sink = sink;
            }
            
            @Override
            public boolean offer(T input) throws Exception {
                return sink.offer(input);
            }
            
            @Override
            public void complete() throws Exception {
                sink.complete();
            }
            
            @Override
            public void completeExceptionally(Throwable ex) throws Exception {
                Objects.requireNonNull(ex);
                boolean running = false;
                try {
                    var source = Objects.requireNonNull(mapper.apply(ex));
                    try (var scope = new FailureHandlingScope("recoverStep-completeExceptionally",
                                                              Thread.ofVirtual().name("thread-", 0).factory(),
                                                              asyncExceptionHandler)) {
                        source.run(scopeExecutor(scope));
                        running = true;
                        joinAfterCall(scope, () -> {
                            try (source) {
                                return source.drainToSink(sink);
                            }
                        });
                    }
                    sink.complete(); // Note: This may be the second time calling...
                } catch (Error | Exception e) {
                    try {
                        sink.completeExceptionally(running ? e : ex);
                    } catch (Throwable t) {
                        e.addSuppressed(t);
                    }
                    throw e;
                }
            }
            
            @Override
            public void run(Executor executor) {
                sink.run(executor);
            }
        }
        
        return RecoverStep::new;
    }
    
    public static <T> Conduit.SinkOperator<T, T> recover(Function<? super Throwable, ? extends Conduit.StepSource<? extends T>> mapper,
                                                         Consumer<? super Throwable> asyncExceptionHandler) {
        class Recover implements Conduit.Sink<T> {
            final Conduit.Sink<T> sink;
            
            Recover(Conduit.Sink<T> sink) {
                this.sink = sink;
            }
            
            @Override
            public boolean drainFromSource(Conduit.StepSource<? extends T> source) throws Exception {
                return sink.drainFromSource(source);
            }
            
            @Override
            public void complete() throws Exception {
                sink.complete();
            }
            
            @Override
            public void completeExceptionally(Throwable ex) throws Exception {
                Objects.requireNonNull(ex);
                boolean running = false;
                try {
                    var source = Objects.requireNonNull(mapper.apply(ex));
                    try (var scope = new FailureHandlingScope("recover-completeExceptionally",
                                                              Thread.ofVirtual().name("thread-", 0).factory(),
                                                              asyncExceptionHandler)) {
                        source.run(scopeExecutor(scope));
                        running = true;
                        joinAfterCall(scope, () -> {
                            try (source) {
                                return sink.drainFromSource(source);
                            }
                        });
                    }
                    sink.complete(); // Note: This may be the second time calling...
                } catch (Error | Exception e) {
                    try {
                        sink.completeExceptionally(running ? e : ex);
                    } catch (Throwable t) {
                        e.addSuppressed(t);
                    }
                    throw e;
                }
            }
            
            @Override
            public void run(Executor executor) {
                sink.run(executor);
            }
        }
        
        return Recover::new;
    }
    
    public static <T> Conduit.SourceOperator<T, T> alsoClose(Conduit.Source<?> sourceToClose) {
        Objects.requireNonNull(sourceToClose);
        
        class AlsoClose extends FanInSource<T> {
            final Conduit.Source<T> source;
            
            AlsoClose(Conduit.Source<T> source) {
                this.source = Objects.requireNonNull(source);
            }
            
            @Override
            public boolean drainToSink(Conduit.StepSink<? super T> sink) throws Exception {
                return source.drainToSink(sink);
            }
            
            @Override
            protected Stream<? extends Conduit.Source<?>> sources() {
                return Stream.of(source, sourceToClose);
            }
        }
        
        return AlsoClose::new;
    }
    
    public static <T> Conduit.SinkOperator<T, T> alsoComplete(Conduit.Sink<?> sinkToComplete) {
        Objects.requireNonNull(sinkToComplete);
        
        class AlsoComplete extends FanOutSink<T> {
            final Conduit.Sink<T> sink;
            
            AlsoComplete(Conduit.Sink<T> sink) {
                this.sink = Objects.requireNonNull(sink);
            }
            
            @Override
            public boolean drainFromSource(Conduit.StepSource<? extends T> source) throws Exception {
                return sink.drainFromSource(source);
            }
            
            @Override
            protected Stream<? extends Conduit.Sink<?>> sinks() {
                return Stream.of(sink, sinkToComplete);
            }
        }
        
        return AlsoComplete::new;
    }
    
    public static <T> Conduit.Sink<T> split(Predicate<? super T> predicate,
                                            boolean splitAfter,
                                            boolean eagerCancel,
                                            Consumer<? super Throwable> asyncExceptionHandler,
                                            Function<T, Conduit.StepSink<? super T>> sinkFactory) {
        Objects.requireNonNull(predicate);
        Objects.requireNonNull(asyncExceptionHandler);
        Objects.requireNonNull(sinkFactory);
        
        class Split implements Conduit.Sink<T> {
            @Override
            public boolean drainFromSource(Conduit.StepSource<? extends T> source) throws Exception {
                try (var scope = new FailureHandlingScope("split-drainFromSource",
                                                          Thread.ofVirtual().name("thread-", 0).factory(),
                                                          asyncExceptionHandler)) {
                    return joinAfterCall(scope, () -> {
                        Conduit.StepSink<? super T> subSink = Conduits.stepSink(e -> true);
                        var exec = scopeExecutor(scope);
                        try {
                            boolean drained = true;
                            boolean split = true;
                            for (T val; (val = source.poll()) != null; ) {
                                if ((!splitAfter && predicate.test(val)) || split) {
                                    subSink.complete();
                                    scope.join();
                                    subSink = sinkFactory.apply(val);
                                    subSink.run(exec);
                                }
                                split = splitAfter && predicate.test(val);
                                if (!subSink.offer(val)) {
                                    if (eagerCancel) {
                                        drained = false;
                                        break;
                                    }
                                    split = true;
                                }
                            }
                            subSink.complete();
                            return drained;
                        } catch (Error | Exception e) {
                            try {
                                subSink.completeExceptionally(e);
                            } catch (Throwable t) {
                                e.addSuppressed(t);
                            }
                            throw e;
                        }
                    });
                }
            }
        }
        
        return new Split();
    }
    
    public static <T, K> Conduit.Sink<T> groupBy(Function<T, K> classifier,
                                                 boolean eagerCancel,
                                                 Consumer<? super Throwable> asyncExceptionHandler,
                                                 BiFunction<K, T, Conduit.StepSink<? super T>> sinkFactory) {
        Objects.requireNonNull(classifier);
        Objects.requireNonNull(asyncExceptionHandler);
        Objects.requireNonNull(sinkFactory);
        
        record ScopedSink<T>(SubScope scope, Conduit.StepSink<? super T> sink) { }
        
        class GroupBy implements Conduit.Sink<T> {
            static final Object TOMBSTONE = new Object();
            
            private static Stream<Conduit.Sink<?>> sinks(Map<?, Object> scopedSinkByKey) {
                return scopedSinkByKey.values().stream()
                    .mapMulti((o, downstream) -> {
                        if (o instanceof ScopedSink<?> ss) {
                            downstream.accept(ss.sink);
                        }
                    });
            }
            
            @Override
            public boolean drainFromSource(Conduit.StepSource<? extends T> source) throws Exception {
                Map<K, Object> scopedSinkByKey = new HashMap<>();
                try (var scope = new FailureHandlingScope("groupBy-drainFromSource",
                                                          Thread.ofVirtual().name("thread-", 0).factory(),
                                                          asyncExceptionHandler)) {
                    return joinAfterCall(scope, () -> {
                        try {
                            boolean drained = true;
                            for (T val; (val = source.poll()) != null; ) {
                                K key = classifier.apply(val);
                                var s = scopedSinkByKey.get(key);
                                if (s == TOMBSTONE) {
                                    continue;
                                }
                                @SuppressWarnings("unchecked")
                                var scopedSink = (ScopedSink<T>) s;
                                if (s == null) {
                                    var subSink = Objects.requireNonNull(sinkFactory.apply(key, val));
                                    var subScope = new SubScope(scope);
                                    scopedSink = new ScopedSink<>(subScope, subSink);
                                    scopedSinkByKey.put(key, scopedSink);
                                    // Note that running a sink per key could produce unbounded threads.
                                    // We leave this to the sinkFactory to resolve if necessary, eg by tracking
                                    // incomplete sinks and returning a no-op Sink if maxed (thus dropping elements).
                                    subSink.run(subScope);
                                }
                                if (!scopedSink.sink.offer(val)) {
                                    if (eagerCancel) {
                                        drained = false;
                                        break;
                                    }
                                    scopedSink.sink.complete();
                                    scopedSink.scope.join();
                                    scopedSinkByKey.put(key, TOMBSTONE);
                                }
                            }
                            composedComplete(sinks(scopedSinkByKey));
                            return drained;
                        } catch (Error | Exception e) {
                            try {
                                composedCompleteExceptionally(sinks(scopedSinkByKey), e);
                            } catch (Throwable t) {
                                e.addSuppressed(t);
                            }
                            throw e;
                        }
                    });
                }
            }
        }
        
        return new GroupBy();
    }
    
    public static <T, U> Conduit.StepSinkOperator<T, U> flatMap(Function<? super T, ? extends Conduit.Source<? extends U>> mapper,
                                                                Consumer<? super Throwable> asyncExceptionHandler) {
        class FlatMap extends ProxySink<T> implements Conduit.StepSink<T> {
            final Conduit.StepSink<U> sink;
            boolean draining = true;
            
            FlatMap(Conduit.StepSink<U> sink) {
                this.sink = sink;
            }
            
            @Override
            public boolean offer(T input) throws Exception {
                Objects.requireNonNull(input);
                if (!draining) {
                    return false;
                }
                var subSource = mapper.apply(input);
                if (subSource == null) {
                    return true;
                }
                try (var scope = new FailureHandlingScope("flatMap-offer",
                                                          Thread.ofVirtual().name("thread-", 0).factory(),
                                                          asyncExceptionHandler)) {
                    subSource.run(scopeExecutor(scope));
                    return draining = joinAfterCall(scope, () -> {
                        try (subSource) {
                            return subSource.drainToSink(sink);
                        }
                    });
                }
            }
            
            @Override
            protected Conduit.Sink<?> sink() {
                return sink;
            }
        }
        
        return FlatMap::new;
    }
    
    public static <T, U> Conduit.SinkOperator<T, U> adaptSourceOfSink(Conduit.StepSourceOperator<T, U> sourceMapper,
                                                                      Consumer<? super Throwable> asyncExceptionHandler) {
        class SourceAdaptedSink extends ProxySink<T> {
            final Conduit.Sink<U> sink;
            
            SourceAdaptedSink(Conduit.Sink<U> sink) {
                this.sink = sink;
            }
            
            @Override
            public boolean drainFromSource(Conduit.StepSource<? extends T> source) throws Exception {
                class SignalSource implements Conduit.StepSource<T> {
                    volatile boolean drained = false;
                    
                    @Override
                    public T poll() throws Exception {
                        var result = source.poll();
                        if (result == null) {
                            drained = true;
                        }
                        return result;
                    }
                }
                
                var signalSource = new SignalSource();
                var newSource = sourceMapper.compose(signalSource);
                
                try (var scope = new FailureHandlingScope("adaptSourceOfSink-drainFromSource",
                                                          Thread.ofVirtual().name("thread-", 0).factory(),
                                                          asyncExceptionHandler)) {
                    // Basically, we are trying to make
                    //   `source.andThen(adaptSourceOfSink(sourceMapper).andThen(sink))`
                    // behave as if it was
                    //   `source.andThen(sourceMapper).andThen(sink)`
                    newSource.run(scopeExecutor(scope));
                    joinAfterCall(scope, () -> {
                        try (newSource) {
                            return sink.drainFromSource(newSource);
                        }
                    });
                }
                
                return signalSource.drained;
            }
            
            @Override
            protected Conduit.Sink<?> sink() {
                return sink;
            }
        }
        
        return SourceAdaptedSink::new;
    }
    
    // TODO: Do these compose well? eg, for A:t->a, B:a->b, C:b->c, compare:
    //  A-B
    //    .andThen(adaptSinkOfSource(A.andThen(B)))
    //    .andThen(adaptSinkOfSource(A).andThen(adaptSinkOfSource(B)))
    //    .andThen(adaptSinkOfSource(A)).andThen(adaptSinkOfSource(B))
    //  A-B-C
    //    .andThen(adaptSinkOfSource(A.andThen(B.andThen(C))))
    //    .andThen(adaptSinkOfSource(A.andThen(B).andThen(C)))
    //    .andThen(adaptSinkOfSource(A.andThen(B)).andThen(adaptSinkOfSource(C)))
    //    .andThen(adaptSinkOfSource(A.andThen(B))).andThen(adaptSinkOfSource(C))
    //    .andThen(adaptSinkOfSource(A).andThen(adaptSinkOfSource(B.andThen(C))))
    //    .andThen(adaptSinkOfSource(A).andThen(adaptSinkOfSource(B).andThen(adaptSinkOfSource(C))))
    //    .andThen(adaptSinkOfSource(A).andThen(adaptSinkOfSource(B)).andThen(adaptSinkOfSource(C)))
    //    .andThen(adaptSinkOfSource(A).andThen(adaptSinkOfSource(B))).andThen(adaptSinkOfSource(C))
    //    .andThen(adaptSinkOfSource(A)).andThen(adaptSinkOfSource(B.andThen(C)))
    //    .andThen(adaptSinkOfSource(A)).andThen(adaptSinkOfSource(B).andThen(adaptSinkOfSource(C)))
    //    .andThen(adaptSinkOfSource(A)).andThen(adaptSinkOfSource(B)).andThen(adaptSinkOfSource(C))
    
    public static <T, U> Conduit.SourceOperator<T, U> adaptSinkOfSource(Conduit.StepSinkOperator<T, U> sinkMapper,
                                                                        Consumer<? super Throwable> asyncExceptionHandler) {
        class SinkAdaptedSource extends ProxySource<U> {
            final Conduit.Source<T> source;
            
            SinkAdaptedSource(Conduit.Source<T> source) {
                this.source = source;
            }
            
            @Override
            public boolean drainToSink(Conduit.StepSink<? super U> sink) throws Exception {
                class SignalSink implements Conduit.StepSink<U> {
                    volatile boolean drained = true;
                    
                    @Override
                    public boolean offer(U input) throws Exception {
                        if (!sink.offer(input)) {
                            return drained = false;
                        }
                        return true;
                    }
                }
                
                var signalSink = new SignalSink();
                var newSink = sinkMapper.andThen(signalSink);
                
                try (var scope = new FailureHandlingScope("adaptSinkOfSource-drainToSink",
                                                          Thread.ofVirtual().name("thread-", 0).factory(),
                                                          asyncExceptionHandler)) {
                    // Basically, we are trying to make
                    //   `source.andThen(adaptSinkOfSource(sinkMapper)).andThen(sink)`
                    // behave as if it was
                    //   `source.andThen(sinkMapper.andThen(sink))`
                    //
                    // A potential difference is in exception handling. We process newSink in this thread, and throw if
                    // the 'silo' below throws. This means that exception may be passed to sink.completeExceptionally
                    // sometime after this method exits. If there is an async boundary between newSink and sink, sink
                    // would see a different exception than it would have in the `sinkMapper.andThen(sink)` version. No
                    // heroics here can perfectly match the behavior of that version, so instead of trying and failing
                    // in subtle ways, we go with the more obvious implementation.
                    newSink.run(scopeExecutor(scope));
                    joinAfterCall(scope, () -> {
                        try {
                            source.drainToSink(newSink);
                            newSink.complete();
                            return null;
                        } catch (Error | Exception e) {
                            try {
                                newSink.completeExceptionally(e);
                            } catch (Throwable t) {
                                e.addSuppressed(t);
                            }
                            throw e;
                        }
                    });
                }
                
                return signalSink.drained;
            }
            
            @Override
            protected Conduit.Source<?> source() {
                return source;
            }
        }
        
        return SinkAdaptedSource::new;
    }
    
    public static <In, T, A> Conduit.StepSinkOperator<In, T> gather(Gatherer<In, A, T> gatherer) {
        Objects.requireNonNull(gatherer);
        
        var supplier = gatherer.supplier();
        var integrator = gatherer.integrator();
        var finisher = gatherer.finisher();
        
        class Gather implements Conduit.StepSink<In> {
            final Conduit.StepSink<T> sink;
            final Gatherer.Sink<T> gsink;
            A acc = null;
            int state = NEW;
            
            static final int NEW       = 0;
            static final int RUNNING   = 1;
            static final int COMPLETED = 2;
            static final int CLOSED    = 3;
            
            Gather(Conduit.StepSink<T> sink) {
                this.sink = sink;
                this.gsink = el -> {
                    try {
                        return sink.offer(el);
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
                if (state == NEW) {
                    acc = supplier.get();
                    state = RUNNING;
                }
            }
            
            @Override
            public boolean offer(In input) throws Exception {
                try {
                    if (state >= COMPLETED) {
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
                }
            }
            
            @Override
            public void complete() throws Exception {
                try {
                    if (state >= COMPLETED) {
                        return;
                    }
                    initIfNew();
                    finisher.accept(acc, gsink);
                    sink.complete();
                    state = COMPLETED;
                } catch (WrappingException e) {
                    if (e.getCause() instanceof InterruptedException) {
                        Thread.interrupted();
                    }
                    throw e.getCause();
                }
            }
            
            @Override
            public void completeExceptionally(Throwable ex) throws Exception {
                Objects.requireNonNull(ex);
                if (state == CLOSED) {
                    return;
                }
                state = CLOSED;
                sink.completeExceptionally(ex);
            }
            
            @Override
            public void run(Executor executor) {
                sink.run(executor);
            }
        }
        
        return Gather::new;
    }
    
    public static <T, K, U> Conduit.SinkStepSource<T, U> mapBalancePartitioned(int parallelism,
                                                                               int permitsPerPartition,
                                                                               int bufferLimit,
                                                                               Function<? super T, K> classifier,
                                                                               BiFunction<? super T, ? super K, ? extends Callable<U>> mapper) {
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
        
        if (parallelism < 1 || permitsPerPartition < 1 || bufferLimit < 1) {
            throw new IllegalArgumentException("parallelism, permitsPerPartition, and bufferLimit must be positive");
        }
        Objects.requireNonNull(classifier);
        Objects.requireNonNull(mapper);
        
        if (permitsPerPartition >= parallelism) {
            return mapBalanceOrdered(parallelism, bufferLimit, t -> mapper.apply(t, classifier.apply(t)));
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
        
        class MapBalancePartitioned implements Conduit.SinkStepSource<T, U> {
            final ReentrantLock sourceLock = new ReentrantLock();
            final ReentrantLock lock = new ReentrantLock();
            final Condition completionNotFull = lock.newCondition();
            final Condition outputReady = lock.newCondition();
            final Deque<Item> completionBuffer = new ArrayDeque<>(bufferLimit);
            final Map<K, Partition> partitionByKey = new HashMap<>();
            int state = RUNNING;
            Throwable exception = null;
            
            static final int RUNNING    = 0;
            static final int COMPLETING = 1;
            static final int CLOSED     = 2;
            
            class Worker implements Conduit.Sink<T> {
                @Override
                public boolean drainFromSource(Conduit.StepSource<? extends T> source) throws Exception {
                    K key = null;
                    Item item = null;
                    Callable<U> callable = null;
                    Throwable exception = null;
                    
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
                        } catch (Error | Exception e) {
                            exception = e;
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
                                        if (exception == null && (item = partition.buffer.poll()) != null) {
                                            @SuppressWarnings("unchecked")
                                            K k = key = (K) item.out;
                                            try {
                                                callable = mapper.apply(item.in, key);
                                            } catch (Error | Exception e) {
                                                exception = e;
                                                continue;
                                            }
                                        } else if (++partition.permits == permitsPerPartition) {
                                            partitionByKey.remove(key);
                                        }
                                        throwAsException(exception);
                                        break;
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
                    try (var scope = new StructuredTaskScope.ShutdownOnFailure("mapBalancePartitioned-drainFromSource",
                                                                               Thread.ofVirtual().name("thread-", 0).factory())) {
                        var tasks = IntStream.range(0, parallelism)
                            .mapToObj(i -> new Worker())
                            .map(sink -> scope.fork(() -> sink.drainFromSource(source)))
                            .toList();
                        scope.join().throwIfFailed();
                        return tasks.stream().anyMatch(StructuredTaskScope.Subtask::get);
                    }
                }
                
                @Override
                public void complete() throws Exception {
                    lock.lockInterruptibly();
                    try {
                        if (state >= COMPLETING) {
                            return;
                        }
                        state = COMPLETING;
                        if (completionBuffer.isEmpty()) {
                            outputReady.signalAll();
                        }
                    } finally {
                        lock.unlock();
                    }
                }
                
                @Override
                public void completeExceptionally(Throwable ex) {
                    Objects.requireNonNull(ex);
                    lock.lock();
                    try {
                        if (state == CLOSED) {
                            return;
                        }
                        state = CLOSED;
                        exception = ex;
                        completionNotFull.signalAll();
                        outputReady.signalAll();
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
                                    if (exception != null) {
                                        throw new UpstreamException(exception);
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
                        if (state == CLOSED) {
                            return;
                        }
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
        
        return new MapBalancePartitioned();
    }
    
    public static <T, U> Conduit.SinkStepSource<T, U> mapBalanceOrdered(int parallelism,
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
        
        class MapBalanceOrdered implements Conduit.SinkStepSource<T, U> {
            final ReentrantLock sourceLock = new ReentrantLock();
            final ReentrantLock lock = new ReentrantLock();
            final Condition completionNotFull = lock.newCondition();
            final Condition outputReady = lock.newCondition();
            final Deque<Item> completionBuffer = new ArrayDeque<>(bufferLimit);
            int state = RUNNING;
            Throwable exception = null;
            
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
                    try (var scope = new StructuredTaskScope.ShutdownOnFailure("mapBalanceOrdered-drainFromSource",
                                                                               Thread.ofVirtual().name("thread-", 0).factory())) {
                        var tasks = IntStream.range(0, parallelism)
                            .mapToObj(i -> new Worker())
                            .map(sink -> scope.fork(() -> sink.drainFromSource(source)))
                            .toList();
                        scope.join().throwIfFailed();
                        return tasks.stream().anyMatch(StructuredTaskScope.Subtask::get);
                    }
                }
                
                @Override
                public void complete() throws Exception {
                    lock.lockInterruptibly();
                    try {
                        if (state >= COMPLETING) {
                            return;
                        }
                        state = COMPLETING;
                        if (completionBuffer.isEmpty()) {
                            outputReady.signalAll();
                        }
                    } finally {
                        lock.unlock();
                    }
                }
                
                @Override
                public void completeExceptionally(Throwable ex) {
                    Objects.requireNonNull(ex);
                    lock.lock();
                    try {
                        if (state == CLOSED) {
                            return;
                        }
                        state = CLOSED;
                        exception = ex;
                        completionNotFull.signalAll();
                        outputReady.signalAll();
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
                                    if (exception != null) {
                                        throw new UpstreamException(exception);
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
                        if (state == CLOSED) {
                            return;
                        }
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
        
        return new MapBalanceOrdered();
    }
    
    public static <T, C extends Callable<T>> Conduit.StepToSourceOperator<C, T> balanceMergeSource(int parallelism) {
        if (parallelism < 1) {
            throw new IllegalArgumentException("parallelism must be positive");
        }
        
        return source -> {
            Conduit.StepSource<T> worker = () -> {
                for (C c; (c = source.poll()) != null; ) {
                    var t = c.call();
                    if (t != null) { // Skip nulls
                        return t;
                    }
                }
                return null;
            };
            return merge(IntStream.range(0, parallelism).mapToObj(i -> worker).toList())
                .andThen(alsoClose(source));
        };
    }
    
    public static <T, C extends Callable<T>> Conduit.SinkToStepOperator<C, T> balanceMergeSink(int parallelism) {
        if (parallelism < 1) {
            throw new IllegalArgumentException("parallelism must be positive");
        }
        
        return sink -> {
            Conduit.StepSink<C> worker = c -> {
                var t = c.call();
                return t == null || sink.offer(t); // Skip nulls
            };
            return balance(IntStream.range(0, parallelism).mapToObj(i -> worker).toList())
                .compose(alsoComplete(sink));
        };
    }
    
    public static <T> Conduit.Sink<T> balance(List<? extends Conduit.Sink<T>> sinks) {
        class Balance extends FanOutSink<T> {
            @Override
            public boolean drainFromSource(Conduit.StepSource<? extends T> source) throws InterruptedException, ExecutionException {
                try (var scope = new StructuredTaskScope.ShutdownOnFailure("balance-drainFromSource",
                                                                           Thread.ofVirtual().name("thread-", 0).factory())) {
                    var tasks = sinks.stream()
                        .map(sink -> scope.fork(() -> sink.drainFromSource(source)))
                        .toList();
                    scope.join().throwIfFailed();
                    return tasks.stream().anyMatch(StructuredTaskScope.Subtask::get);
                }
            }
            
            @Override
            protected Stream<? extends Conduit.Sink<?>> sinks() {
                return sinks.stream();
            }
        }
        
        return new Balance();
    }
    
    public static <T> Conduit.StepSink<T> broadcast(List<? extends Conduit.StepSink<T>> sinks) {
        class Broadcast extends FanOutSink<T> implements Conduit.StepSink<T> {
            @Override
            public boolean offer(T input) throws Exception {
                for (var sink : sinks) {
                    if (!sink.offer(input)) {
                        return false;
                    }
                }
                return true;
            }
            
            @Override
            protected Stream<? extends Conduit.Sink<?>> sinks() {
                return sinks.stream();
            }
        }
        
        return new Broadcast();
    }
    
    public static <T, U> Conduit.StepSink<T> route(BiConsumer<T, BiConsumer<Integer, U>> router,
                                                   boolean eagerCancel,
                                                   List<? extends Conduit.StepSink<U>> sinks) {
        BitSet active = new BitSet(sinks.size());
        active.set(0, sinks.size(), true);
        BiConsumer<Integer, U> pusher = (i, u) -> {
            try {
                var sink = sinks.get(i); // Note: Can throw IOOBE
                if (active.get(i) && !sink.offer(u)) {
                    active.clear(i);
                }
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
        
        class Route extends FanOutSink<T> implements Conduit.StepSink<T> {
            boolean done = false;
            
            @Override
            public boolean offer(T input) throws Exception {
                if (done) {
                    return false;
                }
                try {
                    router.accept(input, pusher); // Note: User-defined callback can throw exception
                } catch (WrappingException e) {
                    if (e.getCause() instanceof InterruptedException) {
                        Thread.interrupted();
                    }
                    throw e.getCause();
                } finally {
                    done = eagerCancel ? active.cardinality() < sinks.size() : active.isEmpty();
                }
                return !done;
            }
            
            @Override
            protected Stream<? extends Conduit.Sink<?>> sinks() {
                return sinks.stream();
            }
        }
        
        return new Route();
    }
    
    public static <T> Conduit.StepSink<T> spillStep(List<? extends Conduit.StepSink<T>> sinks) {
        class SpillStep extends FanOutSink<T> implements Conduit.StepSink<T> {
            int i = 0;
            
            @Override
            public boolean offer(T input) throws Exception {
                for (; i < sinks.size(); i++) {
                    if (sinks.get(i).offer(input)) {
                        return true;
                    }
                }
                return false;
            }
            
            @Override
            protected Stream<? extends Conduit.Sink<?>> sinks() {
                return sinks.stream();
            }
        }
        
        return new SpillStep();
    }
    
    public static <T> Conduit.Sink<T> spill(List<? extends Conduit.Sink<T>> sinks) {
        class Spill extends FanOutSink<T> {
            @Override
            public boolean drainFromSource(Conduit.StepSource<? extends T> source) throws Exception {
                for (var sink : sinks) {
                    if (sink.drainFromSource(source)) {
                        return true;
                    }
                }
                return false;
            }
            
            @Override
            protected Stream<? extends Conduit.Sink<?>> sinks() {
                return sinks.stream();
            }
        }
        
        return new Spill();
    }
    
    // --- Sources ---
    
    public static <T> Conduit.StepSource<T> concatStep(List<? extends Conduit.StepSource<T>> sources) {
        class ConcatStep extends FanInSource<T> implements Conduit.StepSource<T> {
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
            protected Stream<? extends Conduit.Source<?>> sources() {
                return sources.stream();
            }
        }
        
        return new ConcatStep();
    }
    
    public static <T> Conduit.Source<T> concat(List<? extends Conduit.Source<T>> sources) {
        class Concat extends FanInSource<T> {
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
            protected Stream<? extends Conduit.Source<?>> sources() {
                return sources.stream();
            }
        }
        
        return new Concat();
    }
    
    public static <T> Conduit.Source<T> merge(List<? extends Conduit.Source<T>> sources) {
        class Merge extends FanInSource<T> {
            @Override
            public boolean drainToSink(Conduit.StepSink<? super T> sink) throws InterruptedException, ExecutionException {
                try (var scope = new StructuredTaskScope.ShutdownOnFailure("merge-drainToSink",
                                                                           Thread.ofVirtual().name("thread-", 0).factory())) {
                    var tasks = sources.stream()
                        .map(source -> scope.fork(() -> source.drainToSink(sink)))
                        .toList();
                    scope.join().throwIfFailed();
                    return tasks.stream().allMatch(StructuredTaskScope.Subtask::get);
                }
            }
            
            @Override
            protected Stream<? extends Conduit.Source<?>> sources() {
                return sources.stream();
            }
        }
        
        return new Merge();
    }
    
    public static <T> Conduit.StepSource<T> mergeSorted(List<? extends Conduit.StepSource<T>> sources,
                                                        Comparator<? super T> comparator) {
        class MergeSorted extends FanInSource<T> implements Conduit.StepSource<T> {
            final PriorityQueue<Indexed<T>> latest = new PriorityQueue<>(sources.size(), Comparator.comparing(i -> i.element, comparator));
            int lastIndex = NEW;
            
            static final int NEW       = -1;
            static final int COMPLETED = -2;
            
            @Override
            public T poll() throws Exception {
                if (lastIndex <= NEW) {
                    if (lastIndex == COMPLETED) {
                        return null;
                    }
                    // First poll - poll all sources to bootstrap the queue
                    for (int i = 0; i < sources.size(); i++) {
                        var t = sources.get(i).poll();
                        if (t != null) {
                            latest.offer(new Indexed<>(t, i));
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
                lastIndex = COMPLETED;
                return null;
            }
            
            @Override
            protected Stream<? extends Conduit.Source<?>> sources() {
                return sources.stream();
            }
        }
        
        return new MergeSorted();
    }
    
    public static <T1, T2, T> Conduit.StepSource<T> zip(Conduit.StepSource<T1> source1,
                                                        Conduit.StepSource<T2> source2,
                                                        BiFunction<? super T1, ? super T2, T> combiner) {
        Objects.requireNonNull(source1);
        Objects.requireNonNull(source2);
        Objects.requireNonNull(combiner);
        
        class Zip extends FanInSource<T> implements Conduit.StepSource<T> {
            boolean done = false;
            
            @Override
            public T poll() throws Exception {
                if (done) {
                    return null;
                }
                T1 e1;
                T2 e2;
                if ((e1 = source1.poll()) == null || (e2 = source2.poll()) == null) {
                    done = true;
                    return null;
                }
                return Objects.requireNonNull(combiner.apply(e1, e2));
            }
            
            @Override
            protected Stream<? extends Conduit.Source<?>> sources() {
                return Stream.of(source1, source2);
            }
        }
        
        return new Zip();
    }
    
    public static <T1, T2, T> Conduit.Source<T> zipLatest(Conduit.Source<T1> source1,
                                                          Conduit.Source<T2> source2,
                                                          BiFunction<? super T1, ? super T2, T> combiner) {
        Objects.requireNonNull(source1);
        Objects.requireNonNull(source2);
        Objects.requireNonNull(combiner);
        
        class ZipLatest extends FanInSource<T> {
            T1 latest1 = null;
            T2 latest2 = null;
            
            @Override
            public boolean drainToSink(Conduit.StepSink<? super T> sink) throws InterruptedException, ExecutionException {
                ReentrantLock lock = new ReentrantLock();
                Condition ready = lock.newCondition();
                
                abstract class HelperSink<X, Y> implements Conduit.StepSink<X> {
                    @Override
                    public boolean offer(X e) throws Exception {
                        Objects.requireNonNull(e);
                        lock.lockInterruptibly();
                        try {
                            if (setLatest1(e) == null) {
                                if (getLatest2() == null) {
                                    // Wait until we have the first element from both sources
                                    do {
                                        ready.await();
                                    } while (getLatest2() == null);
                                    return true; // First emission handled by other thread
                                }
                                ready.signal();
                            }
                            T t = combiner.apply(latest1, latest2);
                            return sink.offer(t);
                        } finally {
                            lock.unlock();
                        }
                    }
                    
                    abstract X setLatest1(X x);
                    abstract Y getLatest2();
                }
                class HelperSink1 extends HelperSink<T1, T2> {
                    @Override T1 setLatest1(T1 t) { var r = latest1; latest1 = t; return r; }
                    @Override T2 getLatest2() { return latest2; }
                }
                class HelperSink2 extends HelperSink<T2, T1> {
                    @Override T2 setLatest1(T2 t) { var r = latest2; latest2 = t; return r; }
                    @Override T1 getLatest2() { return latest1; }
                }
                
                try (var scope = new StructuredTaskScope.ShutdownOnFailure("zipLatest-drainToSink",
                                                                           Thread.ofVirtual().name("thread-", 0).factory())) {
                    var task1 = scope.fork(() -> source1.drainToSink(new HelperSink1()));
                    var task2 = scope.fork(() -> source2.drainToSink(new HelperSink2()));
                    scope.join().throwIfFailed();
                    return task1.get() && task2.get();
                }
            }
            
            @Override
            protected Stream<? extends Conduit.Source<?>> sources() {
                return Stream.of(source1, source2);
            }
        }
        
        return new ZipLatest();
    }
    
    // --- Segues ---
    
    public static <T, A> Conduit.StepSegue<T, A> batch(Supplier<? extends A> batchSupplier,
                                                       BiConsumer<? super A, ? super T> accumulator,
                                                       Function<? super A, Optional<Instant>> deadlineMapper) {
        Objects.requireNonNull(batchSupplier);
        Objects.requireNonNull(accumulator);
        Objects.requireNonNull(deadlineMapper);
        
        class Batch implements TimedSegue.Core<T, A> {
            A batch = null;
            boolean done = false;
            
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
            public void onPoll(TimedSegue.SourceController<A> ctl) {
                if (done) {
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
            public void onComplete(TimedSegue.SinkController ctl) {
                done = true;
                ctl.latchSourceDeadline(Instant.MIN);
            }
        }
        
        var core = new Batch();
        return new TimedSegue<>(core);
    }
    
    public static <T> Conduit.StepSegue<T, T> tokenBucket(Duration tokenInterval,
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
            public void onPoll(TimedSegue.SourceController<T> ctl) {
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
                    ctl.latchSinkDeadline(Instant.MIN); // TODO: Might not be below costLimit yet
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
            public void onComplete(TimedSegue.SinkController ctl) {
                done = true;
                if (queue.isEmpty()) {
                    ctl.latchSourceDeadline(Instant.MIN);
                }
            }
        }
        
        var core = new Throttle();
        return new TimedSegue<>(core);
    }
    
    public static <T> Conduit.StepSegue<T, T> delay(Function<? super T, Instant> deadlineMapper,
                                                    int bufferLimit) {
        Objects.requireNonNull(deadlineMapper);
        if (bufferLimit < 1) {
            throw new IllegalArgumentException("bufferLimit must be positive");
        }
        
        class Delay implements TimedSegue.Core<T, T> {
            PriorityQueue<Expiring<T>> pq = null;
            boolean done = false;
            
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
            public void onPoll(TimedSegue.SourceController<T> ctl) {
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
            public void onComplete(TimedSegue.SinkController ctl) {
                done = true;
                if (pq.isEmpty()) {
                    ctl.latchSourceDeadline(Instant.MIN);
                }
            }
        }
       
        var core = new Delay();
        return new TimedSegue<>(core);
    }
    
    public static <T> Conduit.StepSegue<T, T> keepAlive(Duration timeout,
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
            public void onPoll(TimedSegue.SourceController<T> ctl) {
                T head = queue.poll();
                if (head != null) {
                    ctl.latchSinkDeadline(Instant.MIN);
                    ctl.latchOutput(head);
                    ctl.latchSourceDeadline((!queue.isEmpty() || done) ? Instant.MIN : clock().instant().plus(timeout));
                } else if (!done) {
                    ctl.latchOutput(extraSupplier.get());
                    ctl.latchSourceDeadline(clock().instant().plus(timeout));
                } else {
                    ctl.latchClose();
                }
            }
            
            @Override
            public void onComplete(TimedSegue.SinkController ctl) {
                done = true;
                ctl.latchSourceDeadline(Instant.MIN);
            }
        }
        
        var core = new KeepAlive();
        return new TimedSegue<>(core);
    }

    public static <T> Conduit.StepSegue<T, T> extrapolate(T initial,
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
            public void onPoll(TimedSegue.SourceController<T> ctl) {
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
            public void onComplete(TimedSegue.SinkController ctl) {
                done = true;
                ctl.latchSourceDeadline(Instant.MIN);
            }
        }
        
        var core = new Extrapolate();
        return new TimedSegue<>(core);
    }
    
    public static <T> Conduit.Source<T> source(Stream<T> stream) {
        class StreamSource implements Conduit.Source<T> {
            boolean called = false;
            
            @Override
            public boolean drainToSink(Conduit.StepSink<? super T> sink) throws Exception {
                if (called) {
                    return true;
                }
                called = true;
                
                try {
                    return stream.allMatch(el -> {
                        Objects.requireNonNull(el);
                        try {
                            return sink.offer(el);
                        } catch (Error | RuntimeException ex) {
                            throw ex;
                        } catch (Exception ex) {
                            if (ex instanceof InterruptedException) {
                                Thread.currentThread().interrupt();
                            }
                            throw new WrappingException(ex);
                        }
                    });
                } catch (WrappingException e) {
                    if (e.getCause() instanceof InterruptedException) {
                        Thread.interrupted();
                    }
                    throw e.getCause();
                }
            }
            
            @Override
            public void close() {
                stream.close();
            }
        }
        
        return new StreamSource();
    }
    
    public static <T> Conduit.StepSource<T> stepSource(Iterator<T> iterator) {
        return stepSource(iterator, () -> { });
    }
    
    public static <T> Conduit.StepSource<T> stepSource(Iterator<T> iterator, AutoCloseable onClose) {
        class IteratorSource implements Conduit.StepSource<T> {
            @Override
            public T poll() {
                return iterator.hasNext() ? Objects.requireNonNull(iterator.next()) : null;
            }
            
            @Override
            public void close() throws Exception {
                onClose.close();
            }
        }
        
        return new IteratorSource();
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
    
    public static Executor scopeExecutor(StructuredTaskScope<?> scope) {
        return runnable -> scope.fork(Executors.callable(runnable, null));
    }
    
    static void composedClose(Stream<? extends Conduit.Source<?>> sources) throws Exception {
        Throwable[] ex = { null };
        sources.sequential().forEach(source -> {
            try {
                source.close();
            } catch (Error | Exception e) {
                if (e instanceof InterruptedException) {
                    // Only fair to reinstate interrupt for later sources
                    Thread.currentThread().interrupt();
                }
                if (ex[0] == null) {
                    ex[0] = e;
                } else {
                    ex[0].addSuppressed(e);
                }
            }
        });
        throwAsException(ex[0]);
    }
    
    static void composedComplete(Stream<? extends Conduit.Sink<?>> sinks) throws Exception {
        Throwable[] ex = { null };
        sinks.sequential().forEach(sink -> {
            try {
                sink.complete();
            } catch (Error | Exception e) {
                if (e instanceof InterruptedException) {
                    // Only fair to reinstate interrupt for later sinks
                    Thread.currentThread().interrupt();
                }
                if (ex[0] == null) {
                    ex[0] = e;
                } else {
                    ex[0].addSuppressed(e);
                }
            }
        });
        throwAsException(ex[0]);
    }
    
    static void composedCompleteExceptionally(Stream<? extends Conduit.Sink<?>> sinks, Throwable exception) throws Exception {
        Throwable[] ex = { null };
        sinks.sequential().forEach(sink -> {
            try {
                sink.completeExceptionally(exception);
            } catch (Error | Exception e) {
                if (e instanceof InterruptedException) {
                    // Only fair to reinstate interrupt for later sinks
                    Thread.currentThread().interrupt();
                }
                if (ex[0] == null) {
                    ex[0] = e;
                } else {
                    ex[0].addSuppressed(e);
                }
            }
        });
        throwAsException(ex[0]);
    }
    
    private static void throwAsException(Throwable ex) throws Exception {
        switch (ex) {
            case null -> { }
            case InterruptedException e -> { Thread.interrupted(); throw e; }
            case Exception e -> throw e;
            case Error e -> throw e;
            case Throwable e -> throw new IllegalArgumentException("Unexpected Throwable", e);
        }
    }
    
    private static <T> T joinAfterCall(StructuredTaskScope<?> scope, Callable<T> callable) throws Exception {
        Callable<T> wrapped;
        try {
            T t = callable.call();
            wrapped = () -> t;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            wrapped = () -> { Thread.interrupted(); throw e; };
        } catch (Error | Exception e) {
            wrapped = () -> { throw e; };
        }
        try {
            scope.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return wrapped.call();
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
    
    private static class WrappingException extends RuntimeException {
        WrappingException(Exception e) {
            super(e);
        }
        
        @Override
        public synchronized Exception getCause() {
            return (Exception) super.getCause();
        }
    }
    
    // Used by groupBy (when eagerCancel=false) to wait for run() tasks to finish after completing a Sink
    private static class SubScope implements Executor {
        final StructuredTaskScope<?> scope;
        final Phaser phaser;
        
        SubScope(StructuredTaskScope<?> scope) {
            this.scope = scope;
            this.phaser = new Phaser(1);
        }
        
        @Override
        public void execute(Runnable task) {
            if (phaser.register() < 0) {
                // In case someone holds on to the Executor reference when they shouldn't
                throw new IllegalStateException("SubScope is closed");
            }
            try {
                scope.fork(() -> {
                    try {
                        task.run();
                        return null;
                    } finally {
                        phaser.arriveAndDeregister();
                    }
                });
            } catch (Error | RuntimeException e) {
                // Task was not forked
                phaser.arriveAndDeregister();
                throw e;
            }
        }
        
        public void join() throws InterruptedException {
            int phase = phaser.arrive();
            phaser.awaitAdvanceInterruptibly(phase);
        }
    }
    
    static final class ClosedSilo<T> implements Conduit.Silo {
        final Conduit.Source<? extends T> source;
        final Conduit.Sink<? super T> sink;
        boolean ran = false;
        
        static final VarHandle RAN;
        static {
            try {
                RAN = MethodHandles.lookup().findVarHandle(ClosedSilo.class, "ran", boolean.class);
            } catch (ReflectiveOperationException e) {
                throw new ExceptionInInitializerError(e);
            }
        }
        
        ClosedSilo(Conduit.Source<? extends T> source, Conduit.Sink<? super T> sink) {
            this.source = source;
            this.sink = sink;
        }
        
        @Override
        public void run(Executor executor) {
            source.run(executor);
            sink.run(executor);
            executor.execute(() -> {
                if (!RAN.compareAndSet(this, false, true)) {
                    return;
                }
                try (source) {
                    if (sink instanceof Conduit.StepSink<? super T> ss) {
                        source.drainToSink(ss);
                    } else if (source instanceof Conduit.StepSource<? extends T> ss) {
                        sink.drainFromSource(ss);
                    }
                    sink.complete();
                } catch (Throwable ex) {
                    try {
                        sink.completeExceptionally(ex);
                    } catch (Throwable t) {
                        ex.addSuppressed(t);
                    }
                    throw new CompletionException(ex);
                }
            });
        }
    }
    
    private abstract static class Chain {
        final Conduit.Stage left;
        final Conduit.Stage right;
        
        Chain(Conduit.Stage left, Conduit.Stage right) {
            this.left = left;
            this.right = right;
        }
        
        public void run(Executor executor) {
            left.run(executor);
            right.run(executor);
        }
    }
    
    static final class ChainSilo extends Chain implements Conduit.Silo {
        ChainSilo(Conduit.Silo left, Conduit.Silo right) {
            super(left, right);
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
        public void complete() throws Exception {
            sink.complete();
        }
        
        @Override
        public void completeExceptionally(Throwable ex) throws Exception {
            sink.completeExceptionally(ex);
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
    
    record ChainSegue<In, Out>(Conduit.Sink<In> sink, Conduit.Source<Out> source) implements Conduit.Segue<In, Out> { }
    record ChainStepSinkSource<In, Out>(Conduit.StepSink<In> sink, Conduit.Source<Out> source) implements Conduit.StepSinkSource<In, Out> { }
    record ChainSinkStepSource<In, Out>(Conduit.Sink<In> sink, Conduit.StepSource<Out> source) implements Conduit.SinkStepSource<In, Out> { }
    record ChainStepSegue<In, Out>(Conduit.StepSink<In> sink, Conduit.StepSource<Out> source) implements Conduit.StepSegue<In, Out> { }
}
