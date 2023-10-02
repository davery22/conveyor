
1. Designing the #batch operator
   - Call-out: A Motivating Example (https://blog.colinbreck.com/akka-streams-a-motivating-example/)
   - Note: Duration vs deadlines, using Instant for timing
   - Note: Alternatives, synchronous timing
   - Note: Signaling completion
   - Note: Relation to Collector
   - Note: Throwing exceptions, locks and scopes
2. Initial operator impls
   - Note: Birth of Controller interfaces (https://github.com/davery22/conveyor/commit/36e6be25d78bce40865ee2e3acd046bd5d54cf1f#diff-359c76bb1e4be9b1f9e3363be3bd9eb8d2afae41643d4ef23bd75139b82c44a2R507)
   - Note: Designing the #throttle operator
3. Birth of TimedSegue ('Tunnel') (https://github.com/davery22/conveyor/commit/a5c615378e6fd18fa25d1bb7e6004c8b047f66dd#diff-359c76bb1e4be9b1f9e3363be3bd9eb8d2afae41643d4ef23bd75139b82c44a2R522)
   - Note: offer/complete/poll/close
   - Note: Retaining Controller interfaces, composition
4. Birth of StepSource and StepSink for #zip (https://github.com/davery22/conveyor/commit/f6e66af82bac63cb90166051255d0ea76321ac33#diff-10d25d78f420787c7dea4bec78f4ec80b6a47ce9f8ec754a25762c7ae5179089R245)
   - Note: First hints of plain Source (#zipLatest)
5. Incorporation of Gatherers (https://github.com/davery22/conveyor/commit/227285e05762a0e81e60265e96c276254cb083e1#diff-10d25d78f420787c7dea4bec78f4ec80b6a47ce9f8ec754a25762c7ae5179089R250)
   - Note: Wrapping check exceptions for outer catch + re-throw
6. Birth of #completeAbruptly (https://github.com/davery22/conveyor/commit/fb3da7576feabb3f8e2eb64b12e0a387ce4868c1)
   - Note: Originally unified with #complete
7. Birth of plain Source ('Push') for #zipLatest (https://github.com/davery22/conveyor/commit/8bb757a544c32d7874d2551cc2df6f80aecaa88f#diff-2965734b40cf589e4198f2fcd65abfad3e84f85fa17bf29220f5cc86ec5b0dfbR11)
8. Full realization of step-vs-plain Sink/Source (https://github.com/davery22/conveyor/commit/265265e30ea6f6335e258980c25d87df13515d7a#diff-9cd061a2b6024c4ff669be89332f6869a2a60a2496c643c035fd07a43eaffcdcR6)
   - Note: Source.drainToSink(StepSink), Sink.drainFromSource(StepSource)
   - Note: Birth of more fan-in/fan-out (#merge, #balance, #broadcast)
   - Note: Birth of chaining ('Pipeline')
9. Birth of chaining, run(), 'Pipeline' (https://github.com/davery22/conveyor/commit/dbef666b4f1e91b033bab25654330457e664d813#diff-cbb9dfeb0617a41e8f6ca43cee0ba2e5ef0f98191320b8dfcd8f9ae38bbb5f68R20)
   - Note: First hints of Silos
10. The #mapBalance saga
    - Note: Ordered fan-out Sink (https://github.com/davery22/conveyor/commit/6a55e31a78a887f8330c7442768b761ca85ed99a#diff-16763a30542f42f326a66ee23751b7a00f2b19655d7b47506e4d8a721a96d67eR334)
    - Note: #mapBalancePartitioned as a synchronous Sink (https://github.com/davery22/conveyor/commit/da0a09cca1155e3725545027776d135495ad30a4#diff-16763a30542f42f326a66ee23751b7a00f2b19655d7b47506e4d8a721a96d67eR450)
    - Note: 'Resume-able after exception' requirement (https://github.com/davery22/conveyor/commit/59fe5e408129d753c01e1859f69aeb7dd82e310e)
    - Note: #mapBalancePartitioned as a Sink with 2 boundaries inside (https://github.com/davery22/conveyor/commit/7d2b673899a37b96e58ed7035d50d97505a954ea#diff-16763a30542f42f326a66ee23751b7a00f2b19655d7b47506e4d8a721a96d67eR558)
    - Note: #mapBalancePartitioned as a Segue (https://github.com/davery22/conveyor/commit/3aa2d76fc29bf4a3ddcebd4d6624d6767a141a79)
    - Note: #mapBalancePartitioend as a StepSource(?!) with a Sink inside (https://github.com/davery22/conveyor/commit/76c79d887c6815a70f7289029ce0344e1b8b6755)
    - Note: Birth of Segues (https://github.com/davery22/conveyor/commit/d39fd0be78e3dc397607cc38c53b4de38999699d)
    - Note: #balanceMergeSource and #balanceMergeSink (https://github.com/davery22/conveyor/commit/8558a5d55bb4d2df913c97ca75ff94e7bbea821a)
11. Return boolean from drain methods (https://github.com/davery22/conveyor/commit/9829484a5f1586ad427698e0b0aa8ce3555b9f52)
    - Note: Use in #concat
    - Note: Conformance of matching default impls in StepSource/Sink
    - Note: Reminds implementors to be cancellation-aware
12. Merging of Pipelines (chaining, running) into baseline interfaces
    - Note: Avoids duplicating the set of interfaces
    - Note: Implication that safe usage requires calling #run
    - Note: Implication that proxy operators need to run delegates
    - Note: Effectively-final default chaining methods
13. Assorted (https://github.com/davery22/conveyor/commit/42f395ffda09f235069be74057dcbea903a842df)
    - Note: First consideration of #complete returning boolean (resolved later in #adaptSinkOfSource)
    - Note: First hints of equivalence between plain operators and #rendezvous step operators (#mergeSorted)
14. Completion of TimedSegue interface with duo-deadlines (https://github.com/davery22/conveyor/commit/23442deda30aeb60d055ff13832ad2dbe8b4f701)
15. Spec of drain methods boolean return (https://github.com/davery22/conveyor/commit/7d2b673899a37b96e58ed7035d50d97505a954ea#diff-16763a30542f42f326a66ee23751b7a00f2b19655d7b47506e4d8a721a96d67eR1164)
16. Birth of Segues (from #mapBalance saga) (https://github.com/davery22/conveyor/commit/d39fd0be78e3dc397607cc38c53b4de38999699d)
    - Note: Alongside birth of Silos (https://github.com/davery22/conveyor/commit/d39fd0be78e3dc397607cc38c53b4de38999699d#diff-db26c1af2b0dda12e76cd4a4cfebf9f847d8d75f7455438a04d5599b8612f88fR90)
17. Idempotent #run (https://github.com/davery22/conveyor/commit/a383b388a5f399efeac7ed333571d13a7fafd744)
18. Removal of short-lived threading (https://github.com/davery22/conveyor/commit/47af88f3e214e0074ce4b38eb77f20ae33cc77b8)
    - 100x to loop
19. Birth of #groupBy (https://github.com/davery22/conveyor/commit/ca2b363aa41979f495f165d47f6e6f072d84acb3)
    - Note: Experiment with capturing Executor (https://github.com/davery22/conveyor/commit/882c021167f21f6e3373d3736304ace7f8c0a97f)
      - End experiment, 'asyncExceptionHandler' (https://github.com/davery22/conveyor/commit/e7d8550fe9de09312ec0792bcffbd72f51dee202)
    - Note: Violation of structured concurrency
20. Birth of Operators (https://github.com/davery22/conveyor/commit/a55ce132dfca46759156409556b5ef6149cf8919)
    - Note: Chain continuity
    - Note: Overloads for automatic shape conversion
    - Note: Modify chaining into fan-in/out
21. Revisit #flatMap and #groupBy to respect structured concurrency (https://github.com/davery22/conveyor/commit/a92be21f782c8f740e95843e8b57a1b43da45784)
    - Note: Wait on Silos when subSink/Source completes/closes
22. Restructure Operators to indicate dataflow-order (https://github.com/davery22/conveyor/commit/26f72fff5b9ff848648c4bc5348e0fe08d00e8a6)
23. Throw from #completeAbruptly; finalize exception path/suppression (https://github.com/davery22/conveyor/commit/7696f1ecbcd616f1fb208977c788c247b5d262d8)
24. FailureHandlingScope (https://github.com/davery22/conveyor/commit/66d8086dbc14303f996909c28eb24f6196a5f1ec)
    - Note: The STS story - Inappropriateness of ShutdownOnFailure/Success
25. Birth of interrupt status propagation (https://github.com/davery22/conveyor/commit/d28d3919f2995426707ff320189f55a001824820)
    - Refinement: Applies to #complete (even suppressed) but not #close (https://github.com/davery22/conveyor/commit/4c5d8d0e0ce274e29491a5ab5263fe60963db5e5)
26. The fan-out-merge / fan-in-balance saga - lift close/complete responsibility
    - Birth of #alsoClose and #alsoComplete for (https://github.com/davery22/conveyor/commit/848bde23c0fa5d59293e84839aabec523d923608)
    - #close/#complete-stripping strategy (https://github.com/davery22/conveyor/commit/61ec98842619947f5980b11b9f32c00f418edce0)
27. Removal of clever from #adaptSinkOfSource (https://github.com/davery22/conveyor/commit/6ae99063c0a511a4538dbdde92e510e3d354cfda)
28. Birth of #recover (https://github.com/davery22/conveyor/commit/e3cabb7d1e4f240c8e2c69394393c042049a9251)
29. CAS inside execute in #run (https://github.com/davery22/conveyor/commit/d2c4d9ac68cb9de2954810078051b2cd0087426d)
    - Note: Steal from a slow Executor, or recover from a failed #run
30. Remove 'called' checks and overzealous state-handling (https://github.com/davery22/conveyor/commit/1e8cee82f0f360c7a86a3ff5408b76c1783a2d0b)
31. Birth of ProxySink and ProxySource (https://github.com/davery22/conveyor/commit/20d97db3c033da59eb6392a9bc5ca31421a66d40)
32. Spec for passing null to #completeAbruptly (https://github.com/davery22/conveyor/commit/bd1e6217f383b0759aeccb0d71ef415fdb1c417f)


Discuss:

Why no 'plain' variants of some operators
- Equivalence with #rendezvous 'step' variant
- Using a #buffer (or more substance), instead of #rendezvous, helps amortize cost of context-switching (assuming similar rates)

Document:

A Source that is replayable should either be plain or Supplier&lt;Step&gt;
- The same StepSource's poll can't yield the same sequence at different times to different callers
- ...If drainToSink can yield the same elements on subsequent calls, than #spill is naive...
- ...If it can't, need Supplier<Step> for 'cold' flows...
- No guarantees about non-repetition from plain Sinks/Sources (unless DOCUMENTED)
- No guarantees about thread-safety from any Sinks/Sources (unless DOCUMENTED)

Discuss:

There is no way to know that a source is empty without polling it.
And once we poll it and get something, there is no way to put it back.
So if we need to poll 2 sources to produce 1 output, and 1 source is empty,
we will end up discarding the element from the other source.
Likewise, must offer to a Sink to know whether it is cancelled

Discuss:

Lack of 'timed' or 'try' offer/poll

Document:

- drainFromSource(StepSource) should only call #poll on the StepSource
- drainToSink(StepSink) should only call #offer on the StepSink

Discuss:

Fundamentally non-step operators
- Cannot write a stepBalance, even if it takes StepSinks. Even if we wrap the sinks to know which ones are
  "currently" blocked in offer(), that would only be aware of offers that we submitted, not other threads using the sink.
- Cannot write a stepMerge, for the same reason - cannot know which sources may be blocked in poll() by another thread.
- Cannot write a stepZipLatest, because poll() should return as soon as the first interior poll() completes, but
  that would imply unscoped threads running the other interior polls. Even if we waited for all polls, that still
  would not work, because correct behavior means that we should re-poll each source as soon as its previous poll
  finishes, since sources may emit at different rates.

Discuss:

Why can't we write a step stage from non-step stages?
Non-step stages can only "drain". To wrap in step, we would either need to:

StepSource: When we poll(), internal StepSink (eventually) returns an element buffered from original Source.
- In other words we've made a Segue!
- If we drain the original Source on first poll(), we need unbounded buffering to complete within poll()
- Otherwise, the original Source would already need to be draining in an unscoped thread, outside our poll()

StepSink: When we offer(), internal StepSource buffers so original Sink can see result on poll()
- In other words we've made a Segue!
- If we drain the original Sink on first offer(), we deadlock because offer() is now waiting for more offers
- Otherwise, the original Sink would already need to be draining in an unscoped thread, outside our offer()

Discuss:

- Non-step from non-step - ALWAYS possible - At worst, can reuse 'Step from step' impl with Segues
- Non-step from step     - ALWAYS possible - See above
- Step from non-step     - NEVER possible - Requires external asynchrony to pause draining, or unbounded buffering
  (possible using other buffer-overflow handling, eg error, drop)
- Step from step         - SOMETIMES possible - If asynchrony is scoped to the poll/offer, or buffering is bounded

Discuss:

Streams tend to be a good approach to parallelism when the source can be split
- Run the whole pipeline on each split of the source, in its own thread

Belts tend to be a good approach to parallelism when pipeline stages can be detached
- (and stages progress at similar rates / more than 1/COUNT of their time is NOT handoff overhead)
- Run each stage of the pipeline on the whole source, in its own thread

Discuss:

Even if we separated out chaining/#run into 'Pipelines', users would still be able to chain inside #drain methods,
and would still be able to extend Sources/Sinks with chaining/#run methods (though might violate Liskov substitution)

Discuss:

TimedSegue design
- Using deadlines on both sides to avoid the need for direct management of Conditions, timed waits, etc
- Latching deadlines and waiting at the start of offer/poll, to avoid the need for more locks, and make error recovery possible
    - So some use cases, like 'transfer' (wait after updating state) and 'offer multiple' (update + wait multiple times), are inexpressible
    - This is by design - aiming for simplicity for common cases, rather than maximum expressiveness or optimal performance

Discuss:

- Why exception cause chaining
- Semantics of the boolean return of drainFromSource/ToSink, and why the default impls can be the same
- Why run(Executor) instead of run(STS)?

Discuss:

The effect of buffers/staging on processing
- eg in #zip, can't know we will use either value until we have both
- eg in #broadcast with eagerCancel, a sink may fail with buffered elements, meaning other sinks process more than it
    - Just because #offer returned true doesn't mean element was processed by some downstream

Document:

#andThen is left-associative; #compose is right-associative (matters for which side #runs Silos)

Discuss:

Sometimes a 'step' operator can be rewritten as a plain operator, but needs a buffer inside to manage async comm (eg #gather)
This is equivalent to putting a buffer on the input to get a Step input, so the operator variant is superfluous

Discuss:

Capability models
- A Sink demands a stronger capability from its [Step]Source - the capability to poll()
- A Source demands a stronger capability from its [Step]Sink - the capability to offer()
- An operator can sometimes provide a stronger capability by demanding a stronger capability from its inputs
- The capability to #poll/#offer implies that the [Step]Source/Sink does not have a long-lived asynchronous scope

Discuss:

'flatMapMerge' = #balance(...#flatMap.andThen(noCompleteSink)).compose(alsoComplete(sink))

Discuss:

#completeAbruptly sets exception even if #complete returned normally

Discuss:

Why not implicit close/complete[Abruptly] (inside #drainX) for plain Sinks/Sources?
1. Forcing Sinks to replicate the try-catch-complete-suppress dance is obnoxious & error-prone
2. Some operators - like plain flatMap - need Sink.drainFromSource to NOT complete the Sink
3. If a sink throws, its fan-out siblings only see interrupt[edException], not original exception
4. Default impls (ie 'do nothing') are already hands-off - only override if we need propagation

Discuss:

When to define plain Sink/Source?
- When we want sources to race-to-offer (eg merge), or sinks to race-to-poll (eg balance)
- When step-at-a-time is would require extra work/slowdown (eg concat), or unbounded buffering (eg gather)
- When poll/offer would violate structured concurrency (leak threads)

Discuss:

Why not run(STS)?
- This would allow run() far too permissive access to the STS - close, shutdown, join...
- STS is overly-restrictive
    - only need #fork; an Executor should also work
    - makes it harder to 'wrap' behavior in sub-scope situations
        - need a whole new (well-behaved?) STS when all we should really need to worry about is fork(task)

Discuss:

'Lift from substreams'
- concat substreams happens naturally in split, since we join after each sink, and sinks do not overlap
- merge substreams happens naturally in groupBy, since we join after each sink, and sinks overlap

Discuss:

Avoiding deadlock when waiting on nested runs
- If there is a situation where an inner Source/Sink runs a Silo that other inner Sources/Sinks depend on, we can deadlock
- To trigger this, the Silo would have to NOT already be running when the Source/Sink is run
- To avoid this, the outer stage should be solely-responsible for running (and closing/completing) shared Stages

Discuss:
"recovery-safe" / avoid partial effects and deadlock
