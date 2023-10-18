# Sink and Source

`DeadlineSegue` made implementing operators easier by being opinionated, at the cost of some baggage and limitations. So
around the same time, I introduced an unopinionated interface above it: `StepSegue` (both of these had different names
at the time). `StepSegue` had four abstract methods: `offer`, `complete`, `poll`, and `close`. The `offer` and `poll`
methods were borrowed from BlockingQueue (though the semantics do not exactly match anything there), while the
`complete` and `close` methods were added to signal that the 'producer completed' or 'consumer cancelled', respectively.
A 'producer' thread would repeatedly `offer` and eventually `complete`, while a 'consumer' thread would repeatedly
`poll` and eventually `close`. This was very much like a BlockingQueue with termination signals built-in, and it was
meant to be used that way. For example, applying an early version of the `batch` segue would have looked something like
this:

``` java
try (var scope = new StructuredTaskScope<>()) {
    int bufferCapacity = 1000;
    StepSegue<Result, List<Result>> batch = Belts.batch(
        () -> new ArrayList<Result>(bufferCapacity),
        List::add,
        buffer -> buffer.size() == 1 ? Optional.of(Instant.now().plusSeconds(5))
                : buffer.size() == bufferCapacity ? Optional.of(Instant.MIN)
                : Optional.empty()
    );
    
    Subtask<Void> producer = scope.fork(() -> {
        try {
            for (Callable<Result> unit : work) {
                Result result = unit.call();
                if (!batch.offer(result)) {
                    break;
                }
            }
            batch.complete(null); // Originally had complete(Throwable), not complete() and completeAbruptly(Throwable)
        } catch (Throwable t) {
            batch.complete(t); // This catch ought to handle interrupts, suppress exceptions, and re-throw, but I didn't know that yet
        }
        return null;
    });
    
    Subtask<Void> consumer = scope.fork(() -> {
        try (batch) {
            for (List<Result> buffer; (buffer = batch.poll()) != null; ) {
                database.batchWrite(buffer);
            }
        }
        return null;
    });
    
    scope.join();
}
```

This is still imperatively managing threads, but offers much better encapsulation than before `StepSegue` existed. The
`producer` and `consumer` names might be starting to feel odd - in reality both threads are fulfilling a 'producer' and
'consumer' role, but the consumption by `batch.offer` and production by `batch.poll` is glossed over. Though we could
still write the code very similarly to this today, we would probably factor the 'work processing' into a Source, and the
'batch writing' into a Sink. This would allow us to connect and `run` the underlying Silos, which under the hood would
pretty much look like the two threads above (but with the surrounding 'complete/close protocol' correctly handled for
us).

At the time, Source and Sink did not exist, but that was about to change. As I looked toward new kinds of operators
to implement, one of the first ones I decided to tackle was `zip`. (Btw, shout out to Akka Streams'
[categorization of operators](https://doc.akka.io/docs/akka/current/stream/operators/index.html), and to
https://github.com/drewhk, whose apparent "lurking" around interesting Rust crates coincided with my own, and
incidentally provided me some
[useful context and exercises](https://github.com/rust-lang/futures-rs/issues/110#issuecomment-244320924).) `zip` is an
operator that traditionally waits for two 'sources' to emit, then emits a combined element. It was immediately clear
that the first half of `StepSegue` was irrelevant to implementing this operator, and furthermore, `zip` could only
create the second half. This lead to the extraction of the `StepSource` and `StepSink` interfaces - or at least, that's
what I eventually named them. For the time being, `StepSegue` inherited from both of these. `zip` could now take two
`StepSource`s and return a `StepSource`.

A trickier situation  was `zip`'s cousin: `zipLatest` (sometimes called `combineLatest`). Like `zip`, `zipLatest` works
off of two sources, but emits a combined element whenever <em>either</em> source emits, using the latest seen element
from both. This wouldn't be possible if `zipLatest` had to `poll` both sources, because "latest" in this context implies
that elements are arriving independently of the operator asking for them. What I found I could do is consume all
elements from both sources in concurrent threads, with some synchronization to combine the latest elements as they
arrive, and offer the result to a downstream sink. Interestingly, this new kind of 'source' (which became `Source`)
could not support polling, but <em>required</em> a 'sink' that supported offering - `StepSink`. I encountered the dual
to this when I implemented `balance` - a 'sink' (`Sink`) that could not support offering, but required a 'source' that
supported polling - `StepSource`.

The complete set of interfaces up to this point were:
 - `Sink`
 - `Source`
 - `StepSink implements Sink`
 - `StepSource implements Source`
 - `StepSegue implements StepSink, StepSource`

The new `Source` and `Sink` interfaces enabled 'fundamentally asynchronous' operators like `zipLatest` and `balance`, 
but it was also trivial to adapt previous 'driver' code to implement them. Here is the 'work processing' `Source` and
'batch writing' `Sink` alluded to earlier:

``` java
Belt.Source<Result> source = sink -> {
    for (Callable<Result> unit : work) {
        Result result = unit.call();
        if (!sink.offer(result)) {
            return false;
        }
    }
    return true;
}

Belt.Sink<List<Result>> sink = source -> {
    for (List<Result> buffer; (buffer = source.poll()) != null; ) {
        database.batchWrite(buffer);
    }
    return true;
}
```

This standardized setup to a matter of passing a `StepSink` to a `Source`, or a `StepSource` to a `Sink` (and doing the
'complete/close' dance around it). In either case, the source would drain its elements into the sink until one or the
other signaled termination. With this standardization in place, and already having `StepSegue` to represent a 'linked'
`Sink` and `Source` that could communicate elements across threads (across an 'asynchronous boundary'), it was
beginning to look attractive and possible to declaratively string operations together.
