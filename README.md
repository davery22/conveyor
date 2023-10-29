# Conveyor

A library for processing data "like an assembly line". Processing can be split into stations that proceed concurrently.
This facilitates efficient pipelining of IO-intensive work, complementary to `java.util.stream`'s focus on CPU-intensive
work.

Conveyor shares many of the ideals of Reactive Streams, but is reimagined for a world where threads are numerous and
blocking is cheap. This shift enables approachable interfaces - based on blocking `push` and `pull` - that are practical
to understand, implement, and debug.

---

Below is a walk-through introducing concepts.

For more details and specific examples, see the [javadocs](https://davery22.github.io/conveyor/javadoc/).

For a recap of how some aspects of the design came to be, see the [design-notes](https://github.com/davery22/conveyor/blob/master/design-notes/01-the-first-segue.md).

---

<a id="table-of-contents"></a>
## Table of contents

1. [StepSources and StepSinks](#stepsources-and-stepsinks)
2. [Sources and Sinks](#sources-and-sinks)
3. [Segues](#segues)
4. [Operators](#operators)
5. [Fan-in and fan-out](#fan-in-and-fan-out)
6. [Termination and failure](#termination-and-failure)

---

<a id="stepsources-and-stepsinks"></a>
## StepSources and StepSinks [↩](#table-of-contents)

A `StepSource` is a thing we can `pull` elements from.

``` java
Iterator<Integer> iter = List.of(1, 2, 3).iterator();
Belt.StepSource<Integer> source = () -> iter.hasNext() ? iter.next() : null;
source.pull(); // 1
source.pull(); // 2
source.pull(); // 3
source.pull(); // null
```

A `StepSink` is a thing we can `push` elements to.

``` java
Belt.StepSink<Integer> sink = element -> { System.out.println(element); return true; };
sink.push(1); // true
sink.push(2); // true
sink.push(3); // true
```

We can connect a `StepSource` to a `StepSink`, which will return a `Station`.


``` java
Belt.StepSource<Integer> source = Belts.iteratorSource(List.of(1, 2, 3).iterator());
Belt.StepSink<Integer> sink = element -> { System.out.println(element); return true; };
Belt.Station station = source.andThen(sink); // Alternatively: sink.compose(source);
```

A `Station` is a thing we can `run`, which will `pull` from the `StepSource` and `push` to the `StepSink`.

``` java
station.run(executor);
// Prints:
// 1
// 2
// 3
```

As the `Executor` suggests, `run` does its work asynchronously, by submitting a task to the executor. It is generally a
good idea to use a thread-per-task executor here, to avoid deadlock when we start using multiple stations. Each station
will run in its own thread.

One way to create such an executor is by delegating to the `fork` method of a `StructuredTaskScope`.

``` java
Executor executor = runnable -> scope.fork(Executors.callable(runnable, null));
// Or, equivalently:
Executor executor = Belts.scopeExecutor(scope);
```

This approach has the added benefit of providing a means to wait for all stations to finish: `scope.join()`.

---

<a id="sources-and-sinks"></a>
## Sources and Sinks [↩](#table-of-contents)

Now that we have `StepSources` and `StepSinks`, we can talk about their parents.

A `Source` can `push` elements to a `StepSink`.

``` java
Belt.Source<Integer> source = sink -> {
    for (int i = 0; i < 3; i++) {
        if (!sink.push(i + 3)) {
            return false;
        } 
    }
    return true;
}
Belt.StepSink<Integer> sink = el -> { System.out.println(el); return true; }
source.drainToSink(sink);
// Prints:
// 4
// 5
// 6
```

A `Sink` can `pull` elements from a `StepSource`.

``` java
Belt.Sink<Integer> sink = source -> {
    for (Integer i; (i = source.pull()) != null; ) {
        System.out.println(i + 6);
    }
    return true;
}
Belt.StepSource<Integer> source = Belts.iteratorSource(List.of(1, 2, 3).iterator());
sink.drainFromSource(source);
// Prints:
// 7
// 8
// 9
```

We can connect a `Source` to a `StepSink`, or a `StepSource` to a `Sink`. Both of these return a `Station` that we can
`run`, just as before.

``` java
Belt.Station station1 = source.andThen(stepSink);
Belt.Station station2 = stepSource.andThen(sink);
```

What we _cannot_ do with a `Source` or `Sink` alone is "step" through elements. Processing - either by
`Source.drainToSink` or `Sink.drainFromSource` - is all-or-nothing.

`StepSource` is a `Source`. It has a default implementation of `drainToSink` that repeatedly pulls from itself and
pushes to the sink.

`StepSink` is a `Sink`. It has a default implementation of `drainFromSource` that repeatedly pulls from the source and
pushes to itself.

---

<a id="segues"></a>
## Segues [↩](#table-of-contents)

So far, we have talked about connecting a single source to a single sink. What if a sink "fed into" another source?

A `Segue` pairs a `Sink` with a `Source`. The sink and source are generally "linked" by shared state, such as a buffer,
so that elements accepted by the sink can later be yielded by the source.

``` java
Belt.Source<String> source = Belts.streamSource(Stream.of("a", "b", "c"));
Belt.StepSegue<String, String> segue = Belts.buffer(256);
Belt.StepSink<String> sink = str -> { System.out.println(str); return true; };

Belt.Station station1 = source.andThen(segue.sink());
Belt.Station station2 = segue.source().andThen(sink);

station1.run(executor);
station2.run(executor);
// Prints:
// a
// b
// c
```

Notice how we now create 2 stations, and we need to `run` both of them to see results. We could have instead
consolidated the stations under one "composite" station, so that we only need to explicitly `run` once.

``` java
Belt.Station station = station1.andThen(station2);
```

We can "chain" any number of stations, and we typically do so a bit less explicitly.

``` java
Belt.Station station = Belts.streamSource(Stream.of("a", "b", "c"))
    .andThen(Belts.buffer(256))
    .andThen((String str) -> { System.out.println(str); return true; });

station.run(executor);
// Prints:
// a
// b
// c
```

You might also have noticed that `Belts.buffer` returned a `StepSegue`. There are 4 basic kinds of `Segues`, one for
each combination of sink-to-source pairing.

- `Segue` has a `Sink` and a `Source`
- `StepSinkSource` is a `Segue`, whose `Sink` is a `StepSink`
- `SinkStepSource` is a `Segue`, whose `Source` is a `StepSource`
- `StepSegue` is a `StepSinkSource` and a `SinkStepSource`

The linked sink and source in segues work to move elements between threads when we `run` stations. But they can just as
easily transform elements, or transform the sequence, along the way.

Here is a segue called `delay`, that calculates a deadline for each incoming element, and holds each element in a buffer
until its deadline elapses:

``` java
Belts.streamSource(Stream.of("now", "or", "never"))
    .andThen(Belts.delay(
        s -> Instant.now().plusSeconds(s.length()),
        256 // Buffer capacity
    ))
    .andThen((String s) -> { System.out.println(s); return true; })
    .run(executor);
// Prints:
// or    (after 2 seconds)
// now   (after 3 seconds)
// never (after 5 seconds)
```

---

<a id="operators"></a>
## Operators [↩](#table-of-contents)

Some transformations are fundamentally asynchronous, like `delay` above, which relies on timed waits. But many are not,
and it would be a shame if we had to introduce concurrency (by segue-ing to a new station) to express them. This is
what Operators are for.

Operators wrap an existing sink or source in a new "proxy" sink or source, that transforms elements on the way in or out
of the original, respectively. A sink or source that does not proxy might be called a "boundary" sink or source, as it
lies at the boundary of whatever station runs it.

``` java
Belt.StepSource<Integer> boundarySource = Belts.iteratorSource(List.of(1, 2, 3).iterator());
Belt.StepSourceOperator<Integer, Integer> op = Belts.filterMap(i -> i * 10);
Belt.StepSource<Integer> proxySource = op.compose(source);
```

Operators chain, just like everything else.

``` java
Belts.iteratorSource(List.of(1, 2, 3).iterator())
    .andThen(Belts.filterMap(i -> i * 10))
    .andThen((Integer i) -> { System.out.println(i); return true; })
    .run(executor);
// Prints:
// 10
// 20
// 30
```

And they are expressive. If you've heard about [Gatherers](https://openjdk.org/jeps/461) for Streams, well, they can be
reused here too:

``` java
Belts.iteratorSource(List.of(1, 2, 3, 4, 5).iterator())
    .andThen(Belts
        .gather(Gatherers.<Integer>windowSliding(3))
        .andThen((List<Integer> window) -> { System.out.println(window); return true; })
    )
    .run(executor);
// Prints:
// [1, 2, 3]
// [2, 3, 4]
// [3, 4, 5]
```

'Operator' is not an interface. Rather, it refers to a family of functional interfaces, one for each combination of
source-to-source and sink-to-sink proxying.

- `SinkOperator` creates a `Sink` that proxies a `Sink`
- `StepToSinkOperator` is a `SinkOperator` that creates a `StepSink`
- `SinkToStepOperator` creates a `Sink` that proxies a `StepSink`
- `StepSinkOperator` is a `SinkToStepOperator` that creates a `StepSink`
- `SourceOperator` creates a `Source` that proxies a `Source`
- `SourceToStepOperator` is a `SourceOperator` that creates a `StepSource`
- `StepToSourceOperator` creates a `Source` that proxies a `StepSource`
- `StepSourceOperator` is a `StepToSourceOperator` that creates a `StepSource`

---

<a id="fan-in-and-fan-out"></a>
## Fan-in and fan-out [↩](#table-of-contents)

Now that we have introduced the concept of proxying, we can also describe pipelines that are non-linear, ie "fan-in" and
"fan-out".

A fan-in source proxies several sources. An example is `merge`, which yields elements as they arrive from multiple
sources.

``` java
Instant start = Instant.now();
var delays = new Object(){ int fizz = 0; int buzz = 0; };

Belts
    .merge(List.of(
        Belts.streamSource(Stream.generate(() -> "fizz"))
            .andThen(Belts.delay(_ -> start.plusSeconds(delays.fizz += 3), 1)),
        Belts.streamSource(Stream.generate(() -> "buzz"))
            .andThen(Belts.delay(_ -> start.plusSeconds(delays.buzz += 5), 1))
    ))
    .andThen((String s) -> { System.out.println(s); return true; })
    .run(executor);
// Prints:
// fizz  (after  3 seconds)
// buzz  (after  5 seconds)
// fizz  (after  6 seconds)
// fizz  (after  9 seconds)
// buzz  (after 10 seconds)
// ...
```

A fan-out sink proxies several sinks. An example is `balance`, which pushes each input element to the first available
sink.

``` java
Belts.iteratorSource(Stream.iterate(0, i -> i + 1).iterator())
    .andThen(Belts.synchronizeStepSource())
    .andThen(Belts.balance(List.<Belt.StepSink<Integer>>of(
        i -> { System.out.println("A" + i); return true; },
        i -> { System.out.println("B" + i); return true; }
    )))
    .run(executor);
// Possible output:
// B1
// B2
// B3
// A0
// A5
// B4
// ...
```

Sometimes we need to fan-in sinks or fan-out a source. An example can be found in `groupBy`, which is a sink that
dynamically creates inner sinks to process elements with different keys. If we needed to "merge" the inner sinks, we
would arrange for each of them to push to a common sink.

``` java
List<String> list = new ArrayList<>();
Belt.StepSink<String> commonSink = list::add;

Belts.iteratorSource(List.of("now", "or", "never").iterator())
    .andThen(Belts.groupBy(
        (String s) -> s.substring(0, 1),
        false, Throwable::printStackTrace, // Ignore for now
        (k, first) -> commonSink.compose(Belts.flatMap(s -> Belts.streamSource(Stream.of(k, s, first)),
                                                       Throwable::printStackTrace)) // Ignore for now
    ))
    .run(executor);
// list will be [n, now, now, o, or, or, n, never, now]
```

Finally, it's also possible to have cyclic flow. Below is an example that cycles after 5 elements, incrementing elements
in each cycle.

``` java
var buffer = Belts.<Integer>buffer(5);
Belts
    .concat(List.of(
        Belts.streamSource(Stream.iterate(1, i -> i * 2).limit(5)),
        buffer.source().andThen(Belts.filterMap(i -> i + 1))
    ))
    .andThen(Belts.delay(_ -> Instant.now().plusSeconds(1), 1))
    .andThen(Belts.broadcast(List.of(
        i -> { System.out.println(i); return true; },
        buffer.sink()
    )))
    .run(executor);
// Prints:
// 1
// 2
// 4
// 8
// 16
// 2
// 3
// 5
// 9
// 17
// 3
// 4
// 6
// 10
// 18
// ...
```

---

<a id="termination-and-failure"></a>
## Termination and failure [↩](#table-of-contents)︎

Up to this point we have ignored how termination and exceptions are communicated. If we peered into `Station.run`, we
would see that the task it submits to the executor looks something like this:

``` java
try (source) {
    // At least one of these must be true
    if (sink instanceof Belt.StepSink ss) {
        source.drainToSink(ss);
    } else if (source instanceof Belt.StepSource ss) {
        sink.drainFromSource(ss);
    }
    sink.complete();
} catch (Throwable e) {
    // Important bit is completeAbruptly; rest is interrupt-handling and exception suppression around it
    if (e instanceof InterruptedException)
        Thread.currentThread().interrupt();
    try {
        sink.completeAbruptly(e);
    } catch (Throwable t) {
        if (t instanceof InterruptedException)
            Thread.currentThread().interrupt();
        e.addSuppressed(t);
    }
    throw new CompletionException(e);
}
```

The first big news is that `Source` is an `AutoCloseable`. By default, its `close` method does nothing.

The second big news is that `Sink` has methods to `complete` or `completeAbruptly`. These methods also default to doing
nothing. What gives?

When we consider a single `Station`, no-op implementations of these methods are fine. If no exceptions are thrown, the
task will exit normally after we have drained elements from the source to the sink. If an exception is thrown, we will
wrap it in `CompletionException` and throw that from the task.

When we consider chained `Stations`, connected across the boundary by a linked sink and source, proper implementations
of these methods become more important:
- When a linked boundary source closes, it stops yielding elements and tells the upstream linked sink to immediately
  stop accepting elements, as they will have nowhere to go. Subsequently, `Sink.push` and `Sink.drainFromSource` will
  short-circuit and return `false`, conveying the cancellation upstream.
- When a linked boundary sink completes, it stops accepting elements and tells the downstream linked source to expect no
  more. Subsequently, after yielding any outstanding elements, `Source.poll` and `Source.drainToSink` will short-circuit
  and return `null` and `true`, respectively, conveying the completion downstream.
- When a linked boundary sink completes _abruptly_, it stops accepting elements and tells the downstream linked source
  to immediately stop yielding elements. Subsequently, `Source.poll` and `Source.drainToSink` will short-circuit and
  throw an `UpstreamException` wrapping the cause passed to `Sink.completeAbruptly`, conveying the exception downstream.
- Proxy sinks and sources simply forward these signals onward.

These semantics enable termination and failure signals to travel across stations. Exceptions first travel "up" to the
station boundary via throwing, and then "down" and across boundaries via `completeAbruptly`, until either a downstream
is able to recover from it, or there are no more downstreams. If an exception makes it "up" to the station boundary, it
will be thrown from the task, with any subsequent exceptions from the same station suppressed onto it. What happens next
depends on the `Executor` running the task, but exceptions are never just "lost".

With this machinery in place, let's finally check out what kinds of stack traces we might see when things go wrong.

In these examples, we'll use a `StructuredTaskScope` as the `Executor`, so we can wait for all stations to finish
running. The particular `StructuredTaskScope` is a `FailureHandlingScope`, which allows us to act on task failures - in
this case, just naively printing a stack trace.

``` java
 1  import io.avery.conveyor.Belts;
 2  import io.avery.conveyor.FailureHandlingScope;
 3  
 4  import java.util.List;
 5  
 6  void main() throws Exception {
 7      try (var scope = new FailureHandlingScope(Throwable::printStackTrace)) {
 8          Belts
 9              .concat(List.of(
10                  Belts.iteratorSource(List.of(1, 2, 3, 4, 5).iterator()),
11                  () -> { throw new IllegalStateException("uh-oh"); }
12              ))
13              .andThen((Integer i) -> { System.out.println(i); return true; })
14              .run(Belts.scopeExecutor(scope));
15  
16          scope.join();
17      }
18  }
```
``` text
1
2
3
4
5
java.util.concurrent.CompletionException: java.lang.IllegalStateException: uh-oh
	at io.avery.conveyor.Belts$ClosedStation.lambda$run$0(Belts.java:3199)
	at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:572)
	at java.base/java.util.concurrent.StructuredTaskScope$SubtaskImpl.run(StructuredTaskScope.java:889)
	at java.base/java.lang.VirtualThread.run(VirtualThread.java:309)
Caused by: java.lang.IllegalStateException: uh-oh
	at Main.lambda$main$0(Main.java:11)
	at io.avery.conveyor.Belt$StepSource.drainToSink(Belt.java:710)
	at io.avery.conveyor.Belts$1Concat.drainToSink(Belts.java:1952)
	at io.avery.conveyor.Belts$ClosedStation.lambda$run$0(Belts.java:3190)
	... 3 more
```

Reading the stack trace from the bottom-up: We first see the task lambda inside `Station.run`, which tried to drain our
`concat`, which eventually hit our planted `IllegalStateException`. This was caught at the station boundary and wrapped
in a `CompletionException`.

This trace was fairly short, but was only running one station. What happens when we introduce more stations?

The next example has 3 stations, with buffers between them. The second station throws an exception.

``` java
 1  import io.avery.conveyor.Belts;
 2  import io.avery.conveyor.FailureHandlingScope;
 3  
 4  import java.util.List;
 5  
 6  void main() throws Exception {
 7      try (var scope = new FailureHandlingScope(Throwable::printStackTrace)) {
 8          Belts
 9              .iteratorSource(List.of(1, 2, 3, 4, 5).iterator())
10              .andThen(Belts.<Integer>buffer(1)
11                  .andThen(Belts.filterMap(i -> {
12                      if (i == 3) {
13                          throw new IllegalStateException("uh-oh");
14                      }
15                      return i;
16                  }))
17              )
18              .andThen(Belts.buffer(1))
19              .andThen((Integer i) -> { System.out.println(i); return true; })
20              .run(Belts.scopeExecutor(scope));
21  
22          scope.join();
23      }
24  }
```
``` text
1
2
java.util.concurrent.CompletionException: java.lang.IllegalStateException: uh-oh
	at io.avery.conveyor.Belts$ClosedStation.lambda$run$0(Belts.java:3199)
	at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:572)
	at java.base/java.util.concurrent.StructuredTaskScope$SubtaskImpl.run(StructuredTaskScope.java:889)
	at java.base/java.lang.VirtualThread.run(VirtualThread.java:309)
Caused by: java.lang.IllegalStateException: uh-oh
	at Main.lambda$main$0(Main.java:13)
	at io.avery.conveyor.Belts$1FilterMap.pull(Belts.java:431)
	at io.avery.conveyor.Belt$StepSource.drainToSink(Belt.java:710)
	at io.avery.conveyor.Belts$ChainSource.drainToSink(Belts.java:3260)
	at io.avery.conveyor.Belts$ClosedStation.lambda$run$0(Belts.java:3190)
	... 3 more
java.util.concurrent.CompletionException: io.avery.conveyor.UpstreamException: java.lang.IllegalStateException: uh-oh
	at io.avery.conveyor.Belts$ClosedStation.lambda$run$0(Belts.java:3199)
	at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:572)
	at java.base/java.util.concurrent.StructuredTaskScope$SubtaskImpl.run(StructuredTaskScope.java:889)
	at java.base/java.lang.VirtualThread.run(VirtualThread.java:309)
Caused by: io.avery.conveyor.UpstreamException: java.lang.IllegalStateException: uh-oh
	at io.avery.conveyor.DeadlineSegue$Source.pull(DeadlineSegue.java:439)
	at io.avery.conveyor.Belt$StepSource.drainToSink(Belt.java:710)
	at io.avery.conveyor.Belts$ChainSource.drainToSink(Belts.java:3260)
	at io.avery.conveyor.Belts$ClosedStation.lambda$run$0(Belts.java:3190)
	... 3 more
Caused by: java.lang.IllegalStateException: uh-oh
	at Main.lambda$main$0(Main.java:13)
	at io.avery.conveyor.Belts$1FilterMap.pull(Belts.java:431)
	... 6 more
```

What we're seeing here is 2 stack traces, from the second and third stations, respectively. The second station threw an
`IllegalStateException` in `filterMap`, which was caught at the station boundary and passed downstream, then wrapped in
a `CompletionException`. The third station saw the `IllegalStateException` when it pulled from the buffer, and wrapped
it in an `UpstreamException`, which was caught at the station boundary and wrapped again. Since the original exception
happened downstream of the first station, all it saw was a cancellation, and it exited normally.

Now let's try to recover downstream from the exception.

``` java
  1  import io.avery.conveyor.Belts;
  2  import io.avery.conveyor.FailureHandlingScope;
  3  
  4  import java.util.List;
  5  
  6  void main() throws Exception {
  7      try (var scope = new FailureHandlingScope(Throwable::printStackTrace)) {
  8          Belts
  9              .iteratorSource(List.of(1, 2, 3, 4, 5).iterator())
 10              .andThen(Belts.<Integer>buffer(1)
 11                  .andThen(Belts.filterMap(i -> {
 12                      if (i == 3) {
 13                          throw new IllegalStateException("uh-oh");
 14                      }
 15                      return i;
 16                  }))
 17              )
 18              .andThen(Belts
 19                  .recoverStep(
 20                      _ -> Belts.iteratorSource(List.of(8, 9, 10).iterator()),
 21                      Throwable::printStackTrace // Ignore; new source does not enclose any stations
 22                  )
 23                  .andThen(Belts.buffer(1))
 24              )
 25              .andThen((Integer i) -> { System.out.println(i); return true; })
 26              .run(Belts.scopeExecutor(scope));
 27 
 28          scope.join();
 29      }   
 30  }
```
``` text
1
2
8
9
10
java.util.concurrent.CompletionException: java.lang.IllegalStateException: uh-oh
	at io.avery.conveyor.Belts$ClosedStation.lambda$run$0(Belts.java:3199)
	at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:572)
	at java.base/java.util.concurrent.StructuredTaskScope$SubtaskImpl.run(StructuredTaskScope.java:889)
	at java.base/java.lang.VirtualThread.run(VirtualThread.java:309)
Caused by: java.lang.IllegalStateException: uh-oh
	at Main.lambda$main$0(Main.java:13)
	at io.avery.conveyor.Belts$1FilterMap.pull(Belts.java:431)
	at io.avery.conveyor.Belt$StepSource.drainToSink(Belt.java:710)
	at io.avery.conveyor.Belts$ChainSource.drainToSink(Belts.java:3260)
	at io.avery.conveyor.Belts$ClosedStation.lambda$run$0(Belts.java:3190)
	... 3 more
```

Here, the trace we see is from the second station. When it tries to abruptly complete the downstream buffer's sink, the
downstream recovers, so the exception never reaches the third station.
