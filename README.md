# Conveyor

A tool for processing data like an assembly line. Processing can be split into stations that proceed concurrently.

Conveyor shares many of the ideals of Reactive Streams, but is reimagined for a world where threads are numerous and
blocking is cheap. This enables approachable interfaces - based on blocking `push` and `pull` - that are practical to
understand, implement, and debug.

---

Below is a walk-through introducing concepts.

For more details and specific examples, see the [javadocs](TODO).

For a recap of how some aspects of the design came to be, see the [design-notes](TODO).

---

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
Station station = source.andThen(sink);
```

A `Station` is a thing we can `run`, which will `pull` from the `StepSource` and `push` to the `StepSink`.

``` java
station.run(executor);
// Prints:
// 1
// 2
// 3
```

As the `Executor` suggests, `run` does its work asynchronously, by submitting a task to the executor.

---

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
Station station1 = source.andThen(stepSink);
Station station2 = stepSource.andThen(sink);
```

What we _cannot_ do with a `Source` or `Sink` alone is "step" through elements. Processing - either by
`Source.drainToSink` or `Sink.drainFromSource` - is all-or-nothing.

---

So far, we have talked about connecting a single source to a single sink. What if a sink "fed into" another source?

A `Segue` pairs a `Sink` with a `Source`. The sink and source are generally "linked" by shared state, such as a buffer,
so that elements accepted by the sink can later be yielded by the source.

``` java
Belt.Source<String> source = Belts.streamSource(Stream.of("a", "b", "c"));
Belt.StepSegue<String, String> segue = Belts.buffer(256);
Belt.StepSink<String> sink = str -> { System.out.println(str); return true; };

Station station1 = source.andThen(segue.sink());
Station station2 = segue.source().andThen(sink);

station1.run(executor);
station2.run(executor);
// Prints:
// a
// b
// c
```

Notice how we now create 2 stations, and we need to `run` both of them to see results. We could have instead
consolidated the stations under one "composite" station, so that we only need to `run` once.

``` java
Station station = station1.andThen(station2);
```

We can "chain" any number of stations, and we typically do so a bit less explicitly.

``` java
Station station = Belts.streamSource(Stream.of("a", "b", "c"))
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
        256
    ))
    .andThen((String s) -> { System.out.println(s); return true; })
    .run(executor);
// Prints:
// or    (after 2 seconds)
// now   (after 3 seconds)
// never (after 5 seconds)
```

---

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

And they are expressive. If you've heard about [Gatherers](https://cr.openjdk.org/~vklang/Gatherers.html) for Streams,
well, they can be leveraged here too:

``` java
Belts.iteratorSource(List.of(1, 2, 3).iterator())
    .andThen(Belts
        .gather(Gatherers.flatMap((Integer i) -> Stream.of(i-1, i+1)))
        .andThen((Integer i) -> { System.out.println(i); return true; })
    )
    .run(executor);
// Prints:
// 0
// 2
// 1
// 3
// 2
// 4
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

Now that we have introduced the concept of proxying, we can also describe "assembly lines" that are non-linear, ie
"fan-in" and "fan-out".

A fan-in source proxies several sources. A motivating example is `merge`, which yields elements as they arrive from
multiple sources:

``` java
Belts
    .merge(List.of(
        TODO
    ))
    .andThen((String s) -> { System.out.println(s); return true; })
    .run(executor);
```

A fan-out sink proxies several sinks. A motivating example is `balance`, which pushes each input element to the first
available sink:

``` java
List<String> queue1 = new ArrayList<>();
List<String> queue2 = new ArrayList<>();
List<String> queue3 = new ArrayList<>();


    .balance(List.of(
        (Belt.StepSink<String>) queue1::add,
        (Belt.StepSink<String>) queue2::add,
        (Belt.StepSink<String>) queue3::add,
    ))
    .run(executor);
```

Sometimes we need to fan-in sinks or fan-out a source. A motivating example is `groupBy`, which is a sink that
dynamically creates inner sinks to process elements with different keys. If we needed to "merge" the inner sinks, we
would arrange for each of them to push to a common sink:

``` java
List<String> list = new ArrayList<>();
Belt.StepSink<String> commonSink = list::add;

Belts.iteratorSource(List.of("now", "or", "never").iterator())
    .andThen(Belts.groupBy(
        (String s) -> s.substring(0, 1),
        false,
        Throwable::printStackTrace,
        (k, first) -> commonSink.compose(Belts.flatMap(s -> Belts.streamSource(Stream.of(k, s, first)),
                                                       Throwable::printStackTrace))
    ))
    .run(executor);
// When done, list is [n, now, now, o, or, or, n, never, now]
```

Finally, it's also possible to have cyclic flow. Here is a contrived example that sums the length of lines passed to
`System.in`, printing the current sum after each input:

``` java
// Create a buffer seeded with an initial 0
var buffer = Belts.extrapolate(0, e -> Collections.emptyIterator(), 256);
var iter = Stream.generate(new Scanner(System.in)::nextLine)
    .takeWhile(line -> !"stop".equalsIgnoreCase(line))
    .iterator();
Belt.StepSource<Integer> probe = () -> iter.hasNext() ? iter.next().length() : null;

Belts
    .zip(buffer.source(), probe, Integer::sum)
    .andThen(Belts.broadcast(List.of(
        buffer.sink(),
        e -> { System.out.println(e); return true; })
    ))
    .run(executor);
```

---

Up to this point we have ignored how termination and exceptions are communicated, especially across concurrent stations.
