# Chaining and Running

A `Sink` could drain elements from a kind of `Source` called a `StepSource`.

A `Source` could drain elements to a kind of `Sink` called a `StepSink`.

A `StepSegue` was both a `StepSink` and a `StepSource`.

At the time, these were the only relationships to consider. The simple "drain" connections left no loose ends, but with
`StepSegue`s involved, it was possible to set up multiple segues end-to-end, such that one's 'source-part' drains into
the next's 'sink-part'. Each segue's 'sink-part' would run on a different thread than its 'source-part', resulting in a
series of threads that each own a specific phase of element processing - pulling from upstream, transforming, and
pushing downstream - like stations on an assembly line. This was simple to envision, but harder to express in the code:
The segues would have to be declared up-front, and then separately called from a series of threads, in the correct
order, while correctly propagating completion and cancellation signals.

``` java
try (var scope = new StructuredTaskScope<>()) {
    Source<A> source = ...
    StepSegue<A, B> segue1 = ...
    StepSegue<B, C> segue2 = ...
    Sink<C> sink = ...
    
    // First station
    scope.fork(() -> {
        try (source) {
            source.drainToSink(segue1);
            segue1.complete();
            return null;
        } catch (Throwable e) {
            // The rest of the 'complete/close' dance - just pretend we're doing this in each fork
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            try {
                segue1.completeAbruptly(t);
            } catch (Throwable t) {
                if (t instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                e.addSuppressed(t);
            }
            throw new CompletionException(e);
        }
    });
    
    // Second station
    scope.fork(() -> {
        try (segue1) {
            segue2.drainFromSource(segue1);
            segue2.complete();
            return null;
        } catch (Throwable e) {
            // completeAbruptly...
        }
    });
    
    // Third station
    scope.fork(() -> {
        try (segue2) {
            sink.drainFromSource(segue2);
            sink.complete();
            return null;
        } catch (Throwable e) {
            // completeAbruptly...
        }
    });
    
    scope.join();
}
```

Even if we factored the 'complete/close' ceremony into methods, the separation between instance declaration and usage in
threads starts to obstruct legibility, and opens more room for error. The story gets worse in the presence of fan-out or
fan-in operators, where the threads can no longer be arranged in a linear progression corresponding to the data flow,
because the data flow itself is non-linear. Not to mention operators like `flatMap` or `groupBy`, that can dynamically
create nested pipelines, each of which may have a different number of stations, and thus, threads.

This reasoning led me to pursue a more fluent API, where pipelines could be built by chaining calls to operators -
similar to how Java Streams (and most Reactive Streams vendors) work. I still liked and wanted to preserve the
simplicity of the existing interfaces, so my first inclination was to build this API on top of a new set of 'pipeline'
interfaces. However, as I continued to refine the approach, the new set of interfaces ended up being almost 1:1 with the
existing interfaces: Each 'pipeline' interface worked by wrapping (and exposing) an instance of the corresponding
'plain' interface, and provided methods - `andThen` and `compose` - for connecting to (the 'pipeline' variants of)
sources, sinks, or segues, as applicable. There was one additional 'pipeline' interface: The equivalent of what is now
called `Station`, which represented a source connected to a sink, or a sequence of such connections. Finally, the
'pipeline' interfaces introduced a `run` method, that would concurrently execute any enclosed stations, draining each
source to its paired sink, and handling every 'complete/close' dance.

The `run` method was the payout of chaining, eliminating the previous separation and ceremony. But, its existence
implied that the only safe way to use an arbitrary 'pipeline' source or sink was to call `run` first. This is because
the 'pipeline' source or sink <em>might</em> have stations inside, and not running those could cause the exposed 'plain'
source or sink to deadlock when used (by waiting forever for a non-empty or non-full internal buffer, respectively).
This posed a burden of opportunity on operators, like `zip`: If an operator accepted 'pipeline' inputs, it could return
a 'pipeline' output that would `run` the inputs when it runs. If an operator <em>didn't</em> do this, then it could
return a 'plain' output that didn't need to `run`... but that would shift responsibility onto the caller to remember and
coordinate whatever inputs it needs to `run`. Following this logic, friendly operators would always accept 'pipeline'
inputs and return a 'pipeline' output that handled running the inputs, just in case the inputs needed it. This made
'pipeline' instances pervasive and infectious.

Seeing how this would play out, and not really enjoying the idea of having doppelganger interfaces anyway, I decided to
move the chaining methods directly onto the original interfaces, along with the `run` method. This has the bitter
implication from earlier - that if we don't know better, we should assume that any `Source` or `Sink` might have
stations inside - so even if we want to drain manually, we still need to `run` things. But by this point, I expected
that draining manually would be far less common usage than chaining stages to form a closed `Station`, and then running
that. Between the ergonomics of chaining, the implicit handling of threads and the 'complete/close' dance, and the
collective influence of operators whose output may now need to be `run`, chaining and running had become the endorsed
usage.
