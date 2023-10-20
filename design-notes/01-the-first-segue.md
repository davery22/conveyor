# The First Segue

I started this project as a fun personal challenge to see if I could translate some Reactive Streams operators to an
imperative equivalent using (virtual) threads. This would give me a deeper intuition of the semantics of the operators,
and whether virtual threads really make reactive programming obsolete.

One of the first use cases I had in mind was one I had run into several times at work, and very reminiscent of Colin
Breck's [Motivating Example](https://blog.colinbreck.com/akka-streams-a-motivating-example/) for Akka Streams. The
general idea is: We have a program that processes units of work, each of which produces some result that we want to
write to a database. To reduce networking overhead, we eventually decide to buffer results, so we can issue a single
batch write to the database when the buffer fills up. It's easy to picture doing this synchronously, ie the same thread
that processes units of work also adds results to the buffer, and runs a batch write when the buffer fills.

``` java
int bufferCapacity = 1000;
List<Result> buffer = new ArrayList<>(bufferCapacity);
for (Callable<Result> unit : work) {
    Result result = unit.call();
    buffer.add(result);
    if (buffer.size() == bufferCapacity) {
        database.batchWrite(buffer);
        buffer.clear();
    }
}
if (!buffer.isEmpty()) {
    database.batchWrite(buffer);
}
```

But using one thread means we aren't processing work while we are writing to the database. We can take a big step by
running the batch writes asynchronously, in another thread, so that the first thread can continue processing work and
buffering results for the next batch write (at least until it fills the next buffer), while the previous batch write
runs. (If you checked out Colin's blog, he assumes asynchronous and overlapping writes from the start. In my experience,
people often stop after synchronous batch writes, to avoid dealing with concurrency.) Next, we might observe periods of
slow work where the buffer is not filling up, so batch writes are not being issued, so buffered results are not
appearing in the database timely. To improve consistency, we can take one more step and add a timing component to issue
a batch write if results have been buffered too long, even if the buffer is not full.

After fiddling with different approaches to this problem for some time, I had a first-approximation solution that looked
something like this:

``` java
try (var scope = new StructuredTaskScope<>()) {
    long bufferTimeout = Duration.ofSeconds(5).toNanos();
    int bufferCapacity = 1000;
    List<Result> buffer = new ArrayList<>(bufferCapacity);
    ReentrantLock lock = new ReentrantLock();
    Condition bufferFlushed = lock.newCondition();
    Condition bufferFilled = lock.newCondition();
    var signal = new Object() { boolean completed = false; boolean cancelled = false; };
    
    Subtask<Void> producer = scope.fork(() -> {
        try {
            for (Callable<Result> unit : work) {
                // Process a unit of work
                Result result = unit.call();
                lock.lockInterruptibly();
                try {
                    // Stop early if downstream cancelled
                    if (signal.cancelled) {
                        return null;
                    }
                    // Wait for full buffer to be flushed, or cancellation
                    while (buffer.size() == bufferCapacity) {
                        bufferFlushed.await();
                        if (signal.cancelled) {
                            return null;
                        }
                    }
                    // Add to buffer and signal downstream if full
                    buffer.add(result);
                    if (buffer.size() == bufferCapacity) {
                        bufferFilled.signal();
                    }
                } finally {
                    lock.unlock();
                }
            }
            return null;
        } finally {
            // Signal downstream we are done (possibly due to an exception)
            lock.lock();
            try {
                signal.completed = true;
                bufferFilled.signal();
            } finally {
                lock.unlock();
            }
        }
    });
    
    Subtask<Void> consumer = scope.fork(() -> {
        long nextDeadline = System.nanoTime() + bufferTimeout;
        try {
            for (;;) {
                List<Result> copyBuffer;
                lock.lockInterruptibly();
                try {
                    // Stop when upstream is done and buffer is empty
                    if (signal.completed && buffer.isEmpty()) {
                        return null;
                    }
                    // Wait for buffer to fill up, or timeout, or completion
                    long nanosRemaining = nextDeadline - System.nanoTime();
                    while (buffer.size() != bufferCapacity && nanosRemaining > 0 && !signal.completed) {
                        nanosRemaining = bufferFilled.awaitNanos(nanosRemaining);
                    }
                    // Compute next deadline, replace the buffer, and signal upstream
                    nextDeadline = System.nanoTime() + bufferTimeout;
                    copyBuffer = List.copyOf(buffer);
                    buffer.clear();
                    bufferFlushed.signal();
                } finally {
                    lock.unlock();
                }
                // Flush the buffer
                if (!copyBuffer.isEmpty()) {
                    database.batchWrite(copyBuffer);
                }
            }
        } finally {
            // Signal upstream we are done (possibly due to an exception)
            lock.lock();
            try {
                signal.cancelled = true;
                bufferFlushed.signal();
            } finally {
                lock.unlock();
            }
        }
    });
    
    scope.join();
}
```

Now let's address the elephants in the room. This is a lot of code to take in, and it's explicitly handling multiple
threads, a lock, Conditions with timed and untimed waits, nested `try-finally`s, and a shared buffer and flags. It is
complex, rigid, and a breeding ground for subtle bugs. Lacking better abstractions, it's hard to say that an approach
like this beats Reactive Streams.

However, this pretty much gets the desired behavior. Ideally we would probably have the `producer` set the next deadline
when the first element is added to an empty buffer, instead of the `consumer` setting it upon flush. And exception
handling is iffy at this point - we can inspect the Subtasks after joining, and we do make sure to set `completed` or
`cancelled` flags so that the other side doesn't deadlock, but we don't catch exceptions or try to communicate them to
the other side. Other than that, we have an implementation that concurrently runs batch writes while processing work,
flushes the buffer periodically in case it doesn't fill up, and even avoids accidentally deadlocking itself. This is
also explicit imperative code, that we can stick breakpoints in and debug, or dump meaningful stack traces from, without
the threads completely changing context on us. Maybe, with some better abstractions, this could be promising?

I began working through other example operators (borrowed largely from Akka Streams), especially timing-related
operators, as those would likely require asynchronous boundaries akin to the previous example. I tried simplifying, and
found that it worked well to specify the 'producer' and 'consumer' in terms of what ran inside the critical section,
thus factoring out a loop and lock, and implicitly synchronizing access to shared state. Then I factored out the
Conditions and waiting, by using deadlines to communicate when each side was allowed to wake up and enter its critical
section. When operators didn't care about exact timing, they could use `Instant.MIN` to mean 'now', or `Instant.MAX` to
mean 'never'. These usability improvements trimmed-down my implementations, and formed the basis of what became
`DeadlineSegue`. (Yes, `DeadlineSegue` came before even `Source` and `Sink`.)

The first Segue to fall out of my original motivating example is called `batch`. It is designed to be more flexible than
that example needed, but today, a solution using `batch` could look something like this:

``` java
try (var scope = new FailureHandlingScope(exceptionHandler)) {
    int bufferCapacity = 1000;
    Iterator<Callable<Result>> iter = work.iterator();
    Belt.StepSource<Result> source = () -> iter.hasNext() ? iter.next().call() : null;
    
    source
        .andThen(Belts.batch(
            () -> new ArrayList<Result>(bufferCapacity),
            List::add,
            buffer -> buffer.size() == 1 ? Optional.of(Instant.now().plusSeconds(5))
                    : buffer.size() == bufferCapacity ? Optional.of(Instant.MIN)
                    : Optional.empty()
        ))
        .andThen(database::batchWrite)
        .run(Belts.scopeExecutor(scope));
    
    scope.join();
}
```

Notice how we kinda sorta became declarative again after all. I'll talk about that soon.
