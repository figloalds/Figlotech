# WorkQueuer lifecycle and resource use

`WorkQueuer` uses `System.Threading.Channels` to dispatch asynchronous work to a
limited pool of workers. Channels remain a good fit here: they provide asynchronous
producer backpressure, cancellation, and direct delivery to waiting readers without
requiring dedicated blocked threads or an additional queue implementation.
See [Microsoft's Channels documentation](https://learn.microsoft.com/en-us/dotnet/core/extensions/channels).

## Waiting and shutdown

Workers await a one-time channel-initialization signal before the first enqueue,
then await `ChannelReader.ReadAsync`. Producers use `ChannelWriter.WriteAsync` for
asynchronous enqueueing. These waits suspend tasks; there is no application-level
spin loop, periodic delay, or polling timer for queue work. Direct reads also avoid
waking all idle workers to compete for one item.

`Stop(true)` stops admission, cancels pending asynchronous writes, and drains
already-published work. `Stop(false)` cancels buffered work and waits for active
actions. It does not forcibly interrupt active user code. Reader cancellation is
separate from producer cancellation, allowing readers to remain asleep while the
last active job finishes. `Start()` can restart a fully stopped queue.

Drain accounting covers accepted held work, pending writes, dequeue transitions,
and active jobs. Concurrent calls to `WaitForIdleAsync` share the current drain
signal. Work accepted before `Start` remains held until startup or is terminally
rejected by shutdown. Awaiting idle with held work therefore requires starting or
stopping the queue. An idle notification describes the current drain; later
enqueueing begins another one.

Workers grow to meet demand, up to the effective parallel limit, without waiting
for a later enqueue to trigger a scaling timer. The pool retains its suspended
workers until shutdown; worker tasks are not dedicated operating-system threads.

## Memory bounds

`ChannelCapacity` is captured when the channel is first created. Its default of
zero retains the existing **unbounded buffer** behavior. Set a positive capacity
and await each `EnqueueAsync` call to apply backpressure. Synchronous enqueueing
throws if the bounded buffer is full.

The buffer bound is not a bound on all memory associated with the queuer:

- Active actions retain their own state, up to the worker count.
- Work accepted before startup is held outside the channel.
- Each concurrent producer waiting for admission retains its request. Launching
  unlimited enqueue tasks without awaiting them defeats the memory benefit.
- Schedules and caller-retained execution requests also retain their payloads.

For a started queue with capacity B, W active workers, and P producers waiting
for admission, retained work is roughly B + W + P, plus schedules and any held
work. Controlling producer concurrency is therefore part of bounding memory.

`BlockingCollection` would require blocked threads for this async workload.
A custom concurrent queue plus semaphore would duplicate synchronization and
cancellation logic already supplied by Channels. A dataflow framework may help
with larger processing graphs, but adds no necessary primitive for this queue.

## Scheduling and callbacks

Schedules use one timer. Stopped queues suspend scheduling and reconcile overdue
entries on startup: `FireIfMissed` runs a missed entry once; otherwise one-shot
entries are discarded and recurring entries advance to their next occurrence.
Recurrence intervals must be positive, options are copied on registration, and
missed intervals are skipped arithmetically rather than by looping.

Scheduled admission uses asynchronous backpressure. Each schedule has at most
one execution or pending admission in flight. Replacing a running schedule keeps
the replacement's registration independent of the old execution's cleanup.

Every async event subscriber is awaited individually. Enqueue notifications
remain asynchronous with respect to the enqueue call; dequeue and completion
notifications are awaited by the worker. The `finished` callback runs on both
successful and failed actions, whether or not an error handler is installed.

## Verification and measurements

On 2026-09-07, `dotnet build figlotech.sln` succeeded with warnings, and all 118
tests in `Figlotech.Core.Tests` passed. Regression coverage includes the queue,
scheduling, callback, and cancellation defects, plus a `ParallelFlow` race where
a fast source completed before a downstream stage was attached.

An isolated Debug/.NET 10 process comparison used 16 queues with 16 workers each,
a 250 ms warmup, then a one-second sample. Logging and debug stack capture were
disabled. Counts are process-wide, including library static queues. Allocations
were measured with `GC.GetTotalAllocatedBytes(true)` and completed work items
with `ThreadPool.CompletedWorkItemCount`.

| Scenario | Original work items | Revised work items | Original allocations | Revised allocations |
| --- | ---: | ---: | ---: | ---: |
| Before first enqueue | 18,673 | 1 | 3,439,448 B | 2,736 B |
| Idle after one job per queue | 2,018 | 1 | 372,072 B | 744 B |
| Stopping, one active job per queue | 17,658 | 1 | 3,006,440 B | 1,312 B |

The single revised work item is the measurement delay's continuation. The
original 2 ms delays were timer-based polling, not continuous CPU spin, but still
created repeated allocations and thread-pool work. These samples demonstrate
removal of that overhead; they are not throughput benchmarks or precise CPU
measurements, and they do not measure memory retained by application payloads.
