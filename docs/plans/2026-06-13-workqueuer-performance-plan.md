# WorkQueuer Performance Improvements Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Apply the approved performance design to `Figlotech.Core/WorkQueuer.cs`, reducing allocations, adding bounded-channel backpressure, and improving disposal safety while preserving `ActiveTaskStat()` semantics and public API compatibility.

**Architecture:** Keep the existing `WorkQueuer` structure but replace the unbounded channel with a configurable bounded channel, reuse the scheduler timer, optimize cancellation-token linking, guard hot-path logging, and clean up timing math. Add an `EnqueueAsync` async enqueue path alongside existing synchronous `Enqueue` overloads.

**Tech Stack:** C# 10, .NET Standard 2.1, `System.Threading.Channels`, `System.Diagnostics.Stopwatch`.

---

## Task 1: Initialize bounded channel with configurable capacity

**Files:**
- Modify: `Figlotech.Core/WorkQueuer.cs:203-207`
- Test: `Figlotech.Core.Tests/WorkQueuerTests.cs` (add a new test)

**Step 1: Write the failing test**

Add to `Figlotech.Core.Tests/WorkQueuerTests.cs`:

```csharp
[Fact]
public void ChannelCapacity_Default_IsBounded() {
    var queuer = new WorkQueuer("BoundedTest", 2);
    // default capacity = MaxParallelTasks * 4 = 8
    for (int i = 0; i < 8; i++) {
        queuer.Enqueue(async () => await Task.Delay(Timeout.Infinite, CancellationToken.None));
    }
    Assert.Throws<InvalidOperationException>(() => queuer.Enqueue(() => Fi.EmptyValueTask));
    queuer.Dispose();
}
```

**Step 2: Run test to verify it fails**

Run: `dotnet test Figlotech.Core.Tests/Figlotech.Core.Tests.csproj --filter "FullyQualifiedName~ChannelCapacity_Default_IsBounded"`

Expected: FAIL because the channel is currently unbounded.

**Step 3: Implement bounded channel**

In `Figlotech.Core/WorkQueuer.cs`:

1. Remove the eager field initialization:
   ```csharp
   private readonly Channel<WorkJobExecutionRequest> _workChannel = Channel.CreateUnbounded<WorkJobExecutionRequest>(...);
   ```
2. Replace with a lazy field and a helper:
   ```csharp
   private Channel<WorkJobExecutionRequest> _workChannel;
   private readonly object _channelLock = new object();

   public int ChannelCapacity { get; set; } = 0;

   private int EffectiveChannelCapacity() {
       var cap = ChannelCapacity;
       if (cap <= 0) {
           cap = Math.Max(1, EffectiveParallelLimit()) * 4;
       }
       return cap;
   }

   private Channel<WorkJobExecutionRequest> GetOrCreateChannel() {
       if (_workChannel != null) return _workChannel;
       lock (_channelLock) {
           if (_workChannel != null) return _workChannel;
           _workChannel = Channel.CreateBounded<WorkJobExecutionRequest>(new BoundedChannelOptions(EffectiveChannelCapacity()) {
               FullMode = BoundedChannelFullMode.Wait,
               SingleReader = false,
               SingleWriter = false,
               AllowSynchronousContinuations = false
           });
           return _workChannel;
       }
   }
   ```
3. In `Start()`, ensure the channel exists:
   ```csharp
   GetOrCreateChannel();
   ```
4. Replace every `_workChannel.Reader` / `_workChannel.Writer` usage with `GetOrCreateChannel().Reader` / `Writer`.

**Step 4: Run test to verify it passes**

Run: `dotnet test Figlotech.Core.Tests/Figlotech.Core.Tests.csproj --filter "FullyQualifiedName~ChannelCapacity_Default_IsBounded"`

Expected: PASS

**Step 5: Commit**

```bash
git add Figlotech.Core/WorkQueuer.cs Figlotech.Core.Tests/WorkQueuerTests.cs
git commit -m "feat(WorkQueuer): add configurable bounded channel with default capacity"
```

---

## Task 2: Add asynchronous EnqueueAsync path

**Files:**
- Modify: `Figlotech.Core/WorkQueuer.cs:514-560`
- Test: `Figlotech.Core.Tests/WorkQueuerTests.cs`

**Step 1: Write the failing test**

```csharp
[Fact]
public async Task EnqueueAsync_Waits_When_Channel_Full() {
    var queuer = new WorkQueuer("EnqueueAsyncTest", 1) { ChannelCapacity = 1 };
    var gate = new TaskCompletionSource<object>();
    queuer.Enqueue(async () => await gate.Task);

    var secondJobTask = queuer.EnqueueTask(async () => { });
    // give the worker time to pick the first job
    await Task.Delay(100);
    Assert.Equal(1, queuer.InQueue);

    gate.SetResult(null);
    await secondJobTask;
    queuer.Dispose();
}
```

Expected: FAIL because `EnqueueAsync` does not exist.

**Step 2: Implement EnqueueAsync**

Add these overloads in `WorkQueuer.cs`:

```csharp
public async Task<WorkJobExecutionRequest> EnqueueAsync(WorkJob job, CancellationToken? requestCancellation = null) {
    var request = new WorkJobExecutionRequest(job, requestCancellation) {
        EnqueuedTime = DateTime.UtcNow,
        Status = WorkJobRequestStatus.Queued,
        WorkQueuer = this
    };
    if (FiTechCoreExtensions.DebugTasks) {
        request.StackTrace = new StackTrace();
    }

    Interlocked.Increment(ref _totalWorkInternal);

    if (IsClosed) {
        request.Status = WorkJobRequestStatus.Failed;
        request._tcsNotifyDequeued.TrySetException(new ObjectDisposedException(nameof(WorkQueuer), $"WorkQueuer \"{Name}\" has been disposed."));
        request.TaskCompletionSource.TrySetException(new ObjectDisposedException(nameof(WorkQueuer), $"WorkQueuer \"{Name}\" has been disposed."));
        return request;
    }

    if (Active) {
        await WriteQueuedJobAsync(request).ConfigureAwait(false);
    } else {
        HeldJobs.Enqueue(request);
    }

    _ = SafeInvoke(OnWorkEnqueued, request);
    return request;
}

private async Task WriteQueuedJobAsync(WorkJobExecutionRequest job) {
    Interlocked.Increment(ref _inQueueInternal);
    try {
        var channel = GetOrCreateChannel();
        await channel.Writer.WaitToWriteAsync(_runCts.Token).ConfigureAwait(false);
        if (!channel.Writer.TryWrite(job)) {
            throw new InvalidOperationException($"Unable to queue work item on \"{Name}\".");
        }
    } catch {
        Interlocked.Decrement(ref _inQueueInternal);
        throw;
    }
    EnsureWorkerCapacityForDemand();
}
```

Also add a convenience overload:

```csharp
public Task<WorkJobExecutionRequest> EnqueueTaskAsync(Func<CancellationToken, ValueTask> a, Func<Exception, ValueTask> exceptionHandler = null, Func<bool, ValueTask> finished = null) {
    var retv = new WorkJob(a, exceptionHandler, finished);
    return EnqueueAsync(retv);
}
```

**Step 3: Run test to verify it passes**

Run: `dotnet test Figlotech.Core.Tests/Figlotech.Core.Tests.csproj --filter "FullyQualifiedName~EnqueueAsync_Waits_When_Channel_Full"`

Expected: PASS

**Step 4: Commit**

```bash
git add Figlotech.Core/WorkQueuer.cs Figlotech.Core.Tests/WorkQueuerTests.cs
git commit -m "feat(WorkQueuer): add async enqueue path for bounded backpressure"
```

---

## Task 3: Reuse scheduler timer

**Files:**
- Modify: `Figlotech.Core/WorkQueuer.cs:252`, `Figlotech.Core/WorkQueuer.cs:765-798`, `Figlotech.Core/WorkQueuer.cs:1022-1033`

**Step 1: Write the failing test**

There is no direct public API for the timer. Add an internal-observation test or skip if not feasible. Instead, rely on build + existing scheduling tests.

**Step 2: Implement timer reuse**

1. Change declaration:
   ```csharp
   private Timer _consolidatedTimer;
   ```
   to:
   ```csharp
   private Timer _consolidatedTimer;
   private readonly object _timerLock = new object();
   ```
2. In `RescheduleConsolidatedTimerUnsafe`:
   ```csharp
   private void RescheduleConsolidatedTimerUnsafe() {
       if (_scheduledTaskQueue.Count == 0) {
           _consolidatedTimer?.Change(Timeout.Infinite, Timeout.Infinite);
           return;
       }

       var nextEntry = _scheduledTaskQueue.Min;
       var now = DateTime.UtcNow;
       var delay = nextEntry.ScheduledTime - now;
       var delayMs = Math.Max(0, (long)delay.TotalMilliseconds);
       const int maxTimerIntervalMs = 60000;
       var timerDelay = (int)Math.Min(delayMs, maxTimerIntervalMs);

       if (_consolidatedTimer == null) {
           _consolidatedTimer = new Timer(_ => OnConsolidatedTimerFired(), null, timerDelay, Timeout.Infinite);
       } else {
           _consolidatedTimer.Change(timerDelay, Timeout.Infinite);
       }
   }
   ```
3. In `DisposeScheduledTasks`, dispose once:
   ```csharp
   _consolidatedTimer?.Change(Timeout.Infinite, Timeout.Infinite);
   _consolidatedTimer?.Dispose();
   _consolidatedTimer = null;
   ```

**Step 3: Run scheduling tests**

Run: `dotnet test Figlotech.Core.Tests/Figlotech.Core.Tests.csproj --filter "FullyQualifiedName~Scheduling"`

Expected: PASS

**Step 4: Commit**

```bash
git add Figlotech.Core/WorkQueuer.cs
git commit -m "perf(WorkQueuer): reuse scheduler timer instead of recreating it"
```

---

## Task 4: Skip linked CancellationTokenSource when no external token

**Files:**
- Modify: `Figlotech.Core/WorkQueuer.cs:596-604`

**Step 1: Write the test**

```csharp
[Fact]
public async Task Job_Without_External_Cancellation_Runs() {
    var queuer = new WorkQueuer("NoExternalCts", 1);
    var tcs = new TaskCompletionSource<bool>();
    var job = queuer.EnqueueTask(async () => {
        tcs.SetResult(true);
        await Fi.EmptyValueTask;
    });
    await job;
    Assert.True(tcs.Task.Result);
    queuer.Dispose();
}
```

**Step 2: Implement optimization**

Replace:
```csharp
CancellationTokenSource ctsLinked = null;
try {
    ctsLinked = CancellationTokenSource.CreateLinkedTokenSource(job.Cancellation.Token, job.RequestCancellation);
    job.WorkJob.ActionTask = job.WorkJob.action(ctsLinked.Token);
    await job.WorkJob.ActionTask.ConfigureAwait(false);
} finally {
    ctsLinked?.Dispose();
}
```

With:
```csharp
if (job.RequestCancellation.CanBeCanceled) {
    using var ctsLinked = CancellationTokenSource.CreateLinkedTokenSource(job.Cancellation.Token, job.RequestCancellation);
    await job.WorkJob.action(ctsLinked.Token).ConfigureAwait(false);
} else {
    await job.WorkJob.action(job.Cancellation.Token).ConfigureAwait(false);
}
```

**Step 3: Run tests**

Run: `dotnet test Figlotech.Core.Tests/Figlotech.Core.Tests.csproj --filter "FullyQualifiedName~NoExternalCts"`

Expected: PASS

**Step 4: Commit**

```bash
git add Figlotech.Core/WorkQueuer.cs Figlotech.Core.Tests/WorkQueuerTests.cs
git commit -m "perf(WorkQueuer): skip linked CancellationTokenSource when no external token"
```

---

## Task 5: Optimize worker scaling

**Files:**
- Modify: `Figlotech.Core/WorkQueuer.cs:191-243`, `Figlotech.Core/WorkQueuer.cs:394-427`, `Figlotech.Core/WorkQueuer.cs:511`

**Step 1: Cache EffectiveParallelLimit**

1. Add fields:
   ```csharp
   private int _effectiveParallelLimit;
   private void RefreshEffectiveParallelLimit() {
       _effectiveParallelLimit = Math.Min(AbsoluteMaxParallelLimit, Math.Max(MaxParallelTasks, 1));
   }
   ```
2. Replace `EffectiveParallelLimit()` body:
   ```csharp
   private int EffectiveParallelLimit() => _effectiveParallelLimit;
   ```
3. Refresh in the constructor and whenever `MaxParallelTasks` / `AbsoluteMaxParallelLimit` changes. Because `MaxParallelTasks` is an auto-property, convert it to:
   ```csharp
   private int _maxParallelTasks;
   public int MaxParallelTasks {
       get => _maxParallelTasks;
       set {
           _maxParallelTasks = value;
           RefreshEffectiveParallelLimit();
       }
   }
   ```
4. For `AbsoluteMaxParallelLimit`, the static setter cannot easily refresh every instance; refresh at construction and at the start of `EnsureWorkerCapacityForDemand` / `EnsureWorkers` as a safety fallback.

**Step 2: Use integer math and ticks in EnsureWorkerCapacityForDemand**

```csharp
private void EnsureWorkerCapacityForDemand() {
    if (!IsRunning || _runCts == null || _runCts.IsCancellationRequested) return;

    var lastScaleTicks = Interlocked.Read(ref _lastWorkerScaleTicks);
    var nowTicks = DateTime.UtcNow.Ticks;
    if ((nowTicks - lastScaleTicks) < TimeSpan.FromMilliseconds(MinScaleIntervalMs).Ticks) {
        return;
    }

    var limit = EffectiveParallelLimit();
    var currentWorkers = Volatile.Read(ref _numberOfActualWorkers);
    var demand = Volatile.Read(ref _executingInternal) + Volatile.Read(ref _inQueueInternal);

    var desiredWorkers = Math.Max(InitialWorkerCount(), Math.Min(limit, demand));
    if (desiredWorkers > currentWorkers && desiredWorkers * 2 > currentWorkers * 3) {
        Interlocked.Exchange(ref _lastWorkerScaleTicks, nowTicks);
        EnsureWorkers(desiredWorkers);
    }
}
```

**Step 3: Run existing scaling tests**

Run: `dotnet test Figlotech.Core.Tests/Figlotech.Core.Tests.csproj --filter "FullyQualifiedName~Scale"`

Expected: PASS

**Step 4: Commit**

```bash
git add Figlotech.Core/WorkQueuer.cs
git commit -m "perf(WorkQueuer): cache parallel limit and use integer math for scaling"
```

---

## Task 6: Guard hot-path logging

**Files:**
- Modify: `Figlotech.Core/WorkQueuer.cs:487`, `501`, `618`, `673`, `710`, `712`

**Step 1: Add helper**

```csharp
private static bool WorkQueuerLogEnabled => FiTechCoreExtensions.EnableStdoutLogs && FiTechCoreExtensions.EnabledSystemLogs["FTH:WorkQueuer"];
```

**Step 2: Guard each call**

Change each:
```csharp
Fi.Tech.WriteLineInternal("FTH:WorkQueuer", state, Formatter);
```
to:
```csharp
if (WorkQueuerLogEnabled) {
    Fi.Tech.WriteLineInternal("FTH:WorkQueuer", state, Formatter);
}
```

**Step 3: Run build**

Run: `dotnet build figlotech.sln`

Expected: BUILD SUCCEEDED

**Step 4: Commit**

```bash
git add Figlotech.Core/WorkQueuer.cs
git commit -m "perf(WorkQueuer): guard hot-path logging to avoid closure allocations"
```

---

## Task 7: Clean up Stopwatch timing and metrics

**Files:**
- Modify: `Figlotech.Core/WorkQueuer.cs:236-239`, `Figlotech.Core/WorkQueuer.cs:562-708`

**Step 1: Add cached frequency ratio / helper**

```csharp
private static readonly double StopwatchTickToTimeSpanTickRatio = (double)TimeSpan.TicksPerSecond / Stopwatch.Frequency;

private static TimeSpan ElapsedFromTimestamps(long start, long end) {
#if NET6_0_OR_GREATER
    return Stopwatch.GetElapsedTime(start, end);
#else
    return TimeSpan.FromTicks((long)((end - start) * StopwatchTickToTimeSpanTickRatio));
#endif
}
```

**Step 2: Replace elapsed math**

In `ExecuteJob` finally block:
```csharp
var elapsed = ElapsedFromTimestamps(startTimestamp, endTimestamp);
```

**Step 3: Atomic AverageTaskResolutionTime**

```csharp
public TimeSpan AverageTaskResolutionTime {
    get {
        var total = Interlocked.Read(ref _totalTaskResolutionTimeTicks);
        var done = Volatile.Read(ref _workDoneInternal);
        return done > 0 ? TimeSpan.FromTicks(total / done) : TimeSpan.Zero;
    }
}
```

**Step 4: Run build and tests**

Run: `dotnet test Figlotech.Core.Tests/Figlotech.Core.Tests.csproj --filter "FullyQualifiedName~Average"`

Expected: PASS (or no such test; then run full WorkQueuer tests)

**Step 5: Commit**

```bash
git add Figlotech.Core/WorkQueuer.cs
git commit -m "perf(WorkQueuer): improve timing math and atomic average metric"
```

---

## Task 8: Remove WorkJob.ActionTask field

**Files:**
- Modify: `Figlotech.Core/WorkQueuer.cs:114-145`, `Figlotech.Core/WorkQueuer.cs:596-604`

**Step 1: Remove field**

Remove:
```csharp
public ValueTask ActionTask { get; internal set; }
```

**Step 2: Update execution site**

Already done in Task 4; ensure the action is awaited as a local `ValueTask`.

**Step 3: Run build**

Run: `dotnet build figlotech.sln`

Expected: BUILD SUCCEEDED

**Step 4: Commit**

```bash
git add Figlotech.Core/WorkQueuer.cs
git commit -m "refactor(WorkQueuer): remove unused ValueTask storage field"
```

---

## Task 9: Improve Dispose behavior

**Files:**
- Modify: `Figlotech.Core/WorkQueuer.cs:1036-1064`

**Step 1: Implement safer Dispose**

```csharp
public void Dispose() {
    IsClosed = true;
    try {
        _runCts?.Cancel();
        using (var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30))) {
            try {
                Stop(true).Wait(cts.Token);
            } catch (OperationCanceledException) {
                // timeout reached, force cancellation again
                _runCts?.Cancel();
            }
        }
    } catch (Exception) {
        // Ignore exceptions during dispose
    } finally {
        DisposeHeldJobs();
        DisposeScheduledTasks();
        _workChannel?.Writer.TryComplete();
        _runCts?.Dispose();
    }
}

public async ValueTask DisposeAsync() {
    IsClosed = true;
    try {
        _runCts?.Cancel();
        await Stop(true).ConfigureAwait(false);
    } catch (Exception) {
        // Ignore exceptions during dispose
    } finally {
        DisposeHeldJobs();
        DisposeScheduledTasks();
        _workChannel?.Writer.TryComplete();
        _runCts?.Dispose();
    }
}
```

**Step 2: Run dispose tests**

Run: `dotnet test Figlotech.Core.Tests/Figlotech.Core.Tests.csproj --filter "FullyQualifiedName~Dispose"`

Expected: PASS

**Step 3: Commit**

```bash
git add Figlotech.Core/WorkQueuer.cs
git commit -m "fix(WorkQueuer): make Dispose safer and DisposeAsync preferred"
```

---

## Task 10: Final verification

**Step 1: Build solution**

Run: `dotnet build figlotech.sln`

Expected: BUILD SUCCEEDED

**Step 2: Run all WorkQueuer tests**

Run: `dotnet test Figlotech.Core.Tests/Figlotech.Core.Tests.csproj --filter "FullyQualifiedName~WorkQueuer"`

Expected: ALL PASS

**Step 3: Run full test suite**

Run: `dotnet test Figlotech.Core.Tests/Figlotech.Core.Tests.csproj`

Expected: ALL PASS

**Step 4: Commit any remaining changes**

```bash
git add .
git status
git commit -m "chore(WorkQueuer): finalize performance improvements"
```

---

## Notes
- The design document is at `docs/plans/2026-06-13-workqueuer-performance-design.md`.
- Preserve existing brace style (K&R) and 4-space indentation.
- Do not add comments unless necessary for clarity.
- No external analyzers or formatters.
