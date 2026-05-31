# WorkQueuer Fixes Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix all identified race conditions, efficiency issues, and resource leaks in WorkQueuer.cs

**Architecture:** Group fixes into 4 independent domains (Scheduling, Lifecycle, Workers, Enqueue) to enable parallel implementation. Each domain has isolated changes with minimal cross-file impact.

**Tech Stack:** .NET/C#, Xunit, System.Threading.Channels

---

## Pre-Investigation: Global Timer Architecture

The codebase ALREADY uses a consolidated global timer (`_consolidatedTimer`) with a `SortedSet<ScheduledTaskEntry>` priority queue. This is the correct architecture. The issues are bugs in this existing implementation, not a need to redesign it. **Do NOT redesign the timer architecture.** Fix the existing one.

---

## Domain A: Scheduling Subsystem (Issues 1, 2, 5)
**Agent:** `fix-scheduling-races`
**Files:** `Figlotech.Core/WorkQueuer.cs:913-922, 771-843, 745-769`

### Task A1: Fix Race Condition in UnscheduleInternal

**Problem:** `_scheduledTasks` dictionary is mutated outside the lock in `UnscheduleInternal`.

**Current code (line 913-922):**
```csharp
private void UnscheduleInternal(string identifier) {
    if (_scheduledTasks.TryGetValue(identifier, out var entry)) {
        entry.Cancellation?.Cancel();
        lock (_scheduledTasksLock) {
            _scheduledTaskQueue.Remove(entry);
        }
        entry.Cancellation?.Dispose();
        _scheduledTasks.Remove(identifier);  // BUG: Outside lock!
    }
}
```

**Fix:** Move all dictionary operations inside the lock:
```csharp
private void UnscheduleInternal(string identifier) {
    if (_scheduledTasks.TryGetValue(identifier, out var entry)) {
        entry.Cancellation?.Cancel();
        lock (_scheduledTasksLock) {
            _scheduledTaskQueue.Remove(entry);
            _scheduledTasks.Remove(identifier);
        }
        entry.Cancellation?.Dispose();
    }
}
```

**Step 1:** Modify `UnscheduleInternal` to move `_scheduledTasks.Remove` inside the lock
**Step 2:** Add a test that concurrently unschedules the same identifier from multiple threads
**Step 3:** Run tests - `dotnet test --filter "FullyQualifiedName~ScheduleTaskTests"`

### Task A2: Add Disposed State Guard to Timer Callback

**Problem:** `OnConsolidatedTimerFired` can run after `Dispose()` has started.

**Current code (line 771):**
```csharp
private void OnConsolidatedTimerFired() {
    // No guard against disposed state
```

**Fix:** Add early return if disposed or not running:
```csharp
private void OnConsolidatedTimerFired() {
    if (_isClosed || !_isRunning) return;
    // ... rest of method
```

**Step 1:** Add disposed guard at the start of `OnConsolidatedTimerFired`
**Step 2:** Add a test that disposes the queuer while a scheduled task is about to fire
**Step 3:** Run tests

### Task A3: Reduce Timer Precision Waste

**Problem:** Timer capped at 1 minute causes unnecessary fires.

**Current code (line 759):**
```csharp
const int maxTimerIntervalMs = 60000;
var timerDelay = (int)Math.Min(delayMs, maxTimerIntervalMs);
```

**Fix:** Increase cap or calculate exact delay. Since the timer callback already handles multiple due entries, the 1-minute cap is actually defensive for clock changes. However, for schedules within the next few minutes, we should fire more precisely. Keep the cap but make it configurable or larger (e.g., 5 minutes).

Actually, re-reading the code - the timer callback processes ALL entries that are due in a while loop. The 1-minute cap is fine as a safety measure. The real issue is the precision window of 50ms. **Leave the timer interval as-is** - it's defensive programming. Focus on the race condition fixes instead.

**Decision:** Skip Task A3. The 1-minute cap is intentional defensive programming.

---

## Domain B: Core Queuer Lifecycle & Drain (Issues 3, 8, 9)
**Agent:** `fix-lifecycle-races`
**Files:** `Figlotech.Core/WorkQueuer.cs:291-318, 475-480, 504-511`

### Task B1: Fix _drainTcs Race in Stop

**Problem:** Multiple concurrent `Stop()` calls can create separate TCS instances.

**Current code (line 291-318):**
```csharp
public async Task Stop(bool wait = true) {
    if (!IsRunning) return;
    _drainOnStop = wait;
    if (wait) {
        if (Volatile.Read(ref _inQueueInternal) > 0 || !ActiveJobs.IsEmpty) {
            var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            _drainTcs = tcs;  // Race: two threads could overwrite each other
            // ...
        }
    }
}
```

**Fix:** Use `Interlocked.CompareExchange`:
```csharp
public async Task Stop(bool wait = true) {
    if (!IsRunning) return;
    _drainOnStop = wait;
    if (wait) {
        if (Volatile.Read(ref _inQueueInternal) > 0 || !ActiveJobs.IsEmpty) {
            var tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var existing = Interlocked.CompareExchange(ref _drainTcs, tcs, null);
            if (existing != null) {
                // Another thread already set it, wait on that one
                await existing.Task.ConfigureAwait(false);
                return;
            }
            // Double-check
            if (Volatile.Read(ref _inQueueInternal) <= 0 && ActiveJobs.IsEmpty) {
                tcs.TrySetResult(true);
            }
            await tcs.Task.ConfigureAwait(false);
            Interlocked.Exchange(ref _drainTcs, null);
        }
    }
}
```

**Step 1:** Modify `Stop` to use `Interlocked.CompareExchange` for `_drainTcs`
**Step 2:** Add a test that calls `Stop()` concurrently from multiple threads
**Step 3:** Run tests - `dotnet test --filter "FullyQualifiedName~WorkQueuerReliabilityTests"`

### Task B2: Fix SignalDrainIfComplete Negative Count Check

**Problem:** `<= 0` suggests `_inQueueInternal` can go negative.

**Current code (line 477):**
```csharp
private void SignalDrainIfComplete() {
    var tcs = _drainTcs;
    if (tcs != null && Volatile.Read(ref _inQueueInternal) <= 0 && ActiveJobs.IsEmpty) {
        tcs.TrySetResult(true);
    }
}
```

**Investigation needed:** Find where double-decrement could occur. Check `ProcessQueuedJob` (line 443-454) - if a job is cancelled before execution, it decrements `_inQueueInternal`. If `WriteQueuedJob` also decrements on failure... actually `WriteQueuedJob` only decrements on channel write failure, which is rare.

**Fix:** Change to `== 0` and add an assertion/debug check for negative values:
```csharp
private void SignalDrainIfComplete() {
    var tcs = _drainTcs;
    var inQueue = Volatile.Read(ref _inQueueInternal);
    if (inQueue < 0 && Debugger.IsAttached) {
        Debugger.Break(); // Catch counting bugs in debug
    }
    if (tcs != null && inQueue <= 0 && ActiveJobs.IsEmpty) {
        tcs.TrySetResult(true);
    }
}
```

Actually, keep `<= 0` as defensive but add a `Debug.Assert` or internal log when negative is detected. This helps catch the root cause without breaking production.

**Step 1:** Add negative value detection/logging to `SignalDrainIfComplete`
**Step 2:** Trace all increment/decrement sites of `_inQueueInternal` to find the bug
**Step 3:** Fix the double-decrement if found
**Step 4:** Run tests

### Task B3: Fix EnqueueAfterDispose Behavior

**Problem:** `SemaphoreDisposed_NoException` test expects no exception, but `EnqueueAfterDispose_FaultsTaskCompletionSource` expects `ObjectDisposedException`.

**Current code (line 504-511):**
```csharp
if (IsClosed) {
    request.Status = WorkJobRequestStatus.Failed;
    request._tcsNotifyDequeued.TrySetException(
        new ObjectDisposedException(nameof(WorkQueuer), $"WorkQueuer \"{Name}\" has been disposed."));
    request.TaskCompletionSource.TrySetException(
        new ObjectDisposedException(nameof(WorkQueuer), $"WorkQueuer \"{Name}\" has been disposed."));
    return request;
}
```

**Analysis:** The `SemaphoreDisposed_NoException` test enqueues after dispose and just asserts `true` - it doesn't actually await the request. The `EnqueueAfterDispose_FaultsTaskCompletionSource` test DOES await and expects the exception. The behavior is consistent: enqueue returns a request, but awaiting it throws. The first test name is misleading.

**Fix:** The test `SemaphoreDisposed_NoException` should be renamed or updated to verify the actual behavior (returns a faulted request, doesn't throw synchronously). The production code is correct.

**Step 1:** Rename `SemaphoreDisposed_NoException` to `EnqueueAfterDispose_ReturnsFaultedRequest` and assert that the request's TaskCompletionSource is faulted
**Step 2:** Run tests

---

## Domain C: Worker Management & Scaling (Issues 4, 6)
**Agent:** `fix-worker-scaling`
**Files:** `Figlotech.Core/WorkQueuer.cs:384-409, 185-187, 376-378`

### Task C1: Fix Over-eager Worker Scaling

**Problem:** `EnsureWorkerCapacityForDemand()` is called on every enqueue and can spawn too many workers.

**Current code (line 384-394):**
```csharp
private void EnsureWorkerCapacityForDemand() {
    if (!IsRunning || _runCts == null || _runCts.IsCancellationRequested) return;
    var limit = EffectiveParallelLimit();
    var currentWorkers = Volatile.Read(ref _numberOfActualWorkers);
    var demand = Volatile.Read(ref _executingInternal) + Volatile.Read(ref _inQueueInternal);
    var desiredWorkers = Math.Max(InitialWorkerCount(), Math.Min(limit, demand));
    if (desiredWorkers > currentWorkers) {
        EnsureWorkers(desiredWorkers);
    }
}
```

**Fix:** Add a threshold - only scale if demand exceeds current workers by some margin or if enough time has passed:
```csharp
private long _lastWorkerScaleTicks = DateTime.UtcNow.Ticks;
private const int MinScaleIntervalMs = 100; // Don't scale more than every 100ms

private void EnsureWorkerCapacityForDemand() {
    if (!IsRunning || _runCts == null || _runCts.IsCancellationRequested) return;
    
    var lastScale = new DateTime(Interlocked.Read(ref _lastWorkerScaleTicks), DateTimeKind.Utc);
    if (DateTime.UtcNow - lastScale < TimeSpan.FromMilliseconds(MinScaleIntervalMs)) {
        return; // Too soon since last scale
    }
    
    var limit = EffectiveParallelLimit();
    var currentWorkers = Volatile.Read(ref _numberOfActualWorkers);
    var demand = Volatile.Read(ref _executingInternal) + Volatile.Read(ref _inQueueInternal);
    
    // Only scale up if demand significantly exceeds current capacity
    var desiredWorkers = Math.Max(InitialWorkerCount(), Math.Min(limit, demand));
    if (desiredWorkers > currentWorkers * 1.5) { // 50% threshold
        Interlocked.Exchange(ref _lastWorkerScaleTicks, DateTime.UtcNow.Ticks);
        EnsureWorkers(desiredWorkers);
    }
}
```

**Step 1:** Add `_lastWorkerScaleTicks` field and `MinScaleIntervalMs` constant
**Step 2:** Modify `EnsureWorkerCapacityForDemand` with rate limiting and threshold
**Step 3:** Add a test that enqueues many jobs rapidly and verifies worker count doesn't spike
**Step 4:** Run tests - `dotnet test --filter "FullyQualifiedName~WorkQueuerTests"`

### Task C2: Optimize ActiveJobs Tracking

**Problem:** `ConcurrentDictionary` is overkill for active job tracking.

**Current code (line 185-187):**
```csharp
private readonly ConcurrentDictionary<int, WorkJobExecutionRequest> ActiveJobs = new ConcurrentDictionary<int, WorkJobExecutionRequest>();
public WorkJobExecutionStat[] ActiveTaskStat() =>
    ActiveJobs.Values.Select(x => new WorkJobExecutionStat(x)).ToArray();
```

**Fix:** Since `ProcessQueuedJob` is single-threaded per worker, we can use a simple list with a lock, or keep `ConcurrentDictionary` but use it more efficiently. Actually, `ActiveTaskStat()` is called externally and needs thread safety. The simplest optimization is to avoid LINQ in the hot path and use a direct array copy:

```csharp
public WorkJobExecutionStat[] ActiveTaskStat() {
    var values = ActiveJobs.Values; // This is a snapshot
    var result = new WorkJobExecutionStat[values.Count];
    int i = 0;
    foreach (var x in values) {
        result[i++] = new WorkJobExecutionStat(x);
    }
    return result;
}
```

Actually, the LINQ is fine for this use case. The bigger issue is that `TryAdd`/`TryRemove` in `ProcessQueuedJob` are called for every job. We could replace with an `Interlocked` counter if we don't need the dictionary for `ActiveTaskStat`. But `ActiveTaskStat` returns stats, not just a count. **Keep ConcurrentDictionary** but add a fast path for just counting:

```csharp
private int _activeJobCount;
public int ActiveJobCount => Volatile.Read(ref _activeJobCount);
```

And update it alongside the dictionary operations. But this adds complexity. **Decision:** Skip this optimization - the ConcurrentDictionary is the right choice for the current API surface.

**Step 1:** Leave ActiveJobs as-is - the overhead is acceptable for the functionality provided
**Step 2:** Document the decision in comments

---

## Domain D: Enqueue Path & Locking (Issues 7, 10)
**Agent:** `fix-enqueue-efficiency`
**Files:** `Figlotech.Core/WorkQueuer.cs:182, 346-357, 516-518, 105-111, 645-684`

### Task D1: Replace HeldJobs Lock with ConcurrentQueue

**Problem:** `HeldJobs` lock serializes all enqueues when not active.

**Current code (line 182):**
```csharp
private readonly List<WorkJobExecutionRequest> HeldJobs = new List<WorkJobExecutionRequest>();
```

**Fix:** Use `ConcurrentQueue<WorkJobExecutionRequest>`:
```csharp
private readonly ConcurrentQueue<WorkJobExecutionRequest> HeldJobs = new ConcurrentQueue<WorkJobExecutionRequest>();
```

And update `Enqueue` and `FlushHeldJobsToQueue`:
```csharp
private void FlushHeldJobsToQueue() {
    while (HeldJobs.TryDequeue(out var job)) {
        WriteQueuedJob(job, alreadyCounted: false);
    }
}
```

```csharp
// In Enqueue:
if (Active) {
    WriteQueuedJob(request, alreadyCounted: false);
} else {
    HeldJobs.Enqueue(request);
}
```

**Step 1:** Change `HeldJobs` type to `ConcurrentQueue<WorkJobExecutionRequest>`
**Step 2:** Update `FlushHeldJobsToQueue` to use `TryDequeue`
**Step 3:** Update `Enqueue` to use `Enqueue` instead of lock
**Step 4:** Remove the `lock (HeldJobs)` blocks
**Step 5:** Add a stress test that enqueues while Start() is called
**Step 6:** Run tests

### Task D2: Ensure Request Disposal for Non-executed Jobs

**Problem:** `WorkJobExecutionRequest` resources may leak if never dequeued.

**Current code:** `ExecuteJob` disposes `Cancellation` and `LoggingActivity`, but if a job is held and never flushed (e.g., queuer disposed before start), those resources leak.

**Fix:** Add disposal tracking in `Stop` or `Dispose`:
```csharp
// In FlushHeldJobsToQueue or Dispose:
private void DisposeHeldJobs() {
    while (HeldJobs.TryDequeue(out var job)) {
        job.Dispose();
    }
}
```

And call it in `Dispose`:
```csharp
public void Dispose() {
    IsClosed = true;
    // ... existing dispose code ...
    DisposeHeldJobs();
    DisposeScheduledTasks();
    // ...
}
```

**Step 1:** Add `DisposeHeldJobs` method
**Step 2:** Call it from both `Dispose` and `DisposeAsync`
**Step 3:** Add a test that verifies held jobs are disposed when the queuer is disposed
**Step 4:** Run tests

---

## Test Strategy

After all agents complete:
```bash
dotnet test Figlotech.Core.Tests/Figlotech.Core.Tests.csproj --filter "FullyQualifiedName~WorkQueuer"
```

Also run the full test suite:
```bash
dotnet test Figlotech.Core.Tests/Figlotech.Core.Tests.csproj
```

## Execution Order

1. **Parallel Phase:** Dispatch all 4 agents simultaneously (domains are independent)
2. **Integration Phase:** Review all changes, resolve any conflicts
3. **Verification Phase:** Run full test suite

## Global Timer Investigation Conclusion

The codebase ALREADY implements a global consolidated timer (`_consolidatedTimer`) with a `SortedSet<ScheduledTaskEntry>` priority queue. This is the correct design. The issues are:
- Race conditions in the scheduling code (Task A1)
- Missing disposed-state guards (Task A2)

**Do NOT redesign the timer system.** Fix the existing implementation.
