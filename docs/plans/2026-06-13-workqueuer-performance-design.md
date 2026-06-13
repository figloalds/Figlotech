# WorkQueuer Performance and Efficiency Improvements

## Date
2026-06-13

## Goal
Reduce allocation pressure and CPU overhead in `Figlotech.Core.WorkQueuer`, add bounded-channel backpressure, and improve disposal safety without changing observable semantics or breaking the public API.

## Scope
Target only `Figlotech.Core/WorkQueuer.cs`.

## Background
`WorkQueuer` is a work-scheduling primitive used throughout Figlotech (global queuer, HTTP hosts, data loaders, parallel flow steps, etc.). It currently uses an unbounded `Channel`, allocates heavily on the hot path, recreates its scheduler `Timer` every time the schedule changes, and performs a sync-over-async wait during `Dispose()`.

## Design

### 1. Bounded channel with configurable capacity
- Replace `Channel.CreateUnbounded<WorkJobExecutionRequest>` with `Channel.CreateBounded<WorkJobExecutionRequest>`.
- Add a public `ChannelCapacity` property:
  - `0` means default.
  - Default value at start time: `Math.Max(1, MaxParallelTasks) * 4`.
- The channel must be initialized lazily or on `Start()` so that `MaxParallelTasks` set after construction is honored.
- Add an asynchronous enqueue path:
  - `EnqueueAsync(WorkJob, CancellationToken?)` awaits `WaitToWriteAsync` when the channel is full.
- Keep the synchronous `Enqueue` overloads:
  - If the channel is full, `TryWrite` fails and we throw `InvalidOperationException` instead of blocking synchronously.
- FullBehavior: `Wait` (producers block asynchronously until capacity is available).

### 2. Reuse scheduler timer
- Keep one `Timer` instance for the consolidated scheduler.
- In `RescheduleConsolidatedTimerUnsafe`, call `_consolidatedTimer.Change(timerDelay, Timeout.Infinite)` instead of creating a new `Timer`.
- Dispose the timer exactly once in `DisposeScheduledTasks`.

### 3. Skip linked CancellationTokenSource when unnecessary
- Only call `CancellationTokenSource.CreateLinkedTokenSource` when `job.RequestCancellation.CanBeCanceled` is true.
- Otherwise pass `job.Cancellation.Token` directly to the job action.

### 4. Optimize worker scaling
- Cache `EffectiveParallelLimit()` in a private field updated when `MaxParallelTasks` or `AbsoluteMaxParallelLimit` changes.
- Replace `desiredWorkers > currentWorkers * 1.5` with integer math: `desiredWorkers * 2 > currentWorkers * 3`.
- Avoid reconstructing `DateTime` on the hot path by comparing UTC ticks directly for the minimum scale interval.

### 5. Guard hot-path logging
- Wrap each `Fi.Tech.WriteLineInternal("FTH:WorkQueuer", ...)` call with:
  ```csharp
  if (FiTechCoreExtensions.EnableStdoutLogs && FiTechCoreExtensions.EnabledSystemLogs["FTH:WorkQueuer"])
  ```
  This avoids allocating closure/state objects when logging is disabled.

### 6. Timing and metrics cleanup
- Use `Stopwatch.GetElapsedTime(startTimestamp, endTimestamp)` on .NET 6+ builds; fall back to a cached `Stopwatch.Frequency` ratio on `netstandard2.1`.
- Make `AverageTaskResolutionTime` read an atomic snapshot of both total-resolution ticks and completed count to avoid torn reads.

### 7. Remove stored ValueTask field
- Remove `WorkJob.ActionTask`.
- Execute the action as a local `ValueTask` and await it immediately.

### 8. Improve Dispose behavior
- Keep `Dispose()` usable from synchronous contexts but make it safer:
  - Cancel `_runCts` first.
  - Wait for workers with a timeout.
  - Swallow expected cancellation exceptions.
- `DisposeAsync()` remains the preferred async path.

## Public API Additions

```csharp
public int ChannelCapacity { get; set; } // 0 = default

public async Task<WorkJobExecutionRequest> EnqueueAsync(
    WorkJob job,
    CancellationToken? requestCancellation = null)
```

No existing public members are removed or changed in behavior except for the default channel becoming bounded (a capacity of `int.MaxValue` is no longer unlimited; setting `ChannelCapacity = int.MaxValue` restores unbounded semantics).

## Non-goals
- Replacing `ActiveJobs` `ConcurrentDictionary` with a counter-only structure, because `ActiveTaskStat()` must continue to enumerate active requests.
- Changing exception-handling or telemetry semantics.
- Adding analyzers or formatters.

## Verification
- Build solution: `dotnet build figlotech.sln`.
- Run existing WorkQueuer tests: `dotnet test Figlotech.Core.Tests/Figlotech.Core.Tests.csproj --filter FullyQualifiedName~WorkQueuer`.
- Benchmark hot path in `test/test.csproj` if a relevant benchmark exists.

## Risks
- Bounded channel changes backpressure semantics. Callers that previously relied on an infinite queue must now either use `EnqueueAsync`, set a large capacity, or handle synchronous enqueue failures.
- Lazy channel initialization means `MaxParallelTasks` changes after `Start()` will not affect channel capacity unless capacity is explicitly set.

## References
- `Figlotech.Core/WorkQueuer.cs`
- `Figlotech.Core/FiTechCoreExtensions.cs` (logging flags)
- `Figlotech.Core.Tests/WorkQueuerTests.cs`
- `Figlotech.Core/Figlotech.Core.csproj`
