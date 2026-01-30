# WorkQueuer & ScheduleTask Fix Summary

## Overview
This document summarizes all the fixes applied to resolve critical issues in the WorkQueuer and ScheduleTask mechanisms, including a major refactor that moved scheduling from a global static system to WorkQueuer-owned scheduling.

## Major Refactor: WorkQueuer-Owned Scheduling

### Previous Architecture (Removed)
- Global static `GlobalScheduledJobs` dictionary in `FiTechCoreExtensions.cs`
- `ScheduledWorkJob` class with Timer management
- Global timer logic (`Reschedule`, `_timerFn`, `ResetTimer`)
- Schedules were tied to the global context, not individual WorkQueuers

### New Architecture (Current)
- Scheduling logic moved into `WorkQueuer` class itself
- Each `WorkQueuer` instance manages its own schedules
- `ScheduledTaskEntry` private inner class handles per-schedule state
- `_scheduledTasks` dictionary keyed by identifier (no QID needed anymore)
- `_missedSchedules` list for schedules that fired while WorkQueuer was inactive

### New Classes Added

**ScheduledTaskOptions** (WorkQueuer.cs:28-33)
```csharp
public sealed class ScheduledTaskOptions {
    public TimeSpan? RecurrenceInterval { get; set; }
    public bool FireIfMissed { get; set; } = false;
    public DateTime? ScheduledTime { get; set; }
    public CancellationToken? CancellationToken { get; set; }
}
```

**ScheduledTaskEntry** (WorkQueuer.cs:228-236) - Private inner class
```csharp
private sealed class ScheduledTaskEntry {
    public string Identifier { get; set; }
    public WorkJob Job { get; set; }
    public ScheduledTaskOptions Options { get; set; }
    public Timer Timer { get; set; }
    public CancellationTokenSource Cancellation { get; set; }
    public DateTime ScheduledTime { get; set; }
    public bool IsExecuting { get; set; }
}
```

### New WorkQueuer Scheduling Methods (WorkQueuer.cs:632-813)
- `ScheduleTask(string identifier, WorkJob job, ScheduledTaskOptions options)` - Schedule a task
- `Unschedule(string identifier)` - Cancel and remove a scheduled task
- `IsScheduled(string identifier)` - Check if a schedule exists
- `GetScheduledIdentifiers()` - Get all scheduled task identifiers
- `ProcessMissedSchedules()` - Called in `Start()` to fire missed schedules with `FireIfMissed=true`

### Key Behaviors
1. **Timer caps at 60 seconds max** - Handles clock changes and long-running schedules
2. **50ms precision window** for firing (configurable via `timerPrecisionMs`)
3. **Recurring tasks skip execution** if previous is still running (`IsExecuting` flag)
4. **When WorkQueuer is inactive and timer fires:**
   - If `FireIfMissed=true`: adds to `_missedSchedules` list
   - If recurring: reschedules for next occurrence
   - If one-time with `FireIfMissed=false`: cleans up
5. **Loop-based interval calculation** to prevent drift in recurring tasks
6. **Timer leak fix** - `SetupTimer` now disposes existing timer before creating new one

## Phase 1: Critical Thread-Safety & Resource Leaks

### Changes Made:

1. **WorkJobExecutionRequest.Dispose()** (WorkQueuer.cs:98-104)
   - Changed finalizer to not call Dispose() directly
   - Finalizer now only sets `_disposed = true` flag
   - Prevents accessing already-finalized managed objects

2. **WorkQueuer.Dispose()** (WorkQueuer.cs:827-843)
   - Changed from blocking `.GetAwaiter().GetResult()` to sync wait with timeout
   - Uses `CancellationTokenSource` with 30-second timeout
   - Calls `DisposeScheduledTasks()` to clean up all schedules
   - Prevents deadlocks when Dispose() called from sync context

3. **SignalDispatcher()** (WorkQueuer.cs:330-339)
   - Added `ObjectDisposedException` handling
   - Prevents crashes when semaphore is disposed during signal

4. **ExecuteJob() CTS Disposal** (WorkQueuer.cs:490-497)
   - Changed from `using` statement to explicit try-finally
   - Ensures CTS is disposed even if action throws

## Phase 2: Timer Precision & Drift (Now in WorkQueuer)

### Changes Made:

1. **Timer Constants** (WorkQueuer.cs:670, 689)
   - `maxTimerIntervalMs = 60000` (1 minute max wait)
   - `timerPrecisionMs = 50` (50ms precision window)

2. **SetupTimer()** (WorkQueuer.cs:661-679)
   - Disposes existing timer before creating new one (prevents leak)
   - Timer wakes up every minute max to recalculate
   - Handles clock changes and long-running schedules

3. **OnTimerFired()** (WorkQueuer.cs:681-715)
   - Checks if within precision window before firing
   - Handles inactive WorkQueuer (missed schedules)
   - Reschedules recurring tasks

4. **RescheduleEntry()** (WorkQueuer.cs:748-762)
   - Uses `do-while` loop to keep adding intervals until future time
   - Better handles system clock changes and suspensions

## Phase 3: Exception Handling & TaskCompletionSource

### Changes Made:

1. **ExecuteJob() Exception Handling** (WorkQueuer.cs:512-565)
   - Wrapped `Fi.Tech.SwallowException()` calls in try-catch blocks
   - Ensures `terminalException` is always set before catch block exits
   - Prevents TCS from never completing if SwallowException throws

2. **Exception Capture Pattern**
   - All exception paths now capture to `terminalException` first
   - Then attempt to call `Fi.Tech.SwallowException()` for observability
   - Finally block always has an exception to set on TCS

## Phase 4: Memory Leaks

### Changes Made:

1. **ScheduledTaskEntry Cleanup** (WorkQueuer.cs:764-769)
   - `CleanupSchedule()` disposes Timer and CancellationTokenSource
   - Removes entry from `_scheduledTasks` dictionary

2. **DisposeScheduledTasks()** (WorkQueuer.cs:815-825)
   - Called during Dispose/DisposeAsync
   - Cancels all scheduled tasks
   - Disposes all timers and cancellation tokens
   - Clears both `_scheduledTasks` and `_missedSchedules`

3. **ExecuteScheduledJob() Cleanup** (WorkQueuer.cs:735-745)
   - OnCompleted callback properly marks `IsExecuting = false`
   - Reschedules or cleans up based on recurrence settings

## Phase 5: Metrics & State Consistency

### Changes Made:

1. **Thread-Safe Metrics** (WorkQueuer.cs:216-219)
   - Changed `TotalTaskResolutionTime` from `decimal` to `long` (ticks)
   - Uses `Interlocked.Read()` for thread-safe access
   - Returns `TimeSpan` instead of `decimal`

2. **WentIdle Thread Safety** (WorkQueuer.cs:198-200)
   - Changed from `DateTime` field to `long _wentIdleTicks`
   - Uses `Interlocked.Exchange()` for updates
   - Thread-safe property accessor

3. **Configurable Parallel Limit** (WorkQueuer.cs:419)
   - Added `AbsoluteMaxParallelLimit` static property
   - Default value 500, but now configurable

## Updated FiTechCoreExtensions.cs

### Removed:
- `ScheduledWorkJob` class
- `GlobalScheduledJobs` dictionary
- Old timer logic (`Reschedule`, `_timerFn`, `ResetTimer`)
- Timer constants (moved to WorkQueuer)

### Updated Methods (FiTechCoreExtensions.cs:569-612):
- `ScheduleTask()` - Now wraps `WorkQueuer.ScheduleTask()`
- `Unschedule()` - Now wraps `WorkQueuer.Unschedule()`
- `ScheduleExists()` - Now wraps `WorkQueuer.IsScheduled()`
- `ClearSchedules()` - Now wraps `WorkQueuer.GetScheduledIdentifiers()` + `Unschedule()`
- `GetSchedulerStats()` - Returns basic stats from `FiTechFireTaskWorker`

All methods use `FiTechFireTaskWorker` (global WorkQueuer) as default when no queuer is specified.

## Files Modified:

1. **Figlotech.Core/WorkQueuer.cs**
   - Added `ScheduledTaskOptions` class
   - Added `ScheduledTaskEntry` private class
   - Added scheduling infrastructure (dictionaries, locks)
   - Added scheduling methods (#region Scheduling)
   - Thread safety, exception handling, metrics fixes

2. **Figlotech.Core/FiTechCoreExtensions.cs**
   - Removed old scheduling classes and global state
   - Updated wrapper methods to delegate to WorkQueuer

## Breaking Changes:

1. **TotalTaskResolutionTime** type changed from `decimal` to `TimeSpan`
2. **AverageTaskResolutionTime** type changed from `decimal` to `TimeSpan`
3. **FiTechSchedulerStats** lost some fields (Created, Next, Recurrence) - these could be restored by adding `GetScheduleInfo()` method

These are minor breaking changes that improve type safety and consistency.

## Testing

The test project (`test/test.csproj`) is a console app with BenchmarkDotNet benchmarks. No dedicated unit tests have been added yet for the new scheduling behavior.

Run benchmarks with:
```bash
dotnet run --project test/test.csproj -c Release
```

## Potential Future Enhancements

1. **Add `GetScheduleInfo(identifier)` method** - Return schedule details (next fire time, recurrence, etc.) to restore `FiTechSchedulerStats` fields
2. **Add unit tests** for scheduling behavior
3. **Consider exposing `FireIfMissed`** in the convenience wrapper methods
