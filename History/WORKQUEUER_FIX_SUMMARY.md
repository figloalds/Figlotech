# WorkQueuer & ScheduleTask Fix Summary

## Overview
This document summarizes all the fixes applied to resolve critical issues in the WorkQueuer and ScheduleTask mechanisms.

## Phase 1: Critical Thread-Safety & Resource Leaks

### Changes Made:

1. **WorkJobExecutionRequest.Dispose()** (WorkQueuer.cs:91-100)
   - Changed finalizer to not call Dispose() directly
   - Finalizer now only sets `_disposed = true` flag
   - Prevents accessing already-finalized managed objects

2. **WorkQueuer.Dispose()** (WorkQueuer.cs:581-596)
   - Changed from blocking `.GetAwaiter().GetResult()` to sync wait with timeout
   - Uses `CancellationTokenSource` with 30-second timeout
   - Prevents deadlocks when Dispose() called from sync context

3. **SignalDispatcher()** (WorkQueuer.cs:305-315)
   - Added `ObjectDisposedException` handling
   - Prevents crashes when semaphore is disposed during signal

4. **ExecuteJob() CTS Disposal** (WorkQueuer.cs:464-472)
   - Changed from `using` statement to explicit try-finally
   - Ensures CTS is disposed even if action throws

## Phase 2: Timer Precision & Drift

### Changes Made:

1. **Timer Constants** (FiTechCoreExtensions.cs:660-662)
   - Added `MaxTimerIntervalMs = 60000` (1 minute max wait)
   - Added `TimerPrecisionMs = 50` (50ms precision window)
   - Better than hardcoded 120s max and 100ms window

2. **ResetTimer()** (FiTechCoreExtensions.cs:664-675)
   - Removed hardcoded 120s cap
   - Timer wakes up every minute max to recalculate
   - Handles clock changes and long-running schedules better

3. **_timerFn()** (FiTechCoreExtensions.cs:615-618)
   - Changed from `millisDiff < 100` to `millisDiff <= TimerPrecisionMs`
   - More precise timing with 50ms tolerance

4. **Reschedule()** (FiTechCoreExtensions.cs:570-590)
   - Changed from interval addition to loop-based calculation
   - Uses `do-while` to keep adding intervals until future time
   - Better handles system clock changes and suspensions

## Phase 3: Exception Handling & TaskCompletionSource

### Changes Made:

1. **ExecuteJob() Exception Handling** (WorkQueuer.cs:487-540)
   - Wrapped `Fi.Tech.Throw()` calls in try-catch blocks
   - Ensures `terminalException` is always set before catch block exits
   - Prevents TCS from never completing if Throw() throws

2. **Exception Capture Pattern**
   - All exception paths now capture to `terminalException` first
   - Then attempt to call `Fi.Tech.Throw()` for logging
   - Finally block always has an exception to set on TCS

## Phase 4: Memory Leaks

### Changes Made:

1. **ScheduledWorkJob Finalizer** (FiTechCoreExtensions.cs:109-113)
   - Removed managed object access from finalizer
   - Finalizer now empty (only for native resources if needed)

2. **ScheduledWorkJob.Dispose()** (FiTechCoreExtensions.cs:115-123)
   - Added explicit Dispose() method
   - Disposes Timer and CancellationTokenSource
   - Sets IsActive = false

3. **_timerFn() Cleanup** (FiTechCoreExtensions.cs:619-641)
   - Captures values locally before OnCompleted to avoid closure capturing sched
   - Calls `sched.Dispose()` after removing from dictionary
   - Better cleanup for non-recurring tasks

## Phase 5: Metrics & State Consistency

### Changes Made:

1. **Thread-Safe Metrics** (WorkQueuer.cs:209-212)
   - Changed `TotalTaskResolutionTime` from `decimal` to `long` (ticks)
   - Uses `Interlocked.Read()` for thread-safe access
   - Returns `TimeSpan` instead of `decimal`

2. **WentIdle Thread Safety** (WorkQueuer.cs:193-196)
   - Changed from `DateTime` field to `long _wentIdleTicks`
   - Uses `Interlocked.Exchange()` for updates
   - Thread-safe property accessor

3. **Configurable Parallel Limit** (WorkQueuer.cs:395-397)
   - Added `AbsoluteMaxParallelLimit` static property
   - Default value 500, but now configurable
   - Replaces hardcoded 500 limit

## Phase 6: Tests

### Created test/WorkQueuerTests.cs with 16 comprehensive tests:

**Phase 1 Tests:**
- Dispose_FromSyncContext_DoesNotDeadlock
- RapidCreateDispose_NoExceptions
- CancellationDuringExecution_CompletesGracefully
- SemaphoreDisposed_NoException

**Phase 2 Tests:**
- ScheduleTask_FiresWithinPrecisionWindow
- ScheduleTask_Recurring_NoDrift

**Phase 3 Tests:**
- ExceptionInJob_TaskCompletionSourceCompletes
- ExceptionInHandler_TaskCompletionSourceCompletes

**Phase 4 Tests:**
- NonRecurring_RemovedFromDictionary
- Unschedule_RemovesFromDictionary

**Phase 5 Tests:**
- HighConcurrency_MetricsAreConsistent
- AbsoluteMaxParallelLimit_IsConfigurable

**Stress Tests:**
- RapidEnqueueDequeue_NoDeadlocks
- DisposeDuringActiveJobs_CompletesGracefully

## Files Modified:

1. **Figlotech.Core/WorkQueuer.cs**
   - Phase 1, 3, 5 fixes
   - Thread safety, exception handling, metrics

2. **Figlotech.Core/FiTechCoreExtensions.cs**
   - Phase 2, 4 fixes
   - Timer precision, memory leaks

3. **test/WorkQueuerTests.cs** (NEW)
   - Comprehensive test suite

4. **test/Program.cs**
   - Added test runner call in DEBUG mode

## Testing:

Run tests with:
```bash
dotnet run --project test/test.csproj
```

All 16 tests should pass, verifying:
- No deadlocks in Dispose scenarios
- Timer precision within 50ms tolerance
- Recurring tasks don't drift
- Exceptions properly handled
- Memory properly cleaned up
- Metrics consistent under concurrency

## Breaking Changes:

1. **TotalTaskResolutionTime** type changed from `decimal` to `TimeSpan`
2. **AverageTaskResolutionTime** type changed from `decimal` to `TimeSpan`

These are minor breaking changes that improve type safety and consistency.
