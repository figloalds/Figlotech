# WorkQueuer & ScheduleTask Fix Plan

## Current Architecture Overview

### WorkQueuer
- Manages a pool of worker tasks with configurable parallelism
- Uses overflow queue + dispatch loop pattern
- Tracks metrics via interlocked counters
- Supports async disposal and graceful shutdown

### ScheduleTask Mechanism
- Global static dictionary `GlobalScheduledJobs` holds all scheduled tasks
- Uses `System.Threading.Timer` for scheduling
- Supports one-time and recurring tasks
- Tasks enqueue to a `WorkQueuer` when timer fires

## Phase 1: Critical Thread-Safety & Resource Leaks

### Goals
- Fix deadlock risk in Dispose()
- Fix finalizer thread safety issues
- Fix CancellationTokenSource disposal

### Changes Required
1. **WorkQueuer.Dispose()**: Change from blocking `.GetAwaiter().GetResult()` to async pattern
2. **WorkJobExecutionRequest finalizer**: Remove Dispose() call, use safer cleanup
3. **ExecuteJob**: Ensure CTS disposal in all paths using `try-finally` or `using`
4. **SignalDispatcher**: Add null check before semaphore release

### Testing
- Test Dispose() from sync context doesn't deadlock
- Test rapid create/dispose cycles
- Test cancellation during job execution

## Phase 2: Timer Precision & Drift

### Goals
- Fix 100ms early-fire window
- Fix cumulative timer drift
- Fix 120s max check interval causing imprecision

### Changes Required
1. **_timerFn**: Replace `millisDiff < 100` with more precise check
2. **ResetTimer**: Remove 120s cap or make configurable
3. **Reschedule**: Use absolute time calculations instead of interval additions
4. Consider: Use `PeriodicTimer` for recurring tasks (if .NET 6+ available)

### Testing
- Test scheduled tasks fire at correct time (within 50ms tolerance)
- Test recurring tasks don't drift over time
- Test system clock changes handling

## Phase 3: Exception Handling & TaskCompletionSource

### Goals
- Ensure TaskCompletionSource always completes
- Fix unobserved exception issues
- Fix SafeInvoke silent failures

### Changes Required
1. **ExecuteJob**: Wrap entire method in try-finally to guarantee TCS completion
2. **SafeInvoke**: Add logging or callback for event exceptions
3. **Fire-and-forget ContinueWith**: Use proper exception observation
4. **Fi.Tech.Throw**: Wrap in try-catch to ensure cleanup runs

### Testing
- Test exceptions in job action
- Test exceptions in error handler
- Test exceptions in finished callback
- Verify no unobserved task exceptions

## Phase 4: Memory Leaks

### Goals
- Fix ScheduledWorkJob holding references too long
- Fix GlobalScheduledJobs growth
- Fix closure captures in OnCompleted

### Changes Required (Preserving Intended Behavior)
1. **ScheduledWorkJob**: Keep in GlobalScheduledJobs until:
   - Task fires AND has no recurrence → remove immediately after enqueue
   - Unschedule() called → remove immediately
   - This is already working correctly!
2. **Timer cleanup**: Ensure Timer.Dispose() called after task fires (non-recurring)
3. **WeakReference consideration**: For long-term scheduled tasks, consider weak references
4. **OnCompleted closure**: Use weak reference pattern or explicit cleanup

### Testing
- Test scheduled task removed after firing (non-recurring)
- Test scheduled task stays for recurrence
- Test Unschedule removes from dictionary
- Memory profiling for long-running schedules

## Phase 5: Metrics & State Consistency

### Goals
- Fix thread-unsafe metric updates
- Fix _inQueueInternal inconsistency
- Fix WentIdle race conditions

### Changes Required
1. **TotalTaskResolutionTime**: Use `Interlocked.Add` for decimal (or switch to long)
2. **_inQueueInternal**: Audit all increment/decrement locations
3. **WentIdle**: Use `Interlocked.Exchange` for DateTime (or use long ticks)
4. **EffectiveParallelLimit**: Make 500 limit configurable

### Testing
- Test concurrent enqueue/dequeue operations
- Test metrics accuracy under load
- Test boundary conditions (max parallelism)

## Phase 6: Comprehensive Tests

### Test Categories
1. **Unit Tests**: Individual component testing
2. **Integration Tests**: WorkQueuer + ScheduleTask interaction
3. **Concurrency Tests**: Multi-threaded stress tests
4. **Performance Tests**: Benchmark before/after

### Test Scenarios
- Rapid enqueue/dequeue cycles
- High concurrency (1000+ parallel jobs)
- Long-running scheduled tasks (hours/days)
- Rapid scheduling/unscheduling
- Dispose during active operations
- Cancellation at various stages

## Implementation Order

1. Phase 1 (Critical fixes) - Must be stable before proceeding
2. Phase 3 (Exception handling) - Affects stability
3. Phase 2 (Timer precision) - Affects correctness
4. Phase 4 (Memory) - Affects long-running apps
5. Phase 5 (Metrics) - Nice to have, not critical
6. Phase 6 (Tests) - Parallel with each phase

## Success Criteria

- All existing functionality preserved
- No deadlocks in Dispose scenarios
- Scheduled tasks fire within 50ms of target
- No memory growth over 24-hour test
- All exceptions properly handled and observed
- Metrics accurate under concurrent load
