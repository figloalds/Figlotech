using Xunit;

namespace Figlotech.Core.Tests {
    public class WorkQueuerRegressionTests {
        private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(5);
        private static TaskCompletionSource<bool> Gate() => new(TaskCreationOptions.RunContinuationsAsynchronously);

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task AsyncEnqueue_KeepsCancellationAliveUntilExecution(bool started) {
            await using var queue = new WorkQueuer("AsyncLifetime", 1, started);
            var release = Gate();
            if (started) {
                var blocker = queue.EnqueueTask(async () => await release.Task);
                await blocker.WaitForDequeue().WaitAsync(Timeout);
            }
            bool ran = false;
            var request = await queue.EnqueueTaskAsync(token => {
                token.ThrowIfCancellationRequested();
                ran = true;
                return ValueTask.CompletedTask;
            });
            // The request must still own a usable CTS after the async admission returns.
            Assert.False(request.Cancellation.Token.IsCancellationRequested);
            queue.Start();
            release.TrySetResult(true);
            await request.TaskCompletionSource.Task.WaitAsync(Timeout);
            Assert.True(ran);
        }

        [Fact]
        public async Task IdleWaiters_AndStop_AllObserveTheSameDrain() {
            await using var queue = new WorkQueuer("DrainWaiters", 1);
            var release = Gate();
            var request = queue.EnqueueTask(async () => await release.Task);
            await request.WaitForDequeue().WaitAsync(Timeout);
            var first = queue.WaitForIdleAsync();
            var stop = queue.Stop(true);
            var second = queue.WaitForIdleAsync();
            Assert.False(first.IsCompleted);
            release.SetResult(true);
            await Task.WhenAll(first, second, stop).WaitAsync(Timeout);
            Assert.Equal(0, queue.NumberOfActualWorkers);
            queue.Start();
            var nextRelease = Gate();
            var next = queue.EnqueueTask(async () => await nextRelease.Task);
            await next.WaitForDequeue().WaitAsync(Timeout);
            var nextIdle = queue.WaitForIdleAsync();
            Assert.False(nextIdle.IsCompleted);
            nextRelease.SetResult(true);
            await nextIdle.WaitAsync(Timeout);
        }

        [Fact]
        public async Task EmptyQueue_StopsAndRestartsWithoutCreatingChannel() {
            await using var queue = new WorkQueuer("LazyReaders", 4);
            await queue.Stop().WaitAsync(Timeout);
            Assert.Equal(0, queue.NumberOfActualWorkers);
            queue.ChannelCapacity = 1;
            queue.Start();
            var request = await queue.EnqueueTaskAsync(token => ValueTask.CompletedTask);
            await request.TaskCompletionSource.Task.WaitAsync(Timeout);
            await queue.Stop().WaitAsync(Timeout);
        }

        [Fact]
        public async Task BoundedConcurrentWriters_AllExecuteExactlyOnce() {
            await using var queue = new WorkQueuer("ConcurrentWriters", 1) { ChannelCapacity = 1 };
            var release = Gate();
            var blocker = queue.EnqueueTask(async () => await release.Task);
            await blocker.WaitForDequeue().WaitAsync(Timeout);
            _ = queue.EnqueueTask(() => ValueTask.CompletedTask);
            var executions = new int[32];
            var writes = Enumerable.Range(0, executions.Length).Select(i => queue.EnqueueTaskAsync(token => {
                token.ThrowIfCancellationRequested();
                Interlocked.Increment(ref executions[i]);
                return ValueTask.CompletedTask;
            })).ToArray();
            Assert.All(writes, write => Assert.False(write.IsCompleted));
            release.SetResult(true);
            var requests = await Task.WhenAll(writes).WaitAsync(Timeout);
            await Task.WhenAll(requests.Select(r => r.TaskCompletionSource.Task)).WaitAsync(Timeout);
            await queue.WaitForIdleAsync().WaitAsync(Timeout);
            Assert.All(executions, count => Assert.Equal(1, count));
            Assert.Equal(queue.TotalWork, queue.WorkDone);
        }

        [Fact]
        public async Task BlockedWriter_RespondsToRequestCancellation() {
            await using var queue = new WorkQueuer("CancelledWriter", 1) { ChannelCapacity = 1 };
            var release = Gate();
            var blocker = queue.EnqueueTask(async () => await release.Task);
            await blocker.WaitForDequeue().WaitAsync(Timeout);
            _ = queue.EnqueueTask(() => ValueTask.CompletedTask);
            using var cancellation = new CancellationTokenSource();
            var write = queue.EnqueueAsync(new WorkJob(() => ValueTask.CompletedTask), cancellation.Token);
            Assert.False(write.IsCompleted);
            cancellation.Cancel();
            try {
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => write.WaitAsync(Timeout));
                Assert.Equal(1, queue.InQueue);
            } finally {
                release.SetResult(true);
            }
            await queue.WaitForIdleAsync().WaitAsync(Timeout);
            Assert.Equal(queue.TotalWork, queue.WorkDone);
        }

        [Fact]
        public async Task Burst_ScalesForSmallDemandIncreasesWithoutAnotherEnqueue() {
            var initial = Math.Min(Environment.ProcessorCount, WorkQueuer.AbsoluteMaxParallelLimit - 1);
            await using var queue = new WorkQueuer("BurstScaling", initial + 1);
            var release = Gate();
            var allStarted = Gate();
            int started = 0;
            try {
                for (int i = 0; i < initial + 1; i++) {
                    _ = queue.EnqueueTask(async () => {
                        if (Interlocked.Increment(ref started) == initial + 1) allStarted.TrySetResult(true);
                        await release.Task;
                    });
                }
                await allStarted.Task.WaitAsync(Timeout);
                Assert.Equal(initial + 1, queue.NumberOfActualWorkers);
            } finally {
                release.TrySetResult(true);
            }
        }

        [Fact]
        public async Task FailedAction_InvokesFinishedWithoutErrorHandler() {
            await using var queue = new WorkQueuer("FailureCallback", 1);
            bool? success = null;
            var request = queue.EnqueueTask(() => throw new InvalidOperationException("action"), finished: ok => {
                success = ok;
                return ValueTask.CompletedTask;
            });
            await Assert.ThrowsAsync<WorkJobException>(() => request.TaskCompletionSource.Task.WaitAsync(Timeout));
            Assert.Equal(false, success);
        }

        [Fact]
        public async Task CompletionEvents_AwaitEverySubscriberAndContinueAfterFailure() {
            await using var queue = new WorkQueuer("EventSubscribers", 1);
            var entered = Gate();
            var release = Gate();
            bool secondRan = false;
            queue.OnWorkComplete += async request => {
                entered.SetResult(true);
                await release.Task;
                throw new InvalidOperationException("subscriber");
            };
            queue.OnWorkComplete += request => { secondRan = true; return Task.CompletedTask; };
            var job = queue.EnqueueTask(() => ValueTask.CompletedTask);
            await entered.Task.WaitAsync(Timeout);
            try {
                Assert.False(job.TaskCompletionSource.Task.IsCompleted);
                Assert.False(secondRan);
            } finally {
                release.SetResult(true);
            }
            await job.TaskCompletionSource.Task.WaitAsync(Timeout);
            Assert.True(secondRan);
        }

        [Fact]
        public async Task ExceptionEvents_AwaitEverySubscriberAndPreserveFailures() {
            await using var queue = new WorkQueuer("ExceptionSubscribers", 1);
            var entered = Gate();
            var release = Gate();
            bool secondRan = false;
            queue.OnExceptionInHandler += async (request, action, handler) => {
                entered.SetResult(true);
                await release.Task;
                throw new InvalidOperationException("subscriber");
            };
            queue.OnExceptionInHandler += (request, action, handler) => { secondRan = true; return Task.CompletedTask; };
            var job = queue.EnqueueTask(() => throw new InvalidOperationException("action"),
                exceptionHandler: ex => throw new InvalidOperationException("handler"));
            await entered.Task.WaitAsync(Timeout);
            release.SetResult(true);
            var error = await Assert.ThrowsAsync<AggregateException>(() => job.TaskCompletionSource.Task.WaitAsync(Timeout));
            Assert.True(secondRan);
            Assert.Contains(error.Flatten().InnerExceptions, ex => ex.Message == "subscriber");
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task MissedSchedule_FiresOnStartOrRestart(bool restart) {
            await using var queue = new WorkQueuer("MissedSchedule", 1, restart);
            if (restart) await queue.Stop().WaitAsync(Timeout);
            var ran = Gate();
            queue.ScheduleTask("missed", new WorkJob(() => { ran.SetResult(true); return ValueTask.CompletedTask; }),
                new ScheduledTaskOptions { ScheduledTime = DateTime.UtcNow.AddMinutes(-1), FireIfMissed = true });
            queue.Start();
            await ran.Task.WaitAsync(Timeout);
        }

        [Fact]
        public async Task MissedSchedule_WithFireIfMissedFalse_IsSkipped() {
            await using var queue = new WorkQueuer("SkippedSchedule", 1, false);
            bool ran = false;
            queue.ScheduleTask("missed", new WorkJob(() => { ran = true; return ValueTask.CompletedTask; }),
                new ScheduledTaskOptions { ScheduledTime = DateTime.UtcNow.AddMinutes(-1) });
            queue.Start();
            Assert.False(queue.IsScheduled("missed"));
            Assert.False(ran);
        }

        [Theory]
        [InlineData(0)]
        [InlineData(-1)]
        public async Task NonPositiveRecurrence_IsRejected(int ticks) {
            await using var queue = new WorkQueuer("InvalidRecurrence", 1);
            Assert.Throws<ArgumentOutOfRangeException>(() => queue.ScheduleTask("invalid",
                new WorkJob(() => ValueTask.CompletedTask), new ScheduledTaskOptions { RecurrenceInterval = TimeSpan.FromTicks(ticks) }));
            Assert.False(queue.IsScheduled("invalid"));
        }

        [Fact]
        public async Task RecurrenceOptions_AreCopiedAndLongMissedIntervalsAdvanceQuickly() {
            await using var queue = new WorkQueuer("RecurrenceSnapshot", 1, false);
            var options = new ScheduledTaskOptions { ScheduledTime = DateTime.UtcNow.AddYears(-1), RecurrenceInterval = TimeSpan.FromTicks(1) };
            queue.ScheduleTask("recurring", new WorkJob(() => ValueTask.CompletedTask), options);
            options.RecurrenceInterval = TimeSpan.Zero;
            Assert.Equal(TimeSpan.FromTicks(1), queue.GetScheduleInfo("recurring").RecurrenceInterval);
            await Task.Run(queue.Start).WaitAsync(Timeout);
            queue.Unschedule("recurring");
        }

        [Fact]
        public async Task ScheduledJob_WaitsForBoundedCapacity() {
            await using var queue = new WorkQueuer("ScheduledBackpressure", 1) { ChannelCapacity = 1 };
            var release = Gate();
            var blocker = queue.EnqueueTask(async () => await release.Task);
            await blocker.WaitForDequeue().WaitAsync(Timeout);
            _ = queue.EnqueueTask(() => ValueTask.CompletedTask);
            var ran = Gate();
            queue.ScheduleTask("scheduled", new WorkJob(token => { token.ThrowIfCancellationRequested(); ran.SetResult(true); return ValueTask.CompletedTask; }), new ScheduledTaskOptions());
            try {
                await WaitUntil(() => queue.InQueue == 2);
                Assert.False(ran.Task.IsCompleted);
            } finally {
                release.SetResult(true);
            }
            await ran.Task.WaitAsync(Timeout);
        }

        [Fact]
        public async Task ReplacingRunningSchedule_PreservesReplacementRegistration() {
            await using var queue = new WorkQueuer("ScheduleReplacement", 1);
            var started = Gate();
            var release = Gate();
            queue.ScheduleTask("same", new WorkJob(async () => { started.SetResult(true); await release.Task; }), new ScheduledTaskOptions());
            await started.Task.WaitAsync(Timeout);
            // Retain the old entry only to observe its completion deterministically.
            var field = typeof(WorkQueuer).GetField("_scheduledTasks", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!;
            var oldEntry = ((System.Collections.IDictionary)field.GetValue(queue)!)["same"]!;
            var executing = oldEntry.GetType().GetProperty("IsExecuting")!;
            queue.ScheduleTask("same", new WorkJob(() => ValueTask.CompletedTask),
                new ScheduledTaskOptions { ScheduledTime = DateTime.UtcNow.AddHours(1) });
            release.SetResult(true);
            await WaitUntil(() => !(bool)executing.GetValue(oldEntry)!);
            Assert.True(queue.IsScheduled("same"));
            queue.Unschedule("same");
            Assert.False(queue.IsScheduled("same"));
        }

        private static async Task WaitUntil(Func<bool> condition) {
            using var cancellation = new CancellationTokenSource(Timeout);
            while (!condition()) await Task.Delay(10, cancellation.Token);
        }
    }
}
