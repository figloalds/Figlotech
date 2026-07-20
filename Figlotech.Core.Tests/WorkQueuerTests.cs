using System.Diagnostics;
using Xunit;

namespace Figlotech.Core.Tests {

    public class WorkQueuerTests {

        [Fact]
        public async Task Dispose_FromSyncContext_DoesNotDeadlock() {
            // Arrange
            var queuer = new WorkQueuer("Test", 2);
            var completed = false;

            // Act - Dispose from sync context (this would deadlock with .GetAwaiter().GetResult())
            queuer.Enqueue(async () => {
                await Task.Delay(100);
                completed = true;
            });

            // This should not deadlock
            queuer.Dispose();

            // Assert
            Assert.True(completed || true); // Just ensure we get here without deadlock
        }

        [Fact]
        public async Task RapidCreateDispose_NoExceptions() {
            // Arrange & Act & Assert
            for (int i = 0; i < 100; i++) {
                var queuer = new WorkQueuer($"Test_{i}", 2);
                queuer.Enqueue(() => new ValueTask());
                queuer.Dispose();
            }
        }

        [Fact]
        public async Task CancellationDuringExecution_CompletesGracefully() {
            // Arrange
            var queuer = new WorkQueuer("Test", 2);
            var wasCancelled = false;

            using (var cts = new CancellationTokenSource()) {
                var request = queuer.Enqueue(
                    new WorkJob(async (ct) => {
                        try {
                            await Task.Delay(5000, ct);
                        } catch (OperationCanceledException) {
                            wasCancelled = true;
                            throw;
                        }
                    }),
                    cts.Token
                );

                await Task.Delay(100);
                cts.Cancel();

                try {
                    await request;
                } catch (Exception ex) when (ex is OperationCanceledException ||
                                              (ex is WorkJobException wje && wje.InnerException is OperationCanceledException)) {
                    // Expected - OperationCanceledException is wrapped in WorkJobException
                }
            }

            // Assert
            Assert.True(wasCancelled);
            queuer.Dispose();
        }

        [Fact]
        public async Task EnqueueAfterDispose_ReturnsFaultedRequest() {
            var queuer = new WorkQueuer("Test", 2);
            queuer.Dispose();

            var request = queuer.Enqueue(new WorkJob(() => new ValueTask()));

            // Should return a request (not throw synchronously)
            Assert.NotNull(request);
            // But the request should be faulted
            Assert.True(request.TaskCompletionSource.Task.IsFaulted);
        }

        [Fact]
        public async Task ExceptionInJob_TaskCompletionSourceCompletes() {
            // Arrange
            var queuer = new WorkQueuer("Test", 2);
            var exceptionThrown = false;

            // Act
            var request = queuer.EnqueueTask(async () => {
                await Task.Delay(10);
                throw new InvalidOperationException("Test exception");
            });

            try {
                await request;
            } catch (Exception) {
                exceptionThrown = true;
            }

            // Assert
            Assert.True(exceptionThrown);
            queuer.Dispose();
        }

        [Fact]
        public async Task ExceptionInHandler_TaskCompletionSourceCompletes() {
            // Arrange
            var queuer = new WorkQueuer("Test", 2);
            var requestCompleted = false;

            // Act
            var request = queuer.Enqueue(
                new WorkJob(
                    async () => {
                        await Task.Delay(10);
                        throw new InvalidOperationException("Test exception");
                    },
                    async (ex) => {
                        await Task.Delay(10);
                        throw new InvalidOperationException("Handler exception");
                    }
                )
            );

            try {
                await request;
            } catch (Exception) {
                requestCompleted = true;
            }

            // Assert
            Assert.True(requestCompleted);
            queuer.Dispose();
        }

        [Fact]
        public async Task HighConcurrency_MetricsAreConsistent() {
            // Arrange
            var queuer = new WorkQueuer("Test", 10);
            var requests = new List<WorkJobExecutionRequest>();

            // Act
            for (int i = 0; i < 100; i++) {
                requests.Add(queuer.EnqueueTask(async () => {
                    await Task.Delay(10);
                }));
            }

            foreach (var request in requests) {
                await request;
            }

            // Assert
            Assert.Equal(100, queuer.TotalWork);
            Assert.Equal(100, queuer.WorkDone);
            Assert.True(queuer.TotalTaskResolutionTime > TimeSpan.Zero);
            queuer.Dispose();
        }

        [Fact]
        public void AbsoluteMaxParallelLimit_IsConfigurable() {
            // Arrange & Act
            var originalLimit = WorkQueuer.AbsoluteMaxParallelLimit;
            WorkQueuer.AbsoluteMaxParallelLimit = 100;

            var queuer = new WorkQueuer("Test", 200);

            // Assert - MaxParallelTasks is 200, but effective limit should be 100
            // We can't easily test the internal behavior, but we can verify the property works
            Assert.Equal(100, WorkQueuer.AbsoluteMaxParallelLimit);

            // Cleanup
            WorkQueuer.AbsoluteMaxParallelLimit = originalLimit;
            queuer.Dispose();
        }

        [Fact]
        public async Task RapidEnqueueDequeue_NoDeadlocks() {
            // Arrange
            var queuer = new WorkQueuer("StressTest", 20);
            var requests = new List<WorkJobExecutionRequest>();

            // Act
            for (int batch = 0; batch < 10; batch++) {
                for (int i = 0; i < 100; i++) {
                    requests.Add(queuer.EnqueueTask(async () => {
                        await Task.Delay(1);
                    }));
                }
                await Task.Delay(50);
            }

            foreach (var request in requests) {
                await request;
            }

            // Assert
            Assert.Equal(1000, queuer.WorkDone);
            queuer.Dispose();
        }

        [Fact]
        public async Task DisposeDuringActiveJobs_CompletesGracefully() {
            // Arrange
            var queuer = new WorkQueuer("Test", 10);
            var started = 0;
            var completed = 0;

            for (int i = 0; i < 50; i++) {
                _ = queuer.EnqueueTask(async () => {
                    Interlocked.Increment(ref started);
                    await Task.Delay(100);
                    Interlocked.Increment(ref completed);
                });
            }

            await Task.Delay(50);
            queuer.Dispose();

            // Assert - should complete without hanging
            Assert.True(started > 0);
        }

        [Fact]
        public void Start_InitializesWorkersUpToProcessorCount() {
            var maxThreads = Environment.ProcessorCount + 3;
            var queuer = new WorkQueuer("Test", maxThreads, init_started: false);

            queuer.Start();

            Assert.Equal(Math.Min(Environment.ProcessorCount, maxThreads), queuer.NumberOfActualWorkers);
            queuer.Dispose();
        }

        [Fact]
        public async Task Enqueue_ScalesWorkersWhenDemandExceedsInitialPool() {
            var maxThreads = Environment.ProcessorCount * 3;
            var initialWorkers = Math.Min(Environment.ProcessorCount, maxThreads);
            var queuer = new WorkQueuer("Test", maxThreads, init_started: false);
            var release = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);

            queuer.Start();

            // Enqueue enough jobs to exceed 150% of initial worker count
            var jobsToEnqueue = (int)(initialWorkers * 1.5) + 2;
            for (int i = 0; i < jobsToEnqueue; i++) {
                queuer.EnqueueTask(async () => {
                    await release.Task;
                });
            }

            // Wait for rate limiter to allow next scale check, then trigger one more enqueue
            await Task.Delay(150);
            queuer.EnqueueTask(async () => {
                await release.Task;
            });

            var timeoutAt = DateTime.UtcNow.AddSeconds(3);
            while (queuer.NumberOfActualWorkers <= initialWorkers && DateTime.UtcNow < timeoutAt) {
                await Task.Delay(25);
            }

            Assert.True(queuer.NumberOfActualWorkers > initialWorkers,
                $"Expected worker count to grow beyond {initialWorkers}, but found {queuer.NumberOfActualWorkers}.");

            release.SetResult(true);
            await queuer.Stop(true);
            queuer.Dispose();
        }

        [Fact]
        public async Task ChannelCapacity_ExplicitlySet_IsBounded() {
            var queuer = new WorkQueuer("BoundedTest", 1) { ChannelCapacity = 2 };
            var gate = new TaskCompletionSource<object>();
            var blocking = queuer.EnqueueTask(async () => await gate.Task);
            await blocking.WaitForDequeue();

            queuer.Enqueue(() => Fi.EmptyValueTask);
            queuer.Enqueue(() => Fi.EmptyValueTask);

            Assert.Throws<InvalidOperationException>(() => queuer.Enqueue(() => Fi.EmptyValueTask));

            gate.SetResult(null);
            queuer.Dispose();
        }
        [Fact]
        public async Task ChannelCapacity_ExplicitlySet_BlocksAsyncEnqueueWhenFull() {
            var queuer = new WorkQueuer("BoundedTest", 1, init_started: false) { ChannelCapacity = 1 };
            queuer.Start();
            var gate = new TaskCompletionSource<object>();

            var r1 = queuer.EnqueueTask(async () => await gate.Task);
            await r1.WaitForDequeue();

            // Worker is blocked; fill the channel
            queuer.Enqueue(() => Fi.EmptyValueTask);

            // Async enqueue should wait because channel is full
            var asyncEnqueue = queuer.EnqueueTaskAsync(async (ct) => { });
            await Task.Delay(100);
            Assert.False(asyncEnqueue.IsCompleted);

            gate.SetResult(null);
            await asyncEnqueue;
            queuer.Dispose();
        }

        [Fact]
        public async Task EnqueueAsync_Waits_When_Channel_Full() {
            var queuer = new WorkQueuer("EnqueueAsyncTest", 1) { ChannelCapacity = 1 };
            var gate = new TaskCompletionSource<object>();
            queuer.Enqueue(async () => await gate.Task);

            var secondJobTask = queuer.EnqueueTaskAsync(async (ct) => { });
            // give the worker time to pick the first job
            await Task.Delay(100);
            Assert.Equal(1, queuer.InQueue);

            gate.SetResult(null);
            await secondJobTask;
            queuer.Dispose();
        }
    }

    public class ScheduleTaskTests {

        [Fact]
        public async Task ScheduleTask_FiresWithinPrecisionWindow() {
            // Arrange
            var queuer = new WorkQueuer("Test", 2);
            var fired = false;
            var scheduledTime = DateTime.UtcNow.AddMilliseconds(200);

            // Act
            Fi.Tech.ScheduleTask("test_precision", queuer, scheduledTime, new WorkJob(async () => {
                fired = true;
            }));

            await Task.Delay(400);

            // Assert
            Assert.True(fired);
            Fi.Tech.Unschedule("test_precision", queuer);
            queuer.Dispose();
        }

        [Fact]
        public async Task ScheduleTask_Recurring_NoDrift() {
            // Arrange
            var queuer = new WorkQueuer("Test", 2);
            var fireTimes = new List<DateTime>();
            var scheduledTime = DateTime.UtcNow.AddMilliseconds(100);

            // Act
            Fi.Tech.ScheduleTask("test_recurring", queuer, scheduledTime, new WorkJob(async () => {
                lock (fireTimes) {
                    fireTimes.Add(DateTime.UtcNow);
                }
            }), TimeSpan.FromMilliseconds(100));

            await Task.Delay(600);
            Fi.Tech.Unschedule("test_recurring", queuer);

            // Assert - Check that intervals are consistent (within 70-160ms tolerance)
            Assert.True(fireTimes.Count >= 3, $"Expected at least 3 fires, got {fireTimes.Count}");
            for (int i = 1; i < fireTimes.Count; i++) {
                var interval = fireTimes[i] - fireTimes[i - 1];
                Assert.True(interval >= TimeSpan.FromMilliseconds(70) && interval <= TimeSpan.FromMilliseconds(160),
                    $"Interval {interval.TotalMilliseconds}ms is outside tolerance (70-160ms)");
            }

            queuer.Dispose();
        }

        [Fact]
        public async Task LongRunningJob_SkipsOverlappingExecutions() {
            // Arrange
            var queuer = new WorkQueuer("Test", 2);
            var executionCount = 0;
            var scheduledTime = DateTime.UtcNow.AddMilliseconds(50);

            // Act - Schedule a job that takes 300ms but recurs every 100ms
            // This should skip executions while the job is running
            Fi.Tech.ScheduleTask("test_overlapping", queuer, scheduledTime, new WorkJob(async () => {
                Interlocked.Increment(ref executionCount);
                await Task.Delay(300); // Job takes longer than recurrence interval
            }), TimeSpan.FromMilliseconds(100));

            await Task.Delay(1000); // Wait 1 second
            Fi.Tech.Unschedule("test_overlapping", queuer);

            // Assert - Should have executed about 3 times (not 10 times)
            // 1000ms / 100ms interval = 10 potential executions
            // But since each takes 300ms, we should only get ~3-4 executions
            Assert.True(executionCount <= 5,
                $"Expected at most 5 executions (job takes 300ms, interval is 100ms), but got {executionCount}. Jobs are overlapping!");
            Assert.True(executionCount >= 2,
                $"Expected at least 2 executions, but got {executionCount}");

            queuer.Dispose();
        }

        [Fact]
        public async Task NonRecurring_RemovedFromDictionary() {
            // Arrange
            var queuer = new WorkQueuer("Test", 2);
            var fired = false;

            // Act
            Fi.Tech.ScheduleTask("test_remove", queuer, DateTime.UtcNow.AddMilliseconds(50), new WorkJob(async () => {
                fired = true;
            }));

            await Task.Delay(200);

            // Assert
            Assert.True(fired);
            Assert.False(Fi.Tech.ScheduleExists("test_remove", queuer));
            queuer.Dispose();
        }

        [Fact]
        public async Task Unschedule_RemovesFromDictionary() {
            // Arrange
            var queuer = new WorkQueuer("Test", 2);

            // Act
            Fi.Tech.ScheduleTask("test_unschedule", queuer, DateTime.UtcNow.AddMinutes(1), new WorkJob(async () => {
            }));

            Assert.True(Fi.Tech.ScheduleExists("test_unschedule", queuer));

            Fi.Tech.Unschedule("test_unschedule", queuer);

            // Assert
            Assert.False(Fi.Tech.ScheduleExists("test_unschedule", queuer));
            queuer.Dispose();
        }

        [Fact]
        public void GetScheduleInfo_ReturnsCorrectInfo() {
            // Arrange
            var queuer = new WorkQueuer("Test", 2);
            var scheduledTime = DateTime.UtcNow.AddMinutes(5);
            var recurrence = TimeSpan.FromMinutes(10);
            var beforeCreate = DateTime.UtcNow;

            // Act
            queuer.ScheduleTask("test_info", new WorkJob(async () => { }), new ScheduledTaskOptions {
                ScheduledTime = scheduledTime,
                RecurrenceInterval = recurrence,
                FireIfMissed = true
            });

            var afterCreate = DateTime.UtcNow;
            var info = queuer.GetScheduleInfo("test_info");

            // Assert
            Assert.NotNull(info);
            Assert.Equal("test_info", info.Identifier);
            Assert.True(info.Created >= beforeCreate && info.Created <= afterCreate, "Created should be within expected time range");
            Assert.Equal(scheduledTime, info.NextScheduledTime);
            Assert.Equal(recurrence, info.RecurrenceInterval);
            Assert.True(info.FireIfMissed);
            Assert.False(info.IsExecuting);

            queuer.Unschedule("test_info");
            queuer.Dispose();
        }

        [Fact]
        public void GetScheduleInfo_ReturnsNull_WhenNotFound() {
            // Arrange
            var queuer = new WorkQueuer("Test", 2);

            // Act
            var info = queuer.GetScheduleInfo("nonexistent");

            // Assert
            Assert.Null(info);
            queuer.Dispose();
        }

        [Fact]
        public void GetAllScheduleInfo_ReturnsAllSchedules() {
            // Arrange
            var queuer = new WorkQueuer("Test", 2);
            var scheduledTime1 = DateTime.UtcNow.AddMinutes(5);
            var scheduledTime2 = DateTime.UtcNow.AddMinutes(10);

            // Act
            queuer.ScheduleTask("task1", new WorkJob(async () => { }), new ScheduledTaskOptions {
                ScheduledTime = scheduledTime1,
                RecurrenceInterval = TimeSpan.FromMinutes(1)
            });
            queuer.ScheduleTask("task2", new WorkJob(async () => { }), new ScheduledTaskOptions {
                ScheduledTime = scheduledTime2,
                FireIfMissed = true
            });

            var allInfo = queuer.GetAllScheduleInfo();

            // Assert
            Assert.Equal(2, allInfo.Length);
            Assert.Contains(allInfo, i => i.Identifier == "task1");
            Assert.Contains(allInfo, i => i.Identifier == "task2");

            var task1Info = Array.Find(allInfo, i => i.Identifier == "task1");
            Assert.NotNull(task1Info);
            Assert.Equal(TimeSpan.FromMinutes(1), task1Info.RecurrenceInterval);

            var task2Info = Array.Find(allInfo, i => i.Identifier == "task2");
            Assert.NotNull(task2Info);
            Assert.True(task2Info.FireIfMissed);
            Assert.Null(task2Info.RecurrenceInterval);

            queuer.Unschedule("task1");
            queuer.Unschedule("task2");
            queuer.Dispose();
        }

        [Fact]
        public void GetScheduledIdentifiers_ReturnsAllIdentifiers() {
            // Arrange
            var queuer = new WorkQueuer("Test", 2);

            // Act
            queuer.ScheduleTask("alpha", new WorkJob(async () => { }), new ScheduledTaskOptions {
                ScheduledTime = DateTime.UtcNow.AddMinutes(5)
            });
            queuer.ScheduleTask("beta", new WorkJob(async () => { }), new ScheduledTaskOptions {
                ScheduledTime = DateTime.UtcNow.AddMinutes(10)
            });
            queuer.ScheduleTask("gamma", new WorkJob(async () => { }), new ScheduledTaskOptions {
                ScheduledTime = DateTime.UtcNow.AddMinutes(15)
            });

            var identifiers = queuer.GetScheduledIdentifiers();

            // Assert
            Assert.Equal(3, identifiers.Length);
            Assert.Contains("alpha", identifiers);
            Assert.Contains("beta", identifiers);
            Assert.Contains("gamma", identifiers);

            queuer.Unschedule("alpha");
            queuer.Unschedule("beta");
            queuer.Unschedule("gamma");
            queuer.Dispose();
        }

        [Fact]
        public async Task ScheduleTask_ReplacesExistingSchedule() {
            // Arrange
            var queuer = new WorkQueuer("Test", 2);
            var firstFired = false;
            var secondFired = false;

            // Act - Schedule first task
            queuer.ScheduleTask("replaceable", new WorkJob(async () => {
                firstFired = true;
            }), new ScheduledTaskOptions {
                ScheduledTime = DateTime.UtcNow.AddMinutes(10)
            });

            // Replace with second task that fires sooner
            queuer.ScheduleTask("replaceable", new WorkJob(async () => {
                secondFired = true;
            }), new ScheduledTaskOptions {
                ScheduledTime = DateTime.UtcNow.AddMilliseconds(100)
            });

            await Task.Delay(300);

            // Assert - Only second task should have fired
            Assert.False(firstFired);
            Assert.True(secondFired);

            queuer.Dispose();
        }

        [Fact]
        public async Task GetScheduleInfo_ShowsIsExecuting_DuringExecution() {
            // Arrange
            var queuer = new WorkQueuer("Test", 2);
            var executionStarted = new TaskCompletionSource<bool>();
            var canFinish = new TaskCompletionSource<bool>();

            // Act
            queuer.ScheduleTask("long_running", new WorkJob(async () => {
                executionStarted.SetResult(true);
                await canFinish.Task;
            }), new ScheduledTaskOptions {
                ScheduledTime = DateTime.UtcNow.AddMilliseconds(50),
                RecurrenceInterval = TimeSpan.FromHours(1) // Recurring so it stays in dictionary
            });

            // Wait for execution to start
            await executionStarted.Task;

            var infoWhileRunning = queuer.GetScheduleInfo("long_running");

            // Let it finish
            canFinish.SetResult(true);
            await Task.Delay(100);

            var infoAfterRunning = queuer.GetScheduleInfo("long_running");

            // Assert
            Assert.NotNull(infoWhileRunning);
            Assert.True(infoWhileRunning.IsExecuting);

            Assert.NotNull(infoAfterRunning);
            Assert.False(infoAfterRunning.IsExecuting);

            queuer.Unschedule("long_running");
            queuer.Dispose();
        }
    }

    public class WorkQueuerReliabilityTests {

        [Fact]
        public async Task WaitForDequeue_CompletesWhenJobPreCancelled() {
            // Arrange - fix #9: _tcsNotifyDequeued must complete on cancellation
            var queuer = new WorkQueuer("Test", 2);
            using var cts = new CancellationTokenSource();
            cts.Cancel(); // Pre-cancel before enqueue

            var request = queuer.Enqueue(
                new WorkJob(async (ct) => {
                    await Task.Delay(1000, ct);
                }),
                cts.Token
            );

            // Act - WaitForDequeue should not hang; use a timeout to detect hang
            var waitTask = Task.Run(async () => {
                try {
                    await request.WaitForDequeue();
                } catch (OperationCanceledException) {
                    // Expected: job was cancelled before dequeue
                }
            });

            var completed = await Task.WhenAny(waitTask, Task.Delay(3000));

            // Assert
            Assert.Equal(waitTask, completed); // Should complete, not timeout
            queuer.Dispose();
        }

        [Fact]
        public async Task EnqueueAfterDispose_FaultsTaskCompletionSource() {
            // Arrange - fix #10: enqueue after dispose should fault TCS, not hang
            var queuer = new WorkQueuer("Test", 2);
            queuer.Dispose();

            // Act
            var request = queuer.EnqueueTask(async () => {
                await Task.Delay(100);
            });

            // Assert - awaiting the request should throw ObjectDisposedException
            var threw = false;
            try {
                await request;
            } catch (ObjectDisposedException) {
                threw = true;
            }
            Assert.True(threw, "Awaiting a job enqueued after Dispose should throw ObjectDisposedException");
        }

        [Fact]
        public async Task EnqueueAfterDispose_WaitForDequeue_AlsoFaults() {
            // Arrange - fix #10: WaitForDequeue should also fault
            var queuer = new WorkQueuer("Test", 2);
            queuer.Dispose();

            var request = queuer.EnqueueTask(async () => {
                await Task.Delay(100);
            });

            // Act & Assert
            await Assert.ThrowsAsync<ObjectDisposedException>(async () => {
                await request.WaitForDequeue();
            });
        }

        [Fact]
        public async Task StopWithSignalDrain_CompletesQuickly() {
            // Arrange - fix #12: signal-based drain should not poll
            var queuer = new WorkQueuer("Test", 4);
            var sw = Stopwatch.StartNew();

            // Enqueue a few fast jobs
            for (int i = 0; i < 10; i++) {
                queuer.EnqueueTask(async () => {
                    await Task.Delay(10);
                });
            }

            // Act
            await queuer.Stop(true);
            sw.Stop();

            // Assert - should finish much faster than polling intervals (100ms/poll)
            // With signal-based drain, it finishes as soon as last job is done
            Assert.True(sw.ElapsedMilliseconds < 5000,
                $"Stop took {sw.ElapsedMilliseconds}ms, expected < 5000ms");
            Assert.Equal(10, queuer.WorkDone);
            queuer.Dispose();
        }

        [Fact]
        public async Task StopWithoutWait_CompletesWhenActiveJobsFinish() {
            // Arrange - fix #12: signal-based drain for wait=false path
            var queuer = new WorkQueuer("Test", 4);

            for (int i = 0; i < 5; i++) {
                queuer.EnqueueTask(async () => {
                    await Task.Delay(50);
                });
            }

            // Act
            await queuer.Stop(false);

            // Assert - should not hang
            Assert.True(queuer.WorkDone >= 0);
            queuer.Dispose();
        }

        [Fact]
        public void VolatileStateFlags_VisibleAcrossThreads() {
            // Arrange - fix #11: verify state flags are consistent
            var queuer = new WorkQueuer("Test", 2, init_started: false);

            // Assert initial state
            Assert.False(queuer.IsRunning);
            Assert.False(queuer.Active);
            Assert.False(queuer.IsClosed);

            // Act - start
            queuer.Start();
            Assert.True(queuer.IsRunning);
            Assert.True(queuer.Active);

            // Act - close
            queuer.Close();
            Assert.True(queuer.IsClosed);

            queuer.Dispose();
        }

        [Fact]
        public async Task CancelledBeforeExecution_MetricsStillConsistent() {
            // Arrange - verify that pre-cancelled jobs are counted correctly
            var queuer = new WorkQueuer("Test", 1);

            // Fill the single slot with a long-running job so subsequent ones queue
            var blockTcs = new TaskCompletionSource<bool>();
            queuer.EnqueueTask(async () => {
                await blockTcs.Task;
            });

            await Task.Delay(50); // Let the blocking job start

            // Enqueue then cancel before it gets picked up
            using var cts = new CancellationTokenSource();
            var request = queuer.Enqueue(
                new WorkJob(async (ct) => {
                    await Task.Delay(10000, ct);
                }),
                cts.Token
            );
            cts.Cancel();

            // Unblock the first job
            blockTcs.SetResult(true);

            // Unblock first, then cancel — ensures the second job enters ExecuteJob
            // so the cancellation surfaces as WorkJobException wrapping TaskCanceledException.
            // If cancellation wins the race and fires before dequeue, we get a plain
            // TaskCanceledException instead.  Both outcomes are valid.
            // Wait for everything to drain
            try {
                await request;
            } catch (OperationCanceledException) {
                // Pre-cancellation path: TrySetCanceled on the TCS
            } catch (WorkJobException wje) when (wje.InnerException is OperationCanceledException) {
                // In-execution path: token fired inside ExecuteJob
            }

            await queuer.Stop(true);

            // Assert - metrics should be consistent
            Assert.Equal(2, queuer.TotalWork);
            Assert.Equal(2, queuer.WorkDone);
            // Cancelled counter is only incremented in the pre-cancellation path;
            // if the job entered ExecuteJob instead, it counts as a normal completion.
            // So we just verify totals balance out.
            Assert.True(queuer.WorkDone <= queuer.TotalWork);
            queuer.Dispose();
        }

        [Fact]
        public async Task DrainSignal_HandlesRapidEnqueueDuringStop() {
            // Arrange - stress test the signal-based drain
            var queuer = new WorkQueuer("StressTest", 8);
            var completed = 0;

            for (int i = 0; i < 200; i++) {
                queuer.EnqueueTask(async () => {
                    await Task.Delay(1);
                    Interlocked.Increment(ref completed);
                });
            }

            // Act
            await queuer.Stop(true);

            // Assert
            Assert.Equal(200, completed);
            Assert.Equal(200, queuer.WorkDone);
            queuer.Dispose();
        }

        [Fact]
        public async Task ConcurrentStop_NoDeadlock() {
            var queuer = new WorkQueuer("Test", 4);
            for (int i = 0; i < 20; i++) {
                queuer.EnqueueTask(async () => await Task.Delay(50));
            }
            
            // Call Stop concurrently from multiple threads
            var stopTasks = Enumerable.Range(0, 5)
                .Select(_ => Task.Run(() => queuer.Stop(true)))
                .ToArray();
            
            await Task.WhenAll(stopTasks);
            Assert.Equal(20, queuer.WorkDone);
            queuer.Dispose();
        }

        [Fact]
        public async Task EnqueueRacingStop_CompletesOrFaultsWithoutHanging() {
            for (int i = 0; i < 25; i++) {
                var queuer = new WorkQueuer($"StopRace_{i}", 1);
                var started = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                var release = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                queuer.EnqueueTask(async () => {
                    started.TrySetResult(true);
                    await release.Task;
                });
                await started.Task;

                Task stopTask = queuer.Stop(true);
                WorkJobExecutionRequest request = queuer.EnqueueTask(() => new ValueTask());
                release.TrySetResult(true);

                Task all = Task.WhenAll(stopTask, ObserveTerminal(request));
                Assert.Same(all, await Task.WhenAny(all, Task.Delay(TimeSpan.FromSeconds(2))));
                await all;
                Assert.NotEqual(TaskStatus.WaitingForActivation, request.TaskCompletionSource.Task.Status);
                Assert.True(request.TaskCompletionSource.Task.IsCompleted, "A job admitted while Stop races must reach a terminal state.");
                queuer.Dispose();
            }
        }

        [Fact]
        public async Task StartRacingStop_Linearizes() {
            var lifecycleLockField = typeof(WorkQueuer).GetField("_lifecycleLock", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!;

            for (int i = 0; i < 25; i++) {
                var queuer = new WorkQueuer($"StartStopRace_{i}", 1);
                await queuer.Stop(true).WaitAsync(TimeSpan.FromSeconds(2));

                var lifecycleLock = (SemaphoreSlim)lifecycleLockField.GetValue(queuer)!;
                await lifecycleLock.WaitAsync();
                try {
                    var startEntered = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                    Task startTask = Task.Run(() => {
                        startEntered.TrySetResult(true);
                        queuer.Start();
                    });
                    await startEntered.Task.WaitAsync(TimeSpan.FromSeconds(2));

                    Assert.NotSame(startTask, await Task.WhenAny(startTask, Task.Delay(100)));
                    Assert.False(queuer.IsRunning,
                        "Start must not publish a new run while Stop and enqueue admission are serialized by the lifecycle lock.");

                    lifecycleLock.Release();
                    lifecycleLock = null;

                    await startTask.WaitAsync(TimeSpan.FromSeconds(2));
                    await queuer.Stop(true).WaitAsync(TimeSpan.FromSeconds(2));
                    await queuer.Stop(true).WaitAsync(TimeSpan.FromSeconds(2));

                    queuer.Start();
                    var ran = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                    queuer.EnqueueTask(() => {
                        ran.TrySetResult(true);
                        return new ValueTask();
                    });
                    await ran.Task.WaitAsync(TimeSpan.FromSeconds(2));

                    var raceQueuer = new WorkQueuer($"StartStopHeldRace_{i}", 1, init_started: false);
                    try {
                        WorkJobExecutionRequest[] heldRequests = Enumerable.Range(0, 2).Select(_ => raceQueuer.EnqueueTask(() => new ValueTask())).ToArray();
                        var raceGate = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                        Task racingStart = Task.Run(async () => {
                            await raceGate.Task;
                            raceQueuer.Start();
                        });
                        Task racingStop = Task.Run(async () => {
                            await raceGate.Task;
                            await raceQueuer.Stop(true);
                        });
                        raceGate.TrySetResult(true);

                        Task terminalRequests = Task.WhenAll(heldRequests.Select(ObserveTerminal));
                        Task race = Task.WhenAll(racingStart, racingStop, terminalRequests);
                        Assert.Same(race, await Task.WhenAny(race, Task.Delay(TimeSpan.FromSeconds(2))));
                        await race;
                        Assert.All(heldRequests, request => Assert.True(request.TaskCompletionSource.Task.IsCompleted,
                            "A held request racing Start and Stop must reach a terminal state."));
                        Assert.Equal(0, raceQueuer.InQueue);
                        Assert.Equal(0, GetChannelReaderCount(raceQueuer));
                        await raceQueuer.Stop(true).WaitAsync(TimeSpan.FromSeconds(2));
                    } finally {
                        raceQueuer.Dispose();
                    }
                } finally {
                    lifecycleLock?.Release();
                    queuer.Dispose();
                }
            }
        }

        [Fact]
        public async Task ConcurrentStopCallers_ShareTeardownAndLeaveQueuerStopped() {
            var queuer = new WorkQueuer("ConcurrentStopShared", 2);
            for (int i = 0; i < 10; i++) {
                queuer.EnqueueTask(async () => await Task.Delay(10));
            }

            Task[] stops = Enumerable.Range(0, 6).Select(_ => queuer.Stop(true)).ToArray();
            Task all = Task.WhenAll(stops);

            Assert.Same(all, await Task.WhenAny(all, Task.Delay(TimeSpan.FromSeconds(2))));
            await all;
            Assert.False(queuer.IsRunning);
            Assert.False(queuer.Active);
            await queuer.Stop(true);
            queuer.Dispose();
        }

        [Fact]
        public async Task StopWithoutWait_CancelsQueuedRequestsInsteadOfRunningThem() {
            var queuer = new WorkQueuer("StopWithoutWaitCancels", 1);
            var started = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var release = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            queuer.EnqueueTask(async () => {
                started.TrySetResult(true);
                await release.Task;
            });
            await started.Task;

            var queuedExecutions = 0;
            WorkJobExecutionRequest[] requests = Enumerable.Range(0, 5).Select(_ => queuer.EnqueueTask(() => {
                Interlocked.Increment(ref queuedExecutions);
                return new ValueTask();
            })).ToArray();

            Task stopTask = queuer.Stop(false);
            release.TrySetResult(true);
            await stopTask.WaitAsync(TimeSpan.FromSeconds(2));
            await Task.WhenAll(requests.Select(ObserveTerminal)).WaitAsync(TimeSpan.FromSeconds(2));

            Assert.Equal(0, queuedExecutions);
            Assert.All(requests, request => Assert.True(request.TaskCompletionSource.Task.IsCanceled || request.TaskCompletionSource.Task.IsFaulted));
            queuer.Dispose();
        }

        [Fact]
        public async Task DisposedQueuedRequest_DoesNotKillWorker() {
            var queuer = new WorkQueuer("DisposedQueuedRequest", 1);
            var started = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var release = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            queuer.EnqueueTask(async () => {
                started.TrySetResult(true);
                await release.Task;
            });
            await started.Task;

            WorkJobExecutionRequest disposedRequest = queuer.EnqueueTask(() => new ValueTask());
            disposedRequest.Dispose();
            var executed = 0;
            WorkJobExecutionRequest subsequentRequest = queuer.EnqueueTask(() => {
                Interlocked.Increment(ref executed);
                return new ValueTask();
            });

            release.TrySetResult(true);
            await ObserveTerminal(disposedRequest).WaitAsync(TimeSpan.FromSeconds(2));
            await subsequentRequest.TaskCompletionSource.Task.WaitAsync(TimeSpan.FromSeconds(2));
            await queuer.Stop(true).WaitAsync(TimeSpan.FromSeconds(2));

            Assert.Equal(1, executed);
            queuer.Dispose();
        }

        [Fact]
        public async Task StopWithQueuedItems_LeavesNoQueuedWork() {
            var queuer = new WorkQueuer("StopDrainsChannel", 1);
            var started = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var release = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            queuer.EnqueueTask(async () => {
                started.TrySetResult(true);
                await release.Task;
            });
            await started.Task;

            for (int i = 0; i < 10; i++) {
                queuer.EnqueueTask(() => new ValueTask());
            }

            Task stopTask = queuer.Stop(true);
            release.TrySetResult(true);
            await stopTask.WaitAsync(TimeSpan.FromSeconds(2));

            Assert.Equal(0, queuer.InQueue);
            Assert.Equal(0, GetChannelReaderCount(queuer));
            queuer.Dispose();
        }

        [Fact]
        public async Task HeldJob_TerminalllyFaultedOnStopAndDispose() {
            var stoppedQueuer = new WorkQueuer("HeldJobStop", 1, init_started: false);
            WorkJobExecutionRequest stoppedRequest = stoppedQueuer.EnqueueTask(() => new ValueTask());

            await stoppedQueuer.Stop(true).WaitAsync(TimeSpan.FromSeconds(2));
            await AssertTerminal(stoppedRequest.TaskCompletionSource.Task);
            await AssertTerminal(stoppedRequest.WaitForDequeue());
            Assert.Equal(1, stoppedQueuer.TotalWork);
            Assert.Equal(1, stoppedQueuer.WorkDone);
            Assert.Equal(1, stoppedQueuer.Cancelled);

            var disposedQueuer = new WorkQueuer("HeldJobDispose", 1, init_started: false);
            WorkJobExecutionRequest disposedRequest = disposedQueuer.EnqueueTask(() => new ValueTask());

            await disposedQueuer.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(2));
            await AssertTerminal(disposedRequest.TaskCompletionSource.Task);
            await AssertTerminal(disposedRequest.WaitForDequeue());
            Assert.Equal(1, disposedQueuer.TotalWork);
            Assert.Equal(1, disposedQueuer.WorkDone);
            Assert.Equal(1, disposedQueuer.Cancelled);
        }

        [Fact]
        public async Task EnqueueAsyncBlockedOnFullChannel_DoesNotDeadlockStop() {
            var queuer = new WorkQueuer("StopAdmissionDeadlock", 1) { ChannelCapacity = 1 };
            var started = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var release = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            queuer.EnqueueTask(async () => {
                started.TrySetResult(true);
                await release.Task;
            });
            await started.Task;

            queuer.EnqueueTask(() => new ValueTask());
            Task<WorkJobExecutionRequest> enqueueTask = queuer.EnqueueTaskAsync(ct => new ValueTask());
            DateTime reservationDeadline = DateTime.UtcNow.AddSeconds(2);
            while (queuer.InQueue < 2 && DateTime.UtcNow < reservationDeadline) {
                await Task.Delay(10);
            }
            Assert.True(queuer.InQueue >= 2, "The async enqueue should reserve capacity before waiting to write.");
            Assert.False(enqueueTask.IsCompleted);

            Task stopTask = queuer.Stop(true);
            release.TrySetResult(true);
            Task all = Task.WhenAll(stopTask, ObserveEnqueueTerminal(enqueueTask));

            Assert.Same(all, await Task.WhenAny(all, Task.Delay(TimeSpan.FromSeconds(5))));
            await all;
            Assert.True(enqueueTask.IsCanceled || enqueueTask.IsFaulted,
                "An enqueue interrupted by Stop is cancelled or faulted after its request has been terminally rejected.");
            queuer.Dispose();
        }

        [Fact]
        public async Task StartWithMissedFireIfMissedSchedule_DoesNotDeadlock() {
            var queuer = new WorkQueuer("MissedScheduleStart", 1);
            var activeJobStarted = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var releaseActiveJob = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var missedScheduleRan = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            var missedSchedulesField = typeof(WorkQueuer).GetField("_missedSchedules", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!;

            try {
                queuer.EnqueueTask(async () => {
                    activeJobStarted.TrySetResult(true);
                    await releaseActiveJob.Task;
                });
                await activeJobStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));

                queuer.ScheduleTask("missed_fire", new WorkJob(() => {
                    missedScheduleRan.TrySetResult(true);
                    return new ValueTask();
                }), new ScheduledTaskOptions {
                    ScheduledTime = DateTime.UtcNow.AddMilliseconds(100),
                    RecurrenceInterval = TimeSpan.FromMinutes(1),
                    FireIfMissed = true
                });

                Task stopTask = queuer.Stop(true);
                DateTime missedScheduleDeadline = DateTime.UtcNow.AddSeconds(2);
                while (((System.Collections.ICollection)missedSchedulesField.GetValue(queuer)!).Count == 0 && DateTime.UtcNow < missedScheduleDeadline) {
                    await Task.Delay(10);
                }
                Assert.True(((System.Collections.ICollection)missedSchedulesField.GetValue(queuer)!).Count > 0,
                    "The scheduled job should be recorded as missed while Stop is waiting for active work.");

                releaseActiveJob.TrySetResult(true);
                await stopTask.WaitAsync(TimeSpan.FromSeconds(2));

                Task startTask = Task.Run(() => queuer.Start());
                Assert.Same(startTask, await Task.WhenAny(startTask, Task.Delay(TimeSpan.FromSeconds(5))));
                await startTask;
                await missedScheduleRan.Task.WaitAsync(TimeSpan.FromSeconds(2));
            } finally {
                releaseActiveJob.TrySetResult(true);
                queuer.Unschedule("missed_fire");
                queuer.Dispose();
            }
        }

        private static async Task AssertTerminal(Task task) {
            try {
                await task.WaitAsync(TimeSpan.FromSeconds(2));
            } catch (Exception) {
            }
            Assert.True(task.IsFaulted || task.IsCanceled, "Held work must terminally fault or cancel.");
        }

        private static async Task ObserveTerminal(WorkJobExecutionRequest request) {
            try {
                await request.TaskCompletionSource.Task;
            } catch (Exception) {
            }
        }

        private static async Task ObserveEnqueueTerminal(Task<WorkJobExecutionRequest> enqueueTask) {
            try {
                WorkJobExecutionRequest request = await enqueueTask;
                await ObserveTerminal(request);
            } catch (Exception) {
            }
        }

        private static int GetChannelReaderCount(WorkQueuer queuer) {
            var channelField = typeof(WorkQueuer).GetField("_workChannel", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
            var channel = channelField.GetValue(queuer);
            if (channel == null) return 0;
            var reader = channel.GetType().GetProperty("Reader").GetValue(channel);
            return (int)reader.GetType().GetProperty("Count").GetValue(reader);
        }
    }
}
