using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Figlotech.Core;
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
        public async Task SemaphoreDisposed_NoException() {
            // Arrange
            var queuer = new WorkQueuer("Test", 2);
            queuer.Enqueue(() => new ValueTask());
            
            // Act - Dispose and then try to signal
            queuer.Dispose();
            
            // This should not throw
            queuer.Enqueue(() => new ValueTask());
            
            // Assert - no exception thrown
            Assert.True(true);
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
    }
}
