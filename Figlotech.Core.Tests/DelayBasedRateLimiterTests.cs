using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Figlotech.Core.Tests {
    public class DelayBasedRateLimiterTests {
        [Theory]
        [InlineData(0, 60, 5, "maxRequests")]
        [InlineData(-1, 60, 5, "maxRequests")]
        [InlineData(30, 0, 5, "windowSeconds")]
        [InlineData(30, -1, 5, "windowSeconds")]
        [InlineData(30, double.NaN, 5, "windowSeconds")]
        [InlineData(30, double.PositiveInfinity, 5, "windowSeconds")]
        [InlineData(30, double.NegativeInfinity, 5, "windowSeconds")]
        [InlineData(30, 60, -1, "maxDelay")]
        public void Constructor_RejectsInvalidConfiguration(int maxRequests, double windowSeconds, double maxDelay, string parameter) {
            var exception = Assert.Throws<ArgumentOutOfRangeException>(
                () => new DelayBasedRateLimiter(maxRequests, windowSeconds, TimeSpan.FromSeconds(maxDelay)));

            Assert.Equal(parameter, exception.ParamName);
        }

        [Fact]
        public async Task WaitAsync_SkipsMicroscopicPenalties() {
            var limiter = new DelayBasedRateLimiter();

            for (var i = 0; i < 6; i++) {
                var wait = limiter.WaitAsync();
                Assert.True(wait.IsCompletedSuccessfully);
                await wait;
            }
        }

        [Fact]
        public async Task WaitAsync_AppliesProgressivePenaltyAsBucketEmpties() {
            var limiter = new DelayBasedRateLimiter(maxRequests: 2, windowSeconds: 3600, maxDelay: TimeSpan.FromMilliseconds(200));
            await limiter.WaitAsync(); // 0.2 / 8 seconds is below the 50 ms threshold.
            var stopwatch = Stopwatch.StartNew();

            await limiter.WaitAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));

            Assert.True(stopwatch.Elapsed >= TimeSpan.FromMilliseconds(150));
        }

        [Fact]
        public async Task WaitAsync_ConcurrentCallersWaitForRefillInsteadOfBeingRejected() {
            var limiter = new DelayBasedRateLimiter(maxRequests: 2, windowSeconds: 0.2, maxDelay: TimeSpan.Zero);
            var stopwatch = Stopwatch.StartNew();

            var requests = Enumerable.Range(0, 8).Select(_ => Task.Run(async () => await limiter.WaitAsync()));
            await Task.WhenAll(requests).WaitAsync(TimeSpan.FromSeconds(10));

            // Two initial tokens plus six refills at ten tokens per second.
            Assert.True(stopwatch.Elapsed >= TimeSpan.FromMilliseconds(550));
        }

        [Fact]
        public async Task WaitAsync_ConcurrentCallersPayPenaltiesSequentially() {
            var limiter = new DelayBasedRateLimiter(maxRequests: 4, windowSeconds: 3600, maxDelay: TimeSpan.FromMilliseconds(200));
            var stopwatch = Stopwatch.StartNew();

            var requests = Enumerable.Range(0, 4).Select(_ => Task.Run(async () => await limiter.WaitAsync()));
            await Task.WhenAll(requests).WaitAsync(TimeSpan.FromSeconds(10));

            // Penalties of 0.2 * 0.75^3 ≈ 84 ms and 200 ms add up instead of overlapping.
            Assert.True(stopwatch.Elapsed >= TimeSpan.FromMilliseconds(260));
        }

        [Fact]
        public async Task WaitAsync_ConcurrentCallersAreReleasedInArrivalOrder() {
            var limiter = new DelayBasedRateLimiter(maxRequests: 1, windowSeconds: 0.06, maxDelay: TimeSpan.Zero);
            await limiter.WaitAsync();
            var released = new ConcurrentQueue<int>();

            var requests = Enumerable.Range(0, 5)
                .Select(i => limiter.WaitAsync().AsTask().ContinueWith(
                    _ => released.Enqueue(i), TaskContinuationOptions.ExecuteSynchronously))
                .ToArray();
            await Task.WhenAll(requests).WaitAsync(TimeSpan.FromSeconds(10));

            Assert.Equal(Enumerable.Range(0, 5), released);
        }

        [Fact]
        public async Task WaitAsync_CanceledWaiterReturnsTokenToNextCaller() {
            var limiter = new DelayBasedRateLimiter(maxRequests: 1, windowSeconds: 0.5, maxDelay: TimeSpan.Zero);
            await limiter.WaitAsync();
            var stopwatch = Stopwatch.StartNew();
            using var cancellation = new CancellationTokenSource();
            var canceled = limiter.WaitAsync(cancellation.Token).AsTask(); // Token due at 0.5 s.
            var queued = limiter.WaitAsync().AsTask();                     // Token due at 1.0 s.

            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => canceled.WaitAsync(TimeSpan.FromSeconds(10)));
            await limiter.WaitAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));
            await queued.WaitAsync(TimeSpan.FromSeconds(10));

            // The returned token lets the new caller go with the queued one instead of at 1.5 s.
            Assert.True(stopwatch.Elapsed < TimeSpan.FromMilliseconds(1300));
        }

        [Fact]
        public async Task WaitAsync_IdleRefillIsCappedAtCapacity() {
            var limiter = new DelayBasedRateLimiter(maxRequests: 2, windowSeconds: 0.2, maxDelay: TimeSpan.Zero);
            await Task.Delay(500);
            var stopwatch = Stopwatch.StartNew();

            for (var i = 0; i < 4; i++) {
                await limiter.WaitAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(10));
            }

            Assert.True(stopwatch.Elapsed >= TimeSpan.FromMilliseconds(150));
        }

        [Fact]
        public async Task WaitAsync_PreCanceledCallDoesNotConsumeToken() {
            var limiter = new DelayBasedRateLimiter(maxRequests: 1, windowSeconds: 3600, maxDelay: TimeSpan.Zero);
            using var cancellation = new CancellationTokenSource();
            cancellation.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => limiter.WaitAsync(cancellation.Token).AsTask());

            var next = limiter.WaitAsync();
            Assert.True(next.IsCompletedSuccessfully);
            await next;
        }

        [Fact]
        public async Task WaitAsync_CanCancelWhileWaitingForToken() {
            var limiter = new DelayBasedRateLimiter(maxRequests: 1, windowSeconds: 3600, maxDelay: TimeSpan.Zero);
            await limiter.WaitAsync();
            using var cancellation = new CancellationTokenSource();
            var pending = limiter.WaitAsync(cancellation.Token).AsTask();
            Assert.False(pending.IsCompleted);

            cancellation.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending.WaitAsync(TimeSpan.FromSeconds(10)));
        }

        [Fact]
        public async Task WaitAsync_CanCancelDuringPenaltyWithoutRefundingConsumedToken() {
            var limiter = new DelayBasedRateLimiter(maxRequests: 2, windowSeconds: 3600, maxDelay: TimeSpan.FromMilliseconds(200));
            await limiter.WaitAsync();
            using var cancellation = new CancellationTokenSource();
            var pending = limiter.WaitAsync(cancellation.Token).AsTask();
            Assert.False(pending.IsCompleted);
            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending.WaitAsync(TimeSpan.FromSeconds(10)));

            // A refunded token would release the next caller after its 200 ms penalty; without a
            // refund it must wait about half an hour for a refill.
            using var nextCancellation = new CancellationTokenSource();
            var next = limiter.WaitAsync(nextCancellation.Token).AsTask();
            await Task.Delay(1000);
            Assert.False(next.IsCompleted);
            nextCancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => next.WaitAsync(TimeSpan.FromSeconds(10)));
        }

        [Fact]
        public async Task WaitAsync_ExtremelyLongWaitsRemainCancelable() {
            var limiter = new DelayBasedRateLimiter(maxRequests: 1, windowSeconds: double.MaxValue, maxDelay: TimeSpan.MaxValue);
            using var penaltyCancellation = new CancellationTokenSource();
            var penalty = limiter.WaitAsync(penaltyCancellation.Token).AsTask();
            Assert.False(penalty.IsCompleted);
            penaltyCancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => penalty.WaitAsync(TimeSpan.FromSeconds(10)));

            using var refillCancellation = new CancellationTokenSource();
            var refill = limiter.WaitAsync(refillCancellation.Token).AsTask();
            Assert.False(refill.IsCompleted);
            refillCancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => refill.WaitAsync(TimeSpan.FromSeconds(10)));
        }
    }
}
