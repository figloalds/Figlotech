using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Figlotech.Core.Tests {
    public class FiAsyncLockTests {
        [Fact]
        public async Task Lock_SerializesConcurrentCallers() {
            using var asyncLock = new FiAsyncLock();
            var activeCount = 0;
            var maximumActiveCount = 0;
            var completedCount = 0;

            var tasks = Enumerable.Range(0, 32).Select(async _ => {
                await using var handle = await asyncLock.Lock();
                var active = Interlocked.Increment(ref activeCount);
                UpdateMaximum(ref maximumActiveCount, active);

                await Task.Yield();
                Interlocked.Increment(ref completedCount);
                Interlocked.Decrement(ref activeCount);
            });

            await Task.WhenAll(tasks);

            Assert.Equal(32, completedCount);
            Assert.Equal(1, maximumActiveCount);
        }

        [Fact]
        public async Task LockWithTimeout_TimesOutWithoutChangingLockState() {
            using var asyncLock = new FiAsyncLock();
            await using var firstHandle = await asyncLock.Lock();

            await Assert.ThrowsAsync<TimeoutException>(
                () => asyncLock.LockWithTimeout(TimeSpan.FromMilliseconds(50)));

            await firstHandle.DisposeAsync();
            await using var nextHandle = await asyncLock.LockWithTimeout(TimeSpan.FromSeconds(1));
        }

        [Fact]
        public async Task LockWithTimeout_PropagatesObjectDisposedExceptionDirectly() {
            var asyncLock = new FiAsyncLock();
            asyncLock.Dispose();

            await Assert.ThrowsAsync<ObjectDisposedException>(() => asyncLock.LockWithTimeout(TimeSpan.Zero));
        }

        [Fact]
        public void LockWithTimeoutSync_PropagatesInvalidTimeoutDirectly() {
            using var asyncLock = new FiAsyncLock();

            Assert.Throws<ArgumentOutOfRangeException>(
                () => asyncLock.LockWithTimeoutSync(TimeSpan.FromMilliseconds(-2)));
            Assert.Throws<ArgumentOutOfRangeException>(
                () => asyncLock.LockWithTimeoutSync(TimeSpan.FromTicks(-1)));
            Assert.Throws<ArgumentOutOfRangeException>(
                () => asyncLock.LockWithTimeoutSync(
                    TimeSpan.FromMilliseconds(int.MaxValue).Add(TimeSpan.FromTicks(1))));
        }

        [Fact]
        public async Task Handle_DisposeIsThreadSafeAndReleasesExactlyOnce() {
            using var asyncLock = new FiAsyncLock();
            var handle = await asyncLock.Lock();

            await Task.WhenAll(Enumerable.Range(0, 16).Select(_ => Task.Run(handle.Dispose)));

            await using var firstHandle = await asyncLock.LockWithTimeout(TimeSpan.FromSeconds(1));
            await Assert.ThrowsAsync<TimeoutException>(
                () => asyncLock.LockWithTimeout(TimeSpan.FromMilliseconds(50)));
        }

        [Fact]
        public async Task Lock_CancellationIsDistinctFromTimeoutAndDoesNotChangeLockState() {
            using var asyncLock = new FiAsyncLock();
            var firstHandle = await asyncLock.Lock();
            using var cancellation = new CancellationTokenSource();

            var pendingHandle = asyncLock.Lock(cancellation.Token);
            cancellation.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await pendingHandle);
            firstHandle.Dispose();
            await using var nextHandle = await asyncLock.LockWithTimeout(TimeSpan.FromSeconds(1));
        }

        [Fact]
        public void LockSync_ObservesCancellation() {
            using var asyncLock = new FiAsyncLock();
            using var cancellation = new CancellationTokenSource();
            cancellation.Cancel();

            Assert.ThrowsAny<OperationCanceledException>(() => asyncLock.LockSync(cancellation.Token));
        }

        [Fact]
        public void AbandonedHandle_DoesNotReleaseSemaphoreFromFinalization() {
            using var semaphore = new SemaphoreSlim(0, 1);
            var weakHandle = CreateAbandonedHandle(semaphore);

            GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, true, true);
            GC.WaitForPendingFinalizers();
            GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, true, true);

            Assert.False(weakHandle.IsAlive);
            Assert.Equal(0, semaphore.CurrentCount);
        }

        [Fact]
        public void Handle_ReportsAnInvalidManualRelease() {
            using var semaphore = new SemaphoreSlim(1, 1);
            var handle = new FiAsyncDisposableLock(semaphore);

            Assert.Throws<SemaphoreFullException>(handle.Dispose);
        }

        [Fact]
        public async Task MultiLock_SerializesSameKeyButAllowsDifferentKeys() {
            var multiLock = new FiAsyncMultiLock();
            await using var firstHandle = await multiLock.Lock("first");

            var sameKey = multiLock.Lock("first", TimeSpan.FromMilliseconds(100));
            await using var otherKey = await multiLock.Lock("second", TimeSpan.FromSeconds(1));

            await Assert.ThrowsAsync<TimeoutException>(async () => await sameKey);
        }

        [Fact]
        public async Task MultiLock_PendingAcquisitionKeepsTheKeyLockedUntilItReleases() {
            var multiLock = new FiAsyncMultiLock();
            var firstHandle = await multiLock.Lock("key");
            var pendingHandle = multiLock.Lock("key", TimeSpan.FromSeconds(2));

            await Task.Delay(50);
            firstHandle.Dispose();

            var secondHandle = await pendingHandle;
            await Assert.ThrowsAsync<TimeoutException>(
                () => multiLock.Lock("key", TimeSpan.FromMilliseconds(50)));

            await secondHandle.DisposeAsync();
            await using var nextHandle = await multiLock.Lock("key", TimeSpan.FromSeconds(1));
        }

        [Fact]
        public async Task MultiLock_DefaultTimeoutIsUsedWhenNoTimeoutIsPassed() {
            var multiLock = new FiAsyncMultiLock { DefaultTimeout = TimeSpan.FromMilliseconds(50) };
            await using var firstHandle = await multiLock.Lock("key", TimeSpan.FromSeconds(1));

            await Assert.ThrowsAsync<TimeoutException>(() => multiLock.Lock("key"));
        }

        [Fact]
        public async Task MultiLock_CancelledWaiterDoesNotChangeLockState() {
            var multiLock = new FiAsyncMultiLock();
            var firstHandle = await multiLock.Lock("key");
            using var cancellation = new CancellationTokenSource();

            var pendingHandle = multiLock.Lock("key", TimeSpan.FromSeconds(5), cancellation.Token);
            cancellation.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await pendingHandle);
            await Assert.ThrowsAsync<TimeoutException>(
                () => multiLock.Lock("key", TimeSpan.FromMilliseconds(50)));

            firstHandle.Dispose();
            await using var nextHandle = await multiLock.Lock("key", TimeSpan.FromSeconds(1));
        }

        [Fact]
        public void MultiLock_DoesNotExposeDictionaryOrCleanupMutationApi() {
            var multiLockType = typeof(FiAsyncMultiLock);

            Assert.False(typeof(IDictionary<string, FiAsyncLock>).IsAssignableFrom(multiLockType));
            Assert.Null(multiLockType.GetProperty("AutoRemoveLocks"));
            Assert.Null(multiLockType.GetProperty("Item"));
            Assert.Null(multiLockType.GetMethod("Add"));
            Assert.Null(multiLockType.GetMethod("Clear"));
            Assert.Null(multiLockType.GetMethod("Remove", new[] { typeof(string) }));
        }

        [Fact]
        public async Task MultiLock_RejectsNullKeys() {
            var multiLock = new FiAsyncMultiLock();

            await Assert.ThrowsAsync<ArgumentNullException>(() => multiLock.Lock(null!));
            Assert.Throws<ArgumentNullException>(() => multiLock.LockSync(null!));
        }

        [Fact]
        public async Task MultiLock_RemovesIdleEntriesAutomatically() {
            var multiLock = new FiAsyncMultiLock();

            for (var index = 0; index < 100; index++) {
                await using (await multiLock.Lock($"key-{index}")) {
                }
            }

            Assert.Equal(0, GetMultiLockEntryCount(multiLock));
        }

        [Fact]
        public async Task MultiLock_RetirementStressMaintainsMutualExclusion() {
            var multiLock = new FiAsyncMultiLock();
            var activeCount = 0;
            var maximumActiveCount = 0;

            var tasks = Enumerable.Range(0, 16).Select(async _ => {
                for (var iteration = 0; iteration < 25; iteration++) {
                    await using var handle = await multiLock.Lock("shared", TimeSpan.FromSeconds(5));
                    var active = Interlocked.Increment(ref activeCount);
                    UpdateMaximum(ref maximumActiveCount, active);
                    await Task.Yield();
                    Interlocked.Decrement(ref activeCount);
                }
            });

            await Task.WhenAll(tasks);

            Assert.Equal(1, maximumActiveCount);
            Assert.Equal(0, GetMultiLockEntryCount(multiLock));
        }

        private static int GetMultiLockEntryCount(FiAsyncMultiLock multiLock) {
            var entriesField = typeof(FiAsyncMultiLock).GetField(
                "_entries",
                BindingFlags.Instance | BindingFlags.NonPublic)
                ?? throw new InvalidOperationException("Could not find the keyed lock entries.");
            var entries = entriesField.GetValue(multiLock)
                ?? throw new InvalidOperationException("Could not read the keyed lock entries.");
            var countProperty = entries.GetType().GetProperty("Count")
                ?? throw new InvalidOperationException("Could not read the keyed lock entry count.");
            return countProperty.GetValue(entries) is int count
                ? count
                : throw new InvalidOperationException("The keyed lock entry count is invalid.");
        }

        private static void UpdateMaximum(ref int maximum, int candidate) {
            var current = Volatile.Read(ref maximum);
            while (candidate > current) {
                var previous = Interlocked.CompareExchange(ref maximum, candidate, current);
                if (previous == current) {
                    return;
                }
                current = previous;
            }
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static WeakReference CreateAbandonedHandle(SemaphoreSlim semaphore) {
            return new WeakReference(new FiAsyncDisposableLock(semaphore));
        }
    }
}
