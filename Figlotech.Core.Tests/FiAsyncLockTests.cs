using System;
using System.Collections.Generic;
using System.Linq;
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
        public async Task MultiLock_AutoRemoveWaitsForPendingAcquisitions() {
            var multiLock = new FiAsyncMultiLock { AutoRemoveLocks = true };
            var firstHandle = await multiLock.Lock("key");
            var pendingHandle = multiLock.Lock("key", TimeSpan.FromSeconds(2));

            await Task.Delay(50);
            firstHandle.Dispose();

            await using var secondHandle = await pendingHandle;
            Assert.True(multiLock.ContainsKey("key"));

            await secondHandle.DisposeAsync();
            Assert.False(multiLock.ContainsKey("key"));
        }

        [Fact]
        public async Task MultiLock_IndexerAcquisitionParticipatesInSafeAutoRemoval() {
            var multiLock = new FiAsyncMultiLock { AutoRemoveLocks = true };
            var keyedLock = multiLock["key"];

            var handle = await keyedLock.Lock();
            Assert.True(multiLock.ContainsKey("key"));

            handle.Dispose();
            Assert.False(multiLock.ContainsKey("key"));
        }

        [Fact]
        public async Task MultiLock_DefaultTimeoutIsUsedWhenNoTimeoutIsPassed() {
            var multiLock = new FiAsyncMultiLock { DefaultTimeout = TimeSpan.FromMilliseconds(50) };
            await using var firstHandle = await multiLock.Lock("key", TimeSpan.FromSeconds(1));

            await Assert.ThrowsAsync<TimeoutException>(() => multiLock.Lock("key"));
        }

        [Fact]
        public async Task MultiLock_CancelledWaiterReleasesItsAutoRemoveReservation() {
            var multiLock = new FiAsyncMultiLock { AutoRemoveLocks = true };
            var firstHandle = await multiLock.Lock("key");
            using var cancellation = new CancellationTokenSource();

            var pendingHandle = multiLock.Lock("key", TimeSpan.FromSeconds(5), cancellation.Token);
            cancellation.Cancel();

            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await pendingHandle);
            Assert.True(multiLock.ContainsKey("key"));

            firstHandle.Dispose();
            Assert.False(multiLock.ContainsKey("key"));
        }

        [Fact]
        public async Task MultiLock_StaleIndexerValueCannotCreateASecondLockForTheSameKey() {
            var multiLock = new FiAsyncMultiLock { AutoRemoveLocks = true };
            var staleReference = multiLock["key"];

            await using (await multiLock.Lock("key")) {
            }
            Assert.False(multiLock.ContainsKey("key"));

            await using var staleHandle = await staleReference.Lock();
            await Assert.ThrowsAsync<TimeoutException>(
                () => multiLock.Lock("key", TimeSpan.FromMilliseconds(50)));
        }

        [Fact]
        public async Task MultiLock_RemoveDoesNotSplitAnActiveLock() {
            var multiLock = new FiAsyncMultiLock();
            await using var handle = await multiLock.Lock("key");

            Assert.False(multiLock.Remove("key"));
            await Assert.ThrowsAsync<TimeoutException>(
                () => multiLock.Lock("key", TimeSpan.FromMilliseconds(50)));
        }

        [Fact]
        public void MultiLock_RemovePairRequiresBothKeyAndValueToMatch() {
            var multiLock = new FiAsyncMultiLock();
            var storedLock = multiLock["key"];
            using var otherLock = new FiAsyncLock();

            var removed = multiLock.Remove(new KeyValuePair<string, FiAsyncLock>("key", otherLock));

            Assert.False(removed);
            Assert.Same(storedLock, multiLock["key"]);
        }

        [Fact]
        public async Task MultiLock_ClearRemovesIdleEntriesWithoutSplittingActiveEntries() {
            var multiLock = new FiAsyncMultiLock();
            var activeHandle = await multiLock.Lock("active");
            _ = multiLock["idle"];

            multiLock.Clear();

            Assert.True(multiLock.ContainsKey("active"));
            Assert.False(multiLock.ContainsKey("idle"));
            await Assert.ThrowsAsync<TimeoutException>(
                () => multiLock.Lock("active", TimeSpan.FromMilliseconds(50)));
            activeHandle.Dispose();
        }

        [Fact]
        public async Task MultiLock_IndexerCannotReplaceAnActiveEntry() {
            var multiLock = new FiAsyncMultiLock();
            var activeHandle = await multiLock.Lock("key");
            using var replacement = new FiAsyncLock();

            Assert.Throws<InvalidOperationException>(() => multiLock["key"] = replacement);
            await Assert.ThrowsAsync<TimeoutException>(
                () => multiLock.Lock("key", TimeSpan.FromMilliseconds(50)));
            activeHandle.Dispose();
        }

        [Fact]
        public async Task MultiLock_UserSuppliedLockCanBeRemovedAndAddedAgain() {
            var multiLock = new FiAsyncMultiLock();
            using var suppliedLock = new FiAsyncLock();
            multiLock.Add("key", suppliedLock);

            Assert.True(multiLock.Remove("key"));
            multiLock.Add("key", suppliedLock);

            await using var handle = await multiLock.Lock("key", TimeSpan.FromSeconds(1));
        }

        [Fact]
        public async Task MultiLock_UserSuppliedLockIsNotAutoRemoved() {
            var multiLock = new FiAsyncMultiLock { AutoRemoveLocks = true };
            using var suppliedLock = new FiAsyncLock();
            multiLock.Add("key", suppliedLock);

            await using (await multiLock.Lock("key", TimeSpan.FromSeconds(1))) {
            }

            Assert.True(multiLock.ContainsKey("key"));
        }

        [Fact]
        public void MultiLock_OwnedLockCannotBeAliasedThroughDictionaryMutation() {
            var multiLock = new FiAsyncMultiLock();
            var ownedLock = multiLock["first"];

            Assert.Throws<ArgumentException>(() => multiLock.Add("second", ownedLock));
            Assert.Throws<ArgumentException>(() => multiLock["second"] = ownedLock);
        }

        [Fact]
        public async Task MultiLock_AutoRemoveStressMaintainsMutualExclusionAndEvictsIdleKeys() {
            var multiLock = new FiAsyncMultiLock { AutoRemoveLocks = true };
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
            Assert.False(multiLock.ContainsKey("shared"));
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
