using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.Core {
    internal sealed class FiAsyncMultiLockEntry : IDisposable {
        readonly SemaphoreSlim _semaphore = new SemaphoreSlim(1, 1);
        int _operationCount;
        int _isDisposed;

        internal SemaphoreSlim Semaphore => _semaphore;

        internal bool IsRetired => Volatile.Read(ref _operationCount) < 0;

        internal bool TryReserve() {
            while (true) {
                int operationCount = Volatile.Read(ref _operationCount);
                if (operationCount < 0) {
                    return false;
                }
                int nextCount = checked(operationCount + 1);
                if (Interlocked.CompareExchange(ref _operationCount, nextCount, operationCount) == operationCount) {
                    return true;
                }
            }
        }

        internal int Release() {
            int remainingOperations = Interlocked.Decrement(ref _operationCount);
            if (remainingOperations < 0) {
                throw new InvalidOperationException("The keyed lock operation count is inconsistent.");
            }
            return remainingOperations;
        }

        internal bool TryRetire() {
            return Interlocked.CompareExchange(ref _operationCount, -1, 0) == 0;
        }

        public void Dispose() {
            if (Interlocked.Exchange(ref _isDisposed, 1) == 0) {
                _semaphore.Dispose();
            }
        }
    }

    /// <summary>
    /// Provides independent asynchronous locks selected by string key. Keyed entries are private
    /// and are reclaimed automatically after their last pending or held acquisition is released.
    /// </summary>
    public sealed class FiAsyncMultiLock {
        readonly ConcurrentDictionary<string, FiAsyncMultiLockEntry> _entries;
        long _defaultTimeoutTicks = TimeSpan.FromSeconds(600).Ticks;

        public FiAsyncMultiLock() {
            _entries = new ConcurrentDictionary<string, FiAsyncMultiLockEntry>();
        }

        /// <summary>
        /// Gets or sets the timeout used by <see cref="Lock(string, TimeSpan?)"/> and
        /// <see cref="LockSync(string, TimeSpan?)"/> when no timeout is supplied.
        /// </summary>
        public TimeSpan DefaultTimeout {
            get => TimeSpan.FromTicks(Interlocked.Read(ref _defaultTimeoutTicks));
            set {
                FiAsyncLock.ValidateTimeout(value, nameof(value));
                Interlocked.Exchange(ref _defaultTimeoutTicks, value.Ticks);
            }
        }

        public Task<FiAsyncDisposableLock> Lock(string key, TimeSpan? timeout = null) {
            return AcquireAsync(key, timeout ?? DefaultTimeout, CancellationToken.None);
        }

        /// <summary>
        /// Acquires the lock for <paramref name="key"/>, observing both a timeout and external
        /// cancellation. A timeout throws <see cref="TimeoutException"/>; cancellation throws
        /// <see cref="OperationCanceledException"/>.
        /// </summary>
        public Task<FiAsyncDisposableLock> Lock(
            string key,
            TimeSpan? timeout,
            CancellationToken cancellationToken) {
            return AcquireAsync(key, timeout ?? DefaultTimeout, cancellationToken);
        }

        public FiAsyncDisposableLock LockSync(string key, TimeSpan? timeout = null) {
            return AcquireSync(key, timeout ?? DefaultTimeout, CancellationToken.None);
        }

        /// <summary>
        /// Synchronously acquires the lock for <paramref name="key"/>, observing both a timeout
        /// and external cancellation.
        /// </summary>
        public FiAsyncDisposableLock LockSync(
            string key,
            TimeSpan? timeout,
            CancellationToken cancellationToken) {
            return AcquireSync(key, timeout ?? DefaultTimeout, cancellationToken);
        }

        internal void ReleaseOperation(string key, FiAsyncMultiLockEntry expectedEntry) {
            int remainingOperations = expectedEntry.Release();
            if (remainingOperations == 0 && expectedEntry.TryRetire()) {
                RemoveRetiredEntry(key, expectedEntry);
            }
        }

        async Task<FiAsyncDisposableLock> AcquireAsync(
            string key,
            TimeSpan timeout,
            CancellationToken cancellationToken) {
            ValidateArguments(key, timeout, cancellationToken);
            FiAsyncMultiLockEntry entry = ReserveOperation(key);
            try {
                bool acquired = await entry.Semaphore.WaitAsync(timeout, cancellationToken).ConfigureAwait(false);
                if (!acquired) {
                    throw new TimeoutException("Timed out waiting to acquire the lock.");
                }
                return new FiAsyncDisposableLock(entry.Semaphore, this, key, entry);
            } catch {
                ReleaseOperation(key, entry);
                throw;
            }
        }

        FiAsyncDisposableLock AcquireSync(
            string key,
            TimeSpan timeout,
            CancellationToken cancellationToken) {
            ValidateArguments(key, timeout, cancellationToken);
            FiAsyncMultiLockEntry entry = ReserveOperation(key);
            try {
                if (!entry.Semaphore.Wait(timeout, cancellationToken)) {
                    throw new TimeoutException("Timed out waiting to acquire the lock.");
                }
                return new FiAsyncDisposableLock(entry.Semaphore, this, key, entry);
            } catch {
                ReleaseOperation(key, entry);
                throw;
            }
        }

        FiAsyncMultiLockEntry ReserveOperation(string key) {
            while (true) {
                FiAsyncMultiLockEntry entry = GetOrCreateEntry(key);
                if (entry.TryReserve()) {
                    return entry;
                }
                RemoveRetiredEntry(key, entry);
            }
        }

        FiAsyncMultiLockEntry GetOrCreateEntry(string key) {
            while (true) {
                if (_entries.TryGetValue(key, out var existingEntry)) {
                    if (!existingEntry.IsRetired) {
                        return existingEntry;
                    }
                    RemoveRetiredEntry(key, existingEntry);
                    continue;
                }

                var createdEntry = new FiAsyncMultiLockEntry();
                if (_entries.TryAdd(key, createdEntry)) {
                    return createdEntry;
                }
                createdEntry.Dispose();
            }
        }

        bool TryRemoveExact(string key, FiAsyncMultiLockEntry expectedEntry) {
            return _entries.TryRemove(
                new KeyValuePair<string, FiAsyncMultiLockEntry>(key, expectedEntry));
        }

        void RemoveRetiredEntry(string key, FiAsyncMultiLockEntry expectedEntry) {
            TryRemoveExact(key, expectedEntry);
            expectedEntry.Dispose();
        }

        static void ValidateArguments(
            string key,
            TimeSpan timeout,
            CancellationToken cancellationToken) {
            if (key == null) {
                throw new ArgumentNullException(nameof(key));
            }
            FiAsyncLock.ValidateTimeout(timeout, nameof(timeout));
            cancellationToken.ThrowIfCancellationRequested();
        }
    }

    /// <summary>
    /// Releases an acquired <see cref="FiAsyncLock"/> or <see cref="FiAsyncMultiLock"/> when disposed.
    /// </summary>
    public sealed class FiAsyncDisposableLock : IDisposable, IAsyncDisposable {
        readonly FiAsyncMultiLockEntry _multiLockEntry;
        readonly string _key;
        readonly FiAsyncMultiLock _multiLock;
        readonly SemaphoreSlim _semaphore;
        int _isDisposed;

        /// <summary>
        /// Creates a release handle for a semaphore acquired by the caller.
        /// Prefer obtaining handles from <see cref="FiAsyncLock"/> or <see cref="FiAsyncMultiLock"/>.
        /// </summary>
        public FiAsyncDisposableLock(SemaphoreSlim semaphore) {
            _semaphore = semaphore ?? throw new ArgumentNullException(nameof(semaphore));
        }

        internal FiAsyncDisposableLock(
            SemaphoreSlim semaphore,
            FiAsyncMultiLock multiLock,
            string key,
            FiAsyncMultiLockEntry multiLockEntry) {
            _semaphore = semaphore;
            _multiLock = multiLock;
            _key = key;
            _multiLockEntry = multiLockEntry;
        }

        public void Dispose() {
            if (Interlocked.Exchange(ref _isDisposed, 1) != 0) {
                return;
            }

            try {
                _semaphore.Release();
            } finally {
                _multiLock?.ReleaseOperation(_key, _multiLockEntry);
            }
        }

        public ValueTask DisposeAsync() {
            Dispose();
            return Fi.CompletedValueTask;
        }
    }

    /// <summary>
    /// An asynchronous mutual-exclusion primitive backed by a
    /// <see cref="SemaphoreSlim"/> with a maximum count of one.
    /// </summary>
    /// <remarks>
    /// This lock is deliberately non-reentrant. A caller that already holds an instance must not
    /// acquire the same instance again before releasing it.
    /// </remarks>
    public sealed class FiAsyncLock : IAsyncDisposable, IDisposable {
        static readonly TimeSpan MaximumTimeout = TimeSpan.FromMilliseconds(int.MaxValue);
        readonly SemaphoreSlim _semaphore = new SemaphoreSlim(1, 1);
        int _isDisposed;

        public FiAsyncLock() {
        }

        /// <summary>
        /// Acquires the lock asynchronously with an unbounded wait.
        /// </summary>
        public Task<FiAsyncDisposableLock> Lock() {
            return Lock(CancellationToken.None);
        }

        /// <summary>
        /// Acquires the lock asynchronously with an unbounded wait and cancellation.
        /// </summary>
        public Task<FiAsyncDisposableLock> Lock(CancellationToken cancellationToken) {
            return AcquireUnboundedAsync(cancellationToken);
        }

        /// <summary>
        /// Acquires the lock synchronously with an unbounded wait.
        /// </summary>
        public FiAsyncDisposableLock LockSync() {
            return LockSync(CancellationToken.None);
        }

        /// <summary>
        /// Acquires the lock synchronously with an unbounded wait and cancellation.
        /// </summary>
        public FiAsyncDisposableLock LockSync(CancellationToken cancellationToken) {
            return AcquireUnboundedSync(cancellationToken);
        }

        public Task<FiAsyncDisposableLock> LockWithTimeout(TimeSpan timeout) {
            return LockWithTimeout(timeout, CancellationToken.None);
        }

        /// <summary>
        /// Acquires the lock asynchronously with a timeout and cancellation.
        /// </summary>
        public Task<FiAsyncDisposableLock> LockWithTimeout(
            TimeSpan timeout,
            CancellationToken cancellationToken) {
            ValidateTimeout(timeout, nameof(timeout));
            return AcquireAsync(timeout, cancellationToken);
        }

        public FiAsyncDisposableLock LockWithTimeoutSync(TimeSpan timeout) {
            return LockWithTimeoutSync(timeout, CancellationToken.None);
        }

        /// <summary>
        /// Acquires the lock synchronously with a timeout and cancellation.
        /// </summary>
        public FiAsyncDisposableLock LockWithTimeoutSync(
            TimeSpan timeout,
            CancellationToken cancellationToken) {
            ValidateTimeout(timeout, nameof(timeout));
            return AcquireSync(timeout, cancellationToken);
        }

        public void Dispose() {
            DisposeSemaphore();
        }

        public ValueTask DisposeAsync() {
            Dispose();
            return Fi.CompletedValueTask;
        }

        async Task<FiAsyncDisposableLock> AcquireAsync(
            TimeSpan timeout,
            CancellationToken cancellationToken) {
            ValidateTimeout(timeout, nameof(timeout));
            bool acquired = await _semaphore.WaitAsync(timeout, cancellationToken).ConfigureAwait(false);
            if (!acquired) {
                throw new TimeoutException("Timed out waiting to acquire the lock.");
            }
            return new FiAsyncDisposableLock(_semaphore);
        }

        FiAsyncDisposableLock AcquireSync(
            TimeSpan timeout,
            CancellationToken cancellationToken) {
            ValidateTimeout(timeout, nameof(timeout));
            if (!_semaphore.Wait(timeout, cancellationToken)) {
                throw new TimeoutException("Timed out waiting to acquire the lock.");
            }
            return new FiAsyncDisposableLock(_semaphore);
        }

        void DisposeSemaphore() {
            if (Interlocked.Exchange(ref _isDisposed, 1) == 0) {
                _semaphore.Dispose();
            }
        }

        internal static void ValidateTimeout(TimeSpan timeout, string parameterName) {
            if (timeout != Timeout.InfiniteTimeSpan
                && (timeout < TimeSpan.Zero || timeout > MaximumTimeout)) {
                throw new ArgumentOutOfRangeException(
                    parameterName,
                    "Timeout must be infinite or between zero and Int32.MaxValue milliseconds.");
            }
        }

        async Task<FiAsyncDisposableLock> AcquireUnboundedAsync(
            CancellationToken cancellationToken) {
            await _semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
            return new FiAsyncDisposableLock(_semaphore);
        }

        FiAsyncDisposableLock AcquireUnboundedSync(
            CancellationToken cancellationToken) {
            _semaphore.Wait(cancellationToken);
            return new FiAsyncDisposableLock(_semaphore);
        }
    }
}
