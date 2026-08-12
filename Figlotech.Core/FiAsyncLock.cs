using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.Core {
    internal sealed class FiAsyncMultiLockEntry {
        int _operationCount;

        internal FiAsyncMultiLockEntry(FiAsyncLock asyncLock) {
            AsyncLock = asyncLock;
        }

        internal FiAsyncLock AsyncLock { get; }

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
    }

    /// <summary>
    /// Provides independent asynchronous locks selected by string key.
    /// </summary>
    public sealed class FiAsyncMultiLock : IDictionary<string, FiAsyncLock> {
        readonly ConcurrentDictionary<string, FiAsyncMultiLockEntry> _entries;
        long _defaultTimeoutTicks = TimeSpan.FromSeconds(600).Ticks;
        bool _autoRemoveLocks;

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

        /// <summary>
        /// When true, locks created by this instance are removed and disposed once they have no
        /// pending or held acquisitions. User-supplied dictionary values are retained because
        /// their use outside this instance cannot be tracked safely.
        /// </summary>
        public bool AutoRemoveLocks {
            get => Volatile.Read(ref _autoRemoveLocks);
            set => Volatile.Write(ref _autoRemoveLocks, value);
        }

        public FiAsyncLock this[string key] {
            get => GetOrCreateLock(key);
            set {
                if (value == null) {
                    throw new ArgumentNullException(nameof(value));
                }

                while (true) {
                    if (!_entries.TryGetValue(key, out var current)) {
                        ValidateExternalValue(value, nameof(value));
                        if (_entries.TryAdd(key, new FiAsyncMultiLockEntry(value))) {
                            return;
                        }
                        continue;
                    }
                    if (ReferenceEquals(current.AsyncLock, value) && !current.IsRetired) {
                        return;
                    }
                    ValidateExternalValue(value, nameof(value));
                    if (!current.TryRetire()) {
                        if (current.IsRetired) {
                            RemoveRetiredLock(key, current);
                            continue;
                        }
                        throw new InvalidOperationException("Cannot replace an active keyed lock.");
                    }
                    var replacement = new FiAsyncMultiLockEntry(value);
                    if (_entries.TryUpdate(key, replacement, current)) {
                        DisposeIfOwned(key, current.AsyncLock);
                        return;
                    }
                    DisposeIfOwned(key, current.AsyncLock);
                }
            }
        }

        public ICollection<string> Keys => _entries.Keys;

        public ICollection<FiAsyncLock> Values {
            get {
                var values = new List<FiAsyncLock>(_entries.Count);
                foreach (var entry in _entries.Values) {
                    values.Add(entry.AsyncLock);
                }
                return values.AsReadOnly();
            }
        }

        public int Count => _entries.Count;

        public bool IsReadOnly => false;

        public void Add(string key, FiAsyncLock value) {
            if (value == null) {
                throw new ArgumentNullException(nameof(value));
            }
            ValidateExternalValue(value, nameof(value));

            if (!_entries.TryAdd(key, new FiAsyncMultiLockEntry(value))) {
                throw new ArgumentException("An item with the same key has already been added.", nameof(key));
            }
        }

        public void Add(KeyValuePair<string, FiAsyncLock> item) {
            Add(item.Key, item.Value);
        }

        /// <summary>
        /// Removes all idle entries. Entries with pending or held acquisitions are left in place,
        /// preventing <see cref="Clear"/> from splitting an active keyed lock.
        /// </summary>
        public void Clear() {
            foreach (var item in _entries) {
                TryRemoveIdleLock(item.Key, item.Value);
            }
        }

        public bool Contains(KeyValuePair<string, FiAsyncLock> item) {
            return _entries.TryGetValue(item.Key, out var entry)
                && EqualityComparer<FiAsyncLock>.Default.Equals(entry.AsyncLock, item.Value);
        }

        public bool ContainsKey(string key) {
            return _entries.ContainsKey(key);
        }

        public void CopyTo(KeyValuePair<string, FiAsyncLock>[] array, int arrayIndex) {
            var snapshot = new List<KeyValuePair<string, FiAsyncLock>>(_entries.Count);
            foreach (var item in _entries) {
                snapshot.Add(new KeyValuePair<string, FiAsyncLock>(item.Key, item.Value.AsyncLock));
            }
            snapshot.CopyTo(array, arrayIndex);
        }

        public IEnumerator<KeyValuePair<string, FiAsyncLock>> GetEnumerator() {
            foreach (var item in _entries) {
                yield return new KeyValuePair<string, FiAsyncLock>(item.Key, item.Value.AsyncLock);
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

        public bool Remove(string key) {
            while (_entries.TryGetValue(key, out var current)) {
                if (!current.TryRetire()) {
                    if (current.IsRetired) {
                        RemoveRetiredLock(key, current);
                        continue;
                    }
                    return false;
                }
                bool removed = TryRemoveExact(key, current);
                DisposeIfOwned(key, current.AsyncLock);
                if (removed) {
                    return true;
                }
            }
            return false;
        }

        public bool Remove(KeyValuePair<string, FiAsyncLock> item) {
            while (_entries.TryGetValue(item.Key, out var current)
                && EqualityComparer<FiAsyncLock>.Default.Equals(current.AsyncLock, item.Value)) {
                if (!current.TryRetire()) {
                    if (current.IsRetired) {
                        bool removedRetired = TryRemoveExact(item.Key, current);
                        DisposeIfOwned(item.Key, current.AsyncLock);
                        return removedRetired;
                    }
                    return false;
                }
                bool removed = TryRemoveExact(item.Key, current);
                DisposeIfOwned(item.Key, current.AsyncLock);
                return removed;
            }
            return false;
        }

        public bool TryGetValue(string key, out FiAsyncLock value) {
            if (_entries.TryGetValue(key, out var entry)) {
                value = entry.AsyncLock;
                return true;
            }
            value = null;
            return false;
        }

        IEnumerator IEnumerable.GetEnumerator() {
            return GetEnumerator();
        }

        internal Task<FiAsyncDisposableLock> AcquireUnboundedAsync(
            string key,
            CancellationToken cancellationToken) {
            return AcquireAsync(key, Timeout.InfiniteTimeSpan, cancellationToken);
        }

        internal FiAsyncDisposableLock AcquireUnboundedSync(
            string key,
            CancellationToken cancellationToken) {
            return AcquireSync(key, Timeout.InfiniteTimeSpan, cancellationToken);
        }

        internal void DisposeOwnedLock(string key, FiAsyncLock expectedLock) {
            while (_entries.TryGetValue(key, out var entry)
                && ReferenceEquals(entry.AsyncLock, expectedLock)) {
                if (!entry.TryRetire()) {
                    if (entry.IsRetired) {
                        RemoveRetiredLock(key, entry);
                        break;
                    }
                    throw new InvalidOperationException("Cannot dispose an active keyed lock.");
                }
                TryRemoveExact(key, entry);
                break;
            }
            expectedLock.DisposeSemaphore();
        }

        internal void ReleaseOperation(string key, FiAsyncMultiLockEntry expectedEntry) {
            int remainingOperations = expectedEntry.Release();
            if (remainingOperations == 0
                && AutoRemoveLocks
                && expectedEntry.AsyncLock.IsOwnedBy(this, key)
                && expectedEntry.TryRetire()) {
                TryRemoveExact(key, expectedEntry);
                expectedEntry.AsyncLock.DisposeSemaphore();
            }
        }

        async Task<FiAsyncDisposableLock> AcquireAsync(
            string key,
            TimeSpan timeout,
            CancellationToken cancellationToken) {
            FiAsyncMultiLockEntry entry = ReserveOperation(key);
            try {
                return await entry.AsyncLock.AcquireAsync(
                    timeout,
                    cancellationToken,
                    this,
                    key,
                    entry).ConfigureAwait(false);
            } catch {
                ReleaseOperation(key, entry);
                throw;
            }
        }

        FiAsyncDisposableLock AcquireSync(
            string key,
            TimeSpan timeout,
            CancellationToken cancellationToken) {
            FiAsyncMultiLockEntry entry = ReserveOperation(key);
            try {
                return entry.AsyncLock.AcquireSync(timeout, cancellationToken, this, key, entry);
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
                RemoveRetiredLock(key, entry);
            }
        }

        FiAsyncLock GetOrCreateLock(string key) {
            return GetOrCreateEntry(key).AsyncLock;
        }

        FiAsyncMultiLockEntry GetOrCreateEntry(string key) {
            while (true) {
                if (_entries.TryGetValue(key, out var existingEntry)) {
                    if (!existingEntry.IsRetired) {
                        return existingEntry;
                    }
                    RemoveRetiredLock(key, existingEntry);
                    continue;
                }

                var createdLock = new FiAsyncLock(this, key);
                var createdEntry = new FiAsyncMultiLockEntry(createdLock);
                if (_entries.TryAdd(key, createdEntry)) {
                    return createdEntry;
                }
                createdLock.DisposeSemaphore();
            }
        }

        bool TryRemoveExact(string key, FiAsyncMultiLockEntry expectedEntry) {
            return ((ICollection<KeyValuePair<string, FiAsyncMultiLockEntry>>)_entries).Remove(
                new KeyValuePair<string, FiAsyncMultiLockEntry>(key, expectedEntry));
        }

        void TryRemoveIdleLock(string key, FiAsyncMultiLockEntry expectedEntry) {
            if (expectedEntry.TryRetire()) {
                TryRemoveExact(key, expectedEntry);
                DisposeIfOwned(key, expectedEntry.AsyncLock);
            } else if (expectedEntry.IsRetired) {
                RemoveRetiredLock(key, expectedEntry);
            }
        }

        void RemoveRetiredLock(string key, FiAsyncMultiLockEntry expectedEntry) {
            TryRemoveExact(key, expectedEntry);
            DisposeIfOwned(key, expectedEntry.AsyncLock);
        }

        void DisposeIfOwned(string key, FiAsyncLock asyncLock) {
            if (asyncLock.IsOwnedBy(this, key)) {
                asyncLock.DisposeSemaphore();
            }
        }

        static void ValidateExternalValue(FiAsyncLock asyncLock, string parameterName) {
            if (asyncLock.HasOwner) {
                throw new ArgumentException(
                    "A lock obtained from a FiAsyncMultiLock cannot be assigned to a dictionary entry.",
                    parameterName);
            }
        }
    }

    /// <summary>
    /// Releases an acquired <see cref="FiAsyncLock"/> when disposed.
    /// </summary>
    public sealed class FiAsyncDisposableLock : IDisposable, IAsyncDisposable {
        readonly FiAsyncMultiLockEntry _multiLockEntry;
        readonly string _key;
        readonly FiAsyncMultiLock _multiLock;
        readonly SemaphoreSlim _semaphore;
        int _isDisposed;

        /// <summary>
        /// Creates a release handle for a semaphore acquired by the caller.
        /// Prefer obtaining handles from <see cref="FiAsyncLock"/>.
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
        readonly FiAsyncMultiLock _owner;
        readonly string _ownerKey;
        readonly SemaphoreSlim _semaphore = new SemaphoreSlim(1, 1);
        int _isDisposed;

        public FiAsyncLock() {
        }

        internal FiAsyncLock(FiAsyncMultiLock owner, string ownerKey) {
            _owner = owner;
            _ownerKey = ownerKey;
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
            if (_owner != null) {
                return _owner.AcquireUnboundedAsync(_ownerKey, cancellationToken);
            }
            return AcquireUnboundedAsync(cancellationToken, null, null);
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
            if (_owner != null) {
                return _owner.AcquireUnboundedSync(_ownerKey, cancellationToken);
            }
            return AcquireUnboundedSync(cancellationToken, null, null);
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
            if (_owner != null) {
                return _owner.Lock(_ownerKey, timeout, cancellationToken);
            }
            return AcquireAsync(timeout, cancellationToken, null, null);
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
            if (_owner != null) {
                return _owner.LockSync(_ownerKey, timeout, cancellationToken);
            }
            return AcquireSync(timeout, cancellationToken, null, null);
        }

        public void Dispose() {
            if (_owner != null) {
                _owner.DisposeOwnedLock(_ownerKey, this);
                return;
            }
            DisposeSemaphore();
        }

        public ValueTask DisposeAsync() {
            Dispose();
            return Fi.CompletedValueTask;
        }

        internal async Task<FiAsyncDisposableLock> AcquireAsync(
            TimeSpan timeout,
            CancellationToken cancellationToken,
            FiAsyncMultiLock multiLock,
            string key,
            FiAsyncMultiLockEntry multiLockEntry = null) {
            ValidateTimeout(timeout, nameof(timeout));
            bool acquired = await _semaphore.WaitAsync(timeout, cancellationToken).ConfigureAwait(false);
            if (!acquired) {
                throw new TimeoutException("Timed out waiting to acquire the lock.");
            }
            return CreateHandle(multiLock, key, multiLockEntry);
        }

        internal FiAsyncDisposableLock AcquireSync(
            TimeSpan timeout,
            CancellationToken cancellationToken,
            FiAsyncMultiLock multiLock,
            string key,
            FiAsyncMultiLockEntry multiLockEntry = null) {
            ValidateTimeout(timeout, nameof(timeout));
            if (!_semaphore.Wait(timeout, cancellationToken)) {
                throw new TimeoutException("Timed out waiting to acquire the lock.");
            }
            return CreateHandle(multiLock, key, multiLockEntry);
        }

        internal bool IsOwnedBy(FiAsyncMultiLock owner, string key) {
            return ReferenceEquals(_owner, owner) && string.Equals(_ownerKey, key, StringComparison.Ordinal);
        }

        internal bool HasOwner => _owner != null;

        internal void DisposeSemaphore() {
            if (Interlocked.Exchange(ref _isDisposed, 1) == 0) {
                _semaphore.Dispose();
            }
        }

        internal static void ValidateTimeout(TimeSpan timeout, string parameterName) {
            if (timeout < Timeout.InfiniteTimeSpan) {
                throw new ArgumentOutOfRangeException(parameterName, "Timeout must be non-negative or infinite.");
            }
        }

        async Task<FiAsyncDisposableLock> AcquireUnboundedAsync(
            CancellationToken cancellationToken,
            FiAsyncMultiLock multiLock,
            string key,
            FiAsyncMultiLockEntry multiLockEntry = null) {
            await _semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
            return CreateHandle(multiLock, key, multiLockEntry);
        }

        FiAsyncDisposableLock AcquireUnboundedSync(
            CancellationToken cancellationToken,
            FiAsyncMultiLock multiLock,
            string key,
            FiAsyncMultiLockEntry multiLockEntry = null) {
            _semaphore.Wait(cancellationToken);
            return CreateHandle(multiLock, key, multiLockEntry);
        }

        FiAsyncDisposableLock CreateHandle(
            FiAsyncMultiLock multiLock,
            string key,
            FiAsyncMultiLockEntry multiLockEntry) {
            if (multiLock == null) {
                return new FiAsyncDisposableLock(_semaphore);
            }
            return new FiAsyncDisposableLock(_semaphore, multiLock, key, multiLockEntry);
        }
    }
}
