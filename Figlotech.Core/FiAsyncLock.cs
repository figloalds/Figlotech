using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.Core {
    public sealed class FiAsyncMultiLock : IDictionary<string, FiAsyncLock> {
        readonly ConcurrentDictionary<string, FiAsyncLock> _dmmy;

        public TimeSpan DefaultTimeout { get; set; } = TimeSpan.FromSeconds(600);

        public FiAsyncMultiLock() {
            this._dmmy = new ConcurrentDictionary<string, FiAsyncLock>();
        }

        /// <summary>
        /// When true, a per-key FiAsyncLock is removed from the dictionary once it is released
        /// and appears idle, so that string keys and lock objects are not retained forever.
        /// Default is false to preserve historical behaviour; opt in per-instance
        /// (e.g. for short-lived keyed locks) to avoid unbounded dictionary growth.
        /// </summary>
        /// <remarks>
        /// <para>
        /// <b>Why not weak references?</b> <see cref="System.Runtime.CompilerServices.ConditionalWeakTable{TKey,TValue}"/>
        /// would auto-evict on GC, but it keys by reference identity, and two equal string
        /// <em>values</em> may be distinct references, which would break the "same key → same lock"
        /// contract. Idle-removal on release is the pragmatic, deterministic alternative.
        /// </para>
        /// <para>
        /// There is a narrow theoretical TOCTOU window between releasing a lock and removing its
        /// entry: a concurrent getter may obtain the same instance just before removal, and a
        /// later getter may create a fresh instance, briefly yielding two semaphore instances for
        /// one key. The removal is reference-matched and idleness-checked to minimise this; for
        /// correctness-critical long-lived keys prefer AutoRemoveLocks=false.
        /// </para>
        /// </remarks>
        public bool AutoRemoveLocks { get; set; } = false;

        public FiAsyncLock this[string key] {
            get {
                // ConcurrentDictionary.GetOrAdd is already atomic; the external lock that used
                // to wrap this added false coordination with no benefit.
                return _dmmy.GetOrAdd(key, k => new FiAsyncLock());
            }
            set => this._dmmy[key] = value;
        }

        public ICollection<string> Keys => this._dmmy.Keys;

        public ICollection<FiAsyncLock> Values => this._dmmy.Values;

        public int Count => this._dmmy.Count;

        public bool IsReadOnly => false;

        public void Add(string key, FiAsyncLock value) {
            if (!this._dmmy.TryAdd(key, value)) {
                throw new Exception("Key already exists");
            }
        }

        public void Add(KeyValuePair<string, FiAsyncLock> item) {
            if (!this._dmmy.TryAdd(item.Key, item.Value)) {
                throw new Exception("Key already exists");
            }
        }

        public void Clear() {
            this._dmmy.Clear();
        }

        public bool Contains(KeyValuePair<string, FiAsyncLock> item) {
            return this._dmmy.Contains(item);
        }

        public bool ContainsKey(string key) {
            return this._dmmy.ContainsKey(key);
        }

        public void CopyTo(KeyValuePair<string, FiAsyncLock>[] array, int arrayIndex) {
            this._dmmy.ToSetAsList().CopyTo(array, arrayIndex);
        }

        public IEnumerator<KeyValuePair<string, FiAsyncLock>> GetEnumerator() {
            return this._dmmy.GetEnumerator();
        }

        public async Task<FiAsyncDisposableLock> Lock(string key, TimeSpan? timeout = null) {
            var retv = await this[key].LockWithTimeout(timeout ?? DefaultTimeout).ConfigureAwait(false);
            retv._multiLock = this;
            retv._key = key;
            return retv;
        }
        public FiAsyncDisposableLock LockSync(string key, TimeSpan? timeout = null) {
            var retv = this[key].LockWithTimeoutSync(timeout ?? DefaultTimeout);
            retv._multiLock = this;
            retv._key = key;
            return retv;
        }

        /// <summary>
        /// Removes the entry for <paramref name="key"/> only if the current entry's
        /// semaphore matches <paramref name="expectedSemaphore"/>. This prevents removing a
        /// fresh lock instance that a concurrent getter added for the same key.
        /// </summary>
        internal bool TryRemoveIfMatch(string key, SemaphoreSlim expectedSemaphore) {
            if (_dmmy.TryGetValue(key, out var current) && ReferenceEquals(current.Semaphore, expectedSemaphore)) {
                return _dmmy.TryRemove(key, out _);
            }
            return false;
        }

        public bool Remove(string key) {
            return this._dmmy.TryRemove(key, out _);
        }

        public bool Remove(KeyValuePair<string, FiAsyncLock> item) {
            return this._dmmy.TryRemove(item.Key, out _);
        }

        public bool TryGetValue(string key, out FiAsyncLock value) {
            return this._dmmy.TryGetValue(key, out value);
        }

        IEnumerator IEnumerable.GetEnumerator() {
            return ((IEnumerable)this._dmmy).GetEnumerator();
        }
    }

    public sealed class FiAsyncDisposableLock : IDisposable, IAsyncDisposable {
        readonly SemaphoreSlim _semaphore;
        internal FiAsyncMultiLock _multiLock;
        internal string _key;
        bool _isDisposed;

        /// <summary>
        /// Public ctor. Prefer obtaining a handle via <see cref="FiAsyncLock.Lock"/> /
        /// <see cref="FiAsyncLock.LockWithTimeout"/>; constructing directly is supported for
        /// legacy callers and manual semaphore scenarios.
        /// </summary>
        public FiAsyncDisposableLock(SemaphoreSlim semaphore) {
            _semaphore = semaphore;
        }

        public void Dispose() {
            if (_isDisposed) {
                return;
            }
            _isDisposed = true;
            ReleaseCore();
        }

        public ValueTask DisposeAsync() {
            if (_isDisposed) {
                return Fi.CompletedValueTask;
            }
            _isDisposed = true;
            ReleaseCore();
            return Fi.CompletedValueTask;
        }

        void ReleaseCore() {
            // Real release: this handle acquired the semaphore, so release it exactly once.
            // No racy CurrentCount pre-check — release is deterministic via the _isDisposed
            // double-dispose guard above.
            try {
                _semaphore.Release();
            } catch (SemaphoreFullException x) {
                // Genuine over-release bug (e.g. someone manually called Release). Log it; do
                // not throw from Dispose.
                ReportError(x);
            } catch (ObjectDisposedException x) {
                ReportError(x);
            }

            // Auto-remove the per-key entry so the multi-lock does not retain string/lock
            // references forever. Only remove if the semaphore is available and the entry still
            // maps to a FiAsyncLock whose semaphore IS this one.
            if (_multiLock != null && _multiLock.AutoRemoveLocks && _semaphore.CurrentCount >= 1) {
                _multiLock.TryRemoveIfMatch(_key, _semaphore);
            }
        }

        static void ReportError(Exception x) {
            if (Debugger.IsAttached) {
                Debugger.Break();
            }
            Fi.Tech.Error(x);
        }
    }

    /// <summary>
    /// An async mutual-exclusion primitive backed by a <see cref="SemaphoreSlim(1,1)"/>.
    /// <b>NOT reentrant.</b>
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Why is this NOT reentrant, and can it be?</b>
    /// A reentrant lock allows the same async-flow to re-acquire without blocking. The natural
    /// mechanism — tracking a held-count in <see cref="AsyncLocal{T}"/> — does NOT work with the
    /// <c>async Task&lt;handle&gt; Lock()</c> API shape, because of how execution contexts (EC)
    /// propagate in async code:
    /// </para>
    /// <para>
    /// When you write to <c>AsyncLocal.Value</c> <i>inside</i> an async method, the write is
    /// captured by that method's own EC (a child of the caller's). EC values flow DOWN into
    /// child work but never flow BACK UP to the caller. So when <c>LockWithTimeout</c> sets a
    /// held-count and returns, the caller's EC still sees the old value (0). A nested
    /// <c>Lock()</c> on the same flow thinks it doesn't hold the lock, enters the semaphore
    /// wait, and self-deadlocks.
    /// </para>
    /// <para>
    /// This was confirmed empirically (see verification notes). Stephen Toub's canonical
    /// <c>AsyncLock</c> is deliberately non-reentrant for this exact reason. Making async
    /// reentrancy work would require a fundamentally different API (e.g. caller-managed
    /// reentrancy tokens passed explicitly to <c>Lock</c>), which is out of scope here.
    /// </para>
    /// <para>
    /// <b>Guidance:</b> structure code so each <see cref="FiAsyncLock"/> instance is acquired
    /// at a single entry point per flow, and nested callees do NOT re-acquire the same lock.
    /// This is the pattern used by <c>AutomacaoTransferenciaAutomaticaAgregada</c>:
    /// <c>ObterMovimentacao</c> acquires once; <c>Refresh</c>/<c>GerarMovimentacaoDia</c> run
    /// within the held lock without re-acquiring.
    /// </para>
    /// </remarks>
    public sealed class FiAsyncLock {
        readonly SemaphoreSlim _semaphore = new SemaphoreSlim(1, 1);

        // Identity accessor for FiAsyncMultiLock.TryRemoveIfMatch (reference-equality check).
        internal SemaphoreSlim Semaphore => _semaphore;

        /// <summary>
        /// Default timeout applied by <see cref="Lock"/>/<see cref="LockSync"/> so that an
        /// abandoned or dead holder cannot block waiters forever. Use
        /// <see cref="LockWithTimeout(TimeSpan)"/> for an explicit timeout.
        /// </summary>
        static readonly TimeSpan DefaultTimeout = TimeSpan.FromMinutes(1);

        /// <summary>
        /// Acquire the lock. Applies a default one-minute timeout so an abandoned holder cannot
        /// block forever. <b>NOT reentrant</b> — see class remarks.
        /// </summary>
        public Task<FiAsyncDisposableLock> Lock() => LockWithTimeout(DefaultTimeout);

        /// <summary>
        /// Acquire the lock synchronously. Applies a default one-minute timeout.
        /// <b>NOT reentrant</b> — see class remarks.
        /// </summary>
        public FiAsyncDisposableLock LockSync() => LockWithTimeoutSync(DefaultTimeout);

        public async Task<FiAsyncDisposableLock> LockWithTimeout(TimeSpan timeout) {
            using var timeoutCancellation = new CancellationTokenSource(timeout);

            try {
                await _semaphore.WaitAsync(timeoutCancellation.Token).ConfigureAwait(false);
            } catch (OperationCanceledException x) {
                // OperationCanceledException is the base of TaskCanceledException; SemaphoreSlim
                // raises it on CancellationToken timeout, so catch the base here.
                throw new TimeoutException("Awaiting for lock timed out", x);
            } catch (Exception x) {
                throw new Exception("Error waiting for Lock", x);
            }

            return new FiAsyncDisposableLock(_semaphore);
        }

        public FiAsyncDisposableLock LockWithTimeoutSync(TimeSpan timeout) {
            using var timeoutCancellation = new CancellationTokenSource(timeout);

            try {
                _semaphore.Wait(timeoutCancellation.Token);
            } catch (OperationCanceledException x) {
                throw new TimeoutException("Awaiting for lock timed out", x);
            } catch (Exception x) {
                throw new Exception("Error waiting for Lock", x);
            }

            return new FiAsyncDisposableLock(_semaphore);
        }
    }
}
