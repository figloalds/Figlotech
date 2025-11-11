using Figlotech.Core.Extensions;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.Core {
    public sealed class FiAsyncMultiLock : IDictionary<string, FiAsyncLock> {
        ConcurrentDictionary<string, FiAsyncLock> _dmmy;

        public TimeSpan DefaultTimeout { get; set; } = TimeSpan.FromSeconds(600);

        public FiAsyncMultiLock() {
            this._dmmy = new ConcurrentDictionary<string, FiAsyncLock>();
        }

        public bool AutoRemoveLocks { get; set; } = false;

        public FiAsyncLock this[string key] {
            get {
                var retv = _dmmy.TryGetValue(key, out var value);
                if(retv) {
                    return value;
                }
                lock(_dmmy) {
                    value = _dmmy.GetOrAdd(key, k => new FiAsyncLock());
                    return value;
                }
            }
            set => this._dmmy[key] = value;
        }

        public ICollection<string> Keys => this._dmmy.Keys;

        public ICollection<FiAsyncLock> Values => this._dmmy.Values;

        public int Count => this._dmmy.Count;

        public bool IsReadOnly => false;

        public void Add(string key, FiAsyncLock value) {
            if(!this._dmmy.TryAdd(key, value)) {
                throw new Exception("Key already exists");
            }
        }

        public void Add(KeyValuePair<string, FiAsyncLock> item) {
            if(!this._dmmy.TryAdd(item.Key, item.Value)) {
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
            retv._lock = this;
            retv._key = key;
            return retv;
        }
        public FiAsyncDisposableLock LockSync(string key, TimeSpan? timeout = null) {
            var retv = this[key].LockWithTimeoutSync(timeout ?? DefaultTimeout);
            retv._lock = this;
            retv._key = key;
            return retv;
        }

        public bool Remove(string key) {
            lock(_dmmy) {
                return this._dmmy.TryRemove(key, out var _);
            }
        }

        public bool Remove(KeyValuePair<string, FiAsyncLock> item) {
            lock (_dmmy) {
                return this._dmmy.TryRemove(item.Key, out var _);
            }
        }

        public bool TryGetValue(string key, out FiAsyncLock value) {
            return this._dmmy.TryGetValue(key, out value);
        }

        IEnumerator IEnumerable.GetEnumerator() {
            return ((IEnumerable)this._dmmy).GetEnumerator();
        }
    }

    public sealed class FiAsyncDisposableLock : IDisposable, IAsyncDisposable {
        SemaphoreSlim _semaphore;
        internal FiAsyncMultiLock _lock;
        internal string _key;
        private bool isDisposed { get; set; }
        public FiAsyncDisposableLock(SemaphoreSlim semaphore) {
            this._semaphore = semaphore;
        }

        ~FiAsyncDisposableLock() {
            Dispose();
        }

        public void Dispose() {
            lock (this) {
                if (isDisposed) {
                    return;
                }
                try {
                    if (_semaphore.CurrentCount == 0) {
                        _semaphore.Release(1);
                    } else {
                        if (Debugger.IsAttached) {
                            Debugger.Break();
                        }
                        Fi.Tech.Error(new Exception("FiAsyncLock had an exception during WaitAsync"));
                    }
                    if (_lock != null && _lock.AutoRemoveLocks && _semaphore.CurrentCount == 1) {
                        _lock.Remove(_key);
                    }
                } catch (Exception x) {
                    if (Debugger.IsAttached) {
                        Debugger.Break();
                    }
                    Fi.Tech.Error(x);
                } finally {
                    isDisposed = true;
                }
            }
        }

        public ValueTask DisposeAsync() {
            lock (this) {
                if (isDisposed) {
                    return Fi.CompletedValueTask;
                }
                try {
                    if (_semaphore.CurrentCount == 0) {
                        _semaphore.Release(1);
                    } else {
                        Fi.Tech.Error(new Exception("FiAsyncLock had an exception during WaitAsync"));
                    }
                    if (_lock != null && _lock.AutoRemoveLocks && _semaphore.CurrentCount == 1) {
                        _lock.Remove(_key);
                    }
                } catch (Exception x) {
                    if (Debugger.IsAttached) {
                        Debugger.Break();
                    }
                    Fi.Tech.Error(x);
                } finally {
                    isDisposed = true;
                }
            }
            return Fi.CompletedValueTask;
        }
    }

    public sealed class FiAsyncLock {
        SemaphoreSlim _semaphore = new SemaphoreSlim(1, 1);

        public async Task<FiAsyncDisposableLock> Lock() {
            await _semaphore.WaitAsync().ConfigureAwait(false);
            return new FiAsyncDisposableLock(_semaphore);
        }
        public FiAsyncDisposableLock LockSync() {
            _semaphore.WaitAsync().ConfigureAwait(false).GetAwaiter().GetResult();
            return new FiAsyncDisposableLock(_semaphore);
        }

        public async Task<FiAsyncDisposableLock> LockWithTimeout(TimeSpan timeout) {
            var timeoutCancellation = new CancellationTokenSource(timeout);

            try {
                await _semaphore.WaitAsync(timeoutCancellation.Token).ConfigureAwait(false);
                return new FiAsyncDisposableLock(_semaphore);
            } catch (TaskCanceledException x) {
                throw new TimeoutException("Awaiting for lock timed out", x);
            } catch (Exception x) {
                throw new Exception("Error waiting for Lock", x);
            }
        }

        public FiAsyncDisposableLock LockWithTimeoutSync(TimeSpan timeout) {
            var timeoutCancellation = new CancellationTokenSource(timeout);

            try {
                _semaphore.WaitAsync(timeoutCancellation.Token).GetAwaiter().GetResult();
                return new FiAsyncDisposableLock(_semaphore);
            } catch (TaskCanceledException x) {
                throw new TimeoutException("Awaiting for lock timed out", x);
            } catch (Exception x) {
                throw new Exception("Error waiting for Lock", x);
            }
        }
    }
}
