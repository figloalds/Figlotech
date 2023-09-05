using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.Core {
    public sealed class FiAsyncMultiLock : IDictionary<string, FiAsyncLock> {
        SelfInitializerDictionary<string, FiAsyncLock> _dmmy;
        public FiAsyncMultiLock() {
            this._dmmy = new SelfInitializerDictionary<string, FiAsyncLock>(s => new FiAsyncLock(), true);
        }

        public bool AutoRemoveLocks { get; set; } = false;

        public FiAsyncLock this[string key] { 
            get => this._dmmy[key]; 
            set => this._dmmy[key] = value; 
        }

        public ICollection<string> Keys => this._dmmy.Keys;

        public ICollection<FiAsyncLock> Values => this._dmmy.Values;

        public int Count => this._dmmy.Count;

        public bool IsReadOnly => this._dmmy.IsReadOnly;

        public void Add(string key, FiAsyncLock value) {
            this._dmmy.Add(key, value);
        }

        public void Add(KeyValuePair<string, FiAsyncLock> item) {
            this._dmmy.Add(item);
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
            this._dmmy.CopyTo(array, arrayIndex);
        }

        public IEnumerator<KeyValuePair<string, FiAsyncLock>> GetEnumerator() {
            return this._dmmy.GetEnumerator();
        }

        public async Task<FiAsyncDisposableLock> Lock(string key, TimeSpan? timeout = null) {
            var retv = await this[key].LockWithTimeout(timeout ?? TimeSpan.FromSeconds(30));
            retv._lock = this;
            retv._key = key;
            return retv;
        }

        public bool Remove(string key) {
            return this._dmmy.Remove(key);
        }

        public bool Remove(KeyValuePair<string, FiAsyncLock> item) {
            return this._dmmy.Remove(item);
        }

        public bool TryGetValue(string key, out FiAsyncLock value) {
            return this._dmmy.TryGetValue(key, out value);
        }

        IEnumerator IEnumerable.GetEnumerator() {
            return ((IEnumerable)this._dmmy).GetEnumerator();
        }
    }

    public sealed class FiAsyncDisposableLock : IDisposable {
        SemaphoreSlim _semaphore;
        internal FiAsyncMultiLock _lock;
        internal string _key;
        public FiAsyncDisposableLock(SemaphoreSlim semaphore) {
            this._semaphore = semaphore;
        }

        ~FiAsyncDisposableLock() {
            Dispose();
        }

        public void Dispose() {
            try {
                if(_semaphore.CurrentCount == 0) {
                    _semaphore.Release(1);
                } else {
                    Fi.Tech.Error(new Exception("FiAsyncLock had an exception during WaitAsync"));
                }
                if(_lock != null && _lock.AutoRemoveLocks && _semaphore.CurrentCount == 1) {
                    _lock.Remove(_key);
                }
            } catch(Exception x) {
                if(Debugger.IsAttached) {
                    Debugger.Break();
                }
                Fi.Tech.Error(x);
            }
        }   
    }

    public sealed class FiAsyncLock {
        SemaphoreSlim _semaphore = new SemaphoreSlim(1, 1);

        public async Task<FiAsyncDisposableLock> Lock() {
            await _semaphore.WaitAsync();
            return new FiAsyncDisposableLock(_semaphore);
        }

        public async Task<FiAsyncDisposableLock> LockWithTimeout(TimeSpan timeout) {
            await Fi.Tech.ThrowIfTimesout(_semaphore.WaitAsync(), timeout, "Awaiting for lock timed out");
            return new FiAsyncDisposableLock(_semaphore);
        }
    }
}
