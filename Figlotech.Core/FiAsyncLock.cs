using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.Core {
    public sealed class FiAsyncMultiLock : IDictionary<string, FiAsyncLock> {
        SelfInitializerDictionary<string, FiAsyncLock> _dmmy;
        public FiAsyncMultiLock() {
            this._dmmy = new SelfInitializerDictionary<string, FiAsyncLock>(s => new FiAsyncLock(), true);
        }

        public FiAsyncLock this[string key] { get => ((IDictionary<string, FiAsyncLock>)this._dmmy)[key]; set => ((IDictionary<string, FiAsyncLock>)this._dmmy)[key] = value; }

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

        public async Task<FiAsyncDisposableLock> Lock(string key) {
            var retv = await this[key].Lock();
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

        public void Dispose() {
            if(_semaphore.CurrentCount == 0) {
                _semaphore.Release();
            } else {
                Fi.Tech.Error(new Exception("FiAsyncLock had an exception during WaitAsync"));
            }
            if(_lock != null) {
                _lock.Remove(_key);
            }
        }   
    }

    public sealed class FiAsyncLock {
        SemaphoreSlim _semaphore = new SemaphoreSlim(1, 1);
        public async Task<T> Lock<T>(Func<Task<T>> Try, Func<Exception, Task<T>> Catch = null, Func<bool, Task> Finally = null) {
            bool isSuccess = true;
            await _semaphore.WaitAsync();
            try {
                return await Try();
            } catch (Exception x) {
                isSuccess = false;
                if (Catch != null) {
                    return await Catch.Invoke(x);
                } else {
                    throw x;
                }
            } finally {
                try {
                    if (Finally != null) {
                        await Finally.Invoke(isSuccess);
                    }
                } catch (Exception fiex) {
                    Fi.Tech.Throw(fiex);
                }
                _semaphore.Release();
            }
        }

        public async Task<FiAsyncDisposableLock> Lock() {
            await _semaphore.WaitAsync();
            return new FiAsyncDisposableLock(_semaphore);
        }

        public Task Lock(Func<Task> Try, Func<Exception, Task> Catch = null, Func<bool, Task> Finally = null) {
            return Lock<int>(
                async () => {
                    await Try();
                    return 0;
                }, Catch != null ? async (x) => {
                    await Catch?.Invoke(x);
                    return 0;
                } : (Func<Exception, Task<int>>) null, 
                Finally);
        }
    }
}
