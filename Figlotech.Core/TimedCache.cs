using Figlotech.Core.Extensions;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.Core {

    public sealed class TimedCache<TKey, T> : IDictionary<TKey, T>, IDisposable, IAsyncDisposable {
        private readonly LenientDictionary<TKey, TimerCachedObject<T>> Dictionary;
        private readonly TimeSpan _cacheDuration;
        private readonly Timer _timer;
        private readonly object _disposeLock = new object();
        private volatile bool _isDisposed;

        public ICollection<TKey> Keys => Dictionary.Keys;

        public ICollection<T> Values => Dictionary.Values.Select(x => x.Object).ToList();

        public int Count => Dictionary.Count;

        public bool IsReadOnly => Dictionary.IsReadOnly;

        public Func<Exception, ValueTask> OnException { get; set; }
        public Func<TKey, Task<T>> OnFailOver { get; set; }
        public Func<TKey, ValueTask> OnFree { get; set; }
        public Func<TKey, T, ValueTask> OnSet { get; set; }

        public Func<T, Task<bool>> CustomCanFinalizeAsync { get; set; } = null;

        public void Dispose() {
            lock (_disposeLock) {
                if (_isDisposed) return;
                _isDisposed = true;
                _timer.Change(Timeout.Infinite, Timeout.Infinite);
                _timer.Dispose();
            }
        }

        public async ValueTask DisposeAsync() {
            lock (_disposeLock) {
                if (_isDisposed) return;
                _isDisposed = true;
                _timer.Change(Timeout.Infinite, Timeout.Infinite);
            }
            await _timer.DisposeAsync().ConfigureAwait(false);
        }

        private static async ValueTask DisposeValueAsync(T value) {
            if (value is IAsyncDisposable adis) {
                await adis.DisposeAsync().ConfigureAwait(false);
            } else if (value is IDisposable dis) {
                dis.Dispose();
            }
        }

        public TimedCache(TimeSpan duration) {
            this._cacheDuration = duration;
            Dictionary = new LenientDictionary<TKey, TimerCachedObject<T>>();

            var timerCheckInterval = TimeSpan.FromSeconds(Math.Max(1, _cacheDuration.TotalSeconds / 10));
            _timer = new Timer((state) => {
                var cache = (TimedCache<TKey, T>)state;
                if (cache._isDisposed) {
                    return;
                }
                Fi.Tech.FireAndForget(async () => {
                    var keys = cache.Dictionary.Keys;
                    var freed = cache.OnFree != null ? new List<TKey>() : null;
                    foreach (var key in keys) {
                        if (!cache.Dictionary.TryGetValue(key, out var item)) {
                            continue;
                        }
                        if (item == null) {
                            cache.Dictionary.Remove(key);
                            continue;
                        }
                        if ((DateTime.UtcNow - item.LastChecked) > cache._cacheDuration) {
                            if (cache.CustomCanFinalizeAsync != null) {
                                if (item.Object == null || !await cache.CustomCanFinalizeAsync(item.Object).ConfigureAwait(false)) {
                                    item.KeepAlive();
                                    continue;
                                }
                            }
                            cache.Dictionary.Remove(key);
                            freed?.Add(key);

                            if (item.Object != null) {
                                try {
                                    await DisposeValueAsync(item.Object).ConfigureAwait(false);
                                } catch (Exception x) {
                                    try {
                                        cache.OnException?.Invoke(x);
                                    } catch (Exception y) {
                                        Fi.Tech.SwallowException(y);
                                    }
                                }
                            }
                        }
                    }
                    if (cache.OnFree != null && freed.Count > 0) {
                        foreach (var key in freed) {
                            try {
                                await cache.OnFree(key).ConfigureAwait(false);
                            } catch (Exception x) {
                                try {
                                    cache.OnException?.Invoke(x);
                                } catch (Exception y) {
                                    Fi.Tech.SwallowException(y);
                                }
                            }
                        }
                    }
                });
            }, this, timerCheckInterval, timerCheckInterval);
        }

        public T this[TKey key] {
            get {
                if (Dictionary.TryGetValue(key, out var item)) {
                    item.KeepAlive();
                    return item.Object;
                }
                return default;
            }
            set {
                if (value == null) {
                    if (Dictionary.TryGetValue(key, out var existing)) {
                        Dictionary.Remove(key);
                        if (existing != null && existing.Object != null) {
                            Fi.Tech.FireAndForget(async () => {
                                await DisposeValueAsync(existing.Object).ConfigureAwait(false);
                            });
                        }
                    }
                    return;
                }
                if (Dictionary.TryGetValue(key, out var old)) {
                    if (old != null && !ReferenceEquals(old.Object, value) && old.Object != null) {
                        Fi.Tech.FireAndForget(async () => {
                            await DisposeValueAsync(old.Object).ConfigureAwait(false);
                        });
                    }
                    old.Object = value;
                    old.KeepAlive();
                    if (OnSet != null) {
                        Fi.Tech.FireAndForget(async () => {
                            await OnSet(key, value);
                        });
                    }
                } else {
                    Dictionary[key] = new TimerCachedObject<T>(value);
                }
            }
        }

        public void Add(TKey key, T value) {
            this[key] = value;
        }

        public bool ContainsKey(TKey key) {
            return Dictionary.ContainsKey(key);
        }

        public bool Remove(TKey key) {
            if (Dictionary.TryGetValue(key, out var value)) {
                Dictionary.Remove(key);
                if (value != null && value.Object != null) {
                    Fi.Tech.FireAndForget(async () => {
                        await DisposeValueAsync(value.Object).ConfigureAwait(false);
                    });
                }
                return true;
            }
            return false;
        }

        public async Task<(bool Success, T Value)> TryGetValueAsync(TKey key) {
            if (Dictionary.TryGetValue(key, out var item)) {
                item.KeepAlive();
                return (true, item.Object);
            }
            if (OnFailOver != null) {
                var ret = await OnFailOver.Invoke(key).ConfigureAwait(false);
                this[key] = ret;
                return (ret != null, ret);
            }
            return (false, default);
        }

        public bool TryGetValue(TKey key, out T value) {
            if (Dictionary.TryGetValue(key, out var val)) {
                value = val.Object;
                val.KeepAlive();
                return true;
            }
            value = default;
            return false;
        }

        public async Task<T> GetOrAddWithLocking(TKey key, Func<TKey, Task<T>> valueFactory) {
            return (await Dictionary._dmmy.GetOrAddWithLocking(
                key,
                async key => new TimerCachedObject<T>(await valueFactory(key))
            )).Object;
        }
        public T GetOrAddWithLocking(TKey key, Func<TKey, T> valueFactory) {
            return Dictionary._dmmy.GetOrAddWithLocking(
                key,
                key => new TimerCachedObject<T>(valueFactory(key))
            ).Object;
        }

        public void Add(KeyValuePair<TKey, T> item) {
            this[item.Key] = item.Value;
        }

        public void Clear() {
            var items = Dictionary.Values.ToList();
            Dictionary.Clear();
            foreach (var item in items) {
                if (item != null && item.Object != null) {
                    Fi.Tech.FireAndForget(async () => {
                        await DisposeValueAsync(item.Object).ConfigureAwait(false);
                    });
                }
            }
        }

        public bool Contains(KeyValuePair<TKey, T> item) {
            if (Dictionary.TryGetValue(item.Key, out var val)) {
                return val != null && val.Object != null && val.Object.Equals(item.Value);
            }
            return false;
        }

        public void CopyTo(KeyValuePair<TKey, T>[] array, int arrayIndex) {
            if (array == null) throw new ArgumentNullException(nameof(array));
            if (arrayIndex < 0 || arrayIndex > array.Length) throw new ArgumentOutOfRangeException(nameof(arrayIndex));
            if (array.Length - arrayIndex < Count) throw new ArgumentException("Destination array is not large enough.");

            int i = arrayIndex;
            foreach (var kvp in Dictionary) {
                array[i++] = new KeyValuePair<TKey, T>(kvp.Key, kvp.Value.Object);
            }
        }

        public bool Remove(KeyValuePair<TKey, T> item) {
            return Remove(item.Key);
        }

        public IEnumerator<KeyValuePair<TKey, T>> GetEnumerator() {
            return Dictionary.Select(x => new KeyValuePair<TKey, T>(x.Key, x.Value.Object)).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator() {
            return Dictionary.Select(x => x.Value.Object).GetEnumerator();
        }
    }
}
