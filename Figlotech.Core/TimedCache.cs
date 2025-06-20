using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.Core {

    public sealed class TimedCache<TKey, T> : IDictionary<TKey, T> {
        private LenientDictionary<TKey, TimerCachedObject<TKey, T>> Dictionary;
        private TimeSpan CacheDuration { get; set; }

        public ICollection<TKey> Keys => Dictionary.Keys;

        public ICollection<T> Values => Dictionary.Values.Select(x=> x.Object).ToList();

        public int Count => Dictionary.Count;

        public bool IsReadOnly => Dictionary.IsReadOnly;

        public Func<TKey, Task<T>> OnFailOver { get; set; }
        public Func<TKey,ValueTask> OnFree { get; set; }
        public Func<TKey, T, ValueTask> OnSet { get; set; }

        Timer timer { get; set; }

        public TimedCache(TimeSpan duration) {
            this.CacheDuration = duration;
            Dictionary = new LenientDictionary<TKey, TimerCachedObject<TKey, T>>();

            var timerCheckInterval = TimeSpan.FromSeconds(Math.Max(1, CacheDuration.Seconds / 10));
            timer = new Timer((state) => {
                var keys = Dictionary.Keys.ToList();
                var freed = OnFree != null ? new List<TKey>(Dictionary.Count) : null;
                foreach (var key in keys) {
                    if ((DateTime.UtcNow - Dictionary[key].LastChecked) > CacheDuration) {
                        Dictionary.Remove(key);
                        freed?.Add(key);
                    }
                }
                if(OnFree != null && freed.Count > 0) {
                    Fi.Tech.FireAndForget(async () => {
                        foreach(var key in freed) {
                            await OnFree(key);
                        }
                    });
                }
            }, null, CacheDuration, CacheDuration);
        }

        ~TimedCache() {
            timer.Change(Timeout.Infinite, Timeout.Infinite);
            Fi.Tech.FireAndForget(async () => {
                await timer.DisposeAsync().ConfigureAwait(false);
            });
        }

        public T this[TKey key] {
            get {
                TimerCachedObject<TKey, T> item;
                if (!Dictionary.TryGetValue(key, out item)) {
                    if (OnFailOver != null) {
                        var ret = OnFailOver.Invoke(key).ConfigureAwait(false).GetAwaiter().GetResult();
                        this[key] = ret;
                        return ret;
                    }
                    return default(T);
                }
                item.KeepAlive();
                return item.Object;
            }
            set {
                if(Dictionary.ContainsKey(key)) {
                    Dictionary[key].Object = value;
                    Dictionary[key].KeepAlive();
                    if (OnSet != null) {
                        Fi.Tech.FireAndForget(async () => {
                            await OnSet(key, value);
                        });
                    }
                } else {
                    Dictionary[key] = new TimerCachedObject<TKey, T>(Dictionary, key, value, CacheDuration);
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
            return Dictionary.Remove(key);
        }

        public bool TryGetValue(TKey key, out T value) {
            if(Dictionary.TryGetValue(key, out var val)) {
                value = val.Object;
                val.KeepAlive();
                return true;
            }
            value = default(T);
            return false;
        }

        public async Task<T> GetOrAddWithLocking(TKey key, Func<TKey, Task<T>> valueFactory) {
            if (Dictionary.TryGetValue(key, out var val)) {
                val.KeepAlive();
                return val.Object;
            }
            var newValue = await valueFactory(key);
            this[key] = newValue;
            return newValue;
        }

        public void Add(KeyValuePair<TKey, T> item) {
            this[item.Key] = item.Value;
        }

        public void Clear() {
            foreach(var key in Dictionary.Keys) {
                Dictionary.Remove(key);
            }
        }

        public bool Contains(KeyValuePair<TKey, T> item) {
            return Dictionary.Keys.Any(x => x.Equals(item.Key) && Dictionary[item.Key].Object.Equals(item.Value));
        }

        public void CopyTo(KeyValuePair<TKey, T>[] array, int arrayIndex) {
            for(int i = arrayIndex; i < array.Length; i++) {
                this[array[arrayIndex].Key] = array[arrayIndex].Value;
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
