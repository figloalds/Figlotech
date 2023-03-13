using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.Core {
    internal class TimerCachedObject<T> : IDisposable {
        public TimeSpan CacheDuration { get; private set; }
        public T Object { get; set; }
        public DateTime LastChecked { get; set; }
        string Key;
        Timer Timer;
        IDictionary<string, TimerCachedObject<T>> CacheSource;

        public bool IsDisposed { get; set; } = false;

        public TimerCachedObject(
            IDictionary<string, TimerCachedObject<T>> cacheSource,
            string key, T objValue,
            TimeSpan duration
        ) {
            this.CacheSource = cacheSource;
            this.Object = objValue;
            this.LastChecked = DateTime.UtcNow;
            this.CacheDuration = duration;
            this.Key = key;
            RefreshTimer();
        }

        public void RefreshTimer() {
            DateTime DataUpdate = DateTime.UtcNow;
            if (this.Timer != null) {
                this.Timer.Dispose();
            }
            var timeout = this.Object != null ? (int)CacheDuration.TotalMilliseconds : 0;
            this.Timer = new Timer((s) => {
                lock (CacheSource)
                    if (CacheSource.ContainsKey(this.Key)) {
                        CacheSource.Remove(this.Key);
                    }
                this.Dispose();
            }, null, timeout, Timeout.Infinite);
        }

        public void Dispose() {
            if (!this.IsDisposed) {
                try {
                    this.Timer.Dispose();
                } catch (Exception x) { }
                try {
                    if (this.Object is IDisposable d) {
                        d.Dispose();
                    }
                } catch (Exception x) { }
                this.IsDisposed = true;
            }
        }

        ~TimerCachedObject() {
            this.Dispose();
        }
    }

    public class SelfInitializedCache<T> : IDictionary<string, T> {
        private SelfInitializerDictionary<string, TimerCachedObject<T>> Dictionary;
        private Func<string, T> GenerationLogic;
        private TimeSpan CacheDuration { get; set; }

        public ICollection<string> Keys => Dictionary.Keys;

        public ICollection<T> Values => Dictionary.Values.Select(x=> x.Object).ToList();

        public int Count => Dictionary.Count;

        public bool IsReadOnly => Dictionary.IsReadOnly;

        public SelfInitializedCache(Func<string, T> generationLogic, TimeSpan duration) {
            this.GenerationLogic = generationLogic;
            this.CacheDuration = duration;
            Dictionary = new SelfInitializerDictionary<string, TimerCachedObject<T>>(key => {
                return new TimerCachedObject<T>(Dictionary!, key, generationLogic(key), duration);
            });
        }

        public T this[string key] {
            get {
                var item = Dictionary[key];
                item.RefreshTimer();
                return item.Object;
            }
            set {
                if(Dictionary.ContainsKey(key)) {
                    Dictionary[key].Object = value;
                    Dictionary[key].RefreshTimer();
                } else {
                    Dictionary[key] = new TimerCachedObject<T>(Dictionary, key, value, CacheDuration);
                }
            }
        }

        public void Add(string key, T value) {
            this[key] = value;
        }

        public bool ContainsKey(string key) {
            return Dictionary.ContainsKey(key);
        }

        public bool Remove(string key) {
            Dictionary[key].Dispose();
            return Dictionary.Remove(key);
        }

        public bool TryGetValue(string key, out T value) {
            if(Dictionary.TryGetValue(key, out var val)) {
                value = val.Object;
                return true;
            }
            value = default(T);
            return false;
        }

        public void Add(KeyValuePair<string, T> item) {
            this[item.Key] = item.Value;
        }

        public void Clear() {
            foreach(var key in Dictionary.Keys) {
                Dictionary.Remove(key);
            }
        }

        public bool Contains(KeyValuePair<string, T> item) {
            return Dictionary.Keys.Any(x => x == item.Key && Dictionary[item.Key].Object.Equals(item.Value));
        }

        public void CopyTo(KeyValuePair<string, T>[] array, int arrayIndex) {
            for(int i = arrayIndex; i < array.Length; i++) {
                this[array[arrayIndex].Key] = array[arrayIndex].Value;
            }
        }

        public bool Remove(KeyValuePair<string, T> item) {
            return Remove(item.Key);
        }

        public IEnumerator<KeyValuePair<string, T>> GetEnumerator() {
            return Dictionary.Select(x => new KeyValuePair<string, T>(x.Key, x.Value.Object)).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator() {
            return Dictionary.Select(x => x.Value.Object).GetEnumerator();
        }
    }
}
