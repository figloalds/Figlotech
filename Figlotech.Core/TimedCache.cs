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

        public TimedCache(TimeSpan duration) {
            this.CacheDuration = duration;
            Dictionary = new LenientDictionary<TKey, TimerCachedObject<TKey, T>>();
        }

        public T this[TKey key] {
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
            Dictionary[key].Dispose();
            return Dictionary.Remove(key);
        }

        public bool TryGetValue(TKey key, out T value) {
            if(Dictionary.TryGetValue(key, out var val)) {
                value = val.Object;
                return true;
            }
            value = default(T);
            return false;
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
