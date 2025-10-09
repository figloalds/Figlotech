using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.Core {
    public sealed class SelfInitializedCache<TKey, T> : IDictionary<TKey, T> {
        private TimedCache<TKey, T> Dictionary;
        private Func<TKey, T> GenerationLogic;
        private TimeSpan CacheDuration { get; }
        
        public ICollection<TKey> Keys => Dictionary.Keys;

        public ICollection<T> Values => Dictionary.Values.ToList();

        public int Count => Dictionary.Count;

        public bool IsReadOnly => Dictionary.IsReadOnly;

        public SelfInitializedCache(Func<TKey, T> generationLogic, TimeSpan duration) {
            this.GenerationLogic = generationLogic;
            this.CacheDuration = duration;
            Dictionary = new TimedCache<TKey, T>(duration);
        }

        public T this[TKey key] {
            get {
                if (!Dictionary.TryGetValue(key, out var item)) {
                    item = GenerationLogic(key);
                    Dictionary[key] = item;
                }
                return item;
            }
            set {
                Dictionary[key] = value;
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
            if (Dictionary.TryGetValue(key, out var item)) {
                value = item;
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
            return Dictionary.Keys.Any(x => x.Equals(item.Key) && Dictionary[item.Key].Equals(item.Value));
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
            return Dictionary.Select(x => new KeyValuePair<TKey, T>(x.Key, x.Value)).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator() {
            return Dictionary.Select(x => x.Value).GetEnumerator();
        }
    }
}
