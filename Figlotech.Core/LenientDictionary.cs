﻿using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core
{
    public sealed class LenientDictionary<TKey, TValue> : IDictionary<TKey, TValue>, IReadOnlyDictionary<TKey, TValue> {
        ConcurrentDictionary<TKey, TValue> _dmmy = new ConcurrentDictionary<TKey, TValue>();
        public TValue this[TKey key] {
            get {
                if(key == null) {
                    return default(TValue);
                }
                if (_dmmy.TryGetValue(key, out var retv)) {
                    return retv;
                }
                return default(TValue);
            }
            set {
                _dmmy[key] = value;
            }
        }
        public static implicit operator ConcurrentDictionary<TKey, TValue>(LenientDictionary<TKey, TValue> a) {
            return a._dmmy;
        }
        public static implicit operator LenientDictionary<TKey, TValue>(Dictionary<TKey, TValue> a) {
            return new LenientDictionary<TKey, TValue>() { _dmmy = new ConcurrentDictionary<TKey, TValue>(a) };
        }

        public ICollection<TKey> Keys {
            get {
                lock(_dmmy) {
                    return new List<TKey>(((IDictionary<TKey, TValue>)_dmmy).Keys);
                }
            }
        }

        public ICollection<TValue> Values {
            get {
                lock(_dmmy) {
                    return new List<TValue>(((IDictionary<TKey, TValue>)_dmmy).Values);
                }
            }
        }

        public int Count => ((IDictionary<TKey, TValue>)_dmmy).Count;

        public bool IsReadOnly => ((IDictionary<TKey, TValue>)_dmmy).IsReadOnly;

        IEnumerable<TKey> IReadOnlyDictionary<TKey, TValue>.Keys => Keys;

        IEnumerable<TValue> IReadOnlyDictionary<TKey, TValue>.Values => Values;

        public void Add(TKey key, TValue value) {
            lock(_dmmy)
                ((IDictionary<TKey, TValue>)_dmmy).Add(key, value);
        }

        public void Add(KeyValuePair<TKey, TValue> item) {
            lock (_dmmy)
                ((IDictionary<TKey, TValue>)_dmmy).Add(item);
        }

        public void Clear() {
            lock (_dmmy)
                ((IDictionary<TKey, TValue>)_dmmy).Clear();
        }

        public bool Contains(KeyValuePair<TKey, TValue> item) {
            lock (_dmmy)
                return ((IDictionary<TKey, TValue>)_dmmy).Contains(item);
        }

        public bool ContainsKey(TKey key) {
            lock (_dmmy)
                return ((IDictionary<TKey, TValue>)_dmmy).ContainsKey(key);
        }

        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex) {
            lock (_dmmy)
                ((IDictionary<TKey, TValue>)_dmmy).CopyTo(array, arrayIndex);
        }

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator() {
            lock (_dmmy)
                return ((IDictionary<TKey, TValue>)_dmmy).GetEnumerator();
        }

        public bool Remove(TKey key) {
            lock (_dmmy)
                return ((IDictionary<TKey, TValue>)_dmmy).Remove(key);
        }

        public bool Remove(KeyValuePair<TKey, TValue> item) {
            lock (_dmmy)
                return ((IDictionary<TKey, TValue>)_dmmy).Remove(item);
        }

        public bool TryGetValue(TKey key, out TValue value) {
            lock (_dmmy)
                return ((IDictionary<TKey, TValue>)_dmmy).TryGetValue(key, out value);
        }

        IEnumerator IEnumerable.GetEnumerator() {
            return ((IDictionary<TKey, TValue>)_dmmy).GetEnumerator();
        }
    }
}
