using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core {
    public class SelfInitializerDictionary<TKey, TValue> : IDictionary<TKey, TValue> {
        Dictionary<TKey, TValue> _dmmy = new Dictionary<TKey, TValue>();
        Func<TKey, TValue> SelfInitFn { get; set; }
        public bool AllowNullValueCaching { get; set; } = true;
        public TValue this[TKey key] {
            get {
                lock (key) {
                    TValue retv;
                    if (!_dmmy.ContainsKey(key)) {
                        if(FullDictionaryLockOnInit) {
                            lock (_dmmy) retv = Initialize(key);
                        } else {
                            retv = Initialize(key);
                        }
                    } else {
                        retv = _dmmy[key];
                    }
                    return retv;
                }
            }
            set {
                lock (key) {
                    if(!AllowNullValueCaching && value ==  null) {
                        return;
                    }
                    _dmmy[key] = value;
                }
            }
        }

        private TValue Initialize(TKey key) {
            var init = SelfInitFn(key);
            if (AllowNullValueCaching || init != null) {
                _dmmy.Add(key, init);
            }
            return init;
        }

        bool FullDictionaryLockOnInit = false;

        public SelfInitializerDictionary(Func<TKey, TValue> initFn, bool fullDictionaryLockOnInit = false) {
            SelfInitFn = initFn;
            FullDictionaryLockOnInit = fullDictionaryLockOnInit;
        }


        public ICollection<TKey> Keys => ((IDictionary<TKey, TValue>)_dmmy).Keys;

        public ICollection<TValue> Values => ((IDictionary<TKey, TValue>)_dmmy).Values;

        public int Count => ((IDictionary<TKey, TValue>)_dmmy).Count;

        public bool IsReadOnly => ((IDictionary<TKey, TValue>)_dmmy).IsReadOnly;

        public void Add(TKey key, TValue value) {
            if(key == null) {
                Debugger.Break();
            }
            ((IDictionary<TKey, TValue>)_dmmy).Add(key, value);
        }

        public void Add(KeyValuePair<TKey, TValue> item) {
            ((IDictionary<TKey, TValue>)_dmmy).Add(item);
        }

        public void Clear() {
            ((IDictionary<TKey, TValue>)_dmmy).Clear();
        }

        public bool Contains(KeyValuePair<TKey, TValue> item) {
            return ((IDictionary<TKey, TValue>)_dmmy).Contains(item);
        }

        public bool ContainsKey(TKey key) {
            return ((IDictionary<TKey, TValue>)_dmmy).ContainsKey(key);
        }

        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex) {
            ((IDictionary<TKey, TValue>)_dmmy).CopyTo(array, arrayIndex);
        }

        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator() {
            return ((IDictionary<TKey, TValue>)_dmmy).GetEnumerator();
        }

        public bool Remove(TKey key) {
            return ((IDictionary<TKey, TValue>)_dmmy).Remove(key);
        }

        public bool Remove(KeyValuePair<TKey, TValue> item) {
            return ((IDictionary<TKey, TValue>)_dmmy).Remove(item);
        }

        public bool TryGetValue(TKey key, out TValue value) {
            return ((IDictionary<TKey, TValue>)_dmmy).TryGetValue(key, out value);
        }

        IEnumerator IEnumerable.GetEnumerator() {
            return ((IDictionary<TKey, TValue>)_dmmy).GetEnumerator();
        }
    }
}
