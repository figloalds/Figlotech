using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core {
    public sealed class AtomicList<T> : IList<T> {
        internal List<T> _dmmy = new List<T>();

        public int Count => ((IList<T>)_dmmy).Count;

        public bool IsReadOnly => ((IList<T>)_dmmy).IsReadOnly;

        public T this[int key] {
            get {
                lock (this) {
                    return _dmmy[key];
                }
            }
            set {
                lock (this) {
                    _dmmy[key] = value;
                }
            }
        }

        public AtomicList() {

        }

        public int IndexOf(T item) {
            lock(this)
                return ((IList<T>)_dmmy).IndexOf(item);
        }

        public void Insert(int index, T item) {
            lock (this)
                ((IList<T>)_dmmy).Insert(index, item);
        }

        public void RemoveAt(int index) {
            lock (this)
                ((IList<T>)_dmmy).RemoveAt(index);
        }

        public void Add(T item) {
            lock (this)
                ((IList<T>)_dmmy).Add(item);
        }

        public void Clear() {
            lock (this)
                ((IList<T>)_dmmy).Clear();
        }

        public bool Contains(T item) {
            lock (this)
                return ((IList<T>)_dmmy).Contains(item);
        }

        public void CopyTo(T[] array, int arrayIndex) {
            lock (this)
                ((IList<T>)_dmmy).CopyTo(array, arrayIndex);
        }

        public bool Remove(T item) {
            lock (this)
                return ((IList<T>)_dmmy).Remove(item);
        }

        public IEnumerator<T> GetEnumerator() {
            lock (this)
                return ((IList<T>)_dmmy).ToArray().AsEnumerable().GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator() {
            lock (this)
                return ((IList<T>)_dmmy).ToArray().GetEnumerator();
        }
    }
}
