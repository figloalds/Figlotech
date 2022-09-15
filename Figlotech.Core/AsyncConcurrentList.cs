using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core
{
    public sealed class AsyncConcurrentList<T> : IEnumerable<T>, IList<T>
    {
        FiAsyncLock alock = new FiAsyncLock();
        List<T> dmmy { get; set; } = new List<T>();

        public int Count {
            get {
                lock (dmmy)
                    return ((ICollection<T>)dmmy).Count;
            }
        }

        public bool IsReadOnly {
            get {
                lock (dmmy) 
                    return ((ICollection<T>)dmmy).IsReadOnly;
            }
        }

        T IList<T>.this[int index] {
            get {
                lock (dmmy)
                    return ((IList<T>)dmmy)[index];
            }
            set {
                lock (dmmy)
                    ((IList<T>)dmmy)[index] = value;
            }
        }

        public Task AddAsync(T item) {
            return alock.Lock(async () => {
                await Task.Yield();
                dmmy.Add(item);
            });
        }
        public Task RemoveAsync(T item) {
            return alock.Lock(async () => {
                await Task.Yield();
                dmmy.Remove(item);
            });
        }
        public Task RemoveAllAsync(Predicate<T> match) {
            return alock.Lock(async () => {
                await Task.Yield();
                dmmy.RemoveAll(match);
            });
        }
        public Task<List<T>> ToList(Predicate<T> match) {
            return alock.Lock(async () => {
                await Task.Yield();
                return dmmy.ToList();
            });
        }

        public IEnumerator<T> GetEnumerator() {
            lock(dmmy)
                return ((IEnumerable<T>)dmmy.ToList()).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator() {
            lock (dmmy)
                return ((IEnumerable)dmmy.ToList()).GetEnumerator();
        }

        public int IndexOf(T item) {
            lock (dmmy)
                return ((IList<T>)dmmy).IndexOf(item);
        }

        public void Insert(int index, T item) {
            lock (dmmy)
                ((IList<T>)dmmy).Insert(index, item);
        }

        public void RemoveAt(int index) {
            lock (dmmy)
                ((IList<T>)dmmy).RemoveAt(index);
        }

        public void Add(T item) {
            lock (dmmy)
                ((ICollection<T>)dmmy).Add(item);
        }

        public void Clear() {
            lock (dmmy)
                ((ICollection<T>)dmmy).Clear();
        }

        public bool Contains(T item) {
            lock (dmmy)
                return ((ICollection<T>)dmmy).Contains(item);
        }

        public void CopyTo(T[] array, int arrayIndex) {
            lock (dmmy)
                ((ICollection<T>)dmmy).CopyTo(array, arrayIndex);
        }

        public bool Remove(T item) {
            lock (dmmy)
                return ((ICollection<T>)dmmy).Remove(item);
        }
    }
}
