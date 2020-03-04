using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core
{
    public class AsyncConcurrentList<T>
    {
        FiAsyncLock alock = new FiAsyncLock();
        List<T> dmmy { get; set; } = new List<T>();
        public Task<T> this[int index] {
            get {
                return alock.Lock(() => Task.FromResult(dmmy[index]));
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
    }
}
