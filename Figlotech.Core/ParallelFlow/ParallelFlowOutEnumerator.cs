using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Figlotech.Core {

    public static partial class FiTechCoreExtensions {
        public class ParallelFlowOutEnumerator<T> : IAsyncEnumerator<T> {
            public T Current { get; set; }

            public ValueTask DisposeAsync() {
                throw new NotImplementedException();
            }

            public async ValueTask<bool> MoveNextAsync() {
                lock(cache) {
                    if(cache.Count > 0) {
                        Current = cache.Dequeue();
                        return true;
                    }
                }
                var retv = await MoveNext.Task;
                if(!retv) {
                    return false;
                }
                Current = cache.Dequeue();
                return retv;
            }

            public void Publish(T o) {
                lock(cache) {
                    cache.Enqueue(o);
                }
                var mn = MoveNext;
                MoveNext = new TaskCompletionSource<bool>();
                mn.SetResult(true);
            }
            public void Finish() {
                MoveNext.SetResult(false);
            }

            Queue<T> cache = new Queue<T>();

            TaskCompletionSource<bool> MoveNext = new TaskCompletionSource<bool>();

        }
    }
}