using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.Core.ParallelFlow {

    public static class AsyncEnumerableWeaver {
        public static async IAsyncEnumerable<T> Weave<T>(Comparison<T> comparer, params IAsyncEnumerable<T>[] batches) {
            var enumerators = new List<IAsyncEnumerator<T>>();
            foreach (var batch in batches) {
                enumerators.Add(batch.GetAsyncEnumerator());
            }
            var currents = new T[enumerators.Count];

            var hasValue = new bool[enumerators.Count];
            var completed = new bool[enumerators.Count];
            while (true) {
                for (int i = 0; i < enumerators.Count; i++) {
                    if (!completed[i] && !hasValue[i]) {
                        if (await enumerators[i].MoveNextAsync()) {
                            currents[i] = enumerators[i].Current;
                            hasValue[i] = true;
                        } else {
                            completed[i] = true;
                        }
                    }
                }
                T? minValue = default;
                int minIndex = -1;
                for (int i = 0; i < enumerators.Count; i++) {
                    if (hasValue[i]) {
                        if (minIndex == -1 || comparer.Invoke(currents[i], minValue!) < 0) {
                            minValue = currents[i];
                            minIndex = i;
                        }
                    }
                }
                if (minIndex == -1) {
                    break; // All done
                }
                yield return minValue!;
                hasValue[minIndex] = false;
            }
            foreach (var enumerator in enumerators) {
                await enumerator.DisposeAsync();
            }
        }
    }
}
