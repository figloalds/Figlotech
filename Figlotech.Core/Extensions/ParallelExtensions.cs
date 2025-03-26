using Figlotech.Core;
using Figlotech.Core.Helpers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace System.Threading.Tasks {
    public static class ParallelExtensions {
        /// <summary>
        /// https://medium.com/@alex.puiu/parallel-foreach-async-in-c-36756f8ebe62
        /// https://stackoverflow.com/questions/38634376/running-async-methods-in-parallel
        /// https://devblogs.microsoft.com/pfxteam/implementing-a-simple-foreachasync-part-2/
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <param name="batches"></param>
        /// <param name="body"></param>
        /// <returns></returns>
        public static async Task ParallelForEachAsync<T>(
            this IEnumerable<T> source,
            CancellationToken cancellation,
            int batches,
            Func<T, Task> body
            ) {
            async Task AwaitPartition(IEnumerator<T> partition) {
                using (partition) {
                    while (partition.MoveNext()) {
                        if(cancellation.IsCancellationRequested) {
                            break;
                        }
                        await Task.Yield(); // prevents a sync/hot thread hangup
                        await body(partition.Current);
                    }
                }
            }

            await Task.WhenAll(
                Partitioner
                    .Create(source)
                    .GetPartitions(batches)
                    .AsParallel()
                    .Select(s => AwaitPartition(s)));
        }
    }
}
