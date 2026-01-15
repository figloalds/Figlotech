using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Figlotech.Core.ParallelFlow {

    /// <summary>
    /// Extension methods for converting enumerables to async enumerables
    /// </summary>
    internal static class AsyncEnumerableExtensions {

        /// <summary>
        /// Converts a synchronous enumerable to an async enumerable
        /// </summary>
        public static async IAsyncEnumerable<T> ToAsyncEnumerable<T>(
            this IEnumerable<T> source,
            [EnumeratorCancellation] System.Threading.CancellationToken cancellationToken = default) {

            foreach (var item in source) {
                cancellationToken.ThrowIfCancellationRequested();
                yield return item;
            }
        }

        /// <summary>
        /// Creates an empty async enumerable
        /// </summary>
        public static async IAsyncEnumerable<T> EmptyAsyncEnumerable<T>() {
            await System.Threading.Tasks.Task.CompletedTask;
            yield break;
        }
    }
}
