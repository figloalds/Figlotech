using Figlotech.BDados.DataAccessAbstractions;
using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.BDados.Helpers {
    public static class BatchDataLoader {
        public static async IAsyncEnumerable<T> LoadBatches<T, TContext>(
            Func<TContext, IAsyncEnumerable<T>> batchLoader,
            Func<TContext, bool> hasMoreBatches,
            TContext context,
            [EnumeratorCancellation] System.Threading.CancellationToken cancellationToken = default
        ) {
            while(hasMoreBatches(context)) {
                cancellationToken.ThrowIfCancellationRequested();
                await foreach(var item in batchLoader(context).WithCancellation(cancellationToken)) {
                    yield return item;
                }
            }
        }
    }
}
