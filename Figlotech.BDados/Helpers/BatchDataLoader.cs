using Figlotech.BDados.DataAccessAbstractions;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Helpers {
    public static class BatchDataLoader {
        public static async IAsyncEnumerable<T> LoadBatches<T, TContext>(
            Func<TContext, IAsyncEnumerable<T>> batchLoader,
            Func<TContext, bool> hasMoreBatches,
            TContext context
        ) {
            while(hasMoreBatches(context)) {
                await foreach(var item in batchLoader(context)) {
                    yield return item;
                }
            }
        }
    }
}
