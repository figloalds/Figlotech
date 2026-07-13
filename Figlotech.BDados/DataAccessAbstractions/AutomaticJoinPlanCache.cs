using System;
using System.Collections.Concurrent;
using System.Threading;

namespace Figlotech.BDados.DataAccessAbstractions {
    /// <summary>
    /// Canonical automatic-plan cache. Compiler exceptions are intentionally cached by Lazy.
    /// </summary>
    public static class AutomaticJoinPlanCache {
        private static readonly ConcurrentDictionary<AutoJoinPlanKey, Lazy<DefinitiveJoinPlan>> _plans = new ConcurrentDictionary<AutoJoinPlanKey, Lazy<DefinitiveJoinPlan>>();

        public static DefinitiveJoinPlan GetOrAdd(Type rootType, AggregateJoinShape shape) {
            if (rootType == null) {
                throw new ArgumentNullException(nameof(rootType));
            }
            if (!Enum.IsDefined(typeof(AggregateJoinShape), shape)) {
                throw new ArgumentOutOfRangeException(nameof(shape), shape, "Aggregate join shape must be a defined value.");
            }

            var key = new AutoJoinPlanKey(rootType, shape, DefinitiveJoinPlanCompiler.CurrentFormatVersion);
            Lazy<DefinitiveJoinPlan> lazy = _plans.GetOrAdd(key, cacheKey => new Lazy<DefinitiveJoinPlan>(
                () => DefinitiveJoinPlanCompiler.Compile(cacheKey.RootType, cacheKey.Shape),
                LazyThreadSafetyMode.ExecutionAndPublication));
            return lazy.Value;
        }

        public static DefinitiveJoinPlan GetOrAdd<T>(AggregateJoinShape shape) {
            return GetOrAdd(typeof(T), shape);
        }
    }
}
