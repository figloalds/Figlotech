using System;
using System.Linq;
using System.Threading.Tasks;
using Figlotech.BDados.DataAccessAbstractions;
using Xunit;

namespace Figlotech.BDados.Tests {
    public class AutomaticJoinPlanCacheAndResolverTests {
        [Fact]
        public void CacheReturnsSamePublishedPlanForSameKeyAndVersion() {
            DefinitiveJoinPlan first = AutomaticJoinPlanCache.GetOrAdd(typeof(GuidRoot), AggregateJoinShape.FullGraph);
            DefinitiveJoinPlan second = AutomaticJoinPlanCache.GetOrAdd(typeof(GuidRoot), AggregateJoinShape.FullGraph);

            Assert.Same(first, second);
            Assert.Equal(DefinitiveJoinPlanCompiler.CurrentFormatVersion, first.FormatVersion);
            Assert.Equal(typeof(GuidRoot), first.RootType);
        }

        [Fact]
        public async Task CachePublishesExactlyOnePlanInstanceUnderConcurrency() {
            Task<DefinitiveJoinPlan>[] callers = Enumerable.Range(0, 40)
                .Select(_ => Task.Run(() => AutomaticJoinPlanCache.GetOrAdd(typeof(RepeatedRelationRoot), AggregateJoinShape.ScalarAggregatesOnly)))
                .ToArray();
            DefinitiveJoinPlan[] plans = await Task.WhenAll(callers);

            Assert.All(plans, plan => Assert.Same(plans[0], plan));
            Assert.All(plans, plan => Assert.Equal(plans[0].StructuralSignature, plan.StructuralSignature));
        }

        [Fact]
        public void CacheIsolatesShapes() {
            DefinitiveJoinPlan scalar = AutomaticJoinPlanCache.GetOrAdd(typeof(GuidRoot), AggregateJoinShape.ScalarAggregatesOnly);
            DefinitiveJoinPlan full = AutomaticJoinPlanCache.GetOrAdd(typeof(GuidRoot), AggregateJoinShape.FullGraph);

            Assert.NotSame(scalar, full);
            Assert.Equal(AggregateJoinShape.ScalarAggregatesOnly, scalar.Shape);
            Assert.Equal(AggregateJoinShape.FullGraph, full.Shape);
            Assert.DoesNotContain(new AggregatePath(new[] { nameof(GuidRoot.AggregateObject) }), scalar.AliasByPath.Keys);
            Assert.Contains(new AggregatePath(new[] { nameof(GuidRoot.AggregateObject) }), full.AliasByPath.Keys);
        }

        [Fact]
        public void CacheRejectsInvalidKeysBeforeCompilingOrPublishingPlans() {
            Assert.Throws<ArgumentNullException>(() => AutomaticJoinPlanCache.GetOrAdd(null!, AggregateJoinShape.FullGraph));
            Assert.Throws<ArgumentOutOfRangeException>(() => AutomaticJoinPlanCache.GetOrAdd(typeof(GuidRoot), (AggregateJoinShape)987));

            DefinitiveJoinPlan plan = AutomaticJoinPlanCache.GetOrAdd(typeof(GuidRoot), AggregateJoinShape.FullGraph);
            Assert.Equal(typeof(GuidRoot), plan.RootType);
            Assert.Equal(AggregateJoinShape.FullGraph, plan.Shape);
        }

        [Fact]
        public void CacheKeepsCyclicCompilerFailureCachedWithoutPublishingAPartialPlan() {
            ArgumentException first = Assert.Throws<ArgumentException>(() => AutomaticJoinPlanCache.GetOrAdd(typeof(CyclicAggregateA), AggregateJoinShape.FullGraph));
            ArgumentException second = Assert.Throws<ArgumentException>(() => AutomaticJoinPlanCache.GetOrAdd(typeof(CyclicAggregateA), AggregateJoinShape.FullGraph));

            Assert.Equal(first.Message, second.Message);
            Assert.Contains(nameof(CyclicAggregateA), first.Message);
            Assert.Contains("cycle", first.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void ResolverUsesFrozenSemanticPathsAndReportsUnknownPaths() {
            DefinitiveJoinPlan plan = AutomaticJoinPlanCache.GetOrAdd(typeof(SharedNestedObjectRoot), AggregateJoinShape.FullGraph);
            var resolver = new DefinitiveAliasResolver(plan);
            var root = new AggregatePath(Array.Empty<string>());
            var first = new AggregatePath(new[] { nameof(SharedNestedObjectRoot.First) });
            var second = new AggregatePath(new[] { nameof(SharedNestedObjectRoot.Second) });
            var nested = new AggregatePath(new[] { nameof(SharedNestedObjectRoot.First), nameof(SharedNestedParent.NestedName) });

            Assert.Equal(plan.AliasByPath[root], resolver.Resolve(root));
            Assert.Equal(plan.AliasByPath[first], resolver.Resolve(first));
            Assert.Equal(resolver.Resolve(first), resolver.Resolve(second));
            Assert.True(resolver.TryResolve(nested, out string alias));
            Assert.Equal(plan.AliasByPath[nested], alias);
            Assert.False(resolver.TryResolve(new AggregatePath(new[] { "Unknown" }), out _));
            ArgumentException exception = Assert.Throws<ArgumentException>(() => resolver.Resolve(new AggregatePath(new[] { "Unknown" })));
            Assert.Contains(nameof(SharedNestedObjectRoot), exception.Message);
            Assert.Contains(nameof(AggregateJoinShape.FullGraph), exception.Message);
            Assert.Contains("Unknown", exception.Message);
            Assert.Contains("Known", exception.Message);
        }

        [Fact]
        public async Task ResolverCoversPublishedPathsTablesAndConcurrentImmutableReads() {
            DefinitiveJoinPlan plan = AutomaticJoinPlanCache.GetOrAdd(typeof(GuidRoot), AggregateJoinShape.FullGraph);
            var resolver = new DefinitiveAliasResolver(plan);
            var root = new AggregatePath(Array.Empty<string>());
            var scalar = new AggregatePath(new[] { nameof(GuidRoot.AggregateName) });
            var far = new AggregatePath(new[] { nameof(GuidRoot.FarAggregateName) });
            var aggregateObject = new AggregatePath(new[] { nameof(GuidRoot.AggregateObject) });
            var list = new AggregatePath(new[] { nameof(GuidRoot.AggregateList) });

            Assert.Equal(plan.AliasByPath[root], resolver.Resolve(root));
            Assert.Equal(resolver.Resolve(root), resolver.Resolve(default));
            Assert.Equal(plan.AliasByPath[scalar], resolver.Resolve(scalar));
            Assert.Equal(plan.AliasByPath[far], resolver.Resolve(far));
            Assert.Equal(plan.AliasByPath[aggregateObject], resolver.Resolve(aggregateObject));
            Assert.Equal(plan.AliasByPath[list], resolver.Resolve(list));
            Assert.True(resolver.TryResolve(list, out string listAlias));
            Assert.Equal(plan.AliasByPath[list], listAlias);
            Assert.False(resolver.TryResolve(new AggregatePath(new[] { "Unknown" }), out _));
            Assert.Equal(nameof(ListAggregate.Id), resolver.ResolveTable(list).Identifier.ColumnName);

            DefinitiveJoinPlan nestedPlan = AutomaticJoinPlanCache.GetOrAdd(typeof(SharedNestedObjectRoot), AggregateJoinShape.FullGraph);
            var nestedResolver = new DefinitiveAliasResolver(nestedPlan);
            var first = new AggregatePath(new[] { nameof(SharedNestedObjectRoot.First) });
            var second = new AggregatePath(new[] { nameof(SharedNestedObjectRoot.Second) });
            var nested = new AggregatePath(new[] { nameof(SharedNestedObjectRoot.First), nameof(SharedNestedParent.NestedName) });
            Assert.Equal(nestedPlan.AliasByPath[nested], nestedResolver.Resolve(nested));
            Assert.Equal(nestedResolver.Resolve(first), nestedResolver.Resolve(second));

            Task[] readers = Enumerable.Range(0, 40).Select(_ => Task.Run(() => {
                for (int i = 0; i < 100; i++) {
                    Assert.Equal(plan.AliasByPath[scalar], resolver.Resolve(scalar));
                    Assert.Equal(plan.AliasByPath[list], resolver.ResolveTable(list).Alias);
                    Assert.Equal(nestedPlan.AliasByPath[nested], nestedResolver.Resolve(nested));
                }
            })).ToArray();
            await Task.WhenAll(readers);
        }

        [Fact]
        public void ResolverExposesFrozenTableForTypedIdentifier() {
            DefinitiveJoinPlan plan = AutomaticJoinPlanCache.GetOrAdd(typeof(GuidRoot), AggregateJoinShape.FullGraph);
            var resolver = new DefinitiveAliasResolver(plan);
            DefinitiveJoinTable table = resolver.ResolveTable(new AggregatePath(new[] { nameof(GuidRoot.AggregateList) }));

            Assert.Equal(nameof(ListAggregate.Id), table.Identifier.ColumnName);
            Assert.Equal(plan.Tables.Single(x => x.Alias == resolver.Resolve(new AggregatePath(new[] { nameof(GuidRoot.AggregateList) }))).Alias, table.Alias);
        }
    }
}
