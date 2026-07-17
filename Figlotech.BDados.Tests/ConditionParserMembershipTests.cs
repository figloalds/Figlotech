using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text.RegularExpressions;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.Exceptions;
using Figlotech.BDados.Helpers;
using Figlotech.Core.Helpers;
using Xunit;

namespace Figlotech.BDados.Tests {
    public class ConditionParserMembershipTests {
        private class ScalarItem {
            public Guid Id { get; set; }
        }

        private sealed class CollectionSourceHolder {
            private readonly IEnumerable<Guid> _values;

            public CollectionSourceHolder(IEnumerable<Guid> values) {
                _values = values;
            }

            public int GetterCallCount { get; private set; }

            public IEnumerable<Guid> Values {
                get {
                    GetterCallCount++;
                    return _values;
                }
            }
        }

        private sealed class CountingGuidEnumerable : IEnumerable<Guid> {
            private readonly IEnumerable<Guid> _values;

            public CountingGuidEnumerable(IEnumerable<Guid> values) {
                _values = values;
            }

            public int GetEnumeratorCallCount { get; private set; }

            public IEnumerator<Guid> GetEnumerator() {
                GetEnumeratorCallCount++;
                return _values.GetEnumerator();
            }

            System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() {
                return GetEnumerator();
            }
        }

        private static void AssertAllSqlAliasesAreFromPlan(DefinitiveJoinPlan plan, string sql) {
            IEnumerable<string> aliases = Regex.Matches(sql, @"\btb[a-zA-Z0-9_]*\b")
                .Cast<Match>()
                .Select(match => match.Value)
                .Distinct(StringComparer.Ordinal);

            Assert.All(aliases, alias => Assert.Contains(alias, plan.AliasByPath.Values));
        }

        [Fact]
        public void CapturedListGuidContainsRootEmitsInWithOneParameterPerValue() {
            List<Guid> ids = new List<Guid> { Guid.Parse("11111111-1111-1111-1111-111111111111"), Guid.Parse("22222222-2222-2222-2222-222222222222"), Guid.Parse("33333333-3333-3333-3333-333333333333") };
            var parser = new ConditionParser();

            var query = parser.ParseExpression<GuidRoot>(x => ids.Contains(x.ScalarAggregateId));
            string sql = query.GetCommandText();

            Assert.Contains(".ScalarAggregateId IN(", sql);
            Assert.Contains("IN", sql, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("LIKE", sql, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(3, query.GetParameters().Count);
            Assert.Equal(ids.OrderBy(g => g), query.GetParameters().Select(p => p.Value).Cast<Guid>().OrderBy(g => g));
        }

        [Fact]
        public void InterfaceTypedStandardCollectionContainsRootEmitsInWithOneParameterPerValue() {
            ICollection<Guid> ids = new List<Guid> {
                Guid.Parse("44444444-4444-4444-4444-444444444444"),
                Guid.Parse("55555555-5555-5555-5555-555555555555")
            };

            var query = new ConditionParser().ParseExpression<GuidRoot>(x => ids.Contains(x.ScalarAggregateId));
            string sql = query.GetCommandText();

            Assert.Contains(".ScalarAggregateId IN(", sql);
            Assert.DoesNotContain("LIKE", sql, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(2, query.GetParameters().Count);
            Assert.Equal(ids.OrderBy(g => g), query.GetParameters().Select(p => p.Value).Cast<Guid>().OrderBy(g => g));
        }

        [Fact]
        public void EnumerableContainsArrayAndHashSetProduceEquivalentMembershipSql() {
            Guid[] array = new Guid[] { Guid.Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"), Guid.Parse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb") };
            HashSet<Guid> hashSet = new HashSet<Guid> { Guid.Parse("cccccccc-cccc-cccc-cccc-cccccccccccc") };

            string arraySql = new ConditionParser().ParseExpression<GuidRoot>(x => array.Contains(x.ScalarAggregateId)).GetCommandText();
            string hashSetSql = new ConditionParser().ParseExpression<GuidRoot>(x => hashSet.Contains(x.ScalarAggregateId)).GetCommandText();
            string enumerableSql = new ConditionParser().ParseExpression<GuidRoot>(x => Enumerable.Contains(array, x.ScalarAggregateId)).GetCommandText();

            Assert.Contains(".ScalarAggregateId IN(", arraySql);
            Assert.Contains(".ScalarAggregateId IN(", hashSetSql);
            Assert.Contains(".ScalarAggregateId IN(", enumerableSql);
            Assert.Contains("@_p0", arraySql);
            Assert.Contains("@_p0", hashSetSql);
            Assert.Contains("@_p0", enumerableSql);
        }

        [Fact]
        public void LocalProjectionSelectContainsEvaluatesSourceAndProjectsValues() {
            List<ScalarItem> items = new List<ScalarItem> {
                new ScalarItem { Id = Guid.Parse("11111111-1111-1111-1111-111111111111") },
                new ScalarItem { Id = Guid.Parse("22222222-2222-2222-2222-222222222222") }
            };

            var query = new ConditionParser().ParseExpression<GuidRoot>(x => items.Select(item => item.Id).Contains(x.ScalarAggregateId));
            string sql = query.GetCommandText();

            Assert.Contains(".ScalarAggregateId IN(", sql);
            Assert.Equal(2, query.GetParameters().Count);
            Assert.Equal(items.Select(i => i.Id).OrderBy(g => g), query.GetParameters().Select(p => p.Value).Cast<Guid>().OrderBy(g => g));
        }

        [Fact]
        public void EnumerableContainsEvaluatesCollectionSourceAndMaterializesItOnce() {
            Guid first = Guid.Parse("11111111-1111-1111-1111-111111111111");
            Guid second = Guid.Parse("22222222-2222-2222-2222-222222222222");
            var values = new CountingGuidEnumerable(new[] { first, second });
            var source = new CollectionSourceHolder(values);

            var query = new ConditionParser().ParseExpression<GuidRoot>(x => Enumerable.Contains(source.Values, x.ScalarAggregateId));
            string sql = query.GetCommandText();

            Assert.Contains(".ScalarAggregateId IN(", sql);
            Assert.Equal(1, source.GetterCallCount);
            Assert.Equal(1, values.GetEnumeratorCallCount);
            Assert.Equal(2, query.GetParameters().Count);
            Assert.Equal(new[] { first, second }.OrderBy(g => g), query.GetParameters().Select(p => p.Value).Cast<Guid>().OrderBy(g => g));
        }

        [Fact]
        public void EmptyAndNullLocalSourcesEmitLogicalFalseWithNoParameters() {
            List<Guid> empty = new List<Guid>();
            List<Guid> nullList = null!;

            string emptySql = new ConditionParser().ParseExpression<GuidRoot>(x => empty.Contains(x.ScalarAggregateId)).GetCommandText();
            string nullSql = new ConditionParser().ParseExpression<GuidRoot>(x => nullList.Contains(x.ScalarAggregateId)).GetCommandText();
            string enumerableEmptySql = new ConditionParser().ParseExpression<GuidRoot>(x => Enumerable.Contains(empty, x.ScalarAggregateId)).GetCommandText();

            Assert.Contains("1=0", emptySql);
            Assert.Contains("1=0", nullSql);
            Assert.Contains("1=0", enumerableEmptySql);
            Assert.DoesNotContain("@", emptySql + nullSql + enumerableEmptySql);
        }

        [Fact]
        public void NullableMemberWithNonNullAndNullEmitsInOrIsNullAndNullNotParameterized() {
            List<Guid?> ids = new List<Guid?> {
                Guid.Parse("11111111-1111-1111-1111-111111111111"),
                null,
                Guid.Parse("33333333-3333-3333-3333-333333333333")
            };
            DefinitiveJoinPlan plan = AutomaticJoinPlanCache.GetOrAdd(typeof(NullableGuidRoot), AggregateJoinShape.FullGraph);
            string rootAlias = plan.AliasByPath[new AggregatePath(Array.Empty<string>())];

            var query = new ConditionParser(plan).ParseExpression<NullableGuidRoot>(x => ids.Contains(x.NullableAggregateId));
            string sql = query.GetCommandText();

            Assert.Contains($"{rootAlias}.NullableAggregateId IN(", sql);
            Assert.Contains($"{rootAlias}.NullableAggregateId IS NULL", sql);
            Assert.Contains("OR", sql);
            Assert.Contains("(", sql);
            Assert.Equal(2, query.GetParameters().Count);
            Assert.DoesNotContain(query.GetParameters(), p => p.Value == null);
        }

        [Fact]
        public void NullableMemberNullOnlyEmitsIsNullAndNoParameters() {
            List<Guid?> ids = new List<Guid?> { null };
            DefinitiveJoinPlan plan = AutomaticJoinPlanCache.GetOrAdd(typeof(NullableGuidRoot), AggregateJoinShape.FullGraph);
            string rootAlias = plan.AliasByPath[new AggregatePath(Array.Empty<string>())];

            var query = new ConditionParser(plan).ParseExpression<NullableGuidRoot>(x => ids.Contains(x.NullableAggregateId));
            string sql = query.GetCommandText();

            Assert.Contains($"{rootAlias}.NullableAggregateId IS NULL", sql);
            Assert.DoesNotContain("@", sql);
            Assert.Empty(query.GetParameters());
        }

        [Fact]
        public void NonNullableMemberWithNullOnlySourceEmitsFalse() {
            List<Guid?> ids = new List<Guid?> { null };

            var query = new ConditionParser().ParseExpression<GuidRoot>(x => ids.Contains(x.ScalarAggregateId));
            string sql = query.GetCommandText();

            Assert.Contains("1=0", sql);
            Assert.DoesNotContain("@", sql);
            Assert.Empty(query.GetParameters());
        }

        [Fact]
        public void NegatedNonNullableMemberWithNullOnlySourceEmitsTrue() {
            List<Guid?> ids = new List<Guid?> { null };

            var query = new ConditionParser().ParseExpression<GuidRoot>(x => !ids.Contains(x.ScalarAggregateId));
            string sql = query.GetCommandText();

            Assert.Contains("1=1", sql);
            Assert.DoesNotContain("NOT(", sql);
            Assert.DoesNotContain("@", sql);
            Assert.Empty(query.GetParameters());
        }

        [Fact]
        public void NegatedNullableMemberWithOnlyNonNullValuesEmitsNotInOrIsNull() {
            Guid id = Guid.Parse("11111111-1111-1111-1111-111111111111");
            List<Guid?> ids = new List<Guid?> { id };
            DefinitiveJoinPlan plan = AutomaticJoinPlanCache.GetOrAdd(typeof(NullableGuidRoot), AggregateJoinShape.FullGraph);
            string rootAlias = plan.AliasByPath[new AggregatePath(Array.Empty<string>())];

            var query = new ConditionParser(plan).ParseExpression<NullableGuidRoot>(x => !ids.Contains(x.NullableAggregateId));
            string sql = query.GetCommandText();

            Assert.True(
                sql.Contains($"{rootAlias}.NullableAggregateId NOT IN(") ||
                (sql.Contains("NOT(") && sql.Contains($"{rootAlias}.NullableAggregateId IN(")));
            Assert.Contains("OR", sql);
            Assert.Contains($"{rootAlias}.NullableAggregateId IS NULL", sql);
            Assert.Single(query.GetParameters());
            Assert.Equal(id, query.GetParameters().Single().Value);
            Assert.DoesNotContain(query.GetParameters(), p => p.Value == null);
        }

        [Fact]
        public void NegatedMembershipNegatesCompleteFragment() {
            List<Guid> ids = new List<Guid> { Guid.Parse("11111111-1111-1111-1111-111111111111") };
            List<Guid?> nullableIds = new List<Guid?> { Guid.Parse("22222222-2222-2222-2222-222222222222"), null };
            DefinitiveJoinPlan nullablePlan = AutomaticJoinPlanCache.GetOrAdd(typeof(NullableGuidRoot), AggregateJoinShape.FullGraph);
            string nullableAlias = nullablePlan.AliasByPath[new AggregatePath(Array.Empty<string>())];

            string nonNullableSql = new ConditionParser().ParseExpression<GuidRoot>(x => !ids.Contains(x.ScalarAggregateId)).GetCommandText();
            string nullableSql = new ConditionParser(nullablePlan).ParseExpression<NullableGuidRoot>(x => !nullableIds.Contains(x.NullableAggregateId)).GetCommandText();

            Assert.Contains("NOT(", nonNullableSql);
            Assert.Contains(".ScalarAggregateId IN(", nonNullableSql);
            Assert.Contains("NOT(", nullableSql);
            Assert.Contains($"{nullableAlias}.NullableAggregateId IN(", nullableSql);
            Assert.Contains($"{nullableAlias}.NullableAggregateId IS NULL", nullableSql);
            Assert.Contains("OR", nullableSql);
        }

        [Fact]
        public void RootMembershipThroughDefaultParserDoesNotCompileInvalidUnrelatedAggregateMetadata() {
            List<string> ids = new List<string> { "value" };
            var parsers = new[] { new ConditionParser(), new ConditionParser(new PrefixMaker()) };

            foreach (var parser in parsers) {
                var query = parser.ParseExpression<InvalidAggregateRoot>(x => ids.Contains(x.RootValue!));
                string sql = query.GetCommandText();

                Assert.Contains(".RootValue IN(", sql);
                Assert.Single(query.GetParameters());
            }
        }

        [Fact]
        public void AggregateMemberMembershipUsesFrozenPlanAlias() {
            List<string> names = new List<string> { "alpha", "beta" };
            DefinitiveJoinPlan plan = AutomaticJoinPlanCache.GetOrAdd(typeof(GuidRoot), AggregateJoinShape.FullGraph);
            string scalarAlias = plan.AliasByPath[new AggregatePath(new[] { nameof(GuidRoot.AggregateName) })];

            var query = new ConditionParser(plan).ParseExpression<GuidRoot>(x => names.Contains(x.AggregateName!));
            string sql = query.GetCommandText();

            Assert.Contains($"{scalarAlias}.Name IN(", sql);
            AssertAllSqlAliasesAreFromPlan(plan, sql);
            Assert.Equal(2, query.GetParameters().Count);
            Assert.Equal(names.OrderBy(s => s), query.GetParameters().Select(p => p.Value).Cast<string>().OrderBy(s => s));
        }

        [Fact]
        public void FullConditionsFalseRootMembershipPreservesExactRootAliasStripped() {
            List<Guid> ids = new List<Guid> { Guid.Parse("11111111-1111-1111-1111-111111111111") };

            var query = new ConditionParser().ParseExpression<GuidRoot>(x => ids.Contains(x.ScalarAggregateId), false);
            string sql = query.GetCommandText();

            Assert.Contains("ScalarAggregateId IN(", sql, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("tba.ScalarAggregateId", sql);
            Assert.Single(query.GetParameters());
        }

        [Fact]
        public void FullConditionsFalseJoinedMembershipProjectsToImplicationSafeTrue() {
            List<string> names = new List<string> { "alpha" };
            DefinitiveJoinPlan plan = AutomaticJoinPlanCache.GetOrAdd(typeof(GuidRoot), AggregateJoinShape.FullGraph);

            var query = new ConditionParser(plan).ParseExpression<GuidRoot>(x => names.Contains(x.AggregateName!), false);

            Assert.Equal("TRUE", query.GetCommandText());
            Assert.Empty(query.GetParameters());
        }

        [Fact]
        public void StringContainsRemainsLikeAndIsNotTreatedAsCollectionMembership() {
            var query = new ConditionParser().ParseExpression<GuidRoot>(x => x.AggregateName!.Contains("needle"));
            string sql = query.GetCommandText();

            Assert.Contains("LIKE", sql, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("IN", sql, StringComparison.OrdinalIgnoreCase);
            Assert.Single(query.GetParameters());
            Assert.Equal("needle", query.GetParameters().Single().Value);
        }

        [Fact]
        public void EnumerableContainsComparerOverloadIsRejected() {
            List<Guid> ids = new List<Guid> { Guid.Parse("11111111-1111-1111-1111-111111111111") };

            BDadosException exception = Assert.Throws<BDadosException>(() =>
                new ConditionParser().ParseExpression<GuidRoot>(x => Enumerable.Contains(ids, x.ScalarAggregateId, EqualityComparer<Guid>.Default)));

            Assert.IsType<NotSupportedException>(exception.InnerException);
            Assert.Contains("Contains", exception.InnerException.Message);
        }

        [Fact]
        public void CollectionSourceWithFreeOuterReferenceIsRejected() {
            List<Guid> ids = new List<Guid> { Guid.Parse("11111111-1111-1111-1111-111111111111") };

            BDadosException exception = Assert.Throws<BDadosException>(() =>
                new ConditionParser().ParseExpression<GuidRoot>(x => ids.Where(id => id == x.ScalarAggregateId).Contains(x.ScalarAggregateId)));

            Assert.IsType<NotSupportedException>(exception.InnerException);
            Assert.Contains("free parameter", exception.InnerException.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void StringRoutedAsCollectionSourceIsRejected() {
            string source = "abc";

            BDadosException exception = Assert.Throws<BDadosException>(() =>
                new ConditionParser().ParseExpression<GuidRoot>(x => Enumerable.Contains(source, 'a')));

            Assert.IsType<NotSupportedException>(exception.InnerException);
            Assert.Contains("string", exception.InnerException.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void UnsupportedCustomSameNameContainsIsRejected() {
            CustomStringLike custom = new CustomStringLike("value");

            BDadosException exception = Assert.Throws<BDadosException>(() =>
                new ConditionParser().ParseExpression<GuidRoot>(x => custom.Contains(x.AggregateName!)));

            Assert.IsType<NotSupportedException>(exception.InnerException);
            Assert.Contains("Contains", exception.InnerException.Message);
        }
    }
}
