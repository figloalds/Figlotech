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
    public class ConditionParserFrozenAliasTests {
        [Fact]
        public void FullGraphParserUsesOnlyFrozenAliasesForRootScalarFarAndNestedObjectFields() {
            DefinitiveJoinPlan plan = AutomaticJoinPlanCache.GetOrAdd(typeof(GuidRoot), AggregateJoinShape.FullGraph);
            var parser = new ConditionParser(plan);
            Guid value = Guid.NewGuid();

            string root = parser.ParseExpression<GuidRoot>(x => x.ScalarAggregateId == value).GetCommandText();
            string scalar = parser.ParseExpression<GuidRoot>(x => x.AggregateName == "name").GetCommandText();
            string far = parser.ParseExpression<GuidRoot>(x => x.FarAggregateName == "name").GetCommandText();
            string nested = parser.ParseExpression<GuidRoot>(x => x.AggregateObject!.Name == "name").GetCommandText();

            Assert.Contains(plan.AliasByPath[new AggregatePath(Array.Empty<string>())] + ".ScalarAggregateId", root);
            Assert.Contains(plan.AliasByPath[new AggregatePath(new[] { nameof(GuidRoot.AggregateName) })] + ".Name", scalar);
            Assert.Contains(plan.AliasByPath[new AggregatePath(new[] { nameof(GuidRoot.FarAggregateName) })] + ".Name", far);
            Assert.Contains(plan.AliasByPath[new AggregatePath(new[] { nameof(GuidRoot.AggregateObject) })] + ".Name", nested);
            Assert.DoesNotContain("tbg", root + scalar + far + nested);
        }

        [Fact]
        public void AnyUsesFrozenListAliasAndTypedIdentifierInsteadOfRid() {
            DefinitiveJoinPlan plan = AutomaticJoinPlanCache.GetOrAdd(typeof(GuidRoot), AggregateJoinShape.FullGraph);
            string sql = new ConditionParser(plan).ParseExpression<GuidRoot>(x => x.AggregateList.Any()).GetCommandText();
            string alias = plan.AliasByPath[new AggregatePath(new[] { nameof(GuidRoot.AggregateList) })];

            Assert.Contains(alias + ".Id IS NOT NULL", sql);
            Assert.DoesNotContain(alias + ".RID", sql);
        }

        [Fact]
        public void DirectAnyPredicateUsesFrozenListAliasTypedIdentifierAndBoundPredicateValue() {
            DefinitiveJoinPlan plan = AutomaticJoinPlanCache.GetOrAdd(typeof(GuidRoot), AggregateJoinShape.FullGraph);
            var query = new ConditionParser(plan).ParseExpression<GuidRoot>(x => x.AggregateList.Any(item => item.Name == "name"));
            string sql = query.GetCommandText();
            string listAlias = plan.AliasByPath[new AggregatePath(new[] { nameof(GuidRoot.AggregateList) })];

            Assert.Contains(listAlias + ".Id IS NOT NULL", sql);
            Assert.Contains("AND", sql);
            Assert.Single(Regex.Matches(sql, Regex.Escape(listAlias + ".Name")).Cast<Match>());
            Assert.Single(query.GetParameters());
            Assert.Equal("name", query.GetParameters().Single().Value);
        }

        [Fact]
        public void NestedAggregateAndWhereUseTheirFrozenSemanticAliases() {
            DefinitiveJoinPlan plan = AutomaticJoinPlanCache.GetOrAdd(typeof(SharedNestedObjectRoot), AggregateJoinShape.FullGraph);
            string nested = new ConditionParser(plan).ParseExpression<SharedNestedObjectRoot>(x => x.First!.NestedName == "name").GetCommandText();
            DefinitiveJoinPlan guidPlan = AutomaticJoinPlanCache.GetOrAdd(typeof(GuidRoot), AggregateJoinShape.FullGraph);
            string where = new ConditionParser(guidPlan)
                .ParseExpression<GuidRoot>(x => x.AggregateList.Where(item => item.Name == "name").Any()).GetCommandText();

            Assert.Contains(plan.AliasByPath[new AggregatePath(new[] { nameof(SharedNestedObjectRoot.First), nameof(SharedNestedParent.NestedName) })] + ".Name", nested);
            Assert.Contains(guidPlan.AliasByPath[new AggregatePath(new[] { nameof(GuidRoot.AggregateList) })] + ".Name", where);
        }

        [Fact]
        public void NestedAggregateInsideCollectionLambdaUsesItsFrozenSemanticAlias() {
            DefinitiveJoinPlan plan = AutomaticJoinPlanCache.GetOrAdd(typeof(SharedNestedListRoot), AggregateJoinShape.FullGraph);
            string sql = new ConditionParser(plan)
                .ParseExpression<SharedNestedListRoot>(x => x.First.Where(item => item.NestedName == "name").Any())
                .GetCommandText();
            string nestedAlias = plan.AliasByPath[new AggregatePath(new[] { nameof(SharedNestedListRoot.First), nameof(SharedNestedParent.NestedName) })];
            string collectionAlias = plan.AliasByPath[new AggregatePath(new[] { nameof(SharedNestedListRoot.First) })];

            Assert.Contains(nestedAlias + ".Name", sql);
            Assert.DoesNotContain(collectionAlias + ".Name", sql);
        }

        [Fact]
        public void NestedObjectRegularFieldAndNestedListInsideCollectionLambdaUseFrozenSemanticAliases() {
            DefinitiveJoinPlan plan = AutomaticJoinPlanCache.GetOrAdd(typeof(SharedNestedListRoot), AggregateJoinShape.FullGraph);
            var parser = new ConditionParser(plan);
            string objectSql = parser.ParseExpression<SharedNestedListRoot>(x => x.First.Where(item => item.NestedObject!.Name == "name").Any()).GetCommandText();
            string listSql = parser.ParseExpression<SharedNestedListRoot>(x => x.First.Where(item => item.NestedList.Any()).Any()).GetCommandText();
            string objectAlias = plan.AliasByPath[new AggregatePath(new[] { nameof(SharedNestedListRoot.First), nameof(SharedNestedParent.NestedObject) })];
            string listAlias = plan.AliasByPath[new AggregatePath(new[] { nameof(SharedNestedListRoot.First), nameof(SharedNestedParent.NestedList) })];

            Assert.Contains(objectAlias + ".Name", objectSql);
            Assert.Contains(listAlias + ".Id IS NOT NULL", listSql);
        }

        [Fact]
        public void ConstructorsAndPrefixMakerFacadeResolveTheSameFrozenRootAlias() {
            DefinitiveJoinPlan plan = AutomaticJoinPlanCache.GetOrAdd(typeof(GuidRoot), AggregateJoinShape.FullGraph);
            var resolver = new DefinitiveAliasResolver(plan);
            Expression<Func<GuidRoot, bool>> expression = x => x.ScalarAggregateId == Guid.Empty;

            string expected = plan.AliasByPath[new AggregatePath(Array.Empty<string>())] + ".ScalarAggregateId";
            Assert.Contains(expected, new ConditionParser().ParseExpression(expression).GetCommandText());
            Assert.Contains(expected, new ConditionParser(AggregateJoinShape.FullGraph).ParseExpression(expression).GetCommandText());
            Assert.Contains(expected, new ConditionParser(plan).ParseExpression(expression).GetCommandText());
            Assert.Contains(expected, new ConditionParser(resolver).ParseExpression(expression).GetCommandText());
            var prefixMaker = new PrefixMaker();
            string prefixFacade = new ConditionParser(prefixMaker).ParseExpression<GuidRoot>(x => x.AggregateName == "name").GetCommandText();
            Assert.Contains(plan.AliasByPath[new AggregatePath(new[] { nameof(GuidRoot.AggregateName) })] + ".Name", prefixFacade);
            Assert.Equal("tba", prefixMaker.GetAliasFor("independent", "relationship", "key"));
            Assert.Contains(expected, new ConditionParser(plan).ParseExpression(new Conditions<GuidRoot>(expression)).GetCommandText());
        }

        [Fact]
        public void PrefixMakerFacadeDoesNotReadOrRequireItsArgument() {
            DefinitiveJoinPlan plan = AutomaticJoinPlanCache.GetOrAdd(typeof(GuidRoot), AggregateJoinShape.FullGraph);
            string sql = new ConditionParser((PrefixMaker)null!).ParseExpression<GuidRoot>(x => x.AggregateName == "name").GetCommandText();
            string expected = plan.AliasByPath[new AggregatePath(new[] { nameof(GuidRoot.AggregateName) })] + ".Name";

            Assert.Contains(expected, sql);
        }

        [Fact]
        public void ScalarParserRejectsObjectPathThroughFrozenResolverAfterFullGraphPrewarm() {
            DefinitiveJoinPlan fullGraph = AutomaticJoinPlanCache.GetOrAdd(typeof(GuidRoot), AggregateJoinShape.FullGraph);
            var parser = new ConditionParser(AggregateJoinShape.ScalarAggregatesOnly);
            BDadosException exception = Assert.Throws<BDadosException>(() => parser.ParseExpression<GuidRoot>(x => x.AggregateObject!.Name == "name"));

            Assert.Equal(typeof(GuidRoot), fullGraph.RootType);
            Assert.Equal(AggregateJoinShape.FullGraph, fullGraph.Shape);
            Assert.Contains(nameof(GuidRoot), exception.ToString());
            Assert.Contains(nameof(AggregateJoinShape.ScalarAggregatesOnly), exception.ToString());
            Assert.Contains(nameof(GuidRoot.AggregateObject), exception.ToString());
        }

        [Fact]
        public void ConfiguredPlanAndResolverRejectExpressionRootMismatchesActionably() {
            DefinitiveJoinPlan plan = AutomaticJoinPlanCache.GetOrAdd(typeof(GuidRoot), AggregateJoinShape.FullGraph);
            BDadosException planException = Assert.Throws<BDadosException>(() => new ConditionParser(plan).ParseExpression<LongRoot>(x => x.Id == 1));
            BDadosException resolverException = Assert.Throws<BDadosException>(() => new ConditionParser(new DefinitiveAliasResolver(plan)).ParseExpression<LongRoot>(x => x.Id == 1));

            Assert.Contains("Configured frozen join plan root type", planException.ToString());
            Assert.Contains(nameof(GuidRoot), planException.ToString());
            Assert.Contains(nameof(LongRoot), planException.ToString());
            Assert.Contains("Configured frozen alias resolver root type", resolverException.ToString());
            Assert.Contains(nameof(GuidRoot), resolverException.ToString());
            Assert.Contains(nameof(LongRoot), resolverException.ToString());
        }

        [Fact]
        public void CapturedValuesAreParameterizedWithoutResolvingAliasesOutsideTheSelectedPlan() {
            DefinitiveJoinPlan plan = AutomaticJoinPlanCache.GetOrAdd(typeof(GuidRoot), AggregateJoinShape.FullGraph);
            string captured = "name";
            string sql = new ConditionParser(plan).ParseExpression<GuidRoot>(x => x.AggregateName == captured).GetCommandText();

            Assert.Contains("@_p0", sql);
            Assert.Contains(plan.AliasByPath[new AggregatePath(new[] { nameof(GuidRoot.AggregateName) })] + ".Name", sql);
            AssertOnlyPlanAliases(plan, sql);
        }

        [Fact]
        public void OuterRootMembersInsideCollectionLambdaKeepTheirOwnFrozenSemanticAlias() {
            DefinitiveJoinPlan plan = AutomaticJoinPlanCache.GetOrAdd(typeof(GuidRoot), AggregateJoinShape.FullGraph);
            string sql = new ConditionParser(plan)
                .ParseExpression<GuidRoot>(x => x.AggregateList.Where(item => item.Name == x.AggregateName).Any())
                .GetCommandText();
            string listAlias = plan.AliasByPath[new AggregatePath(new[] { nameof(GuidRoot.AggregateList) })];
            string scalarAlias = plan.AliasByPath[new AggregatePath(new[] { nameof(GuidRoot.AggregateName) })];

            Assert.Contains(listAlias + ".Name", sql);
            Assert.Contains(scalarAlias + ".Name", sql);
        }

        [Fact]
        public void DirectFirstMemberUsesTheFrozenListAlias() {
            DefinitiveJoinPlan plan = AutomaticJoinPlanCache.GetOrAdd(typeof(GuidRoot), AggregateJoinShape.FullGraph);
            string sql = new ConditionParser(plan).ParseExpression<GuidRoot>(x => x.AggregateList.First().Name == "name").GetCommandText();
            string listAlias = plan.AliasByPath[new AggregatePath(new[] { nameof(GuidRoot.AggregateList) })];

            Assert.Contains(listAlias + ".Name", sql);
            Assert.Single(Regex.Matches(sql, Regex.Escape(listAlias + ".Name")).Cast<Match>());
            AssertOnlyPlanAliases(plan, sql);
        }

        [Fact]
        public void RepresentativeParserSqlUsesOnlyAliasesPublishedByItsSelectedPlans() {
            DefinitiveJoinPlan plan = AutomaticJoinPlanCache.GetOrAdd(typeof(GuidRoot), AggregateJoinShape.FullGraph);
            var parser = new ConditionParser(plan);
            string sql = String.Join(" ", new[] {
                parser.ParseExpression<GuidRoot>(x => x.ScalarAggregateId == Guid.Empty).GetCommandText(),
                parser.ParseExpression<GuidRoot>(x => x.AggregateName == "name").GetCommandText(),
                parser.ParseExpression<GuidRoot>(x => x.FarAggregateName == "name").GetCommandText(),
                parser.ParseExpression<GuidRoot>(x => x.AggregateObject!.Name == "name").GetCommandText(),
                parser.ParseExpression<GuidRoot>(x => x.AggregateList.Any()).GetCommandText(),
                parser.ParseExpression<GuidRoot>(x => x.AggregateList.Where(item => item.Name == "name").Any()).GetCommandText()
            });
            DefinitiveJoinPlan nestedPlan = AutomaticJoinPlanCache.GetOrAdd(typeof(SharedNestedListRoot), AggregateJoinShape.FullGraph);
            string nestedSql = new ConditionParser(nestedPlan)
                .ParseExpression<SharedNestedListRoot>(x => x.First.Where(item => item.NestedName == "name").Any())
                .GetCommandText();

            AssertOnlyPlanAliases(plan, sql);
            AssertOnlyPlanAliases(nestedPlan, nestedSql);
        }

        [Fact]
        public void FullConditionsFalseStripsTheConfiguredFrozenRootAlias() {
            var definition = new JoinDefinition {
                Joins = new List<JoiningTable> {
                    new JoiningTable {
                        ValueObject = typeof(GuidRoot),
                        TableName = nameof(GuidRoot),
                        Alias = "customroot",
                        Prefix = "customroot",
                        Type = JoinType.LEFT,
                        Columns = new List<string> { nameof(GuidRoot.ScalarAggregateId) }
                    }
                },
                Relations = new List<Relation>()
            };
            DefinitiveJoinPlan compiled = DefinitiveJoinPlanCompiler.Compile(definition, typeof(GuidRoot), AggregateJoinShape.FullGraph);
            DefinitiveJoinPlan plan = new DefinitiveJoinPlan(
                compiled.RootType,
                compiled.Shape,
                compiled.RootTableIndex,
                compiled.Tables,
                compiled.Relations,
                compiled.Projection,
                compiled.AliasByPath.Concat(new[] {
                    new KeyValuePair<AggregatePath, string>(new AggregatePath(Array.Empty<string>()), "customroot")
                }),
                compiled.TableIndexByAlias,
                compiled.RootOrdering,
                compiled.FormatVersion);

            string sql = new ConditionParser(plan).ParseExpression<GuidRoot>(x => x.ScalarAggregateId == Guid.Empty, false).GetCommandText();

            Assert.Contains(nameof(GuidRoot.ScalarAggregateId), sql);
            Assert.DoesNotContain("customroot.", sql);
            Assert.DoesNotContain("tba.", sql);
        }

        [Fact]
        public void FullConditionsFalseStripsOnlyRootAlias() {
            string sql = new ConditionParser().ParseExpression<GuidRoot>(x => x.ScalarAggregateId == Guid.Empty, false).GetCommandText();

            Assert.Contains("ScalarAggregateId", sql);
            Assert.DoesNotContain("tba.ScalarAggregateId", sql);
        }
        private static void AssertOnlyPlanAliases(DefinitiveJoinPlan plan, string sql) {
            string[] aliases = plan.AliasByPath.Values.Distinct().ToArray();
            foreach (Match match in Regex.Matches(sql, @"\btb[a-z]+\b")) {
                Assert.Contains(match.Value, aliases);
            }
        }
    }
}
