using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.Helpers;
using Figlotech.BDados.MySqlDataAccessor;
using Figlotech.BDados.PgSQLDataAccessor;
using Figlotech.BDados.SqliteDataAccessor;
using Figlotech.Core.Interfaces;
using Figlotech.Data;
using Npgsql;
using Xunit;

namespace Figlotech.BDados.Tests {
    public class ProviderDefinitiveJoinQueryTests {
        public static IEnumerable<object[]> Generators() {
            yield return new object[] { "MySQL", (IQueryGenerator)new MySqlQueryGenerator() };
            yield return new object[] { "SQLite", (IQueryGenerator)new SqliteQueryGenerator() };
            yield return new object[] { "PostgreSQL", (IQueryGenerator)new PgSQLQueryGenerator() };
        }

        [Theory]
        [MemberData(nameof(Generators))]
        public void LegacyJoinDefinitionExecutionFreezesAndForwardsWithoutMutation(string providerName, IQueryGenerator generator) {
            Assert.NotEmpty(providerName);
            JoinDefinition definition = CreateLegacyGuidRootDefinition();
            string[] columns = definition.Joins.SelectMany(table => table.Columns).ToArray();
            string[] tables = definition.Joins.Select(table => table.TableName + ":" + table.Alias + ":" + table.Prefix).ToArray();
            int relationCount = definition.Relations.Count;
            DefinitiveJoinPlan plan = definition.Freeze(typeof(GuidRoot), AggregateJoinShape.FullGraph);
            var conditions = new QueryBuilder();
            conditions.Append("root.ScalarAggregateId = @p", Guid.Parse("11111111-1111-1111-1111-111111111111"));
            var rootConditions = new QueryBuilder();
            rootConditions.Append("Id <> @p", Guid.Parse("22222222-2222-2222-2222-222222222222"));
            MemberInfo orderingMember = typeof(GuidRoot).GetProperty(nameof(GuidRoot.ScalarAggregateId))!;
            string[] expectedProjection = plan.Projection
                .OrderBy(column => column.Ordinal)
                .Select(column => plan.Tables[column.TableIndex].Prefix + "." + column.SourceColumn + " AS " + column.ResultAlias)
                .ToArray();

            IQueryBuilder frozen = generator.GenerateJoinQuery(plan, conditions, 3, 7, orderingMember, OrderingType.Desc, rootConditions);
#pragma warning disable CS0618
            IQueryBuilder legacy = generator.GenerateJoinQuery(definition, conditions, 3, 7, orderingMember, OrderingType.Desc, rootConditions);
            IQueryBuilder repeated = generator.GenerateJoinQuery(definition, conditions, 3, 7, orderingMember, OrderingType.Desc, rootConditions);
#pragma warning restore CS0618
            string sql = legacy.GetCommandText();
            string[] actualProjection = SplitSelectList(RemoveSqlLineComments(ExtractInnerSelectList(sql)));

            Assert.Equal(frozen.GetCommandText(), sql);
            Assert.Equal(frozen.GetParameters(), legacy.GetParameters());
            Assert.Equal(sql, repeated.GetCommandText());
            Assert.Equal(legacy.GetParameters(), repeated.GetParameters());
            Assert.Equal(expectedProjection, actualProjection);
            Assert.Equal(plan.Projection.OrderBy(column => column.Ordinal).Select(column => column.ResultAlias), actualProjection.Select(ProjectionAlias));
            Assert.DoesNotContain(actualProjection, column => String.Equals(column, "1", StringComparison.Ordinal));
            Assert.Equal(columns, definition.Joins.SelectMany(table => table.Columns));
            Assert.Equal(tables, definition.Joins.Select(table => table.TableName + ":" + table.Alias + ":" + table.Prefix));
            Assert.Equal(relationCount, definition.Relations.Count);
        }

        [Fact]
        public void MutableProviderExecutionOverloadsAreObsoleteAndNoProviderCachesJoinDefinitions() {
            Type[] providers = {
                typeof(MySqlQueryGenerator),
                typeof(SqliteQueryGenerator),
                typeof(PgSQLQueryGenerator)
            };

            MethodInfo contract = LegacyJoinMethod(typeof(IQueryGenerator));
            AssertObsoleteFreezeMessage(contract);
            foreach (Type provider in providers) {
                AssertObsoleteFreezeMessage(LegacyJoinMethod(provider));
                Assert.DoesNotContain(provider.GetFields(BindingFlags.Static | BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic), field =>
                    field.FieldType.IsGenericType
                    && field.FieldType.GetGenericArguments().Contains(typeof(JoinDefinition)));
            }
        }

        [Theory]
        [MemberData(nameof(Generators))]
        public void FrozenPlanProjectionAndTopologyFollowOnlyPublishedMetadata(string providerName, IQueryGenerator generator) {
            Assert.NotEmpty(providerName);
            DefinitiveJoinPlan plan = FullGraphPlan();
            string sql = generator.GenerateJoinQuery(plan, null).GetCommandText();
            string selectList = ExtractInnerSelectList(sql);
            string[] actualProjection = SplitSelectList(selectList);
            string[] expectedProjection = plan.Projection
                .OrderBy(column => column.Ordinal)
                .Select(column => plan.Tables[column.TableIndex].Prefix + "." + column.SourceColumn + " AS " + column.ResultAlias)
                .ToArray();

            Assert.Equal(expectedProjection, actualProjection);
            Assert.Equal(plan.Projection.Length, actualProjection.Length);
            foreach (DefinitiveProjectionColumn column in plan.Projection) {
                Assert.Equal(1, CountAliasDeclaration(selectList, column.ResultAlias));
            }
            Assert.DoesNotContain(actualProjection, fragment => Regex.IsMatch(fragment, @"\bRID\b", RegexOptions.IgnoreCase));

            DefinitiveJoinTable root = plan.Tables[plan.RootTableIndex];
            Assert.Matches(@"FROM \(SELECT \* FROM " + Regex.Escape(root.TableName) + @"\s*\) AS " + Regex.Escape(root.Prefix), Normalize(sql));
            for (int i = 0; i < plan.Tables.Length; i++) {
                if (i == plan.RootTableIndex) {
                    continue;
                }
                DefinitiveJoinTable table = plan.Tables[i];
                Assert.Contains("LEFT JOIN " + table.TableName + " AS " + table.Prefix + " ON " + table.JoinPredicate, Normalize(sql));
            }

            DefinitiveJoinTable listTable = plan.Tables.Single(x => x.EntityType == typeof(ListAggregate));
            Assert.Equal(JoinType.RIGHT, listTable.JoinKind);
            Assert.Contains("LEFT JOIN " + listTable.TableName + " AS " + listTable.Prefix + " ON " + listTable.JoinPredicate, Normalize(sql));
            Assert.DoesNotContain("RIGHT JOIN", Normalize(sql), StringComparison.OrdinalIgnoreCase);
        }

        [Theory]
        [MemberData(nameof(Generators))]
        public void FrozenPlanUsesDeclaredNonzeroRootAndJoinsEveryOtherTableInPlanOrder(string providerName, IQueryGenerator generator) {
            Assert.NotEmpty(providerName);
            DefinitiveJoinPlan plan = NonzeroRootPlan();
            string sql = Normalize(generator.GenerateJoinQuery(plan, null).GetCommandText());
            DefinitiveJoinTable root = plan.Tables[plan.RootTableIndex];
            string[] expectedProjection = plan.Projection.Select(column => plan.Tables[column.TableIndex].Prefix + "." + column.SourceColumn + " AS " + column.ResultAlias).ToArray();

            Assert.Matches(@"FROM \(SELECT \* FROM " + Regex.Escape(root.TableName) + @"\s*\) AS " + Regex.Escape(root.Prefix), sql);
            Assert.Equal(expectedProjection, SplitSelectList(ExtractInnerSelectList(sql)));
            int previousJoinIndex = -1;
            for (int i = 0; i < plan.Tables.Length; i++) {
                if (i == plan.RootTableIndex) {
                    continue;
                }
                DefinitiveJoinTable table = plan.Tables[i];
                string expectedJoin = "LEFT JOIN " + table.TableName + " AS " + table.Prefix + " ON " + table.JoinPredicate;
                int joinIndex = sql.IndexOf(expectedJoin, StringComparison.Ordinal);
                Assert.True(joinIndex > previousJoinIndex, "Expected each non-root table to be joined exactly once in plan-table order.");
                Assert.Equal(1, CountOccurrence(sql, expectedJoin));
                previousJoinIndex = joinIndex;
            }
            AssertFinalOrdering(sql, "sub." + plan.RootOrdering.ResultAlias + " ASC");
        }

        [Theory]
        [MemberData(nameof(Generators))]
        public void FrozenPlanAlwaysOrdersByRootIdentifierAndUsesItAsTieBreaker(string providerName, IQueryGenerator generator) {
            Assert.NotEmpty(providerName);
            DefinitiveJoinPlan plan = FullGraphPlan();
            string rootOrdering = plan.RootOrdering.ResultAlias;

            string defaultOrder = Normalize(generator.GenerateJoinQuery(plan, null).GetCommandText());
            AssertFinalOrdering(defaultOrder, "sub." + rootOrdering + " ASC");

            MemberInfo scalarMember = typeof(GuidRoot).GetProperty(nameof(GuidRoot.ScalarAggregateId))!;
            DefinitiveProjectionColumn scalarProjection = plan.Projection.Single(column => column.TableIndex == plan.RootTableIndex && Equals(column.DestinationMember, scalarMember));
            string userOrder = Normalize(generator.GenerateJoinQuery(plan, null, orderingMember: scalarMember, otype: OrderingType.Desc).GetCommandText());
            AssertFinalOrdering(userOrder, "sub." + scalarProjection.ResultAlias + " DESC, sub." + rootOrdering + " DESC");

            DefinitiveProjectionColumn rootIdProjection = plan.Projection.Single(column => column.TableIndex == plan.RootTableIndex && column.SourceColumn == "Id");
            MemberInfo idMember = rootIdProjection.DestinationMember.DeclaringType!.GetProperty(rootIdProjection.DestinationMember.Name)!;
            Assert.NotEqual(typeof(GuidRoot), idMember.DeclaringType);
            Assert.Equal(idMember, rootIdProjection.DestinationMember);
            string idOrder = Normalize(generator.GenerateJoinQuery(plan, null, orderingMember: idMember).GetCommandText());
            AssertFinalOrdering(idOrder, "sub." + rootOrdering + " ASC");
            Assert.Single(Regex.Matches(ExtractFinalOrderList(idOrder), Regex.Escape("sub." + rootOrdering)).Cast<Match>());
        }

        [Theory]
        [MemberData(nameof(Generators))]
        public void FrozenPlanPagingFollowsDeterministicOrdering(string providerName, IQueryGenerator generator) {
            Assert.NotEmpty(providerName);
            DefinitiveJoinPlan plan = FullGraphPlan();
            string rootOrdering = "ORDER BY sub." + plan.RootOrdering.ResultAlias;

            string skipped = Normalize(generator.GenerateJoinQuery(plan, null, skip: 3, take: 7).GetCommandText());
            string limited = Normalize(generator.GenerateJoinQuery(plan, null, take: 7).GetCommandText());

            AssertPaging(providerName, skipped, 3, 7);
            AssertPaging(providerName, limited, null, 7);
            Assert.True(skipped.IndexOf(") AS sub", StringComparison.Ordinal) < skipped.IndexOf(rootOrdering, StringComparison.Ordinal));
            Assert.True(limited.IndexOf(") AS sub", StringComparison.Ordinal) < limited.IndexOf(rootOrdering, StringComparison.Ordinal));
            Assert.True(skipped.IndexOf(rootOrdering, StringComparison.Ordinal) < PagingIndex(skipped));
            Assert.True(limited.IndexOf(rootOrdering, StringComparison.Ordinal) < PagingIndex(limited));
        }

        [Theory]
        [MemberData(nameof(Generators))]
        public void FrozenPlanSkipOnlyUsesProviderPagingAfterFinalOrdering(string providerName, IQueryGenerator generator) {
            Assert.NotEmpty(providerName);
            DefinitiveJoinPlan plan = FullGraphPlan();
            string sql = Normalize(generator.GenerateJoinQuery(plan, null, skip: 3).GetCommandText());

            AssertFinalOrdering(sql, "sub." + plan.RootOrdering.ResultAlias + " ASC");
            if (providerName == "PostgreSQL") {
                Assert.Contains("LIMIT " + Int32.MaxValue + " OFFSET 3", sql);
            } else {
                Assert.Contains("LIMIT 3, " + Int32.MaxValue, sql);
            }
            Assert.True(sql.IndexOf("ORDER BY sub." + plan.RootOrdering.ResultAlias, StringComparison.Ordinal) < PagingIndex(sql));
        }

        [Theory]
        [MemberData(nameof(Generators))]
        public void FrozenPlanPlacesParserRootConditionsBeforeJoinsAndPreservesConditionBuilders(string providerName, IQueryGenerator generator) {
            Assert.NotEmpty(providerName);
            DefinitiveJoinPlan plan = FullGraphPlan();
            DefinitiveJoinTable root = plan.Tables[plan.RootTableIndex];
            DefinitiveJoinTable joined = plan.Tables.First(table => table != root);
            var conditions = new QueryBuilder();
            conditions.Append(joined.Prefix + ".Name = @condition", "condition value");
            Guid knownGuid = Guid.Parse("11111111-1111-1111-1111-111111111111");
            QueryBuilder rootConditions = new ConditionParser(plan).ParseExpression<GuidRoot>(x => x.Id != knownGuid, fullConditions: false);
            string conditionText = conditions.GetCommandText();
            string rootConditionText = rootConditions.GetCommandText();
            KeyValuePair<string, object>[] conditionParameters = conditions.GetParameters().ToArray();
            KeyValuePair<string, object>[] rootParameters = rootConditions.GetParameters().ToArray();

            Assert.DoesNotContain(root.Prefix + ".", Normalize(rootConditionText), StringComparison.OrdinalIgnoreCase);
            Assert.Contains("Id", rootConditionText, StringComparison.Ordinal);

            IQueryBuilder result = generator.GenerateJoinQuery(plan, conditions, rootConditions: rootConditions);
            string sql = Normalize(result.GetCommandText());

            Assert.Equal(1, CountOccurrence(sql, Normalize(conditionText)));
            Assert.Equal(1, CountOccurrence(sql, Normalize(rootConditionText)));
            string rootSourcePattern = @"FROM \(SELECT \* FROM " + Regex.Escape(root.TableName)
                + @"\s+WHERE\s+" + Regex.Escape(Normalize(rootConditionText)).Replace(@"\ ", @"\s+")
                + @"\s*\) AS " + Regex.Escape(root.Prefix);
            Match rootSource = Regex.Match(sql, rootSourcePattern, RegexOptions.IgnoreCase);

            Assert.True(rootSource.Success, "Expected the parser-stripped root condition inside the root-only derived source.");
            Assert.True(rootSource.Index < sql.IndexOf("LEFT JOIN", StringComparison.Ordinal));
            Assert.True(sql.IndexOf(Normalize(conditionText), StringComparison.Ordinal) > sql.LastIndexOf("LEFT JOIN", StringComparison.Ordinal));
            Assert.Equal(conditionParameters.Concat(rootParameters).OrderBy(x => x.Key), result.GetParameters().OrderBy(x => x.Key));
            Assert.Equal(conditionText, conditions.GetCommandText());
            Assert.Equal(rootConditionText, rootConditions.GetCommandText());
            Assert.Equal(conditionParameters, conditions.GetParameters().ToArray());
            Assert.Equal(rootParameters, rootConditions.GetParameters().ToArray());
        }

        [Theory]
        [MemberData(nameof(Generators))]
        public void FrozenPlanRebindsCollidingRootAndJoinedConditionParametersWithoutMutatingSources(string providerName, IQueryGenerator generator) {
            Assert.NotEmpty(providerName);
            DefinitiveJoinPlan plan = FullGraphPlan();
            DefinitiveJoinTable root = plan.Tables[plan.RootTableIndex];
            DefinitiveJoinTable joined = plan.Tables.First(table => table != root);
            Guid rootValue = Guid.Parse("11111111-1111-1111-1111-111111111111");
            var rootConditions = new ConditionParser(plan).ParseExpression<GuidRoot>(x => x.Id != rootValue, fullConditions: false);
            var conditions = new QueryBuilder();
            conditions.Append(joined.Prefix + ".Name = @_p0 AND " + joined.Prefix + ".Id = @p AND " + joined.Prefix + ".ParentId = @p1", "joined value", "p value", "p1 value");
            string rootText = rootConditions.GetCommandText();
            string joinedText = conditions.GetCommandText();
            KeyValuePair<string, object>[] rootParameters = rootConditions.GetParameters().ToArray();
            KeyValuePair<string, object>[] joinedParameters = conditions.GetParameters().ToArray();

            Assert.Contains("_p0", rootParameters.Select(parameter => parameter.Key));
            Assert.Contains("_p0", joinedParameters.Select(parameter => parameter.Key));

            IQueryBuilder result = generator.GenerateJoinQuery(plan, conditions, rootConditions: rootConditions);
            string sql = Normalize(result.GetCommandText());
            KeyValuePair<string, object>[] resultParameters = result.GetParameters().OrderBy(parameter => parameter.Key).ToArray();

            Assert.Equal(rootParameters.Length + joinedParameters.Length, resultParameters.Length);
            Assert.Equal(new[] { "_p0", "join__p0_0", "p", "p1" }, resultParameters.Select(parameter => parameter.Key));
            Assert.Contains("@_p0", rootText);
            Assert.Contains("@_p0", joinedText);
            Assert.Contains("@_p0", sql);
            Assert.Contains("@join__p0_0", sql);
            Assert.Equal(new object[] { rootValue, "joined value", "p value", "p1 value" }.OrderBy(value => value.ToString()), resultParameters.Select(parameter => parameter.Value).OrderBy(value => value.ToString()));
            Assert.Equal(1, resultParameters.Count(parameter => Equals(parameter.Value, rootValue)));
            Assert.Equal(1, resultParameters.Count(parameter => Equals(parameter.Value, "joined value")));
            Assert.Equal(1, resultParameters.Count(parameter => Equals(parameter.Value, "p value")));
            Assert.Equal(1, resultParameters.Count(parameter => Equals(parameter.Value, "p1 value")));
            Assert.Equal(rootText, rootConditions.GetCommandText());
            Assert.Equal(joinedText, conditions.GetCommandText());
            Assert.Equal(rootParameters, rootConditions.GetParameters().ToArray());
            Assert.Equal(joinedParameters, conditions.GetParameters().ToArray());
            foreach (string name in resultParameters.Select(parameter => parameter.Key)) {
                Assert.NotEmpty(Regex.Matches(sql, "@" + Regex.Escape(name) + @"(?![A-Za-z0-9_])"));
            }
            Assert.Equal(generator.GenerateJoinQuery(plan, conditions, rootConditions: rootConditions).GetCommandText(), result.GetCommandText());
        }

        [Theory]
        [MemberData(nameof(Generators))]
        public void FrozenPlanRebindingSkipsReservedCandidatesAndPreservesPrefixRelatedParameterTokens(string providerName, IQueryGenerator generator) {
            Assert.NotEmpty(providerName);
            DefinitiveJoinPlan plan = FullGraphPlan();
            DefinitiveJoinTable root = plan.Tables[plan.RootTableIndex];
            DefinitiveJoinTable joined = plan.Tables.First(table => table != root);
            var rootConditions = new QueryBuilder();
            rootConditions.Append("Id = @p AND ScalarAggregateId = @p1", "root p value", "root p1 value");
            var conditions = new QueryBuilder();
            conditions.Append(joined.Prefix + ".Name = @p AND " + joined.Prefix + ".Id = @p1 AND " + joined.Prefix + ".Name = @join_p_0", "joined p value", "joined p1 value", "reserved candidate value");
            string rootText = rootConditions.GetCommandText();
            string joinedText = conditions.GetCommandText();
            KeyValuePair<string, object>[] rootParameters = rootConditions.GetParameters().ToArray();
            KeyValuePair<string, object>[] joinedParameters = conditions.GetParameters().ToArray();

            IQueryBuilder result = generator.GenerateJoinQuery(plan, conditions, rootConditions: rootConditions);
            string sql = Normalize(result.GetCommandText());
            KeyValuePair<string, object>[] resultParameters = result.GetParameters().ToArray();
            var expectedParameters = new[] {
                new KeyValuePair<string, object>("p", "root p value"),
                new KeyValuePair<string, object>("p1", "root p1 value"),
                new KeyValuePair<string, object>("join_p_1", "joined p value"),
                new KeyValuePair<string, object>("join_p1_0", "joined p1 value"),
                new KeyValuePair<string, object>("join_p_0", "reserved candidate value")
            };
            string expectedJoinedText = joined.Prefix + ".Name = @join_p_1 AND " + joined.Prefix + ".Id = @join_p1_0 AND " + joined.Prefix + ".Name = @join_p_0";

            Assert.Equal(rootParameters.Length + joinedParameters.Length, resultParameters.Length);
            Assert.Equal(expectedParameters, resultParameters);
            Assert.Equal(1, CountOccurrence(sql, Normalize(rootText)));
            Assert.Equal(1, CountOccurrence(sql, Normalize(expectedJoinedText)));
            foreach (KeyValuePair<string, object> parameter in expectedParameters) {
                Assert.Equal(parameter.Value, result.GetParameters()[parameter.Key]);
                Assert.Equal(1, CountParameterToken(sql, parameter.Key));
                Assert.Equal(1, resultParameters.Count(resultParameter => Equals(resultParameter.Value, parameter.Value)));
            }
            Assert.Equal(rootText, rootConditions.GetCommandText());
            Assert.Equal(joinedText, conditions.GetCommandText());
            Assert.Equal(rootParameters, rootConditions.GetParameters().ToArray());
            Assert.Equal(joinedParameters, conditions.GetParameters().ToArray());

            IQueryBuilder repeated = generator.GenerateJoinQuery(plan, conditions, rootConditions: rootConditions);
            Assert.Equal(result.GetCommandText(), repeated.GetCommandText());
            Assert.Equal(resultParameters, repeated.GetParameters().ToArray());
            Assert.Equal(rootText, rootConditions.GetCommandText());
            Assert.Equal(joinedText, conditions.GetCommandText());
            Assert.Equal(rootParameters, rootConditions.GetParameters().ToArray());
            Assert.Equal(joinedParameters, conditions.GetParameters().ToArray());
        }

        [Theory]
        [MemberData(nameof(Generators))]
        public void FrozenPlanRebindsCaseInsensitiveCrossBuilderParameterCollisionsWithoutMutatingSources(string providerName, IQueryGenerator generator) {
            Assert.NotEmpty(providerName);
            DefinitiveJoinPlan plan = FullGraphPlan();
            DefinitiveJoinTable joined = plan.Tables.First(table => table != plan.Tables[plan.RootTableIndex]);
            var rootConditions = new QueryBuilder();
            rootConditions.Append("Id = @p", "root value");
            var conditions = new QueryBuilder();
            conditions.Append(joined.Prefix + ".Name = @P", "joined value");
            string rootText = rootConditions.GetCommandText();
            string joinedText = conditions.GetCommandText();
            KeyValuePair<string, object>[] rootParameters = rootConditions.GetParameters().ToArray();
            KeyValuePair<string, object>[] joinedParameters = conditions.GetParameters().ToArray();

            IQueryBuilder result = generator.GenerateJoinQuery(plan, conditions, rootConditions: rootConditions);
            string sql = Normalize(result.GetCommandText());
            KeyValuePair<string, object>[] resultParameters = result.GetParameters().ToArray();

            Assert.Equal(resultParameters.Length, resultParameters.Select(parameter => parameter.Key).Distinct(StringComparer.OrdinalIgnoreCase).Count());
            Assert.Equal(new[] { "p", "join_P_0" }, resultParameters.Select(parameter => parameter.Key));
            Assert.Equal(1, resultParameters.Count(parameter => Equals(parameter.Value, "root value")));
            Assert.Equal(1, resultParameters.Count(parameter => Equals(parameter.Value, "joined value")));
            Assert.Equal(1, CountParameterToken(sql, "p"));
            Assert.Equal(1, CountParameterToken(sql, "join_P_0"));
            Assert.DoesNotContain("@P", sql, StringComparison.Ordinal);
            Assert.Equal(rootText, rootConditions.GetCommandText());
            Assert.Equal(joinedText, conditions.GetCommandText());
            Assert.Equal(rootParameters, rootConditions.GetParameters().ToArray());
            Assert.Equal(joinedParameters, conditions.GetParameters().ToArray());

            IQueryBuilder repeated = generator.GenerateJoinQuery(plan, conditions, rootConditions: rootConditions);
            Assert.Equal(result.GetCommandText(), repeated.GetCommandText());
            Assert.Equal(resultParameters, repeated.GetParameters().ToArray());

            if (providerName == "PostgreSQL") {
                using var command = new NpgsqlCommand();
                result.ApplyToCommand(command);
                Assert.Equal(new[] { "p", "join_P_0" }, command.Parameters.Cast<NpgsqlParameter>().Select(parameter => parameter.ParameterName));
                Assert.Equal(new object[] { "root value", "joined value" }, command.Parameters.Cast<NpgsqlParameter>().Select(parameter => parameter.Value));
            }
        }

        [Theory]
        [MemberData(nameof(Generators))]
        public void FrozenPlanRebindsCaseInsensitiveSingleSourceParameterCollisionsWithoutMutatingSources(string providerName, IQueryGenerator generator) {
            Assert.NotEmpty(providerName);
            DefinitiveJoinPlan plan = FullGraphPlan();
            var rootConditions = new QueryBuilder();
            rootConditions.Append("Id = @p AND ScalarAggregateId = @P", "lower-case value", "upper-case value");
            string rootText = rootConditions.GetCommandText();
            KeyValuePair<string, object>[] rootParameters = rootConditions.GetParameters().ToArray();

            IQueryBuilder result = generator.GenerateJoinQuery(plan, null, rootConditions: rootConditions);
            string sql = Normalize(result.GetCommandText());
            KeyValuePair<string, object>[] resultParameters = result.GetParameters().ToArray();

            Assert.Equal(resultParameters.Length, resultParameters.Select(parameter => parameter.Key).Distinct(StringComparer.OrdinalIgnoreCase).Count());
            Assert.Equal(new[] { "p", "root_P_0" }, resultParameters.Select(parameter => parameter.Key));
            Assert.Equal(1, resultParameters.Count(parameter => Equals(parameter.Value, "lower-case value")));
            Assert.Equal(1, resultParameters.Count(parameter => Equals(parameter.Value, "upper-case value")));
            Assert.Equal(1, CountParameterToken(sql, "p"));
            Assert.Equal(1, CountParameterToken(sql, "root_P_0"));
            Assert.DoesNotContain("@P", sql, StringComparison.Ordinal);
            Assert.Equal(rootText, rootConditions.GetCommandText());
            Assert.Equal(rootParameters, rootConditions.GetParameters().ToArray());

            IQueryBuilder repeated = generator.GenerateJoinQuery(plan, null, rootConditions: rootConditions);
            Assert.Equal(result.GetCommandText(), repeated.GetCommandText());
            Assert.Equal(resultParameters, repeated.GetParameters().ToArray());
        }

        [Theory]
        [MemberData(nameof(Generators))]
        public void AutomaticScalarPlanProjectsOnlyPublishedScalarTopologyAndOrdersByTypedRootIdentifier(string providerName, IQueryGenerator generator) {
            Assert.NotEmpty(providerName);
            DefinitiveJoinPlan plan = AutomaticJoinPlanCache.GetOrAdd(typeof(GuidRoot), AggregateJoinShape.ScalarAggregatesOnly);
            DefinitiveJoinPlan cached = AutomaticJoinPlanCache.GetOrAdd(typeof(GuidRoot), AggregateJoinShape.ScalarAggregatesOnly);
            DefinitiveJoinPlan compiled = DefinitiveJoinPlanCompiler.Compile(typeof(GuidRoot), AggregateJoinShape.ScalarAggregatesOnly);
            DefinitiveJoinPlan fullGraph = AutomaticJoinPlanCache.GetOrAdd(typeof(GuidRoot), AggregateJoinShape.FullGraph);
            DefinitiveJoinTable root = plan.Tables[plan.RootTableIndex];
            string[] expectedProjection = plan.Projection
                .OrderBy(column => column.Ordinal)
                .Select(column => plan.Tables[column.TableIndex].Prefix + "." + column.SourceColumn + " AS " + column.ResultAlias)
                .ToArray();
            string first = Normalize(generator.GenerateJoinQuery(plan, null).GetCommandText());
            string second = Normalize(generator.GenerateJoinQuery(plan, null).GetCommandText());
            string selectList = ExtractInnerSelectList(first);
            string[] actualProjection = SplitSelectList(selectList).Where(column => column != "1").ToArray();
            DefinitiveJoinTable[] fullOnlyTables = fullGraph.Tables
                .Where(fullTable => plan.Tables.All(scalarTable => scalarTable.EntityType != fullTable.EntityType))
                .ToArray();
            KeyValuePair<AggregatePath, string>[] fullOnlyAliases = fullGraph.AliasByPath
                .Where(pair => !plan.AliasByPath.ContainsKey(pair.Key))
                .ToArray();

            Assert.Same(plan, cached);
            Assert.Equal(compiled.StructuralSignature, plan.StructuralSignature);
            Assert.Equal(AggregateJoinShape.ScalarAggregatesOnly, plan.Shape);
            Assert.Equal(new[] { typeof(GuidRoot), typeof(ScalarAggregate), typeof(IntermediateAggregate), typeof(FarAggregate) }, plan.Tables.Select(table => table.EntityType));
            Assert.Equal(expectedProjection, actualProjection);
            Assert.Equal(plan.Projection.Length, actualProjection.Length);
            Assert.Equal(Enumerable.Range(0, plan.Projection.Length), plan.Projection.Select(column => column.Ordinal));
            Assert.Equal(plan.Projection.OrderBy(column => column.Ordinal).Select(column => column.ResultAlias), actualProjection.Select(ProjectionAlias));
            foreach (DefinitiveProjectionColumn column in plan.Projection) {
                Assert.Equal(1, CountAliasDeclaration(selectList, column.ResultAlias));
            }

            Assert.Matches(@"FROM \(SELECT \* FROM " + Regex.Escape(root.TableName) + @"\s*\) AS " + Regex.Escape(root.Prefix), first);
            int previousJoinIndex = -1;
            for (int i = 0; i < plan.Tables.Length; i++) {
                if (i == plan.RootTableIndex) {
                    continue;
                }
                DefinitiveJoinTable table = plan.Tables[i];
                string expectedJoin = "LEFT JOIN " + table.TableName + " AS " + table.Prefix + " ON " + table.JoinPredicate;
                int joinIndex = first.IndexOf(expectedJoin, StringComparison.Ordinal);
                Assert.True(joinIndex > previousJoinIndex, "Expected each published scalar-plan table to be joined exactly once in plan-table order.");
                Assert.Equal(1, CountOccurrence(first, expectedJoin));
                previousJoinIndex = joinIndex;
            }
            Assert.Equal(plan.Tables.Length - 1, Regex.Matches(first, @"\bLEFT\s+JOIN\b", RegexOptions.IgnoreCase).Count);
            Assert.Equal(new[] { typeof(ObjectAggregate), typeof(ListAggregate) }, fullOnlyTables.Select(table => table.EntityType));
            foreach (DefinitiveJoinTable table in fullOnlyTables) {
                Assert.DoesNotContain("LEFT JOIN " + table.TableName + " AS " + table.Prefix, first, StringComparison.OrdinalIgnoreCase);
                Assert.DoesNotContain(table.Prefix + ".", first, StringComparison.OrdinalIgnoreCase);
                Assert.DoesNotContain(" AS " + table.Prefix, first, StringComparison.OrdinalIgnoreCase);
            }
            foreach (KeyValuePair<AggregatePath, string> alias in fullOnlyAliases) {
                Assert.DoesNotContain(alias.Value + ".", first, StringComparison.OrdinalIgnoreCase);
                Assert.DoesNotContain(" AS " + alias.Value, first, StringComparison.OrdinalIgnoreCase);
            }

            Assert.Equal(typeof(Guid), root.Identifier.ClrType);
            Assert.Equal(nameof(GuidRoot.Id), root.Identifier.ColumnName);
            Assert.Equal(root.Identifier.ResultAlias, plan.RootOrdering.ResultAlias);
            AssertFinalOrdering(first, "sub." + plan.RootOrdering.ResultAlias + " ASC");
            Assert.DoesNotContain("RID", first, StringComparison.OrdinalIgnoreCase);
            Assert.Equal(first, second);
        }

        [Theory]
        [MemberData(nameof(Generators))]
        public void FrozenPlanGenerationIsDeterministicAndDoesNotChangeFrozenMetadata(string providerName, IQueryGenerator generator) {
            Assert.NotEmpty(providerName);
            DefinitiveJoinPlan plan = FullGraphPlan();
            string signature = plan.StructuralSignature;
            string[] projection = plan.Projection.Select(x => x.Ordinal + ":" + x.TableIndex + ":" + x.SourceColumn + ":" + x.ResultAlias).ToArray();

            string first = generator.GenerateJoinQuery(plan, null, skip: 1, take: 2).GetCommandText();
            string second = generator.GenerateJoinQuery(plan, null, skip: 1, take: 2).GetCommandText();

            Assert.Equal(first, second);
            Assert.Equal(signature, plan.StructuralSignature);
            Assert.Equal(projection, plan.Projection.Select(x => x.Ordinal + ":" + x.TableIndex + ":" + x.SourceColumn + ":" + x.ResultAlias));
        }

        [Theory]
        [MemberData(nameof(Generators))]
        public void FrozenPlanRejectsNullPlanAndUndefinedOrderingTypeActionably(string providerName, IQueryGenerator generator) {
            Assert.NotEmpty(providerName);
            ArgumentNullException nullPlan = Assert.Throws<ArgumentNullException>(() => generator.GenerateJoinQuery((DefinitiveJoinPlan)null!, null));
            ArgumentOutOfRangeException invalidOrder = Assert.Throws<ArgumentOutOfRangeException>(() => generator.GenerateJoinQuery(FullGraphPlan(), null, otype: (OrderingType)123));

            Assert.Equal("plan", nullPlan.ParamName);
            Assert.Contains("ordering", invalidOrder.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Theory]
        [MemberData(nameof(Generators))]
        public void FrozenPlanRejectsNegativePagingValuesActionably(string providerName, IQueryGenerator generator) {
            Assert.NotEmpty(providerName);
            DefinitiveJoinPlan plan = FullGraphPlan();
            ArgumentOutOfRangeException invalidSkip = Assert.Throws<ArgumentOutOfRangeException>(() => generator.GenerateJoinQuery(plan, null, skip: -1));
            ArgumentOutOfRangeException invalidTake = Assert.Throws<ArgumentOutOfRangeException>(() => generator.GenerateJoinQuery(plan, null, take: -1));

            Assert.Equal("skip", invalidSkip.ParamName);
            Assert.Contains("non-negative", invalidSkip.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Equal("take", invalidTake.ParamName);
            Assert.Contains("non-negative", invalidTake.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Null(Record.Exception(() => generator.GenerateJoinQuery(plan, null, skip: 0)));
            Assert.Null(Record.Exception(() => generator.GenerateJoinQuery(plan, null, take: 0)));
        }

        [Theory]
        [MemberData(nameof(Generators))]
        public void FrozenGuidIdentifierUsesIdRatherThanLegacyRid(string providerName, IQueryGenerator generator) {
            Assert.NotEmpty(providerName);
            DefinitiveJoinPlan plan = FullGraphPlan();
            string sql = Normalize(generator.GenerateJoinQuery(plan, null).GetCommandText());
            string rootPrefix = plan.Tables[plan.RootTableIndex].Prefix;

            Assert.Contains(rootPrefix + ".Id AS " + plan.RootOrdering.ResultAlias, ExtractInnerSelectList(sql));
            AssertFinalOrdering(sql, "sub." + plan.RootOrdering.ResultAlias + " ASC");
            Assert.DoesNotContain(rootPrefix + ".RID", sql, StringComparison.OrdinalIgnoreCase);
        }

        [Theory]
        [MemberData(nameof(Generators))]
        public void FrozenPlanRejectsShadowedBaseOrderingMember(string providerName, IQueryGenerator generator) {
            Assert.NotEmpty(providerName);
            DefinitiveJoinPlan plan = DefinitiveJoinPlanCompiler.Compile(typeof(HiddenEffectiveAggregateRoot), AggregateJoinShape.FullGraph);
            MemberInfo derivedMember = typeof(HiddenEffectiveAggregateRoot).GetProperty(nameof(HiddenEffectiveAggregateRoot.ShadowedColumn))!;
            MemberInfo baseMember = typeof(HiddenEffectiveAggregateRootBase).GetProperty(nameof(HiddenEffectiveAggregateRootBase.ShadowedColumn))!;
            DefinitiveProjectionColumn frozenProjection = plan.Projection.Single(column => column.TableIndex == plan.RootTableIndex && column.SourceColumn == nameof(HiddenEffectiveAggregateRoot.ShadowedColumn));

            Assert.Equal(derivedMember, frozenProjection.DestinationMember);
            ArgumentException exception = Assert.Throws<ArgumentException>(() => generator.GenerateJoinQuery(plan, null, orderingMember: baseMember));

            Assert.Equal("orderingMember", exception.ParamName);
            Assert.Contains("frozen root", exception.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Theory]
        [MemberData(nameof(Generators))]
        public void FrozenPlanRejectsForeignOrderingMemberWithSameName(string providerName, IQueryGenerator generator) {
            Assert.NotEmpty(providerName);
            MemberInfo foreignScalarMember = typeof(ForeignOrderingRoot).GetProperty(nameof(ForeignOrderingRoot.ScalarAggregateId))!;

            ArgumentException exception = Assert.Throws<ArgumentException>(() => generator.GenerateJoinQuery(FullGraphPlan(), null, orderingMember: foreignScalarMember));

            Assert.Equal("orderingMember", exception.ParamName);
            Assert.Contains("frozen root", exception.Message, StringComparison.OrdinalIgnoreCase);
        }

        private static DefinitiveJoinPlan NonzeroRootPlan() {
            MemberInfo joinedZeroId = typeof(ScalarAggregate).GetProperty(nameof(ScalarAggregate.Id))!;
            MemberInfo rootId = typeof(GuidRoot).GetProperty(nameof(GuidRoot.Id))!;
            MemberInfo joinedTwoId = typeof(ObjectAggregate).GetProperty(nameof(ObjectAggregate.Id))!;
            var tables = new[] {
                new DefinitiveJoinTable(typeof(ScalarAggregate), "JoinedZeroTable", "joinedZero", "j0", JoinType.LEFT, "root.Id = j0.ParentId", new DefinitiveIdentifier(joinedZeroId, "Id", 0, "j0_Id"), new[] { "Id" }),
                new DefinitiveJoinTable(typeof(GuidRoot), "RootAtOneTable", "root", "root", JoinType.LEFT, null, new DefinitiveIdentifier(rootId, "Id", 1, "root_Id"), new[] { "Id" }),
                new DefinitiveJoinTable(typeof(ObjectAggregate), "JoinedTwoTable", "joinedTwo", "j2", JoinType.LEFT, "root.Id = j2.ParentId", new DefinitiveIdentifier(joinedTwoId, "Id", 2, "j2_Id"), new[] { "Id" })
            };
            var projection = new[] {
                new DefinitiveProjectionColumn(0, 0, "Id", "j0_Id", joinedZeroId),
                new DefinitiveProjectionColumn(1, 1, "Id", "root_Id", rootId),
                new DefinitiveProjectionColumn(2, 2, "Id", "j2_Id", joinedTwoId)
            };
            return new DefinitiveJoinPlan(
                typeof(GuidRoot),
                AggregateJoinShape.FullGraph,
                1,
                tables,
                Array.Empty<DefinitiveJoinRelation>(),
                projection,
                Array.Empty<KeyValuePair<AggregatePath, string>>(),
                new[] {
                    new KeyValuePair<string, int>("joinedZero", 0),
                    new KeyValuePair<string, int>("root", 1),
                    new KeyValuePair<string, int>("joinedTwo", 2)
                },
                new RootOrderingRequirement(1, "Id", 1, "root_Id"),
                1);
        }

        private static JoinDefinition CreateLegacyGuidRootDefinition() {
            return new JoinDefinition {
                Joins = new List<JoiningTable> {
                    new JoiningTable {
                        ValueObject = typeof(GuidRoot),
                        TableName = typeof(GuidRoot).Name,
                        Alias = "root",
                        Prefix = "root",
                        Type = JoinType.LEFT,
                        Columns = new List<string> { nameof(GuidRoot.ScalarAggregateId) }
                    }
                }
            };
        }

        private static DefinitiveJoinPlan FullGraphPlan() {
            return DefinitiveJoinPlanCompiler.Compile(typeof(GuidRoot), AggregateJoinShape.FullGraph);
        }

        private static MethodInfo LegacyJoinMethod(Type type) {
            return type.GetMethods().Single(method =>
                method.Name == nameof(IQueryGenerator.GenerateJoinQuery)
                && method.GetParameters().First().ParameterType == typeof(JoinDefinition));
        }

        private static void AssertObsoleteFreezeMessage(MethodInfo method) {
            ObsoleteAttribute obsolete = method.GetCustomAttribute<ObsoleteAttribute>()!;
            Assert.NotNull(obsolete);
            Assert.Contains("Freeze", obsolete.Message, StringComparison.Ordinal);
            Assert.Contains(nameof(DefinitiveJoinPlan), obsolete.Message, StringComparison.Ordinal);
        }

        private static string ExtractInnerSelectList(string sql) {
            Match match = Regex.Match(sql, @"FROM\s*\(\s*SELECT\s+(?<list>.*?)\s+FROM\s+", RegexOptions.Singleline | RegexOptions.IgnoreCase);
            Assert.True(match.Success, "Expected a joined inner SELECT list.");
            return match.Groups["list"].Value.Trim();
        }

        private static string[] SplitSelectList(string selectList) {
            return selectList.Split(',').Select(Normalize).Where(x => x.Length > 0).ToArray();
        }

        private static string RemoveSqlLineComments(string sql) {
            return Regex.Replace(sql, @"^\s*--.*(?:\r?\n|$)", String.Empty, RegexOptions.Multiline);
        }

        private static int CountAliasDeclaration(string selectList, string alias) {
            return Regex.Matches(selectList, @"\bAS\s+" + Regex.Escape(alias) + @"\b", RegexOptions.IgnoreCase).Count;
        }

        private static int CountOccurrence(string source, string token) {
            return Regex.Matches(source, Regex.Escape(token), RegexOptions.IgnoreCase).Count;
        }

        private static int CountParameterToken(string sql, string parameterName) {
            return Regex.Matches(sql, "@" + Regex.Escape(parameterName) + @"(?![A-Za-z0-9_])").Count;
        }

        private static string ProjectionAlias(string projection) {
            Match match = Regex.Match(projection, @"\s+AS\s+(?<alias>[A-Za-z0-9_]+)$", RegexOptions.IgnoreCase);
            Assert.True(match.Success, "Expected every frozen projection to have a result alias.");
            return match.Groups["alias"].Value;
        }

        private static string Normalize(string value) {
            string normalized = Regex.Replace(value, @"\s+", " ").Trim();
            return Regex.Replace(normalized, @"\s*,\s*", ", ");
        }

        private static void AssertFinalOrdering(string sql, string expectedOrder) {
            int subIndex = sql.IndexOf(") AS sub", StringComparison.Ordinal);
            int orderIndex = sql.IndexOf("ORDER BY " + expectedOrder, StringComparison.Ordinal);
            Assert.True(subIndex >= 0, "Expected the derived joined source to be aliased as sub.");
            Assert.True(orderIndex > subIndex, "Expected final ordering after the derived joined source.");
            Assert.Single(Regex.Matches(sql, "ORDER BY", RegexOptions.IgnoreCase).Cast<Match>());
        }

        private static string ExtractFinalOrderList(string sql) {
            Match match = Regex.Match(sql, @"\)\s+AS\s+sub\s+ORDER BY\s+(?<list>.*?)(?:\s+LIMIT|\s+OFFSET|$)", RegexOptions.IgnoreCase);
            Assert.True(match.Success, "Expected a final ORDER BY clause after the joined subquery.");
            return match.Groups["list"].Value;
        }

        private static void AssertPaging(string providerName, string sql, int? skip, int take) {
            if (providerName == "PostgreSQL") {
                Assert.Contains("LIMIT " + take, sql);
                if (skip != null) {
                    Assert.Contains("OFFSET " + skip.Value, sql);
                }
                return;
            }

            Assert.Contains("LIMIT " + (skip != null ? skip.Value + ", " : "") + take, sql);
        }

        private static int PagingIndex(string sql) {
            int limit = sql.IndexOf("LIMIT", StringComparison.Ordinal);
            int offset = sql.IndexOf("OFFSET", StringComparison.Ordinal);
            return limit >= 0 ? limit : offset;
        }

        private sealed class ForeignOrderingRoot {
            public Guid ScalarAggregateId { get; set; }
        }
    }
}
