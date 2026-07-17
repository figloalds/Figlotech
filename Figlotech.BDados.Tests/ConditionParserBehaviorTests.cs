using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Figlotech.BDados.Builders;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.Exceptions;
using Figlotech.BDados.Helpers;
using Figlotech.Core.Helpers;
using Figlotech.Data;
using Xunit;

namespace Figlotech.BDados.Tests {
    public class ConditionParserBehaviorTests {
        [Fact]
        public void CapturedNullEqualityEmitsIsNullAndBindsNoParameter() {
            string? captured = null;
            var parser = new ConditionParser();

            var query = parser.ParseExpression<GuidRoot>(x => x.AggregateName == captured);
            string sql = query.GetCommandText();

            Assert.Contains("IS NULL", sql);
            Assert.DoesNotContain("@", sql);
            Assert.Empty(query.GetParameters());
        }

        [Fact]
        public void CapturedNullInequalityEmitsIsNotNullAndBindsNoParameter() {
            string? captured = null;
            var parser = new ConditionParser();

            var query = parser.ParseExpression<GuidRoot>(x => x.AggregateName != captured);
            string sql = query.GetCommandText();

            Assert.Contains("IS NOT NULL", sql);
            Assert.DoesNotContain("@", sql);
            Assert.Empty(query.GetParameters());
        }

        [Fact]
        public void CapturedLeftSideNullInequalityEmitsIsNotNullAndBindsNoParameter() {
            string? captured = null;
            var parser = new ConditionParser();

            var query = parser.ParseExpression<GuidRoot>(x => captured != x.AggregateName);
            string sql = query.GetCommandText();

            Assert.Contains("IS NOT NULL", sql);
            Assert.DoesNotContain("@", sql);
            Assert.Empty(query.GetParameters());
        }

        [Fact]
        public void NullableValueOverNullNormalizesToIsNullAndBindsNoParameter() {
            Guid? captured = null;
            var parser = new ConditionParser();

            var query = parser.ParseExpression<GuidRoot>(x => x.ScalarAggregateId == captured!.Value);
            string sql = query.GetCommandText();

            Assert.Contains("IS NULL", sql);
            Assert.DoesNotContain("@", sql);
            Assert.Empty(query.GetParameters());
        }

        [Fact]
        public void ThrowingCapturedGetterPropagatesWithBDadosExceptionContext() {
            var parser = new ConditionParser();

            BDadosException exception = Assert.Throws<BDadosException>(() =>
                parser.ParseExpression<GuidRoot>(x => x.AggregateName == ThrowingSource.ThrowingValue));

            Assert.Contains("Expression parsing failed", exception.ToString());
            Assert.Contains("boom", exception.InnerException?.ToString() ?? String.Empty);
        }

        [Fact]
        public void ThrowingCapturedMethodPropagatesWithBDadosExceptionContext() {
            var parser = new ConditionParser();

            BDadosException exception = Assert.Throws<BDadosException>(() =>
                parser.ParseExpression<GuidRoot>(x => x.AggregateName == ThrowingSource.ThrowingMethod()));

            Assert.Contains("Expression parsing failed", exception.ToString());
            Assert.Contains("method boom", exception.InnerException?.ToString() ?? String.Empty);
        }

        [Fact]
        public void NullLiteralOnLeftSideEmitsIsNull() {
            var parser = new ConditionParser();

            var query = parser.ParseExpression<GuidRoot>(x => null == x.AggregateName);
            string sql = query.GetCommandText();

            Assert.Contains("IS NULL", sql);
            Assert.DoesNotContain("@", sql);
            Assert.Empty(query.GetParameters());
        }

        [Theory]
        [InlineData("Contains", @"LIKE\s+CONCAT\('%',\s*@\w+,\s*'%'\)\s+ESCAPE\s+'!'")]
        [InlineData("StartsWith", @"LIKE\s+CONCAT\(@\w+,\s*'%'\)\s+ESCAPE\s+'!'")]
        [InlineData("EndsWith", @"LIKE\s+CONCAT\('%',\s*@\w+\)\s+ESCAPE\s+'!'")]
        public void StringMethodEmitsExactLikeConcatShape(string method, string expectedPattern) {
            var parser = new ConditionParser();
            string value = "needle";
            Expression<Func<GuidRoot, bool>> expression = method switch {
                "Contains" => x => x.AggregateName!.Contains(value),
                "StartsWith" => x => x.AggregateName!.StartsWith(value),
                "EndsWith" => x => x.AggregateName!.EndsWith(value),
                _ => throw new InvalidOperationException()
            };

            var query = parser.ParseExpression(expression);
            string sql = query.GetCommandText();

            Assert.Matches(expectedPattern, sql);
            Assert.Single(query.GetParameters());
            Assert.Equal(value, query.GetParameters().Single().Value);
        }

        [Fact]
        public void QueryComparisonIgnoreCaseEmitsParameterizedCaseInsensitiveExactComparison() {
            var parser = new ConditionParser();

            var query = parser.ParseExpression<QueryComparisonRoot>(x => x.IgnoreCase == "MixedCase");
            string sql = query.GetCommandText();

            Assert.Matches(@"LOWER\(\s*[^)]*\.IgnoreCase\s*\)\s*=\s*LOWER\(@\w+\)", sql);
            Assert.Single(query.GetParameters());
            Assert.Equal("MixedCase", query.GetParameters().Single().Value);
        }

        [Fact]
        public void DirectAndAttributeLikeComparisonsEscapeLiteralWildcardsAndEscapeMarker() {
            const string value = "a%b_c!";
            const string escapedValue = "a!%b!_c!!";

            var directContains = new ConditionParser().ParseExpression<GuidRoot>(x => x.AggregateName!.Contains(value));
            var directStartsWith = new ConditionParser().ParseExpression<GuidRoot>(x => x.AggregateName!.StartsWith(value));
            var directEndsWith = new ConditionParser().ParseExpression<GuidRoot>(x => x.AggregateName!.EndsWith(value));
            var attributeContaining = new ConditionParser().ParseExpression<QueryComparisonRoot>(x => x.Containing == value);
            var attributeStartingWith = new ConditionParser().ParseExpression<QueryComparisonRoot>(x => x.StartingWith == value);
            var attributeEndingWith = new ConditionParser().ParseExpression<QueryComparisonRoot>(x => x.EndingWith == value);

            AssertLiteralLikeQuery(directContains, value, escapedValue);
            AssertLiteralLikeQuery(directStartsWith, value, escapedValue);
            AssertLiteralLikeQuery(directEndsWith, value, escapedValue);
            AssertLiteralLikeQuery(attributeContaining, value, escapedValue);
            AssertLiteralLikeQuery(attributeStartingWith, value, escapedValue);
            AssertLiteralLikeQuery(attributeEndingWith, value, escapedValue);
        }

        [Theory]
        [InlineData("ExactValue", @"\.ExactValue\s*=\s*@\w+", false)]
        [InlineData("Containing", @"\.Containing\s+LIKE\s+CONCAT\('%',\s*@\w+,\s*'%'\)\s+ESCAPE\s+'!'", true)]
        [InlineData("StartingWith", @"\.StartingWith\s+LIKE\s+CONCAT\(@\w+,\s*'%'\)\s+ESCAPE\s+'!'", true)]
        [InlineData("EndingWith", @"\.EndingWith\s+LIKE\s+CONCAT\('%',\s*@\w+\)\s+ESCAPE\s+'!'", true)]
        [InlineData("IgnoreCase", @"LOWER\(\s*[^)]*\.IgnoreCase\s*\)\s*=\s*LOWER\(@\w+\)", false)]
        public void QueryComparisonModesEmitTheirDocumentedParameterizedSql(string mode, string expectedPattern, bool usesLike) {
            var parser = new ConditionParser();
            const string value = "needle";
            Expression<Func<QueryComparisonRoot, bool>> expression = mode switch {
                "ExactValue" => x => x.ExactValue == value,
                "Containing" => x => x.Containing == value,
                "StartingWith" => x => x.StartingWith == value,
                "EndingWith" => x => x.EndingWith == value,
                "IgnoreCase" => x => x.IgnoreCase == value,
                _ => throw new InvalidOperationException()
            };

            var query = parser.ParseExpression(expression);
            string sql = query.GetCommandText();

            Assert.Matches(expectedPattern, sql);
            Assert.Equal(usesLike, sql.Contains("LIKE", StringComparison.Ordinal));
            Assert.Single(query.GetParameters());
            Assert.Equal(value, query.GetParameters().Single().Value);
        }

        [Fact]
        public void CustomSameNameContainsIsRejected() {
            var parser = new ConditionParser();

            BDadosException exception = Assert.Throws<BDadosException>(() =>
                parser.ParseExpression<CustomStringLike>(x => x.Custom!.Contains("needle")));

            Assert.Contains("Contains", exception.ToString());
        }

        [Fact]
        public void CustomSameNameTrimIsRejected() {
            var parser = new ConditionParser();

            BDadosException exception = Assert.Throws<BDadosException>(() =>
                parser.ParseExpression<CustomStringLike>(x => x.Custom!.Trim() == "abc"));

            Assert.Contains("Trim", exception.ToString());
        }

        [Fact]
        public void StringContainsWithComparisonOverloadThrowsActionableUnsupportedExpression() {
            var parser = new ConditionParser();

            BDadosException exception = Assert.Throws<BDadosException>(() =>
                parser.ParseExpression<GuidRoot>(x => x.AggregateName!.Contains("needle", StringComparison.Ordinal)));

            Assert.Contains("Contains", exception.ToString());
        }

        [Fact]
        public void StringReplaceWithCultureOverloadThrowsActionableUnsupportedExpression() {
            var parser = new ConditionParser();

            BDadosException exception = Assert.Throws<BDadosException>(() =>
                parser.ParseExpression<GuidRoot>(x => x.AggregateName!.Replace("a", "b", StringComparison.Ordinal) == "abc"));

            Assert.Contains("Replace", exception.ToString());
        }

        [Fact]
        public void StringToLowerWithCultureOverloadThrowsActionableUnsupportedExpression() {
            var parser = new ConditionParser();

            BDadosException exception = Assert.Throws<BDadosException>(() =>
                parser.ParseExpression<GuidRoot>(x => x.AggregateName!.ToLower(System.Globalization.CultureInfo.InvariantCulture) == "abc"));

            Assert.Contains("ToLower", exception.ToString());
        }

        [Fact]
        public void StrBuilderNullExpressionReturnsBuilderUnchanged() {
            var parser = new ConditionParser();
            var builder = new QueryBuilder();
            builder.Append("SELECT *", Array.Empty<object>());

            var result = parser.ParseExpression<GuidRoot>(null, strBuilder: builder);

            Assert.Same(builder, result);
            Assert.Equal("SELECT *", result.GetCommandText());
            Assert.Empty(result.GetParameters());
        }

        [Fact]
        public void StrBuilderFullConditionsFalseProjectionTrueReturnsBuilderUnchanged() {
            var parser = new ConditionParser();
            var builder = new QueryBuilder();
            builder.Append("SELECT *", Array.Empty<object>());

            var result = parser.ParseExpression<GuidRoot>(x => true, fullConditions: false, strBuilder: builder);

            Assert.Same(builder, result);
            Assert.Equal("SELECT *", result.GetCommandText());
            Assert.Empty(result.GetParameters());
        }

        [Fact]
        public void StrBuilderIsHonoredAtTopLevelWithoutDuplicateSql() {
            var parser = new ConditionParser();
            var builder = new QueryBuilder();

            var result = parser.ParseExpression<GuidRoot>(x => x.AggregateName == "value", strBuilder: builder);

            Assert.Same(builder, result);
            Assert.Contains(".Name =", result.GetCommandText());
            Assert.Single(result.GetParameters());
            Assert.Equal("value", result.GetParameters().Single().Value);
        }

        [Fact]
        public void ConditionsOverloadMatchesLambdaSqlAndParametersForSupportedPredicate() {
            Guid scalarId = Guid.Parse("11111111-1111-1111-1111-111111111111");
            string name = "condition-value";
            Expression<Func<GuidRoot, bool>> expression = x => x.ScalarAggregateId == scalarId && x.AggregateName == name;

            QueryBuilder lambdaQuery = new ConditionParser().ParseExpression(expression);
            QueryBuilder conditionsQuery = new ConditionParser().ParseExpression(new Conditions<GuidRoot>(expression));

            Assert.Equal(lambdaQuery.GetCommandText(), conditionsQuery.GetCommandText());
            Assert.Equal(
                lambdaQuery.GetParameters().Select(parameter => new KeyValuePair<string, object>(parameter.Key, parameter.Value)),
                conditionsQuery.GetParameters().Select(parameter => new KeyValuePair<string, object>(parameter.Key, parameter.Value)));
        }

        [Fact]
        public void NullConditionsOverloadReturnsTrueWithoutParameters() {
            Conditions<GuidRoot>? conditions = null;

            QueryBuilder query = new ConditionParser().ParseExpression(conditions!);

            Assert.Equal("TRUE", query.GetCommandText());
            Assert.Empty(query.GetParameters());
        }

        [Fact]
        public void UnsupportedConditionsExpressionThrowsActionableConditionsMethodDiagnostic() {
            Expression<Func<GuidRoot, bool>> expression = x => x.AggregateName!.Substring(1) == "name";

            BDadosException exception = Assert.Throws<BDadosException>(() =>
                new ConditionParser().ParseExpression(new Conditions<GuidRoot>(expression)));

            Assert.Contains("Conditions", exception.ToString());
            Assert.Contains("Substring", exception.ToString());
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void DestinationBuilderPreservesExistingParserShapedParameterWhenParsing(bool fullConditions) {
            const string oldValue = "old-value";
            Guid parsedValue = Guid.Parse("22222222-2222-2222-2222-222222222222");
            var builder = new QueryBuilder();
            builder.Append("existing = @_p0", oldValue);

            QueryBuilder result = new ConditionParser().ParseExpression<GuidRoot>(
                x => x.ObjectAggregateId == parsedValue,
                fullConditions,
                builder);
            string sql = result.GetCommandText();

            Assert.Same(builder, result);
            Assert.Contains("existing = @_p0", sql);
            Assert.Contains("@_p1", sql);
            Assert.Equal(2, result.GetParameters().Count);
            Assert.Equal(oldValue, result.GetParameters()["_p0"]);
            Assert.Equal(parsedValue, result.GetParameters()["_p1"]);
            Assert.All(result.GetParameters().Keys, key => Assert.Contains("@" + key, sql));
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void DestinationBuilderWithMaximumAndNoncontiguousParserParametersAllocatesTheNextValidGap(bool fullConditions) {
            const string firstValue = "first-value";
            const string thirdValue = "third-value";
            const string maximumValue = "maximum-value";
            Guid parsedValue = Guid.Parse("33333333-3333-3333-3333-333333333333");
            var builder = new QueryBuilder();
            builder.Append("first = @_p0 AND third = @_p2 AND maximum = @_p2147483647", firstValue, thirdValue, maximumValue);

            QueryBuilder result = new ConditionParser().ParseExpression<GuidRoot>(
                x => x.ObjectAggregateId == parsedValue,
                fullConditions,
                builder);
            string sql = result.GetCommandText();

            Assert.Same(builder, result);
            Assert.Equal(firstValue, result.GetParameters()["_p0"]);
            Assert.Equal(thirdValue, result.GetParameters()["_p2"]);
            Assert.Equal(maximumValue, result.GetParameters()["_p2147483647"]);
            Assert.Equal(parsedValue, result.GetParameters()["_p1"]);
            Assert.All(result.GetParameters().Keys, key => Assert.Matches(@"^_p[0-9]+$", key));
            Assert.DoesNotContain("@_p-", sql);
            Assert.All(result.GetParameters().Keys, key => Assert.Contains("@" + key, sql));
        }

        [Fact]
        public void QhNotInIsLogicalComplementOfIn() {
            var input = new { Id = 2 };
            var matchingList = new List<object> { 1, 2, 3 };
            var nonMatchingList = new List<object> { 1, 3 };
            Func<object, object> selector = x => x;

            Assert.True(Qh.In(input, nameof(input.Id), matchingList, selector));
            Assert.False(Qh.NotIn(input, nameof(input.Id), matchingList, selector));

            Assert.False(Qh.In(input, nameof(input.Id), nonMatchingList, selector));
            Assert.True(Qh.NotIn(input, nameof(input.Id), nonMatchingList, selector));

            Assert.False(Qh.In(input, nameof(input.Id), new List<object>(), selector));
            Assert.True(Qh.NotIn(input, nameof(input.Id), new List<object>(), selector));

            Assert.False(Qh.In(input, nameof(input.Id), (List<object>?)null, selector));
            Assert.True(Qh.NotIn(input, nameof(input.Id), (List<object>?)null, selector));
        }

        [Fact]
        public void QhInUsesValueEqualityForBoxedInt() {
            var input = new { Value = 5 };
            var list = new List<object> { 1, 5, 10 };
            Func<object, object> selector = x => x;

            Assert.True(Qh.In(input, nameof(input.Value), list, selector));
            Assert.False(Qh.NotIn(input, nameof(input.Value), list, selector));
        }

        [Fact]
        public void QhInUsesValueEqualityForGuid() {
            var value = Guid.NewGuid();
            var input = new { Value = value };
            var list = new List<object> { Guid.NewGuid(), value, Guid.NewGuid() };
            Func<object, object> selector = x => x;

            Assert.True(Qh.In(input, nameof(input.Value), list, selector));
            Assert.False(Qh.NotIn(input, nameof(input.Value), list, selector));
        }

        [Fact]
        public void InstanceStringEqualsParsesMappedMemberAndOneParameter() {
            var parser = new ConditionParser();

            var query = parser.ParseExpression<GuidRoot>(x => x.AggregateName!.Equals("name"));
            string sql = query.GetCommandText();

            Assert.Contains(".Name =", sql);
            Assert.Single(query.GetParameters());
            Assert.Equal("name", query.GetParameters().Single().Value);
        }

        [Fact]
        public void StaticStringEqualsThrowsActionableUnsupportedExpression() {
            var parser = new ConditionParser();

            BDadosException exception = Assert.Throws<BDadosException>(() =>
                parser.ParseExpression<GuidRoot>(x => string.Equals(x.AggregateName, "name")));

            Assert.Contains("Equals", exception.ToString());
        }

        [Fact]
        public void StringEqualsWithComparisonOverloadThrowsActionableUnsupportedExpression() {
            var parser = new ConditionParser();

            BDadosException exception = Assert.Throws<BDadosException>(() =>
                parser.ParseExpression<GuidRoot>(x => x.AggregateName!.Equals("name", StringComparison.Ordinal)));

            Assert.Contains("Equals", exception.ToString());
        }

        [Fact]
        public void WhereAnyOnCollectionEmitsSameTypedIdentifierGuardAndPredicateOnce() {
            DefinitiveJoinPlan plan = AutomaticJoinPlanCache.GetOrAdd(typeof(GuidRoot), AggregateJoinShape.FullGraph);
            var parser = new ConditionParser(plan);

            var directAny = parser.ParseExpression<GuidRoot>(x => x.AggregateList.Any(item => item.Name == "name"));
            var whereAny = parser.ParseExpression<GuidRoot>(x => x.AggregateList.Where(item => item.Name == "name").Any());

            string listAlias = plan.AliasByPath[new AggregatePath(new[] { nameof(GuidRoot.AggregateList) })];
            string identifier = plan.Tables[plan.TableIndexByAlias[listAlias]].Identifier.ColumnName;

            string directSql = directAny.GetCommandText();
            string whereAnySql = whereAny.GetCommandText();

            Assert.Contains($"{listAlias}.{identifier} IS NOT NULL", whereAnySql);
            Assert.Contains("AND", whereAnySql);
            Assert.Single(Regex.Matches(whereAnySql, Regex.Escape($"{listAlias}.Name")).Cast<Match>());
            Assert.Single(whereAny.GetParameters());
            Assert.Equal("name", whereAny.GetParameters().Single().Value);
            AssertOnlyPlanAliases(plan, whereAnySql);
        }

        [Fact]
        public void FirstWithoutPredicatePreservesMemberAccessCompatibility() {
            DefinitiveJoinPlan plan = AutomaticJoinPlanCache.GetOrAdd(typeof(GuidRoot), AggregateJoinShape.FullGraph);
            var parser = new ConditionParser(plan);

            var query = parser.ParseExpression<GuidRoot>(x => x.AggregateList.First().Name == "name");
            string sql = query.GetCommandText();
            string listAlias = plan.AliasByPath[new AggregatePath(new[] { nameof(GuidRoot.AggregateList) })];

            Assert.Contains(listAlias + ".Name", sql);
            Assert.Single(query.GetParameters());
            Assert.Equal("name", query.GetParameters().Single().Value);
        }

        [Fact]
        public void FirstWithPredicateThrowsActionableUnsupportedExpression() {
            DefinitiveJoinPlan plan = AutomaticJoinPlanCache.GetOrAdd(typeof(GuidRoot), AggregateJoinShape.FullGraph);
            var parser = new ConditionParser(plan);

            BDadosException exception = Assert.Throws<BDadosException>(() =>
                parser.ParseExpression<GuidRoot>(x => x.AggregateList.First(item => item.Name == "name") != null));

            Assert.Contains("First", exception.ToString());
        }

        [Fact]
        public void FirstWithPredicateInstanceThrowsActionableUnsupportedExpression() {
            DefinitiveJoinPlan plan = AutomaticJoinPlanCache.GetOrAdd(typeof(GuidRoot), AggregateJoinShape.FullGraph);
            var parser = new ConditionParser(plan);

            BDadosException exception = Assert.Throws<BDadosException>(() =>
                parser.ParseExpression<GuidRoot>(x => x.AggregateList.First(item => item.Name == "name").Id == Guid.Empty));

            Assert.Contains("First", exception.ToString());
        }

        [Fact]
        public void UnsupportedMethodCallThrowsActionableUnsupportedExpression() {
            var parser = new ConditionParser();

            BDadosException exception = Assert.Throws<BDadosException>(() =>
                parser.ParseExpression<GuidRoot>(x => x.AggregateName!.Substring(1, 3) == "abc"));

            Assert.Contains("Substring", exception.ToString());
        }

        [Fact]
        public void UnsupportedBinaryExpressionThrowsActionableUnsupportedExpression() {
            var parser = new ConditionParser();

            BDadosException exception = Assert.Throws<BDadosException>(() =>
                parser.ParseExpression<GuidRoot>(x => x.AggregateName! + "x" == "abc"));

            Assert.Contains("binary", exception.ToString(), StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void StringEqualsObjectOverloadThrowsActionableUnsupportedExpression() {
            var parser = new ConditionParser();
            object value = "name";

            BDadosException exception = Assert.Throws<BDadosException>(() =>
                parser.ParseExpression<GuidRoot>(x => x.AggregateName!.Equals(value)));

            Assert.Contains("Equals", exception.ToString());
            Assert.Contains("overload", exception.ToString(), StringComparison.OrdinalIgnoreCase);
            Assert.Empty(parser.ParseExpression<GuidRoot>(x => true, fullConditions: false).GetParameters());
        }

        [Fact]
        public void StringContainsCharOverloadThrowsActionableUnsupportedExpression() {
            var parser = new ConditionParser();
            char value = 'x';

            BDadosException exception = Assert.Throws<BDadosException>(() =>
                parser.ParseExpression<GuidRoot>(x => x.AggregateName!.Contains(value)));

            Assert.Contains("Contains", exception.ToString());
            Assert.Contains("overload", exception.ToString(), StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void StringReplaceCharCharOverloadThrowsActionableUnsupportedExpression() {
            var parser = new ConditionParser();
            char oldChar = 'a';
            char newChar = 'b';

            BDadosException exception = Assert.Throws<BDadosException>(() =>
                parser.ParseExpression<GuidRoot>(x => x.AggregateName!.Replace(oldChar, newChar) == "abc"));

            Assert.Contains("Replace", exception.ToString());
            Assert.Contains("overload", exception.ToString(), StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void StringStartsWithCharOverloadThrowsActionableUnsupportedExpression() {
            var parser = new ConditionParser();
            char value = 'a';

            BDadosException exception = Assert.Throws<BDadosException>(() =>
                parser.ParseExpression<GuidRoot>(x => x.AggregateName!.StartsWith(value)));

            Assert.Contains("StartsWith", exception.ToString());
            Assert.Contains("overload", exception.ToString(), StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void StringEndsWithCharOverloadThrowsActionableUnsupportedExpression() {
            var parser = new ConditionParser();
            char value = 'a';

            BDadosException exception = Assert.Throws<BDadosException>(() =>
                parser.ParseExpression<GuidRoot>(x => x.AggregateName!.EndsWith(value)));

            Assert.Contains("EndsWith", exception.ToString());
            Assert.Contains("overload", exception.ToString(), StringComparison.OrdinalIgnoreCase);
        }

        [Theory]
        [InlineData("ToUpper")]
        [InlineData("ToLower")]
        [InlineData("Trim")]
        public void StringTransformEmitsOneFunctionOneMappedColumnAndOneComparisonParameter(string transform) {
            const string comparison = "transformed";
            Expression<Func<GuidRoot, bool>> expression = transform switch {
                "ToUpper" => x => x.AggregateName!.ToUpper() == comparison,
                "ToLower" => x => x.AggregateName!.ToLower() == comparison,
                "Trim" => x => x.AggregateName!.Trim() == comparison,
                _ => throw new InvalidOperationException()
            };

            QueryBuilder query = new ConditionParser().ParseExpression(expression);
            string sql = query.GetCommandText();

            string sqlFunction = transform switch {
                "ToUpper" => "UPPER",
                "ToLower" => "LOWER",
                "Trim" => "TRIM",
                _ => throw new InvalidOperationException()
            };

            Assert.Single(Regex.Matches(sql, $@"\b{sqlFunction}\s*\("));
            Assert.Single(Regex.Matches(sql, Regex.Escape(".Name")));
            AssertParameterValues(query, comparison);
        }

        [Fact]
        public void StringReplaceEmitsOneFunctionAndPreservesEveryArgumentAndComparisonOnce() {
            const string oldValue = "old";
            const string newValue = "new";
            const string comparison = "replaced";

            QueryBuilder query = new ConditionParser().ParseExpression<GuidRoot>(x =>
                x.AggregateName!.Replace(oldValue, newValue) == comparison);
            string sql = query.GetCommandText();

            Assert.Single(Regex.Matches(sql, @"\bREPLACE\s*\("));
            Assert.Single(Regex.Matches(sql, Regex.Escape(".Name")));
            AssertParameterValues(query, oldValue, newValue, comparison);
        }

        [Fact]
        public void RegExReplaceEmitsOneFunctionAndPreservesPatternReplacementAndComparisonOnce() {
            const string pattern = "[0-9]+";
            const string replacement = "number";
            const string comparison = "normalized";

            QueryBuilder query = new ConditionParser().ParseExpression<GuidRoot>(x =>
                StringExtensions.RegExReplace(x.AggregateName!, pattern, replacement) == comparison);
            string sql = query.GetCommandText();

            Assert.Single(Regex.Matches(sql, @"\bREGEXP_REPLACE\s*\("));
            Assert.Single(Regex.Matches(sql, Regex.Escape(".Name")));
            AssertParameterValues(query, pattern, replacement, comparison);
        }

        [Fact]
        public void RegExReplaceDelegateOverloadThrowsActionableUnsupportedExpressionWithoutAppendingToProvidedBuilder() {
            const string pattern = "[0-9]+";
            const string replacement = "number";
            const string comparison = "normalized";
            var parser = new ConditionParser();
            var builder = new QueryBuilder();
            builder.Append("SELECT *", Array.Empty<object>());

            BDadosException exception = Assert.Throws<BDadosException>(() => parser.ParseExpression<GuidRoot>(x =>
                StringExtensions.RegExReplace(x.AggregateName!, pattern, () => replacement) == comparison, strBuilder: builder));

            Assert.Contains(nameof(StringExtensions.RegExReplace), exception.ToString());
            Assert.Contains("Func", exception.InnerException?.ToString() ?? String.Empty);
            Assert.Equal("SELECT *", builder.GetCommandText());
            Assert.Empty(builder.GetParameters());
        }

        [Fact]
        public void Int32ParseNumberStylesOverloadThrowsActionableUnsupportedExpressionWithoutAppendingToProvidedBuilder() {
            var parser = new ConditionParser();
            var builder = new QueryBuilder();
            builder.Append("SELECT *", Array.Empty<object>());

            BDadosException exception = Assert.Throws<BDadosException>(() => parser.ParseExpression<GuidRoot>(x =>
                Int32.Parse(x.AggregateName!, System.Globalization.NumberStyles.Integer) == 42, strBuilder: builder));

            Assert.Contains(nameof(Int32.Parse), exception.ToString());
            Assert.Contains(nameof(System.Globalization.NumberStyles), exception.InnerException?.ToString() ?? String.Empty);
            Assert.Equal("SELECT *", builder.GetCommandText());
            Assert.Empty(builder.GetParameters());
        }

        [Fact]
        public void Int64ParseCultureOverloadThrowsActionableUnsupportedExpressionWithoutAppendingToProvidedBuilder() {
            var parser = new ConditionParser();
            var builder = new QueryBuilder();
            builder.Append("SELECT *", Array.Empty<object>());

            BDadosException exception = Assert.Throws<BDadosException>(() => parser.ParseExpression<GuidRoot>(x =>
                Int64.Parse(x.AggregateName!, System.Globalization.CultureInfo.InvariantCulture) == 42L, strBuilder: builder));

            Assert.Contains(nameof(Int64.Parse), exception.ToString());
            Assert.Contains(nameof(IFormatProvider), exception.InnerException?.ToString() ?? String.Empty);
            Assert.Equal("SELECT *", builder.GetCommandText());
            Assert.Empty(builder.GetParameters());
        }

        [Fact]
        public void Int32ParseEmitsOneSignedCastAndPreservesTheComparisonOnce() {
            const int comparison = 42;

            QueryBuilder query = new ConditionParser().ParseExpression<GuidRoot>(x =>
                Int32.Parse(x.AggregateName!) == comparison);
            string sql = query.GetCommandText();

            Assert.Single(Regex.Matches(sql, @"\bCAST\s*\("));
            Assert.Single(Regex.Matches(sql, @"\bAS\s+SIGNED\b"));
            Assert.Single(Regex.Matches(sql, Regex.Escape(".Name")));
            AssertParameterValues(query, comparison);
        }

        [Fact]
        public void Int64ParseEmitsOneSignedCastWithoutDuplicatingOperandOrComparison() {
            const long comparison = 42000000000L;

            QueryBuilder query = new ConditionParser().ParseExpression<GuidRoot>(x =>
                Int64.Parse(x.AggregateName!) == comparison);
            string sql = query.GetCommandText();

            Assert.Single(Regex.Matches(sql, @"\bCAST\s*\("));
            Assert.Single(Regex.Matches(sql, @"\bAS\s+SIGNED\b"));
            Assert.Single(Regex.Matches(sql, Regex.Escape(".Name")));
            AssertParameterValues(query, comparison);
        }

        [Fact]
        public void SharedParserSerializesConditionsAndLambdaParseTransactions() {
            const string aggregateValue = "blocked-aggregate";
            const string rootValue = "second-root";
            var getterEntered = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
            var releaseGetter = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
            var secondStarted = new TaskCompletionSource<object?>(TaskCreationOptions.RunContinuationsAsynchronously);
            var source = new BlockingStringSource(aggregateValue, getterEntered, releaseGetter);
            var parser = new ConditionParser();
            Expression<Func<GuidRoot, bool>> firstExpression = x => x.AggregateName == source.Value;
            var firstConditions = new Conditions<GuidRoot>(firstExpression);

            Task<QueryBuilder> firstTask = StartDedicatedWorker(() => parser.ParseExpression(firstConditions));
            Task<QueryBuilder> secondTask;

            try {
                Assert.True(getterEntered.Task.Wait(TimeSpan.FromSeconds(5)), "The first parse did not reach its captured getter.");

                secondTask = StartDedicatedWorker(() => {
                    secondStarted.TrySetResult(null);
                    return parser.ParseExpression<QueryComparisonRoot>(x => x.ExactValue == rootValue);
                });

                Assert.True(secondStarted.Task.Wait(TimeSpan.FromSeconds(5)), "The second parse worker did not begin.");
                Assert.False(secondTask.Wait(TimeSpan.FromMilliseconds(250)), "A second public parse completed while the first parse transaction was blocked.");
            } finally {
                releaseGetter.TrySetResult(null);
            }

            Assert.True(firstTask.Wait(TimeSpan.FromSeconds(5)), "The first parse did not complete after its getter was released.");
            Assert.True(secondTask.Wait(TimeSpan.FromSeconds(5)), "The second parse did not complete after the first transaction finished.");

            AssertEquivalentQuery(
                new ConditionParser().ParseExpression(new Conditions<GuidRoot>(x => x.AggregateName == aggregateValue)),
                firstTask.GetAwaiter().GetResult());
            AssertEquivalentQuery(
                new ConditionParser().ParseExpression<QueryComparisonRoot>(x => x.ExactValue == rootValue),
                secondTask.GetAwaiter().GetResult());
        }

        [Fact]
        public void PrefixMakerGetNewAliasForIsStableForConcurrentSameKeyCalls() {
            const int workerCount = 32;
            var maker = new PrefixMaker();
            using var ready = new CountdownEvent(workerCount);
            using var start = new ManualResetEventSlim(false);

            Task<string>[] workers = Enumerable.Range(0, workerCount)
                .Select(_ => StartDedicatedWorker(() => {
                    ready.Signal();
                    start.Wait();
                    return maker.GetNewAliasFor("root", "concurrent-child", "concurrent-key");
                }))
                .ToArray();

            try {
                Assert.True(ready.Wait(TimeSpan.FromSeconds(5)), "Concurrent alias workers did not all reach their coordinated start.");
            } finally {
                start.Set();
            }

            Assert.True(Task.WaitAll(workers, TimeSpan.FromSeconds(5)), "Concurrent alias workers did not all finish.");

            string[] aliases = workers.Select(worker => worker.GetAwaiter().GetResult()).ToArray();
            Assert.Single(aliases.Distinct());
            Assert.Equal(aliases[0], maker.GetNewAliasFor("root", "concurrent-child", "concurrent-key"));
        }

        private static Task<T> StartDedicatedWorker<T>(Func<T> action) {
            return Task.Factory.StartNew(action, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        }

        private static void AssertEquivalentQuery(QueryBuilder expected, QueryBuilder actual) {
            Assert.Equal(expected.GetCommandText(), actual.GetCommandText());
            Assert.Equal(
                expected.GetParameters().Select(parameter => new KeyValuePair<string, object>(parameter.Key, parameter.Value)),
                actual.GetParameters().Select(parameter => new KeyValuePair<string, object>(parameter.Key, parameter.Value)));
        }

        private sealed class BlockingStringSource {
            private readonly string _value;
            private readonly TaskCompletionSource<object?> _getterEntered;
            private readonly TaskCompletionSource<object?> _releaseGetter;

            public BlockingStringSource(string value, TaskCompletionSource<object?> getterEntered, TaskCompletionSource<object?> releaseGetter) {
                _value = value;
                _getterEntered = getterEntered;
                _releaseGetter = releaseGetter;
            }

            public string Value {
                get {
                    _getterEntered.TrySetResult(null);
                    _releaseGetter.Task.GetAwaiter().GetResult();
                    return _value;
                }
            }
        }

        private static void AssertParameterValues(QueryBuilder query, params object[] expectedValues) {
            object[] actualValues = query.GetParameters().Select(parameter => parameter.Value).ToArray();

            Assert.Equal(expectedValues, actualValues);
        }

        private static void AssertOnlyPlanAliases(DefinitiveJoinPlan plan, string sql) {
            string[] aliases = plan.AliasByPath.Values.Distinct().ToArray();
            foreach (Match match in Regex.Matches(sql, @"\btb[a-z]+\b")) {
                Assert.Contains(match.Value, aliases);
            }
        }

        private static void AssertLiteralLikeQuery(QueryBuilder query, string originalValue, string escapedValue) {
            string sql = query.GetCommandText();

            Assert.Contains("ESCAPE '!'", sql);
            Assert.DoesNotContain(originalValue, sql);
            Assert.DoesNotContain(escapedValue, sql);
            Assert.Single(query.GetParameters());
            Assert.Equal(escapedValue, query.GetParameters().Single().Value);
        }

        // ==== Regression: query-independent evaluable operand nodes (Conditional, Invocation)
        // used to fall through every parser branch and emit an empty fragment, producing
        // malformed SQL such as `( tba.Name = )` with the value silently dropped. The parser
        // must evaluate these query-independent nodes and bind them as a parameter instead.

        private sealed class ConditionalHolder {
            public string? Name { get; set; }
            public int Number { get; set; }
        }

        [Fact]
        public void NullConditionalValueArmBindsParameterInsteadOfEmittingEmptyFragment() {
            ConditionalHolder holder = new ConditionalHolder { Name = "abc" };
            var parser = new ConditionParser();

            var query = parser.ParseExpression<QueryComparisonRoot>(x => x.ExactValue == (holder == null ? null : holder.Name));
            string sql = query.GetCommandText();

            Assert.Matches(@"=\s*@\w+", sql);
            Assert.Single(query.GetParameters());
            Assert.Equal("abc", query.GetParameters().Single().Value);
        }

        [Fact]
        public void NullConditionalNullArmEmitsIsNullAndBindsNoParameter() {
            ConditionalHolder? holder = null;
            var parser = new ConditionParser();

            var query = parser.ParseExpression<QueryComparisonRoot>(x => x.ExactValue == (holder == null ? null : holder.Name));
            string sql = query.GetCommandText();

            Assert.Contains("IS NULL", sql);
            Assert.Empty(query.GetParameters());
        }

        [Fact]
        public void TernaryOnNonStringMemberBindsEvaluatedParameter() {
            ConditionalHolder? holder = null;
            var parser = new ConditionParser();

            var query = parser.ParseExpression<LongRoot>(x => x.Id == (holder == null ? 5 : holder.Number));
            string sql = query.GetCommandText();

            Assert.Matches(@"=\s*@\w+", sql);
            Assert.Single(query.GetParameters());
            Assert.Equal(5L, System.Convert.ToInt64(query.GetParameters().Single().Value));
        }

        [Fact]
        public void ReversedNullConditionalBindsParameterInsteadOfEmittingEmptyFragment() {
            ConditionalHolder holder = new ConditionalHolder { Name = "abc" };
            var parser = new ConditionParser();

            var query = parser.ParseExpression<QueryComparisonRoot>(x => (holder == null ? null : holder.Name) == x.ExactValue);
            string sql = query.GetCommandText();

            Assert.Matches(@"@\w+\s*=", sql);
            Assert.Single(query.GetParameters());
            Assert.Equal("abc", query.GetParameters().Single().Value);
        }

        [Fact]
        public void CapturedDelegateInvocationBindsEvaluatedParameter() {
            Func<int> del = () => 42;
            var parser = new ConditionParser();

            var query = parser.ParseExpression<LongRoot>(x => x.Id == del());
            string sql = query.GetCommandText();

            Assert.Matches(@"=\s*@\w+", sql);
            Assert.Single(query.GetParameters());
            Assert.Equal(42L, System.Convert.ToInt64(query.GetParameters().Single().Value));
        }

        [Fact]
        public void QueryDependentConditionalRemainsRejected() {
            var parser = new ConditionParser();

            Assert.Throws<BDadosException>(() =>
                parser.ParseExpression<GuidRoot>(x => x.AggregateName == (x.ScalarAggregateId == Guid.Empty ? "a" : "b")));
        }

        // ==== Regression: Qh.In / Qh.NotIn with an unqualified column name used as a
        // predicate emitted a bare `Name IN (...)` fragment with no table alias, which does
        // not resolve to the root column. Unqualified Qh column names must be qualified with
        // the resolved root alias; already-qualified names keep their explicit alias.

        private sealed class QhHolder {
            public string? Name { get; set; }
        }

        [Fact]
        public void QhInWithUnqualifiedColumnIsQualifiedWithRootAlias() {
            var list = new List<object> { "a", "b" };
            object holder = new QhHolder { Name = "a" };
            var parser = new ConditionParser();

            var query = parser.ParseExpression<QueryComparisonRoot>(x => Qh.In(holder, "ExactValue", list, s => s));
            string sql = query.GetCommandText();
            string rootAlias = sql.Split('.')[0].TrimStart('(', ' ');

            Assert.Matches(@"\btba\.ExactValue\s+IN\s*\(", sql);
            Assert.Equal(2, query.GetParameters().Count);
        }

        [Fact]
        public void QhNotInWithUnqualifiedColumnIsQualifiedWithRootAlias() {
            var list = new List<object> { "a", "b" };
            object holder = new QhHolder { Name = "a" };
            var parser = new ConditionParser();

            var query = parser.ParseExpression<QueryComparisonRoot>(x => Qh.NotIn(holder, "ExactValue", list, s => s));
            string sql = query.GetCommandText();

            Assert.Matches(@"\btba\.ExactValue\s+NOT\s+IN\s*\(", sql);
            Assert.Equal(2, query.GetParameters().Count);
        }

        [Fact]
        public void QhInWithAlreadyQualifiedColumnKeepsExplicitAlias() {
            var list = new List<object> { "a", "b" };
            object holder = new QhHolder { Name = "a" };
            var parser = new ConditionParser();

            var query = parser.ParseExpression<QueryComparisonRoot>(x => Qh.In(holder, "tba.ExactValue", list, s => s));
            string sql = query.GetCommandText();

            Assert.Matches(@"\btba\.ExactValue\s+IN\s*\(", sql);
            Assert.DoesNotContain("tba.tba", sql);
        }
    }
}
