using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Reflection;
using Figlotech.BDados.DataAccessAbstractions;
using Xunit;

namespace Figlotech.BDados.Tests {
    public class DefinitiveJoinPlanTests {
        [Fact]
        public void PlanPreservesActualTypedIdentifierMetadata() {
            DefinitiveJoinPlan guidPlan = DefinitiveJoinPlanTestFactory.CreateFor(typeof(GuidRoot));
            DefinitiveJoinPlan longPlan = DefinitiveJoinPlanTestFactory.CreateFor(typeof(LongRoot));

            Assert.Equal(typeof(Guid), guidPlan.Tables[guidPlan.RootTableIndex].Identifier.ClrType);
            Assert.Equal(typeof(long), longPlan.Tables[longPlan.RootTableIndex].Identifier.ClrType);
        }

        [Fact]
        public void PlanCollectionInputsAreImmutableSnapshots() {
            MemberInfo member = DefinitiveJoinPlanTestFactory.GetIdentifierMember(typeof(GuidRoot));
            var projectedColumns = new List<string> { "RID" };
            var tables = new List<DefinitiveJoinTable> {
                DefinitiveJoinPlanTestFactory.CreateTable(typeof(GuidRoot), "root", member, projectedColumns)
            };
            var projection = new List<DefinitiveProjectionColumn> {
                new DefinitiveProjectionColumn(0, 0, "RID", "root_RID", member)
            };
            var aliasesByPath = new Dictionary<AggregatePath, string> {
                [new AggregatePath(new[] { "Root" })] = "root"
            };
            var tableIndicesByAlias = new Dictionary<string, int> {
                ["root"] = 0
            };

            DefinitiveJoinPlan plan = DefinitiveJoinPlanTestFactory.Create(
                typeof(GuidRoot),
                tables,
                projection,
                aliasesByPath,
                tableIndicesByAlias);

            projectedColumns.Add("LateColumn");
            tables.Add(DefinitiveJoinPlanTestFactory.CreateTable(typeof(LongRoot), "late", DefinitiveJoinPlanTestFactory.GetIdentifierMember(typeof(LongRoot)), new[] { "RID" }));
            projection.Add(new DefinitiveProjectionColumn(1, 0, "LateColumn", "root_LateColumn", member));
            aliasesByPath[new AggregatePath(new[] { "Late" })] = "late";
            tableIndicesByAlias["late"] = 1;

            Assert.Single(plan.Tables);
            Assert.Single(plan.Projection);
            Assert.DoesNotContain("LateColumn", plan.Tables[0].ProjectedColumns);
            Assert.False(plan.AliasByPath.ContainsKey(new AggregatePath(new[] { "Late" })));
            Assert.False(plan.TableIndexByAlias.ContainsKey("late"));
        }

        [Fact]
        public void AggregatePathUsesStructuralEqualityForImmutableDictionaryLookup() {
            var aliases = ImmutableDictionary<AggregatePath, string>.Empty
                .Add(new AggregatePath(new[] { "Root", "Aggregate" }), "aggregate");

            bool found = aliases.TryGetValue(new AggregatePath(new[] { "Root", "Aggregate" }), out string? alias);

            Assert.True(found);
            Assert.Equal("aggregate", alias);
        }

        [Fact]
        public void AutoJoinPlanKeyEqualityDistinguishesRootShapeAndFormatVersion() {
            var baseline = new AutoJoinPlanKey(typeof(GuidRoot), AggregateJoinShape.ScalarAggregatesOnly, 1);

            Assert.Equal(baseline, new AutoJoinPlanKey(typeof(GuidRoot), AggregateJoinShape.ScalarAggregatesOnly, 1));
            Assert.NotEqual(baseline, new AutoJoinPlanKey(typeof(LongRoot), AggregateJoinShape.ScalarAggregatesOnly, 1));
            Assert.NotEqual(baseline, new AutoJoinPlanKey(typeof(GuidRoot), AggregateJoinShape.FullGraph, 1));
            Assert.NotEqual(baseline, new AutoJoinPlanKey(typeof(GuidRoot), AggregateJoinShape.ScalarAggregatesOnly, 2));
        }

        [Fact]
        public void PlanRejectsDuplicateTableAliases() {
            MemberInfo guidMember = DefinitiveJoinPlanTestFactory.GetIdentifierMember(typeof(GuidRoot));
            MemberInfo longMember = DefinitiveJoinPlanTestFactory.GetIdentifierMember(typeof(LongRoot));
            var tables = new[] {
                DefinitiveJoinPlanTestFactory.CreateTable(typeof(GuidRoot), "duplicate", guidMember, new[] { "RID" }, 0, "guid_RID"),
                DefinitiveJoinPlanTestFactory.CreateTable(typeof(LongRoot), "duplicate", longMember, new[] { "RID" }, 1, "long_RID")
            };
            var projection = new[] {
                new DefinitiveProjectionColumn(0, 0, "RID", "guid_RID", guidMember),
                new DefinitiveProjectionColumn(1, 1, "RID", "long_RID", longMember)
            };

            ArgumentException exception = Assert.Throws<ArgumentException>(() => DefinitiveJoinPlanTestFactory.Create(typeof(GuidRoot), tables, projection));

            Assert.Contains("alias", exception.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void PlanRejectsCaseOnlyTableAliasCollisions() {
            MemberInfo guidMember = DefinitiveJoinPlanTestFactory.GetIdentifierMember(typeof(GuidRoot));
            MemberInfo longMember = DefinitiveJoinPlanTestFactory.GetIdentifierMember(typeof(LongRoot));
            var tables = new[] {
                DefinitiveJoinPlanTestFactory.CreateTable(typeof(GuidRoot), "root", "r", guidMember, new[] { "RID" }, 0, "r_RID"),
                DefinitiveJoinPlanTestFactory.CreateTable(typeof(LongRoot), "ROOT", "l", longMember, new[] { "RID" }, 1, "l_RID")
            };
            var projection = new[] {
                new DefinitiveProjectionColumn(0, 0, "RID", "r_RID", guidMember),
                new DefinitiveProjectionColumn(1, 1, "RID", "l_RID", longMember)
            };
            var mappings = new[] {
                new KeyValuePair<string, int>("root", 0),
                new KeyValuePair<string, int>("ROOT", 1)
            };

            ArgumentException exception = Assert.Throws<ArgumentException>(() => DefinitiveJoinPlanTestFactory.Create(typeof(GuidRoot), tables, projection, tableIndicesByAlias: mappings));

            Assert.Contains("alias", exception.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void PlanRejectsCaseOnlyPrefixCollisions() {
            MemberInfo guidMember = DefinitiveJoinPlanTestFactory.GetIdentifierMember(typeof(GuidRoot));
            MemberInfo longMember = DefinitiveJoinPlanTestFactory.GetIdentifierMember(typeof(LongRoot));
            var tables = new[] {
                DefinitiveJoinPlanTestFactory.CreateTable(typeof(GuidRoot), "root", "sql", guidMember, new[] { "RID" }, 0, "sql_RID"),
                DefinitiveJoinPlanTestFactory.CreateTable(typeof(LongRoot), "child", "SQL", longMember, new[] { "RID" }, 1, "SQL_RID")
            };
            var projection = new[] {
                new DefinitiveProjectionColumn(0, 0, "RID", "sql_RID", guidMember),
                new DefinitiveProjectionColumn(1, 1, "RID", "SQL_RID", longMember)
            };
            var mappings = new[] {
                new KeyValuePair<string, int>("root", 0),
                new KeyValuePair<string, int>("child", 1)
            };

            ArgumentException exception = Assert.Throws<ArgumentException>(() => DefinitiveJoinPlanTestFactory.Create(typeof(GuidRoot), tables, projection, tableIndicesByAlias: mappings));

            Assert.Contains("prefix", exception.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void PlanRejectsCaseOnlyProjectionResultAliasCollisions() {
            MemberInfo member = DefinitiveJoinPlanTestFactory.GetIdentifierMember(typeof(GuidRoot));
            var projection = new[] {
                new DefinitiveProjectionColumn(0, 0, "RID", "root_RID", member),
                new DefinitiveProjectionColumn(1, 0, "Other", "ROOT_rid", member)
            };

            ArgumentException exception = Assert.Throws<ArgumentException>(() => DefinitiveJoinPlanTestFactory.Create(typeof(GuidRoot), projection: projection));

            Assert.Contains("result alias", exception.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void PlanRejectsCaseOnlyTableIndexMappingCollisions() {
            MemberInfo guidMember = DefinitiveJoinPlanTestFactory.GetIdentifierMember(typeof(GuidRoot));
            MemberInfo longMember = DefinitiveJoinPlanTestFactory.GetIdentifierMember(typeof(LongRoot));
            var tables = new[] {
                DefinitiveJoinPlanTestFactory.CreateTable(typeof(GuidRoot), "root", "r", guidMember, new[] { "RID" }, 0, "r_RID"),
                DefinitiveJoinPlanTestFactory.CreateTable(typeof(LongRoot), "child", "c", longMember, new[] { "RID" }, 1, "c_RID")
            };
            var projection = new[] {
                new DefinitiveProjectionColumn(0, 0, "RID", "r_RID", guidMember),
                new DefinitiveProjectionColumn(1, 1, "RID", "c_RID", longMember)
            };
            var mappings = new[] {
                new KeyValuePair<string, int>("root", 0),
                new KeyValuePair<string, int>("ROOT", 0)
            };

            ArgumentException exception = Assert.Throws<ArgumentException>(() => DefinitiveJoinPlanTestFactory.Create(typeof(GuidRoot), tables, projection, tableIndicesByAlias: mappings));

            Assert.Contains("duplicate alias", exception.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void PlanRejectsDuplicateProjectionResultAliases() {
            MemberInfo member = DefinitiveJoinPlanTestFactory.GetIdentifierMember(typeof(GuidRoot));
            var projection = new[] {
                new DefinitiveProjectionColumn(0, 0, "RID", "duplicate", member),
                new DefinitiveProjectionColumn(1, 0, "Other", "duplicate", member)
            };

            ArgumentException exception = Assert.Throws<ArgumentException>(() => DefinitiveJoinPlanTestFactory.Create(typeof(GuidRoot), projection: projection));

            Assert.Contains("result alias", exception.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void PlanRejectsInvalidRootTableIndex() {
            ArgumentOutOfRangeException exception = Assert.Throws<ArgumentOutOfRangeException>(() => DefinitiveJoinPlanTestFactory.Create(typeof(GuidRoot), rootTableIndex: 1));

            Assert.Contains("rootTableIndex", exception.Message);
        }

        [Fact]
        public void PlanRejectsNonContiguousProjectionOrdinals() {
            MemberInfo member = DefinitiveJoinPlanTestFactory.GetIdentifierMember(typeof(GuidRoot));
            var projection = new[] {
                new DefinitiveProjectionColumn(1, 0, "RID", "root_RID", member)
            };

            ArgumentException exception = Assert.Throws<ArgumentException>(() => DefinitiveJoinPlanTestFactory.Create(typeof(GuidRoot), projection: projection));

            Assert.Contains("contiguous", exception.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void StructurallyEquivalentPlansShareSignatureAndBehaviorChangesDoNot() {
            DefinitiveJoinPlan first = DefinitiveJoinPlanTestFactory.CreateFor(typeof(GuidRoot));
            DefinitiveJoinPlan equivalent = DefinitiveJoinPlanTestFactory.CreateFor(typeof(GuidRoot));
            DefinitiveJoinPlan changedProjection = DefinitiveJoinPlanTestFactory.CreateFor(typeof(GuidRoot), columnName: "OtherRid");
            DefinitiveJoinPlan changedShape = DefinitiveJoinPlanTestFactory.CreateFor(typeof(GuidRoot), shape: AggregateJoinShape.FullGraph);

            Assert.Equal(first.StructuralSignature, equivalent.StructuralSignature);
            Assert.NotEqual(first.StructuralSignature, changedProjection.StructuralSignature);
            Assert.NotEqual(first.StructuralSignature, changedShape.StructuralSignature);
        }

        [Fact]
        public void AggregatePathUsesTotalOrdinalStructuralOrdering() {
            var dottedSegment = new AggregatePath(new[] { "a.b" });
            var separateSegments = new AggregatePath(new[] { "a", "b" });
            var equivalent = new AggregatePath(new[] { "a.b" });

            Assert.True(dottedSegment.CompareTo(separateSegments) > 0);
            Assert.True(separateSegments.CompareTo(dottedSegment) < 0);
            Assert.Equal(0, dottedSegment.CompareTo(equivalent));
        }

        [Fact]
        public void DefaultAggregatePathIsAnEmptyStructuralPathAndSafeDictionaryKey() {
            AggregatePath defaultPath = default;
            var emptyPath = new AggregatePath(Array.Empty<string>());
            var dictionary = new Dictionary<AggregatePath, string> {
                [defaultPath] = "empty"
            };
            var immutableDictionary = ImmutableDictionary<AggregatePath, string>.Empty
                .Add(defaultPath, "empty");

            Assert.Equal(emptyPath, defaultPath);
            Assert.Equal(emptyPath.GetHashCode(), defaultPath.GetHashCode());
            Assert.Empty(defaultPath.Segments);
            Assert.Equal(String.Empty, defaultPath.ToString());
            Assert.Equal("empty", dictionary[emptyPath]);
            Assert.Equal("empty", immutableDictionary[emptyPath]);
        }

        [Fact]
        public void PlanSignatureDoesNotDependOnAliasPathInsertionOrderForCollidingRenderings() {
            var dottedSegment = new AggregatePath(new[] { "a.b" });
            var separateSegments = new AggregatePath(new[] { "a", "b" });
            var firstAliases = new[] {
                new KeyValuePair<AggregatePath, string>(dottedSegment, "root"),
                new KeyValuePair<AggregatePath, string>(separateSegments, "root")
            };
            var secondAliases = new[] {
                new KeyValuePair<AggregatePath, string>(separateSegments, "root"),
                new KeyValuePair<AggregatePath, string>(dottedSegment, "root")
            };

            DefinitiveJoinPlan first = DefinitiveJoinPlanTestFactory.CreateForAliases(firstAliases);
            DefinitiveJoinPlan second = DefinitiveJoinPlanTestFactory.CreateForAliases(secondAliases);

            Assert.Equal(first.StructuralSignature, second.StructuralSignature);
        }

        [Fact]
        public void PlanRejectsRootTableWhoseEntityTypeDoesNotMatchRootType() {
            MemberInfo longMember = DefinitiveJoinPlanTestFactory.GetIdentifierMember(typeof(LongRoot));
            var tables = new[] {
                DefinitiveJoinPlanTestFactory.CreateTable(typeof(LongRoot), "root", longMember, new[] { "RID" })
            };
            var projection = new[] {
                new DefinitiveProjectionColumn(0, 0, "RID", "root_RID", longMember)
            };

            ArgumentException exception = Assert.Throws<ArgumentException>(() => DefinitiveJoinPlanTestFactory.Create(typeof(GuidRoot), tables, projection));

            Assert.Contains("root table", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Contains(nameof(GuidRoot), exception.Message);
        }

        [Fact]
        public void RelationRequiresTargetMemberForAggregateBuildKinds() {
            ArgumentNullException exception = Assert.Throws<ArgumentNullException>(() => new DefinitiveJoinRelation(
                0,
                1,
                "ParentId",
                "ChildId",
                AggregateBuildOptions.AggregateObject,
                null!,
                new[] { "ChildId" }));

            Assert.Equal("targetMember", exception.ParamName);
        }

        [Fact]
        public void RelationRejectsMethodTargetMember() {
            MemberInfo method = typeof(RelationTarget).GetMethod(nameof(RelationTarget.SetValue))!;

            ArgumentException exception = Assert.Throws<ArgumentException>(() => new DefinitiveJoinRelation(
                0,
                1,
                "ParentId",
                "ChildId",
                AggregateBuildOptions.AggregateField,
                method,
                new[] { "ChildId" }));

            Assert.Contains("writable", exception.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void RelationRejectsReadOnlyPropertyTargetMember() {
            MemberInfo property = typeof(RelationTarget).GetProperty(nameof(RelationTarget.ReadOnlyValue))!;

            ArgumentException exception = Assert.Throws<ArgumentException>(() => new DefinitiveJoinRelation(
                0,
                1,
                "ParentId",
                "ChildId",
                AggregateBuildOptions.AggregateField,
                property,
                new[] { "ChildId" }));

            Assert.Contains("writable", exception.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void RelationValidatesNonNullTargetMemberForNoneBuildKind() {
            MemberInfo method = typeof(RelationTarget).GetMethod(nameof(RelationTarget.SetValue))!;

            ArgumentException exception = Assert.Throws<ArgumentException>(() => new DefinitiveJoinRelation(
                0,
                1,
                "ParentId",
                "ChildId",
                AggregateBuildOptions.None,
                method,
                new[] { "ChildId" }));

            Assert.Contains("writable", exception.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void RelationAllowsNullTargetMemberForNoneBuildKindAndSnapshotsSourceFields() {
            var sourceFields = new List<string> { "ChildId" };

            var relation = new DefinitiveJoinRelation(
                0,
                1,
                "ParentId",
                "ChildId",
                AggregateBuildOptions.None,
                null,
                sourceFields);
            sourceFields.Add("LateField");

            Assert.Null(relation.TargetMember);
            Assert.Equal(new[] { "ChildId" }, relation.SourceFields);
        }

        private sealed class RelationTarget {
            public string ReadOnlyValue { get; } = String.Empty;

            public void SetValue() {
            }
        }
    }

    internal static class DefinitiveJoinPlanTestFactory {
        public static DefinitiveJoinPlan CreateFor(Type rootType, AggregateJoinShape shape = AggregateJoinShape.ScalarAggregatesOnly, string columnName = "RID") {
            return Create(rootType, shape: shape, columnName: columnName);
        }

        public static DefinitiveJoinPlan CreateForAliases(IEnumerable<KeyValuePair<AggregatePath, string>> aliasesByPath) {
            return Create(typeof(GuidRoot), aliasesByPath: aliasesByPath);
        }

        public static DefinitiveJoinPlan Create(
            Type rootType,
            IEnumerable<DefinitiveJoinTable>? tables = null,
            IEnumerable<DefinitiveProjectionColumn>? projection = null,
            IEnumerable<KeyValuePair<AggregatePath, string>>? aliasesByPath = null,
            IEnumerable<KeyValuePair<string, int>>? tableIndicesByAlias = null,
            int rootTableIndex = 0,
            AggregateJoinShape shape = AggregateJoinShape.ScalarAggregatesOnly,
            string columnName = "RID") {
            MemberInfo member = GetIdentifierMember(rootType);
            tables ??= new[] { CreateTable(rootType, "root", member, new[] { columnName }, 0, "root_RID", columnName) };
            projection ??= new[] { new DefinitiveProjectionColumn(0, 0, columnName, "root_RID", member) };
            aliasesByPath ??= new Dictionary<AggregatePath, string> {
                [new AggregatePath(new[] { "Root" })] = "root"
            };
            tableIndicesByAlias ??= new Dictionary<string, int> {
                ["root"] = 0
            };

            return new DefinitiveJoinPlan(
                rootType,
                shape,
                rootTableIndex,
                tables,
                Array.Empty<DefinitiveJoinRelation>(),
                projection,
                aliasesByPath,
                tableIndicesByAlias,
                new RootOrderingRequirement(0, columnName, 0, "root_RID"),
                1);
        }

        public static DefinitiveJoinTable CreateTable(Type entityType, string alias, MemberInfo identifierMember, IEnumerable<string> projectedColumns, int projectionOrdinal = 0, string resultAlias = "root_RID", string columnName = "RID") {
            return CreateTable(entityType, alias, alias, identifierMember, projectedColumns, projectionOrdinal, resultAlias, columnName);
        }

        public static DefinitiveJoinTable CreateTable(Type entityType, string alias, string prefix, MemberInfo identifierMember, IEnumerable<string> projectedColumns, int projectionOrdinal = 0, string resultAlias = "root_RID", string columnName = "RID") {
            return new DefinitiveJoinTable(
                entityType,
                entityType.Name,
                alias,
                prefix,
                JoinType.LEFT,
                null,
                new DefinitiveIdentifier(identifierMember, columnName, projectionOrdinal, resultAlias),
                projectedColumns);
        }

        public static MemberInfo GetIdentifierMember(Type type) {
            return type.GetProperty(nameof(PlanDataObject<Guid>.Id)) ?? throw new InvalidOperationException("Expected identifier property.");
        }
    }
}
