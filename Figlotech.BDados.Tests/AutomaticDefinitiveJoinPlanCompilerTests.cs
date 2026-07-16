using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using Figlotech.BDados.Helpers;
using Figlotech.Core.Helpers;
using Xunit;

namespace Figlotech.BDados.Tests {
    public class AutomaticDefinitiveJoinPlanCompilerTests {
        [Fact]
        public void CompileAutomaticRootCreatesTypedRootTableAndEmptySemanticPath() {
            DefinitiveJoinPlan plan = DefinitiveJoinPlanCompiler.Compile(typeof(GuidRoot), AggregateJoinShape.ScalarAggregatesOnly);

            Assert.Equal(typeof(GuidRoot), plan.RootType);
            Assert.Equal(0, plan.RootTableIndex);
            Assert.Equal(4, plan.Tables.Length);
            Assert.Equal(typeof(GuidRoot), plan.Tables[0].EntityType);
            Assert.Equal("tba", plan.Tables[0].Alias);
            Assert.Equal("tba", plan.Tables[0].Prefix);
            Assert.Equal(typeof(Guid), plan.Tables[0].Identifier.ClrType);
            Assert.Equal(nameof(GuidRoot.Id), plan.Tables[0].Identifier.ColumnName);
            Assert.Equal("tba_Id", plan.Tables[0].Identifier.ResultAlias);
            Assert.Equal("tba", plan.AliasByPath[new AggregatePath(Array.Empty<string>())]);
            Assert.Equal(plan.Tables[0].Identifier.ProjectionOrdinal, plan.RootOrdering.ProjectionOrdinal);
            Assert.Equal(plan.Projection.Select((column, ordinal) => ordinal), plan.Projection.Select(column => column.Ordinal));
        }

        [Fact]
        public void CompileAutomaticScalarGraphHasLegacyAliasesFarFieldRelationsAndNoObjectListPaths() {
            DefinitiveJoinPlan plan = DefinitiveJoinPlanCompiler.Compile(typeof(GuidRoot), AggregateJoinShape.ScalarAggregatesOnly);

            Assert.Equal(new[] { typeof(GuidRoot), typeof(ScalarAggregate), typeof(IntermediateAggregate), typeof(FarAggregate) }, plan.Tables.Select(x => x.EntityType));
            Assert.Equal(new[] { "tba", "tbb", "tbc", "tbd" }, plan.Tables.Select(x => x.Alias));
            Assert.Equal("tbb", plan.AliasByPath[new AggregatePath(new[] { nameof(GuidRoot.AggregateName) })]);
            Assert.Equal("tbd", plan.AliasByPath[new AggregatePath(new[] { nameof(GuidRoot.FarAggregateName) })]);
            Assert.DoesNotContain(new AggregatePath(new[] { nameof(GuidRoot.AggregateObject) }), plan.AliasByPath.Keys);
            Assert.DoesNotContain(new AggregatePath(new[] { nameof(GuidRoot.AggregateList) }), plan.AliasByPath.Keys);
            Assert.DoesNotContain(plan.Relations, x => x.BuildKind == AggregateBuildOptions.AggregateObject || x.BuildKind == AggregateBuildOptions.AggregateList);
            Assert.Contains(plan.Relations, x => x.BuildKind == AggregateBuildOptions.AggregateField && x.TargetMember!.Name == nameof(GuidRoot.AggregateName) && x.SourceFields.Single() == nameof(ScalarAggregate.Name));
            Assert.Contains(plan.Relations, x => x.BuildKind == AggregateBuildOptions.AggregateField && x.TargetMember!.Name == nameof(GuidRoot.FarAggregateName) && x.SourceFields.Single() == nameof(FarAggregate.Name));
            Assert.Contains(plan.Tables[2].ProjectedColumns, x => x == nameof(IntermediateAggregate.FarAggregateId));
            Assert.Contains(plan.Tables[3].ProjectedColumns, x => x == nameof(FarAggregate.Name));
        }

        [Fact]
        public void CompileAutomaticScalarGraphEmitsExactBidirectionalPhysicalTopologyInStableOrder() {
            DefinitiveJoinPlan plan = DefinitiveJoinPlanCompiler.Compile(typeof(GuidRoot), AggregateJoinShape.ScalarAggregatesOnly);

            Assert.Equal(new[] {
                "0:1:ScalarAggregateId:Id:None:-",
                "1:0:Id:ScalarAggregateId:None:-",
                "0:1:ScalarAggregateId:Id:AggregateField:AggregateName",
                "0:2:IntermediateAggregateId:Id:None:-",
                "2:0:Id:IntermediateAggregateId:None:-",
                "2:3:FarAggregateId:Id:None:-",
                "3:2:Id:FarAggregateId:None:-",
                "0:3:Id:Id:AggregateField:FarAggregateName"
            }, RelationSignatures(plan));
            Assert.Equal(8, plan.Relations.Length);
        }

        [Fact]
        public void CompileAutomaticFullGraphAddsObjectAndListRelationsWithPreservedJoinKinds() {
            DefinitiveJoinPlan plan = DefinitiveJoinPlanCompiler.Compile(typeof(GuidRoot), AggregateJoinShape.FullGraph);

            Assert.Equal(new[] { "tba", "tbb", "tbc", "tbd", "tbe", "tbf" }, plan.Tables.Select(x => x.Alias));
            Assert.Equal(typeof(ObjectAggregate), plan.Tables[4].EntityType);
            Assert.Equal(typeof(ListAggregate), plan.Tables[5].EntityType);
            Assert.Equal(JoinType.LEFT, plan.Tables[4].JoinKind);
            Assert.Equal(JoinType.RIGHT, plan.Tables[5].JoinKind);
            Assert.Equal("tbe", plan.AliasByPath[new AggregatePath(new[] { nameof(GuidRoot.AggregateObject) })]);
            Assert.Equal("tbf", plan.AliasByPath[new AggregatePath(new[] { nameof(GuidRoot.AggregateList) })]);
            Assert.Contains(plan.Relations, x => x.BuildKind == AggregateBuildOptions.AggregateObject && x.TargetMember!.Name == nameof(GuidRoot.AggregateObject));
            Assert.Contains(plan.Relations, x => x.BuildKind == AggregateBuildOptions.AggregateList && x.TargetMember!.Name == nameof(GuidRoot.AggregateList));
            Assert.All(plan.Projection, x => Assert.Equal(x.Ordinal, plan.Projection[x.Ordinal].Ordinal));
        }

        [Fact]
        public void CompileAutomaticFullGraphEmitsExactAggregateForwardAndOrdinaryReverseTopology() {
            DefinitiveJoinPlan plan = DefinitiveJoinPlanCompiler.Compile(typeof(GuidRoot), AggregateJoinShape.FullGraph);

            Assert.Equal(new[] {
                "0:1:ScalarAggregateId:Id:None:-",
                "1:0:Id:ScalarAggregateId:None:-",
                "0:1:ScalarAggregateId:Id:AggregateField:AggregateName",
                "0:2:IntermediateAggregateId:Id:None:-",
                "2:0:Id:IntermediateAggregateId:None:-",
                "2:3:FarAggregateId:Id:None:-",
                "3:2:Id:FarAggregateId:None:-",
                "0:3:Id:Id:AggregateField:FarAggregateName",
                "0:4:ObjectAggregateId:Id:AggregateObject:AggregateObject",
                "4:0:Id:ObjectAggregateId:None:-",
                "0:5:Id:ParentId:AggregateList:AggregateList",
                "5:0:ParentId:Id:None:-"
            }, RelationSignatures(plan));
            Assert.Equal(12, plan.Relations.Length);
        }

        [Fact]
        public void CompileAutomaticListUsesChildRemoteFieldWhenItSharesTheParentIdentifierName() {
            DefinitiveJoinPlan plan = DefinitiveJoinPlanCompiler.Compile(typeof(CollidingListKeyRoot), AggregateJoinShape.FullGraph);

            DefinitiveJoinRelation relation = Assert.Single(plan.Relations.Where(x => x.BuildKind == AggregateBuildOptions.AggregateList));
            Assert.Equal(nameof(CollidingListKeyRoot.ParentReference), relation.ParentKey);
            Assert.Equal(nameof(CollidingListKeyChild.ParentReference), relation.ChildKey);
        }

        [Fact]
        public void CompileAutomaticListRejectsRemoteFieldMissingFromChildEvenWhenParentHasIt() {
            ArgumentException exception = Assert.Throws<ArgumentException>(() => DefinitiveJoinPlanCompiler.Compile(typeof(InvalidListRemoteFieldRoot), AggregateJoinShape.FullGraph));

            Assert.Contains(nameof(InvalidListRemoteFieldRoot), exception.Message);
            Assert.Contains(nameof(InvalidListRemoteFieldRoot.Children), exception.Message);
            Assert.Contains(nameof(InvalidListRemoteFieldRoot.DeceptiveParentField), exception.Message);
            Assert.Contains(nameof(ListAggregate), exception.Message);
            Assert.Contains("list remote field", exception.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void CompileAutomaticIsDeterministicAcrossUncachedCalls() {
            DefinitiveJoinPlan first = DefinitiveJoinPlanCompiler.Compile(typeof(GuidRoot), AggregateJoinShape.FullGraph);
            DefinitiveJoinPlan second = DefinitiveJoinPlanCompiler.Compile(typeof(GuidRoot), AggregateJoinShape.FullGraph);

            Assert.Equal(first.StructuralSignature, second.StructuralSignature);
            Assert.Equal(first.Tables.Select(x => x.Alias), second.Tables.Select(x => x.Alias));
            Assert.Equal(first.Projection.Select(x => x.ResultAlias), second.Projection.Select(x => x.ResultAlias));
            Assert.Equal(first.Projection.Select(x => x.Ordinal), second.Projection.Select(x => x.Ordinal));
        }

        [Fact]
        public void CompileAutomaticGuidRootProjectsExactDeterministicColumnsAndResultAliases() {
            DefinitiveJoinPlan plan = DefinitiveJoinPlanCompiler.Compile(typeof(GuidRoot), AggregateJoinShape.FullGraph);

            Assert.Equal(new[] {
                "tba_Id", "tba_ScalarAggregateId", "tba_IntermediateAggregateId", "tba_ObjectAggregateId",
                "tbb_Id", "tbb_Name", "tbc_Id", "tbc_FarAggregateId", "tbd_Id", "tbd_Name",
                "tbe_Id", "tbe_Name", "tbf_Id", "tbf_ParentId", "tbf_Name"
            }, plan.Projection.Select(x => x.ResultAlias));
            Assert.Equal(Enumerable.Range(0, 15), plan.Projection.Select(x => x.Ordinal));
        }

        [Fact]
        public void CompileAutomaticAllowsRootTypeToRepeatAsChildWhenRootOnlyEdgeStopsTraversal() {
            DefinitiveJoinPlan plan = DefinitiveJoinPlanCompiler.Compile(typeof(RootBoundedCycleParcel), AggregateJoinShape.FullGraph);

            Assert.Equal(new[] { typeof(RootBoundedCycleParcel), typeof(RootBoundedCyclePayment), typeof(RootBoundedCycleParcel) }, plan.Tables.Select(x => x.EntityType));
            Assert.Equal("tba", plan.AliasByPath[new AggregatePath(Array.Empty<string>())]);
            Assert.Equal("tbb", plan.AliasByPath[new AggregatePath(new[] { nameof(RootBoundedCycleParcel.Payment) })]);
            Assert.Equal("tbc", plan.AliasByPath[new AggregatePath(new[] { nameof(RootBoundedCycleParcel.Payment), nameof(RootBoundedCyclePayment.Parcels) })]);
            Assert.DoesNotContain(plan.AliasByPath.Keys, x => x.Segments.SequenceEqual(new[] { nameof(RootBoundedCycleParcel.Payment), nameof(RootBoundedCyclePayment.Parcels), nameof(RootBoundedCycleParcel.Payment) }));
            Assert.Single(plan.Relations.Where(x => x.BuildKind == AggregateBuildOptions.AggregateObject));
            Assert.Single(plan.Relations.Where(x => x.BuildKind == AggregateBuildOptions.AggregateList));
        }

        [Fact]
        public void CompileAutomaticFullGraphRejectsCyclesButScalarGraphDoesNotTraverseThem() {
            ArgumentException exception = Assert.Throws<ArgumentException>(() => DefinitiveJoinPlanCompiler.Compile(typeof(CyclicAggregateA), AggregateJoinShape.FullGraph));
            DefinitiveJoinPlan scalar = DefinitiveJoinPlanCompiler.Compile(typeof(CyclicAggregateA), AggregateJoinShape.ScalarAggregatesOnly);

            Assert.Contains(nameof(CyclicAggregateA), exception.Message);
            Assert.Contains(nameof(CyclicAggregateB), exception.Message);
            Assert.Contains("AggregateB.AggregateA", exception.Message);
            Assert.Single(scalar.Tables);
        }

        [Fact]
        public void AliasAllocatorReusesNormalizedRelationshipsAndSnapshotsBindings() {
            var first = new DefinitiveAliasAllocator();
            var second = new DefinitiveAliasAllocator();
            string firstAlias = first.GetAlias("Root", typeof(GuidRoot), "Key");
            string sameAlias = first.GetAlias("root", typeof(GuidRoot), "key");
            string distinctAlias = first.GetAlias("root", typeof(GuidRoot), "OtherKey");

            Assert.Equal("tba", firstAlias);
            Assert.Equal(firstAlias, sameAlias);
            Assert.NotEqual(firstAlias, distinctAlias);
            Assert.Equal(firstAlias, second.GetAlias("root", typeof(GuidRoot), "key"));
            var path = new AggregatePath(new[] { "One" });
            first.Bind(path, firstAlias);
            first.Bind(new AggregatePath(new[] { "Two" }), firstAlias);
            Assert.Throws<ArgumentException>(() => first.Bind(path, distinctAlias));
            var snapshot = first.SnapshotAliasesByPath();
            Assert.Equal(firstAlias, snapshot[path]);
            Assert.Equal(firstAlias, snapshot[new AggregatePath(new[] { "Two" })]);
        }

        [Fact]
        public void AliasAllocatorSnapshotIsDetachedAndMatchesLegacyBase26SequencePastTwentySix() {
            var allocator = new DefinitiveAliasAllocator();
            string[] aliases = Enumerable.Range(0, 28).Select(index => allocator.GetAlias("root", typeof(GuidRoot), "Key" + index)).ToArray();
            var path = new AggregatePath(new[] { "Frozen" });
            allocator.Bind(path, aliases[0]);
            var snapshot = allocator.SnapshotAliasesByPath();
            allocator.Bind(new AggregatePath(new[] { "Later" }), aliases[1]);

            Assert.Equal(new[] {
                "tba", "tbb", "tbc", "tbd", "tbe", "tbf", "tbg", "tbh", "tbi", "tbj", "tbk", "tbl", "tbm", "tbn",
                "tbo", "tbp", "tbq", "tbr", "tbs", "tbt", "tbu", "tbv", "tbw", "tbx", "tby", "tbz", "tbba", "tbbb"
            }, aliases);
            Assert.Single(snapshot);
            Assert.Equal(aliases[0], snapshot[path]);
        }

        [Fact]
        public void CompileAutomaticRejectsInvalidAggregateMetadataBeforePublishingPlan() {
            ArgumentException exception = Assert.Throws<ArgumentException>(() => DefinitiveJoinPlanCompiler.Compile(typeof(InvalidAggregateRoot), AggregateJoinShape.FullGraph));

            Assert.Contains(nameof(InvalidAggregateRoot.Invalid), exception.Message);
            Assert.Contains("source", exception.Message, StringComparison.OrdinalIgnoreCase);
        }

        // C# attribute arguments cannot express a null System.Type; production RequireType guards remain necessary for malformed reflection metadata.
        [Theory]
        [InlineData(typeof(InvalidObjectKeyRoot), nameof(InvalidObjectKeyRoot.Invalid))]
        [InlineData(typeof(InvalidRemoteSourceRoot), nameof(InvalidRemoteSourceRoot.Invalid))]
        [InlineData(typeof(ReadOnlyAggregateTargetRoot), nameof(ReadOnlyAggregateTargetRoot.Invalid))]
        [InlineData(typeof(ObjectCollectionTargetRoot), nameof(ObjectCollectionTargetRoot.Invalid))]
        [InlineData(typeof(ObjectScalarTargetRoot), nameof(ObjectScalarTargetRoot.Invalid))]
        [InlineData(typeof(ObjectNonDataObjectTargetRoot), nameof(ObjectNonDataObjectTargetRoot.Invalid))]
        [InlineData(typeof(ListElementMismatchRoot), nameof(ListElementMismatchRoot.Invalid))]
        [InlineData(typeof(InvalidFarImmediateKeyRoot), nameof(InvalidFarImmediateKeyRoot.Invalid))]
        [InlineData(typeof(InvalidFarKeyRoot), nameof(InvalidFarKeyRoot.Invalid))]
        [InlineData(typeof(InvalidFarSourceRoot), nameof(InvalidFarSourceRoot.Invalid))]
        public void CompileAutomaticInvalidMetadataNamesRootTypeAndFullMemberContext(Type rootType, string memberName) {
            ArgumentException exception = Assert.Throws<ArgumentException>(() => DefinitiveJoinPlanCompiler.Compile(rootType, AggregateJoinShape.FullGraph));

            Assert.Contains(rootType.Name, exception.Message);
            Assert.Contains(memberName, exception.Message);
        }

        [Fact]
        public void CompileAutomaticReusesOnePhysicalRelationshipForMultipleSemanticDestinations() {
            DefinitiveJoinPlan plan = DefinitiveJoinPlanCompiler.Compile(typeof(RepeatedRelationRoot), AggregateJoinShape.ScalarAggregatesOnly);

            Assert.Equal(2, plan.Tables.Length);
            Assert.Equal("tbb", plan.AliasByPath[new AggregatePath(new[] { nameof(RepeatedRelationRoot.FirstName) })]);
            Assert.Equal("tbb", plan.AliasByPath[new AggregatePath(new[] { nameof(RepeatedRelationRoot.SecondName) })]);
            Assert.Equal(2, plan.Relations.Count(x => x.BuildKind == AggregateBuildOptions.AggregateField));
        }

        [Fact]
        public void CompileAutomaticPromotesSharedScalarObjectEdgeWithoutDroppingScalarTargetOrReverseTopology() {
            DefinitiveJoinPlan plan = DefinitiveJoinPlanCompiler.Compile(typeof(SharedScalarObjectEdgeRoot), AggregateJoinShape.FullGraph);

            Assert.Equal(2, plan.Tables.Length);
            Assert.Equal(new[] {
                "0:1:SharedId:Id:AggregateObject:Aggregate",
                "1:0:Id:SharedId:None:-",
                "0:1:SharedId:Id:AggregateField:AggregateName"
            }, RelationSignatures(plan));
            Assert.Equal(new[] { nameof(PlanDataObject<Guid>.Id), nameof(ScalarAggregate.Name) }, plan.Tables[1].ProjectedColumns);
        }

        [Fact]
        public void CompileAutomaticRetainsOneAggregateRelationPerObjectTargetSharingAnEdge() {
            DefinitiveJoinPlan plan = DefinitiveJoinPlanCompiler.Compile(typeof(MultipleObjectTargetsRoot), AggregateJoinShape.FullGraph);

            Assert.Equal(2, plan.Tables.Length);
            Assert.Equal(1, plan.Relations.Count(x => x.BuildKind == AggregateBuildOptions.None));
            Assert.Equal(new[] { nameof(MultipleObjectTargetsRoot.First), nameof(MultipleObjectTargetsRoot.Second) }, plan.Relations.Where(x => x.BuildKind == AggregateBuildOptions.AggregateObject).Select(x => x.TargetMember!.Name));
        }

        [Fact]
        public void CompileAutomaticUsesDistinctAliasesForSameChildTypeOnDistinctSiblingKeysWithoutCycle() {
            DefinitiveJoinPlan plan = DefinitiveJoinPlanCompiler.Compile(typeof(DistinctSiblingRelationshipRoot), AggregateJoinShape.ScalarAggregatesOnly);

            Assert.Equal(new[] { "tba", "tbb", "tbc" }, plan.Tables.Select(x => x.Alias));
            Assert.Equal("tbb", plan.AliasByPath[new AggregatePath(new[] { nameof(DistinctSiblingRelationshipRoot.FirstName) })]);
            Assert.Equal("tbc", plan.AliasByPath[new AggregatePath(new[] { nameof(DistinctSiblingRelationshipRoot.SecondName) })]);
        }

        [Fact]
        public void CompileAutomaticHonorsRootAndChildFlags() {
            DefinitiveJoinPlan plan = DefinitiveJoinPlanCompiler.Compile(typeof(FlagRoot), AggregateJoinShape.FullGraph);

            Assert.Contains(plan.Relations, x => x.TargetMember?.DeclaringType == typeof(FlagRoot) && x.TargetMember.Name == nameof(FlagRoot.RootOnly));
            Assert.DoesNotContain(plan.Relations, x => x.TargetMember?.DeclaringType == typeof(FlagRoot) && x.TargetMember.Name == nameof(FlagRoot.ChildOnly));
            Assert.Contains(plan.Relations, x => x.TargetMember?.DeclaringType == typeof(FlagChild) && x.TargetMember.Name == nameof(FlagChild.ChildOnly));
            Assert.DoesNotContain(plan.Relations, x => x.TargetMember?.DeclaringType == typeof(FlagChild) && x.TargetMember.Name == nameof(FlagChild.RootOnly));
        }

        [Fact]
        public void CompileAutomaticIdentifierlessChildDuringDirectJoinInversionNamesAutomaticContext() {
            ArgumentException exception = Assert.Throws<ArgumentException>(() => DefinitiveJoinPlanCompiler.Compile(typeof(IdentifierlessListRoot), AggregateJoinShape.FullGraph));

            AssertAutomaticIdentifierContext(exception, typeof(IdentifierlessListRoot), nameof(IdentifierlessListRoot.Children), nameof(IdentifierlessListRoot.Children), typeof(IdentifierlessListChild));
        }

        [Fact]
        public void CompileAutomaticIdentifierlessNestedParentDuringJoinKeyResolutionNamesAutomaticContext() {
            ArgumentException exception = Assert.Throws<ArgumentException>(() => DefinitiveJoinPlanCompiler.Compile(typeof(NestedIdentifierlessRoot), AggregateJoinShape.FullGraph));

            AssertAutomaticIdentifierContext(exception, typeof(NestedIdentifierlessRoot), nameof(NestedIdentifierlessRoot.Children) + "." + nameof(NestedIdentifierlessParent.Grandchild), nameof(NestedIdentifierlessParent.Grandchild), typeof(NestedIdentifierlessChild));
        }

        [Theory]
        [InlineData(typeof(SharedNestedObjectRoot), nameof(SharedNestedObjectRoot.First), nameof(SharedNestedObjectRoot.Second))]
        [InlineData(typeof(SharedNestedListRoot), nameof(SharedNestedListRoot.First), nameof(SharedNestedListRoot.Second))]
        public void CompileAutomaticDeduplicatesNestedMaterializationForReusedPhysicalParentAliases(Type rootType, string firstPath, string secondPath) {
            DefinitiveJoinPlan plan = DefinitiveJoinPlanCompiler.Compile(rootType, AggregateJoinShape.FullGraph);

            Assert.Equal(5, plan.Tables.Length);
            DefinitiveJoinRelation[] rootTargets = plan.Relations.Where(x => x.ParentTableIndex == 0 && x.ChildTableIndex == 1 && x.BuildKind != AggregateBuildOptions.None).ToArray();
            Assert.Equal(new[] { firstPath, secondPath }, rootTargets.Select(x => x.TargetMember.Name));
            Assert.Equal(1, rootTargets.Count(x => x.TargetMember.Name == firstPath));
            Assert.Equal(1, rootTargets.Count(x => x.TargetMember.Name == secondPath));
            Assert.Equal(1, plan.Relations.Count(x => x.ParentTableIndex == 1 && x.ChildTableIndex == 0 && x.BuildKind == AggregateBuildOptions.None));
            Assert.Equal(1, plan.Relations.Count(x => x.ParentTableIndex == 1 && x.ChildTableIndex == 2 && x.BuildKind == AggregateBuildOptions.AggregateField));
            Assert.Equal(1, plan.Relations.Count(x => x.ParentTableIndex == 1 && x.ChildTableIndex == 3 && x.BuildKind == AggregateBuildOptions.AggregateObject));
            Assert.Equal(1, plan.Relations.Count(x => x.ParentTableIndex == 1 && x.ChildTableIndex == 4 && x.BuildKind == AggregateBuildOptions.AggregateList));
            Assert.Equal(1, plan.Relations.Count(x => x.ParentTableIndex == 2 && x.ChildTableIndex == 1 && x.BuildKind == AggregateBuildOptions.None));
            Assert.Equal(1, plan.Relations.Count(x => x.ParentTableIndex == 3 && x.ChildTableIndex == 1 && x.BuildKind == AggregateBuildOptions.None));
            Assert.Equal(1, plan.Relations.Count(x => x.ParentTableIndex == 4 && x.ChildTableIndex == 1 && x.BuildKind == AggregateBuildOptions.None));
            Assert.Equal(10, plan.Relations.Length);

            Assert.Equal("tbb", plan.AliasByPath[new AggregatePath(new[] { firstPath })]);
            Assert.Equal("tbb", plan.AliasByPath[new AggregatePath(new[] { secondPath })]);
            Assert.Equal("tbc", plan.AliasByPath[new AggregatePath(new[] { firstPath, nameof(SharedNestedParent.NestedName) })]);
            Assert.Equal("tbc", plan.AliasByPath[new AggregatePath(new[] { secondPath, nameof(SharedNestedParent.NestedName) })]);
            Assert.Equal("tbd", plan.AliasByPath[new AggregatePath(new[] { firstPath, nameof(SharedNestedParent.NestedObject) })]);
            Assert.Equal("tbd", plan.AliasByPath[new AggregatePath(new[] { secondPath, nameof(SharedNestedParent.NestedObject) })]);
            Assert.Equal("tbe", plan.AliasByPath[new AggregatePath(new[] { firstPath, nameof(SharedNestedParent.NestedList) })]);
            Assert.Equal("tbe", plan.AliasByPath[new AggregatePath(new[] { secondPath, nameof(SharedNestedParent.NestedList) })]);
        }

        [Fact]
        public void CompileAutomaticHighCardinalityDistinctObjectAndListEdgesPreservesExactTopology() {
            DefinitiveJoinPlan plan = DefinitiveJoinPlanCompiler.Compile(typeof(HighCardinalityRoot), AggregateJoinShape.FullGraph);

            Assert.Equal(17, plan.Tables.Length);
            Assert.Equal(32, plan.Relations.Length);
            Assert.Equal(8, plan.Relations.Count(x => x.BuildKind == AggregateBuildOptions.AggregateObject));
            Assert.Equal(8, plan.Relations.Count(x => x.BuildKind == AggregateBuildOptions.AggregateList));
            Assert.Equal(16, plan.Relations.Count(x => x.BuildKind == AggregateBuildOptions.None));
            Assert.Equal(16, plan.Relations.Where(x => x.BuildKind != AggregateBuildOptions.None).Select(x => x.ChildTableIndex).Distinct().Count());
            Assert.Equal(16, plan.Relations.Where(x => x.BuildKind == AggregateBuildOptions.None).Select(x => x.ParentTableIndex).Distinct().Count());
            Assert.Equal(17, plan.Tables.Select(x => x.Alias).Distinct(StringComparer.OrdinalIgnoreCase).Count());
        }

        [Fact]
        public void CompileAutomaticUsesLegacyEffectiveOverrideMemberAndInheritedAggregateAttribute() {
            MemberInfo legacyMember = ReflectionTool.FieldsAndPropertiesOf(typeof(OverrideEffectiveAggregateRoot))
                .Single(x => x.Name == nameof(OverrideEffectiveAggregateRoot.Aggregate));
            DefinitiveJoinPlan plan = DefinitiveJoinPlanCompiler.Compile(typeof(OverrideEffectiveAggregateRoot), AggregateJoinShape.FullGraph);
            DefinitiveJoinRelation relation = Assert.Single(plan.Relations.Where(x => x.BuildKind == AggregateBuildOptions.AggregateObject));

            Assert.Equal(typeof(OverrideEffectiveAggregateRoot), legacyMember.DeclaringType);
            Assert.Equal(legacyMember.DeclaringType, relation.TargetMember.DeclaringType);
            Assert.Equal(legacyMember.Name, relation.TargetMember.Name);
            Assert.Equal(2, plan.Tables.Length);
        }

        [Fact]
        public void CompileAutomaticUsesOneLegacyEffectiveHiddenMemberForProjectionAndAggregation() {
            MemberInfo legacyColumn = ReflectionTool.FieldsAndPropertiesOf(typeof(HiddenEffectiveAggregateRoot))
                .Single(x => x.Name == nameof(HiddenEffectiveAggregateRoot.ShadowedColumn));
            MemberInfo legacyAggregate = ReflectionTool.FieldsAndPropertiesOf(typeof(HiddenEffectiveAggregateRoot))
                .Single(x => x.Name == nameof(HiddenEffectiveAggregateRoot.ShadowedAggregate));
            DefinitiveJoinPlan plan = DefinitiveJoinPlanCompiler.Compile(typeof(HiddenEffectiveAggregateRoot), AggregateJoinShape.FullGraph);

            Assert.Equal(typeof(HiddenEffectiveAggregateRoot), legacyColumn.DeclaringType);
            Assert.Equal(typeof(HiddenEffectiveAggregateRoot), legacyAggregate.DeclaringType);
            DefinitiveProjectionColumn projection = Assert.Single(plan.Projection.Where(x => x.TableIndex == 0 && x.SourceColumn == nameof(HiddenEffectiveAggregateRoot.ShadowedColumn)));
            Assert.Equal(legacyColumn.DeclaringType, projection.DestinationMember.DeclaringType);
            Assert.Equal(legacyColumn.Name, projection.DestinationMember.Name);
            Assert.Single(plan.Tables);
            Assert.DoesNotContain(plan.Relations, x => x.BuildKind == AggregateBuildOptions.AggregateObject && x.TargetMember.Name == nameof(HiddenEffectiveAggregateRoot.ShadowedAggregate));
        }

        [Fact]
        public void CompileAutomaticTreatsWhitespaceOnlyFlagsAsFullAndRejectsUnknownNonBlankFlags() {
            DefinitiveJoinPlan plan = DefinitiveJoinPlanCompiler.Compile(typeof(WhitespaceFlagRoot), AggregateJoinShape.ScalarAggregatesOnly);

            Assert.Contains(plan.Relations, x => x.BuildKind == AggregateBuildOptions.AggregateField && x.TargetMember.Name == nameof(WhitespaceFlagRoot.Name));
            ArgumentException exception = Assert.Throws<ArgumentException>(() => DefinitiveJoinPlanCompiler.Compile(typeof(UnknownFlagRoot), AggregateJoinShape.ScalarAggregatesOnly));
            Assert.Contains("unsupported", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("unknown", exception.Message, StringComparison.OrdinalIgnoreCase);
        }

        private static string[] RelationSignatures(DefinitiveJoinPlan plan) {
            return plan.Relations.Select(x => $"{x.ParentTableIndex}:{x.ChildTableIndex}:{x.ParentKey}:{x.ChildKey}:{x.BuildKind}:{x.TargetMember?.Name ?? "-"}").ToArray();
        }

        private static void AssertAutomaticIdentifierContext(ArgumentException exception, Type rootType, string path, string member, Type entityType) {
            Assert.Contains(rootType.Name, exception.Message);
            Assert.Contains(path, exception.Message);
            Assert.Contains(member, exception.Message);
            Assert.Contains(entityType.Name, exception.Message);
            Assert.Contains("identifier", exception.Message, StringComparison.OrdinalIgnoreCase);
        }
    }
}
