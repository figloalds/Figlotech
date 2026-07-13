using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Figlotech.BDados.DataAccessAbstractions;
using Xunit;

namespace Figlotech.BDados.Tests {
    public class CompiledAggregateMaterializerPlanTests {
        [Fact]
        public void GetOrCreateCachesCanonicalPlanAndExposesOnlyDiagnosticMetadata() {
            DefinitiveJoinPlan source = AutomaticJoinPlanCache.GetOrAdd(typeof(GuidRoot), AggregateJoinShape.FullGraph);

            Assert.Throws<ArgumentNullException>(() => CompiledAggregateMaterializerPlan.GetOrCreate(null!));
            CompiledAggregateMaterializerPlan first = CompiledAggregateMaterializerPlan.GetOrCreate(source);
            CompiledAggregateMaterializerPlan second = CompiledAggregateMaterializerPlan.GetOrCreate(source);

            Assert.Same(first, second);
            Assert.Equal(source.StructuralSignature, first.PlanStructuralSignature);
            Assert.Equal(source.RootType, first.RootType);
            Assert.Equal(source.Shape, first.Shape);
            Assert.Equal(source.Tables.Length, first.TableCount);
            Assert.Empty(typeof(CompiledAggregateMaterializerPlan).GetFields(BindingFlags.Public | BindingFlags.Instance));
            Assert.All(typeof(CompiledAggregateMaterializerPlan).GetProperties(BindingFlags.Public | BindingFlags.Instance), property => {
                Assert.False(property.PropertyType.IsArray);
                Assert.False(typeof(Delegate).IsAssignableFrom(property.PropertyType));
                Assert.False(typeof(System.Collections.IList).IsAssignableFrom(property.PropertyType));
                Assert.False(typeof(System.Collections.IDictionary).IsAssignableFrom(property.PropertyType));
            });
            Assert.All(typeof(CompiledAggregateMaterializerPlan).GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly), method => {
                Assert.False(method.ReturnType.IsArray);
                Assert.False(typeof(Delegate).IsAssignableFrom(method.ReturnType));
                Assert.False(IsMutableCollection(method.ReturnType));
            });
        }

        [Fact]
        public void MaterializeFullGraphUsesProjectionOrdinalsAndNormalizedConversions() {
            DefinitiveJoinPlan source = AutomaticJoinPlanCache.GetOrAdd(typeof(GuidRoot), AggregateJoinShape.FullGraph);
            CompiledAggregateMaterializerPlan compiled = CompiledAggregateMaterializerPlan.GetOrCreate(source);
            Guid rootId = Guid.Parse("11111111-1111-1111-1111-111111111111");
            Guid scalarId = Guid.Parse("22222222-2222-2222-2222-222222222222");
            Guid intermediateId = Guid.Parse("33333333-3333-3333-3333-333333333333");
            Guid farId = Guid.Parse("44444444-4444-4444-4444-444444444444");
            Guid objectId = Guid.Parse("55555555-5555-5555-5555-555555555555");
            Guid childId = Guid.Parse("66666666-6666-6666-6666-666666666666");

            object[] row = Row(source,
                (source.RootTableIndex, nameof(GuidRoot.Id), rootId.ToString("D")),
                (source.RootTableIndex, nameof(GuidRoot.ScalarAggregateId), scalarId.ToString("D")),
                (source.RootTableIndex, nameof(GuidRoot.IntermediateAggregateId), intermediateId.ToString("D")),
                (source.RootTableIndex, nameof(GuidRoot.ObjectAggregateId), objectId.ToString("D")),
                (TableIndex(source, typeof(ScalarAggregate)), nameof(ScalarAggregate.Id), scalarId),
                (TableIndex(source, typeof(ScalarAggregate)), nameof(ScalarAggregate.Name), 42),
                (TableIndex(source, typeof(IntermediateAggregate)), nameof(IntermediateAggregate.Id), intermediateId),
                (TableIndex(source, typeof(IntermediateAggregate)), nameof(IntermediateAggregate.FarAggregateId), farId),
                (TableIndex(source, typeof(FarAggregate)), nameof(FarAggregate.Id), farId),
                (TableIndex(source, typeof(FarAggregate)), nameof(FarAggregate.Name), 84),
                (TableIndex(source, typeof(ObjectAggregate)), nameof(ObjectAggregate.Id), objectId),
                (TableIndex(source, typeof(ObjectAggregate)), nameof(ObjectAggregate.Name), 126),
                (TableIndex(source, typeof(ListAggregate)), nameof(ListAggregate.Id), childId),
                (TableIndex(source, typeof(ListAggregate)), nameof(ListAggregate.ParentId), rootId),
                (TableIndex(source, typeof(ListAggregate)), nameof(ListAggregate.Name), "first"));

            GuidRoot root = Assert.Single(compiled.Materialize<GuidRoot>(new[] { row }));

            Assert.Equal(rootId, root.Id);
            Assert.Equal(scalarId, root.ScalarAggregateId);
            Assert.Equal(intermediateId, root.IntermediateAggregateId);
            Assert.Equal(objectId, root.ObjectAggregateId);
            Assert.Equal("42", root.AggregateName);
            Assert.Equal("84", root.FarAggregateName);
            Assert.NotNull(root.AggregateObject);
            Assert.Equal(objectId, root.AggregateObject!.Id);
            Assert.Equal("126", root.AggregateObject.Name);
            ListAggregate child = Assert.Single(root.AggregateList);
            Assert.Equal(childId, child.Id);
            Assert.Equal(rootId, child.ParentId);
            Assert.Equal("first", child.Name);
        }

        [Fact]
        public void MaterializeDeduplicatesTypedGuidIdentityAndPreservesOrderedDistinctListChildren() {
            DefinitiveJoinPlan source = AutomaticJoinPlanCache.GetOrAdd(typeof(GuidRoot), AggregateJoinShape.FullGraph);
            CompiledAggregateMaterializerPlan compiled = CompiledAggregateMaterializerPlan.GetOrCreate(source);
            Guid rootId = Guid.Parse("a1111111-1111-1111-1111-111111111111");
            Guid objectId = Guid.Parse("a2222222-2222-2222-2222-222222222222");
            Guid firstChildId = Guid.Parse("a3333333-3333-3333-3333-333333333333");
            Guid secondChildId = Guid.Parse("a4444444-4444-4444-4444-444444444444");

            object[] first = Row(source,
                (source.RootTableIndex, nameof(GuidRoot.Id), rootId),
                (source.RootTableIndex, nameof(GuidRoot.ObjectAggregateId), objectId),
                (TableIndex(source, typeof(ObjectAggregate)), nameof(ObjectAggregate.Id), objectId),
                (TableIndex(source, typeof(ObjectAggregate)), nameof(ObjectAggregate.Name), "object"),
                (TableIndex(source, typeof(ListAggregate)), nameof(ListAggregate.Id), firstChildId),
                (TableIndex(source, typeof(ListAggregate)), nameof(ListAggregate.ParentId), rootId),
                (TableIndex(source, typeof(ListAggregate)), nameof(ListAggregate.Name), "one"));
            object[] duplicate = Row(source,
                (source.RootTableIndex, nameof(GuidRoot.Id), rootId.ToString("D")),
                (source.RootTableIndex, nameof(GuidRoot.ObjectAggregateId), objectId),
                (TableIndex(source, typeof(ObjectAggregate)), nameof(ObjectAggregate.Id), objectId),
                (TableIndex(source, typeof(ObjectAggregate)), nameof(ObjectAggregate.Name), "object"),
                (TableIndex(source, typeof(ListAggregate)), nameof(ListAggregate.Id), firstChildId.ToString("D")),
                (TableIndex(source, typeof(ListAggregate)), nameof(ListAggregate.ParentId), rootId),
                (TableIndex(source, typeof(ListAggregate)), nameof(ListAggregate.Name), "one"));
            object[] second = Row(source,
                (source.RootTableIndex, nameof(GuidRoot.Id), rootId),
                (source.RootTableIndex, nameof(GuidRoot.ObjectAggregateId), objectId),
                (TableIndex(source, typeof(ObjectAggregate)), nameof(ObjectAggregate.Id), objectId),
                (TableIndex(source, typeof(ListAggregate)), nameof(ListAggregate.Id), secondChildId),
                (TableIndex(source, typeof(ListAggregate)), nameof(ListAggregate.ParentId), rootId),
                (TableIndex(source, typeof(ListAggregate)), nameof(ListAggregate.Name), "two"));

            GuidRoot root = Assert.Single(compiled.Materialize<GuidRoot>(new[] { first, duplicate, second }));

            Assert.NotNull(root.AggregateObject);
            Assert.Equal(objectId, root.AggregateObject!.Id);
            Assert.Equal(new[] { firstChildId, secondChildId }, root.AggregateList.Select(child => child.Id));
            Assert.Equal(new[] { "one", "two" }, root.AggregateList.Select(child => child.Name));
        }

        [Fact]
        public void MaterializeSkipsNullIdentifiersAndLeavesNonNullableDefaultsUntouched() {
            DefinitiveJoinPlan source = AutomaticJoinPlanCache.GetOrAdd(typeof(GuidRoot), AggregateJoinShape.FullGraph);
            CompiledAggregateMaterializerPlan compiled = CompiledAggregateMaterializerPlan.GetOrCreate(source);
            Guid rootId = Guid.Parse("b1111111-1111-1111-1111-111111111111");
            object[] noRoot = Row(source,
                (source.RootTableIndex, nameof(GuidRoot.Id), DBNull.Value),
                (TableIndex(source, typeof(ListAggregate)), nameof(ListAggregate.Id), Guid.NewGuid()),
                (TableIndex(source, typeof(ListAggregate)), nameof(ListAggregate.ParentId), Guid.NewGuid()));
            object[] nullChildren = Row(source,
                (source.RootTableIndex, nameof(GuidRoot.Id), rootId),
                (source.RootTableIndex, nameof(GuidRoot.ScalarAggregateId), DBNull.Value),
                (TableIndex(source, typeof(ObjectAggregate)), nameof(ObjectAggregate.Id), DBNull.Value),
                (TableIndex(source, typeof(ObjectAggregate)), nameof(ObjectAggregate.Name), "payload"),
                (TableIndex(source, typeof(ListAggregate)), nameof(ListAggregate.Id), DBNull.Value),
                (TableIndex(source, typeof(ListAggregate)), nameof(ListAggregate.Name), "payload"));

            GuidRoot root = Assert.Single(compiled.Materialize<GuidRoot>(new[] { noRoot, nullChildren }));

            Assert.Equal(rootId, root.Id);
            Assert.Equal(Guid.Empty, root.ScalarAggregateId);
            Assert.Null(root.AggregateName);
            Assert.Null(root.AggregateObject);
            Assert.Empty(root.AggregateList);
        }

        [Fact]
        public void MaterializeScalarShapeHasNoObjectOrListSideEffects() {
            DefinitiveJoinPlan source = AutomaticJoinPlanCache.GetOrAdd(typeof(GuidRoot), AggregateJoinShape.ScalarAggregatesOnly);
            CompiledAggregateMaterializerPlan compiled = CompiledAggregateMaterializerPlan.GetOrCreate(source);
            Guid rootId = Guid.Parse("c1111111-1111-1111-1111-111111111111");
            Guid scalarId = Guid.Parse("c2222222-2222-2222-2222-222222222222");

            GuidRoot root = Assert.Single(compiled.Materialize<GuidRoot>(new[] {
                Row(source,
                    (source.RootTableIndex, nameof(GuidRoot.Id), rootId),
                    (source.RootTableIndex, nameof(GuidRoot.ScalarAggregateId), scalarId),
                    (TableIndex(source, typeof(ScalarAggregate)), nameof(ScalarAggregate.Id), scalarId),
                    (TableIndex(source, typeof(ScalarAggregate)), nameof(ScalarAggregate.Name), 9001))
            }));

            Assert.Equal(rootId, root.Id);
            Assert.Equal("9001", root.AggregateName);
            Assert.Null(root.AggregateObject);
            Assert.Empty(root.AggregateList);
        }

        [Fact]
        public void MaterializeScalarAggregateDoesNotRequireRemotePublicConstructor() {
            DefinitiveJoinPlan source = AutomaticJoinPlanCache.GetOrAdd(typeof(PrivateConstructorScalarRoot), AggregateJoinShape.ScalarAggregatesOnly);
            CompiledAggregateMaterializerPlan compiled = CompiledAggregateMaterializerPlan.GetOrCreate(source);
            Guid rootId = Guid.Parse("c3333333-3333-3333-3333-333333333333");
            Guid remoteId = Guid.Parse("c4444444-4444-4444-4444-444444444444");

            PrivateConstructorScalarRoot root = Assert.Single(compiled.Materialize<PrivateConstructorScalarRoot>(new[] {
                Row(source,
                    (source.RootTableIndex, nameof(PrivateConstructorScalarRoot.Id), rootId),
                    (source.RootTableIndex, nameof(PrivateConstructorScalarRoot.RemoteScalarId), remoteId),
                    (TableIndex(source, typeof(PrivateConstructorScalarRemote)), nameof(PrivateConstructorScalarRemote.Id), remoteId),
                    (TableIndex(source, typeof(PrivateConstructorScalarRemote)), nameof(PrivateConstructorScalarRemote.Value), "remote value"))
            }));

            Assert.Equal(rootId, root.Id);
            Assert.Equal(remoteId, root.RemoteScalarId);
            Assert.Equal("remote value", root.RemoteValue);
        }

        [Fact]
        public void MaterializeKeepsRepeatedRelationTargetsSeparateByRelationIdentity() {
            DefinitiveJoinPlan objectSource = AutomaticJoinPlanCache.GetOrAdd(typeof(SharedNestedObjectRoot), AggregateJoinShape.FullGraph);
            CompiledAggregateMaterializerPlan objectCompiled = CompiledAggregateMaterializerPlan.GetOrCreate(objectSource);
            Guid objectRootId = Guid.Parse("d1111111-1111-1111-1111-111111111111");
            Guid sharedId = Guid.Parse("d2222222-2222-2222-2222-222222222222");
            Guid nestedId = Guid.Parse("d3333333-3333-3333-3333-333333333333");

            SharedNestedObjectRoot objectRoot = Assert.Single(objectCompiled.Materialize<SharedNestedObjectRoot>(new[] {
                Row(objectSource,
                    (objectSource.RootTableIndex, nameof(SharedNestedObjectRoot.Id), objectRootId),
                    (objectSource.RootTableIndex, nameof(SharedNestedObjectRoot.SharedId), sharedId),
                    (TableIndex(objectSource, typeof(SharedNestedParent)), nameof(SharedNestedParent.Id), sharedId),
                    (TableIndex(objectSource, typeof(SharedNestedParent)), nameof(SharedNestedParent.NestedObjectId), nestedId),
                    (TableIndex(objectSource, typeof(ObjectAggregate)), nameof(ObjectAggregate.Id), nestedId))
            }));

            Assert.NotNull(objectRoot.First);
            Assert.NotNull(objectRoot.Second);
            Assert.Equal(sharedId, objectRoot.First!.Id);
            Assert.Equal(sharedId, objectRoot.Second!.Id);

            DefinitiveJoinPlan listSource = AutomaticJoinPlanCache.GetOrAdd(typeof(SharedNestedListRoot), AggregateJoinShape.FullGraph);
            CompiledAggregateMaterializerPlan listCompiled = CompiledAggregateMaterializerPlan.GetOrCreate(listSource);
            Guid listRootId = Guid.Parse("d4444444-4444-4444-4444-444444444444");
            Guid childId = Guid.Parse("d5555555-5555-5555-5555-555555555555");
            SharedNestedListRoot listRoot = Assert.Single(listCompiled.Materialize<SharedNestedListRoot>(new[] {
                Row(listSource,
                    (listSource.RootTableIndex, nameof(SharedNestedListRoot.Id), listRootId),
                    (TableIndex(listSource, typeof(SharedNestedParent)), nameof(SharedNestedParent.Id), childId),
                    (TableIndex(listSource, typeof(SharedNestedParent)), nameof(SharedNestedParent.RootId), listRootId))
            }));

            Assert.Equal(new[] { childId }, listRoot.First.Select(child => child.Id));
            Assert.Equal(new[] { childId }, listRoot.Second.Select(child => child.Id));
        }

        [Fact]
        public void CompilationRejectsOwnerIncompatibleRelationTargetsAndAcceptsInheritedTargets() {
            MemberInfo incompatibleTarget = typeof(HiddenEffectiveAggregateRoot).GetProperty(nameof(HiddenEffectiveAggregateRoot.ShadowedAggregate))!;
            DefinitiveJoinPlan incompatible = ObjectRelationPlan(typeof(OverrideEffectiveAggregateRoot), incompatibleTarget);

            ArgumentException exception = Assert.Throws<ArgumentException>(() => CompiledAggregateMaterializerPlan.GetOrCreate(incompatible));

            Assert.Contains("relation at index 0", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Contains(incompatibleTarget.Name, exception.Message);
            Assert.Contains(incompatibleTarget.DeclaringType!.FullName!, exception.Message);
            Assert.Contains(typeof(OverrideEffectiveAggregateRoot).FullName!, exception.Message);

            MemberInfo inheritedTarget = typeof(OverrideEffectiveAggregateRootBase).GetProperty(nameof(OverrideEffectiveAggregateRootBase.Aggregate))!;
            DefinitiveJoinPlan inherited = ObjectRelationPlan(typeof(OverrideEffectiveAggregateRoot), inheritedTarget);
            CompiledAggregateMaterializerPlan compiled = CompiledAggregateMaterializerPlan.GetOrCreate(inherited);

            Assert.Equal(typeof(OverrideEffectiveAggregateRoot), compiled.RootType);
        }

        [Fact]
        public void MaterializeValidatesRowsAndGenericRootTypeWithActionableDiagnostics() {
            DefinitiveJoinPlan source = AutomaticJoinPlanCache.GetOrAdd(typeof(GuidRoot), AggregateJoinShape.ScalarAggregatesOnly);
            CompiledAggregateMaterializerPlan compiled = CompiledAggregateMaterializerPlan.GetOrCreate(source);

            Assert.Throws<ArgumentNullException>(() => compiled.Materialize<GuidRoot>(null!));
            ArgumentException nullRow = Assert.Throws<ArgumentException>(() => compiled.Materialize<GuidRoot>(new object[][] { null! }));
            ArgumentException shortRow = Assert.Throws<ArgumentException>(() => compiled.Materialize<GuidRoot>(new[] { new object[source.Projection.Length - 1] }));
            ArgumentException typeMismatch = Assert.Throws<ArgumentException>(() => compiled.Materialize<ScalarAggregate>(Array.Empty<object[]>()));

            Assert.Contains("row", nullRow.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("row", shortRow.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Contains(nameof(GuidRoot), typeMismatch.Message);
            Assert.Contains(nameof(ScalarAggregate), typeMismatch.Message);
        }

        [Fact]
        public void CompilationAndRepeatedMaterializationLeaveSourceUnchangedAndDeterministic() {
            DefinitiveJoinPlan source = AutomaticJoinPlanCache.GetOrAdd(typeof(GuidRoot), AggregateJoinShape.ScalarAggregatesOnly);
            string signature = source.StructuralSignature;
            string[] projection = source.Projection.Select(column => column.ResultAlias).ToArray();
            CompiledAggregateMaterializerPlan compiled = CompiledAggregateMaterializerPlan.GetOrCreate(source);
            Guid id = Guid.Parse("e1111111-1111-1111-1111-111111111111");
            object[] row = Row(source, (source.RootTableIndex, nameof(GuidRoot.Id), id));

            GuidRoot first = Assert.Single(compiled.Materialize<GuidRoot>(new[] { row }));
            GuidRoot second = Assert.Single(compiled.Materialize<GuidRoot>(new[] { row }));

            Assert.Equal(signature, source.StructuralSignature);
            Assert.Equal(projection, source.Projection.Select(column => column.ResultAlias));
            Assert.Equal(id, first.Id);
            Assert.Equal(id, second.Id);
            Assert.NotSame(first, second);
        }

        private static bool IsMutableCollection(Type type) {
            if (type.IsGenericType && type.GetGenericTypeDefinition().FullName == "System.Collections.Immutable.ImmutableArray`1") {
                return false;
            }
            return typeof(System.Collections.IList).IsAssignableFrom(type)
                || typeof(System.Collections.IDictionary).IsAssignableFrom(type);
        }

        private static int TableIndex(DefinitiveJoinPlan plan, Type entityType) {
            return Array.FindIndex(plan.Tables.ToArray(), table => table.EntityType == entityType);
        }

        private static DefinitiveJoinPlan ObjectRelationPlan(Type rootType, MemberInfo targetMember) {
            MemberInfo rootIdentifier = DefinitiveJoinPlanTestFactory.GetIdentifierMember(rootType);
            MemberInfo childIdentifier = DefinitiveJoinPlanTestFactory.GetIdentifierMember(typeof(ObjectAggregate));
            var tables = new[] {
                DefinitiveJoinPlanTestFactory.CreateTable(rootType, "root", "root", rootIdentifier, new[] { nameof(PlanDataObject<Guid>.Id) }, 0, "root_Id", nameof(PlanDataObject<Guid>.Id)),
                DefinitiveJoinPlanTestFactory.CreateTable(typeof(ObjectAggregate), "child", "child", childIdentifier, new[] { nameof(PlanDataObject<Guid>.Id) }, 1, "child_Id", nameof(PlanDataObject<Guid>.Id))
            };
            var projection = new[] {
                new DefinitiveProjectionColumn(0, 0, nameof(PlanDataObject<Guid>.Id), "root_Id", rootIdentifier),
                new DefinitiveProjectionColumn(1, 1, nameof(PlanDataObject<Guid>.Id), "child_Id", childIdentifier)
            };
            var relations = new[] {
                new DefinitiveJoinRelation(0, 1, nameof(PlanDataObject<Guid>.Id), nameof(PlanDataObject<Guid>.Id), AggregateBuildOptions.AggregateObject, targetMember, Array.Empty<string>())
            };
            return new DefinitiveJoinPlan(
                rootType,
                AggregateJoinShape.FullGraph,
                0,
                tables,
                relations,
                projection,
                new[] { new KeyValuePair<AggregatePath, string>(default, "root") },
                new[] {
                    new KeyValuePair<string, int>("root", 0),
                    new KeyValuePair<string, int>("child", 1)
                },
                new RootOrderingRequirement(0, nameof(PlanDataObject<Guid>.Id), 0, "root_Id"),
                DefinitiveJoinPlanCompiler.CurrentFormatVersion);
        }

        private static object[] Row(DefinitiveJoinPlan plan, params (int TableIndex, string Column, object Value)[] values) {
            var row = Enumerable.Repeat<object>(DBNull.Value, plan.Projection.Length).ToArray();
            foreach ((int tableIndex, string column, object value) in values) {
                DefinitiveProjectionColumn projection = plan.Projection.Single(candidate => candidate.TableIndex == tableIndex && candidate.SourceColumn == column);
                row[projection.Ordinal] = value;
            }
            return row;
        }
    }
}
