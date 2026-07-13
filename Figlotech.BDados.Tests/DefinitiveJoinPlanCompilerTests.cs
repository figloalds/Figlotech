using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Figlotech.BDados.Builders;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.Core.Interfaces;
using Figlotech.Data;
using Xunit;

#pragma warning disable CS0618 // Intentional coverage of legacy mutable construction access.

namespace Figlotech.BDados.Tests {
    public class DefinitiveJoinPlanCompilerTests {
        [Fact]
        public void CompileSnapshotsMutableJoinGraphWithoutMutatingIt() {
            JoinDefinition definition = CreateDefinition();
            DefinitiveJoinPlan plan = DefinitiveJoinPlanCompiler.Compile(definition, typeof(CompilerRoot), AggregateJoinShape.FullGraph);
            string signature = plan.StructuralSignature;

            definition.Joins[0].Columns.Add(nameof(CompilerRoot.LateColumn));
            definition.Joins[1].Prefix = "mutated";
            definition.Relations[0].Fields.Add("LateField");
            definition.Relations[0].NewName = nameof(CompilerRoot.OtherChild);

            Assert.Equal(signature, plan.StructuralSignature);
            Assert.Equal(new[] { nameof(CompilerRoot.Name), nameof(CompilerRoot.Id) }, plan.Tables[0].ProjectedColumns);
            Assert.Equal("root", plan.Tables[0].Prefix);
            Assert.Equal(new[] { nameof(CompilerChild.Value) }, plan.Relations[0].SourceFields);
            Assert.Equal(nameof(CompilerRoot.Child), plan.Relations[0].TargetMember!.Name);
        }

        [Fact]
        public void CompileAddsIdentifierOnlyToFrozenProjectionWithActualType() {
            JoinDefinition definition = CreateDefinition();
            definition.Joins[0].Columns = new List<string> { nameof(CompilerRoot.Name), "id" };
            definition.Joins[1].Columns = new List<string> { nameof(CompilerChild.Value) };

            DefinitiveJoinPlan plan = DefinitiveJoinPlanCompiler.Compile(definition, typeof(CompilerRoot), AggregateJoinShape.FullGraph);
            DefinitiveJoinTable root = plan.Tables[0];

            Assert.Equal(new[] { nameof(CompilerRoot.Name), "id" }, definition.Joins[0].Columns);
            Assert.Equal(new[] { nameof(CompilerRoot.Name), nameof(CompilerRoot.Id) }, root.ProjectedColumns);
            Assert.Equal(typeof(Guid), root.Identifier.ClrType);
            Assert.Equal(typeof(long), plan.Tables[1].Identifier.ClrType);
            Assert.Equal(nameof(CompilerRoot.Id), root.Identifier.ColumnName);
            Assert.Equal("root_Id", root.Identifier.ResultAlias);
            Assert.Equal(root.Identifier.ProjectionOrdinal, plan.Projection.Single(x => x.ResultAlias == "root_Id").Ordinal);
        }

        [Fact]
        public void CompilePreservesLegacyRidAsString() {
            var definition = new JoinDefinition {
                Joins = new List<JoiningTable> {
                    Table(typeof(LegacyCompilerRoot), "legacy", "legacy", new[] { nameof(LegacyCompilerRoot.Name) })
                }
            };

            DefinitiveJoinPlan plan = DefinitiveJoinPlanCompiler.Compile(definition, typeof(LegacyCompilerRoot), AggregateJoinShape.ScalarAggregatesOnly);

            Assert.Equal(nameof(LegacyCompilerRoot.RID), plan.Tables[0].Identifier.ColumnName);
            Assert.Equal(typeof(string), plan.Tables[0].Identifier.ClrType);
            Assert.Contains(plan.Projection, x => x.ResultAlias == "legacy_RID" && x.DestinationType == typeof(string));
        }

        [Fact]
        public void CompileCallsBuilderGetJoinOnceAndMatchesDirectDefinition() {
            JoinDefinition directDefinition = CreateDefinition();
            var builder = new FreshDefinitionBuilder(CreateDefinition);

            DefinitiveJoinPlan direct = DefinitiveJoinPlanCompiler.Compile(directDefinition, typeof(CompilerRoot), AggregateJoinShape.FullGraph);
            DefinitiveJoinPlan fromBuilder = DefinitiveJoinPlanCompiler.Compile(builder, typeof(CompilerRoot), AggregateJoinShape.FullGraph);

            Assert.Equal(1, builder.GetJoinCalls);
            Assert.Equal(direct.StructuralSignature, fromBuilder.StructuralSignature);
            Assert.Equal(direct.Projection.Select(x => x.ResultAlias), fromBuilder.Projection.Select(x => x.ResultAlias));
        }

        [Fact]
        public void FreezeAndBuildPlanExtensionsPublishEquivalentDetachedPlans() {
            JoinDefinition definition = CreateDefinition();
            var builder = new FreshDefinitionBuilder(CreateDefinition);

            DefinitiveJoinPlan frozen = definition.Freeze(typeof(CompilerRoot), AggregateJoinShape.FullGraph);
            DefinitiveJoinPlan built = builder.BuildPlan(typeof(CompilerRoot), AggregateJoinShape.FullGraph);
            definition.Joins[0].Columns.Add(nameof(CompilerRoot.LateColumn));

            Assert.Equal(1, builder.GetJoinCalls);
            Assert.Equal(frozen.StructuralSignature, built.StructuralSignature);
            Assert.Equal(frozen.Projection.Select(column => column.ResultAlias), built.Projection.Select(column => column.ResultAlias));
            Assert.DoesNotContain(nameof(CompilerRoot.LateColumn), frozen.Tables[0].ProjectedColumns);

            MethodInfo getJoin = typeof(IJoinBuilder).GetMethod(nameof(IJoinBuilder.GetJoin))!;
            ObsoleteAttribute obsolete = getJoin.GetCustomAttribute<ObsoleteAttribute>()!;
            Assert.NotNull(obsolete);
            Assert.Contains(nameof(DefinitiveJoinPlanExtensions.BuildPlan), obsolete.Message, StringComparison.Ordinal);
        }

        [Fact]
        public void CompileEquivalentExplicitDefinitionsDeterministically() {
            DefinitiveJoinPlan first = DefinitiveJoinPlanCompiler.Compile(CreateDefinition(), typeof(CompilerRoot), AggregateJoinShape.FullGraph);
            DefinitiveJoinPlan second = DefinitiveJoinPlanCompiler.Compile(CreateDefinition(), typeof(CompilerRoot), AggregateJoinShape.FullGraph);

            Assert.Equal(1, DefinitiveJoinPlanCompiler.CurrentFormatVersion);
            Assert.Equal(first.StructuralSignature, second.StructuralSignature);
            Assert.Equal(first.Tables.Select(x => x.Alias), second.Tables.Select(x => x.Alias));
            Assert.Equal(first.Projection.Select(x => x.ResultAlias), second.Projection.Select(x => x.ResultAlias));
            Assert.Equal(0, first.TableIndexByAlias["root"]);
            Assert.Equal("root", first.AliasByPath[new AggregatePath(new[] { "root" })]);
        }

        [Fact]
        public void CompileRecoversMissingAggregateKeysFromOneOrdinaryRelation() {
            JoinDefinition definition = CreateDefinition();
            definition.Relations.Insert(0, new Relation {
                ParentIndex = 0,
                ChildIndex = 1,
                ParentKey = nameof(CompilerRoot.Id),
                ChildKey = nameof(CompilerChild.ParentId),
                AggregateBuildOption = AggregateBuildOptions.None
            });
            definition.Relations[1].ParentKey = null!;
            definition.Relations[1].ChildKey = null!;

            DefinitiveJoinPlan plan = DefinitiveJoinPlanCompiler.Compile(definition, typeof(CompilerRoot), AggregateJoinShape.FullGraph);
            DefinitiveJoinRelation aggregate = plan.Relations[1];

            Assert.Equal(nameof(CompilerRoot.Id), aggregate.ParentKey);
            Assert.Equal(nameof(CompilerChild.ParentId), aggregate.ChildKey);
            Assert.Equal(AggregateBuildOptions.AggregateObject, aggregate.BuildKind);
        }

        [Fact]
        public void CompileRejectsDuplicateResultAliasesBeforeExecution() {
            JoinDefinition definition = CreateDefinition();
            definition.Joins[1].Prefix = definition.Joins[0].Prefix;

            ArgumentException exception = Assert.Throws<ArgumentException>(() => DefinitiveJoinPlanCompiler.Compile(definition, typeof(CompilerRoot), AggregateJoinShape.FullGraph));

            Assert.Contains("result alias", exception.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Theory]
        [InlineData("duplicate-alias")]
        [InlineData("root-mismatch")]
        [InlineData("unknown-column")]
        [InlineData("missing-identifier")]
        [InlineData("invalid-relation-target")]
        [InlineData("ambiguous-keys")]
        public void CompileRejectsInvalidDefinitionsWithContext(string scenario) {
            JoinDefinition definition = CreateDefinition();
            Type rootType = typeof(CompilerRoot);

            switch (scenario) {
                case "duplicate-alias":
                    definition.Joins[1].Alias = definition.Joins[0].Alias;
                    break;
                case "root-mismatch":
                    rootType = typeof(CompilerChild);
                    break;
                case "unknown-column":
                    definition.Joins[0].Columns.Add("NotAColumn");
                    break;
                case "missing-identifier":
                    definition.Joins[0].ValueObject = typeof(NoIdentifierCompilerRoot);
                    rootType = typeof(NoIdentifierCompilerRoot);
                    definition.Joins.RemoveAt(1);
                    definition.Relations.Clear();
                    break;
                case "invalid-relation-target":
                    definition.Relations[0].NewName = "NoSuchTarget";
                    break;
                case "ambiguous-keys":
                    definition.Relations[0].ParentKey = null!;
                    definition.Relations[0].ChildKey = null!;
                    definition.Relations.Add(new Relation {
                        ParentIndex = 0,
                        ChildIndex = 1,
                        ParentKey = nameof(CompilerRoot.Id),
                        ChildKey = nameof(CompilerChild.ParentId),
                        AggregateBuildOption = AggregateBuildOptions.None
                    });
                    definition.Relations.Add(new Relation {
                        ParentIndex = 0,
                        ChildIndex = 1,
                        ParentKey = nameof(CompilerRoot.Id),
                        ChildKey = nameof(CompilerChild.ParentId),
                        AggregateBuildOption = AggregateBuildOptions.None
                    });
                    break;
            }

            ArgumentException exception = Assert.Throws<ArgumentException>(() => DefinitiveJoinPlanCompiler.Compile(definition, rootType, AggregateJoinShape.FullGraph));

            Assert.Contains(scenario switch {
                "duplicate-alias" => "alias",
                "root-mismatch" => "root",
                "unknown-column" => "column",
                "missing-identifier" => "identifier",
                "invalid-relation-target" => "target",
                _ => "ambiguous"
            }, exception.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void JoinObjectBuilderPassesFrozenPlanDirectlyToGeneratorWithoutMutatingInput() {
            var builder = new JoinObjectBuilder(join => {
                join.Join(typeof(CompilerRoot), "root").OnlyFields(nameof(CompilerRoot.Name));
                join.Join(typeof(CompilerChild), "child", "root.Id=child.ParentId").OnlyFields(nameof(CompilerChild.Value));
            });
            AddRootChildRelation(builder.GetJoin());
            IQueryGenerator generator = CapturingQueryGenerator.Create(out CapturingQueryGenerator capture);

            builder.GenerateQuery(generator, null);

            Assert.NotNull(capture.Received);
            Assert.DoesNotContain(nameof(CompilerRoot.Id), builder.GetJoin().Joins[0].Columns);
            Assert.Equal("root", builder.GetJoin().Joins[0].Prefix);
            Assert.Contains(nameof(CompilerRoot.Id), capture.Received!.Tables[0].ProjectedColumns);
        }

        [Theory]
        [InlineData(" ", "predicate")]
        [InlineData("root.Id", "predicate")]
        [InlineData("missing.Id = child.ParentId", "prefix")]
        [InlineData("root.NotAColumn = child.ParentId", "member")]
        [InlineData("child.Id = child.ParentId", "current")]
        public void CompileRejectsInvalidNonRootJoinPredicates(string predicate, string messageFragment) {
            JoinDefinition definition = CreateDefinition();
            definition.Joins[1].Args = predicate;

            ArgumentException exception = Assert.Throws<ArgumentException>(() => DefinitiveJoinPlanCompiler.Compile(definition, typeof(CompilerRoot), AggregateJoinShape.FullGraph));

            Assert.Contains(messageFragment, exception.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void CompileRejectsForwardJoinPredicateEvenWhenRelationsOtherwiseConnectTables() {
            JoinDefinition definition = CreateThreeTableDefinition();
            definition.Joins[1].Args = "grand.Id = child.ParentId";

            ArgumentException exception = Assert.Throws<ArgumentException>(() => DefinitiveJoinPlanCompiler.Compile(definition, typeof(CompilerRoot), AggregateJoinShape.FullGraph));

            Assert.Contains("earlier", exception.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void CompileRejectsPredicateLessCrossJoin() {
            JoinDefinition definition = CreateDefinition();
            definition.Joins[1].Type = JoinType.CROSS;
            definition.Joins[1].Args = String.Empty;

            ArgumentException exception = Assert.Throws<ArgumentException>(() => DefinitiveJoinPlanCompiler.Compile(definition, typeof(CompilerRoot), AggregateJoinShape.FullGraph));

            Assert.Contains("cross", exception.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void CompileAcceptsWhitespaceAroundEscapedPredicateDots() {
            JoinDefinition definition = CreateDefinition();
            definition.Joins[1].Args = " root . Id = child . ParentId ";

            DefinitiveJoinPlan plan = DefinitiveJoinPlanCompiler.Compile(definition, typeof(CompilerRoot), AggregateJoinShape.FullGraph);

            Assert.Equal(" root . Id = child . ParentId ", plan.Tables[1].JoinPredicate);
        }

        [Fact]
        public void CompileRejectsMultiTablePlanWithoutRelationTopology() {
            JoinDefinition definition = CreateDefinition();
            definition.Relations.Clear();

            ArgumentException exception = Assert.Throws<ArgumentException>(() => DefinitiveJoinPlanCompiler.Compile(definition, typeof(CompilerRoot), AggregateJoinShape.FullGraph));

            Assert.Contains("topology", exception.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void CompileRejectsDisconnectedRelationTopology() {
            JoinDefinition definition = CreateThreeTableDefinition();
            definition.Relations.RemoveAt(1);

            ArgumentException exception = Assert.Throws<ArgumentException>(() => DefinitiveJoinPlanCompiler.Compile(definition, typeof(CompilerRoot), AggregateJoinShape.FullGraph));

            Assert.Contains("topology", exception.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void CompileProjectsCanonicalRelationKeysAndSourceFieldsWithoutMutatingSource() {
            JoinDefinition definition = CreateDefinition();
            Relation relation = definition.Relations[0];
            relation.ParentKey = "joinkey";
            relation.ChildKey = "foreignjoinkey";
            relation.Fields = new List<string> { "sourcepayload" };

            DefinitiveJoinPlan plan = DefinitiveJoinPlanCompiler.Compile(definition, typeof(CompilerRoot), AggregateJoinShape.FullGraph);

            Assert.Equal(new[] { nameof(CompilerRoot.Name) }, definition.Joins[0].Columns);
            Assert.Equal(new[] { nameof(CompilerChild.Value) }, definition.Joins[1].Columns);
            Assert.Equal("joinkey", relation.ParentKey);
            Assert.Equal("foreignjoinkey", relation.ChildKey);
            Assert.Equal(new[] { "sourcepayload" }, relation.Fields);
            Assert.Equal(nameof(CompilerRoot.JoinKey), plan.Relations[0].ParentKey);
            Assert.Equal(nameof(CompilerChild.ForeignJoinKey), plan.Relations[0].ChildKey);
            Assert.Equal(new[] { nameof(CompilerChild.SourcePayload) }, plan.Relations[0].SourceFields);
            Assert.Equal(new[] { nameof(CompilerRoot.Name), nameof(CompilerRoot.Id), nameof(CompilerRoot.JoinKey) }, plan.Tables[0].ProjectedColumns);
            Assert.Equal(new[] { nameof(CompilerChild.Value), nameof(CompilerChild.Id), nameof(CompilerChild.ForeignJoinKey), nameof(CompilerChild.SourcePayload) }, plan.Tables[1].ProjectedColumns);
        }

        [Theory]
        [InlineData("parent")]
        [InlineData("child")]
        [InlineData("field")]
        public void CompileRejectsUnknownRelationMembers(string scenario) {
            JoinDefinition definition = CreateDefinition();
            switch (scenario) {
                case "parent":
                    definition.Relations[0].ParentKey = "NotAParentKey";
                    break;
                case "child":
                    definition.Relations[0].ChildKey = "NotAChildKey";
                    break;
                case "field":
                    definition.Relations[0].Fields = new List<string> { "NotASourceField" };
                    break;
            }

            ArgumentException exception = Assert.Throws<ArgumentException>(() => DefinitiveJoinPlanCompiler.Compile(definition, typeof(CompilerRoot), AggregateJoinShape.FullGraph));

            Assert.Contains(scenario, exception.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Theory]
        [InlineData("alias")]
        [InlineData("prefix")]
        public void CompileRejectsCaseOnlySqlIdentifierCollisions(string scenario) {
            JoinDefinition definition = CreateDefinition();
            if (scenario == "alias") {
                definition.Joins[1].Alias = definition.Joins[0].Alias.ToUpperInvariant();
            } else {
                definition.Joins[1].Prefix = definition.Joins[0].Prefix.ToUpperInvariant();
            }

            ArgumentException exception = Assert.Throws<ArgumentException>(() => DefinitiveJoinPlanCompiler.Compile(definition, typeof(CompilerRoot), AggregateJoinShape.FullGraph));

            Assert.Contains(scenario, exception.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void JoinObjectBuilderFreezesOriginalPlanForSequentialCalls() {
            var builder = new JoinObjectBuilder(join => {
                join.Join(typeof(CompilerRoot), "root").OnlyFields(nameof(CompilerRoot.Name));
                join.Join(typeof(CompilerChild), "child", "root.Id=child.ParentId").OnlyFields(nameof(CompilerChild.Value));
            });
            AddRootChildRelation(builder.GetJoin());
            IQueryGenerator generator = RecordingQueryGenerator.Create(out RecordingQueryGenerator capture);

            builder.GenerateQuery(generator, null);
            builder.GetJoin().Joins[0].Prefix = "mutated";
            builder.GetJoin().Joins[0].Columns.Add(nameof(CompilerRoot.LateColumn));
            builder.GenerateQuery(generator, null);

            DefinitiveJoinPlan[] received = capture.Received.ToArray();
            Assert.Equal(2, received.Length);
            Assert.Same(received[0], received[1]);
            Assert.All(received, snapshot => {
                Assert.Equal("root", snapshot.Tables[0].Prefix);
                Assert.DoesNotContain(nameof(CompilerRoot.LateColumn), snapshot.Tables[0].ProjectedColumns);
            });
        }

        [Fact]
        public void JoinObjectBuilderConcurrentGenerateQueryCallsReceiveSameFrozenPlan() {
            var builder = new JoinObjectBuilder(join => {
                join.Join(typeof(CompilerRoot), "root").OnlyFields(nameof(CompilerRoot.Name));
                join.Join(typeof(CompilerChild), "child", "root.Id=child.ParentId").OnlyFields(nameof(CompilerChild.Value));
            });
            AddRootChildRelation(builder.GetJoin());
            IQueryGenerator generator = RecordingQueryGenerator.Create(out RecordingQueryGenerator capture);

            Task.WaitAll(Enumerable.Range(0, 8).Select(_ => Task.Run(() => builder.GenerateQuery(generator, null))).ToArray());

            DefinitiveJoinPlan[] received = capture.Received.ToArray();
            Assert.Equal(8, received.Length);
            Assert.Single(received.Distinct());
            Assert.All(received, snapshot => Assert.Equal("root", snapshot.Tables[0].Prefix));
        }

        private static JoinDefinition CreateDefinition() {
            return new JoinDefinition {
                Joins = new List<JoiningTable> {
                    Table(typeof(CompilerRoot), "root", "root", new[] { nameof(CompilerRoot.Name) }),
                    Table(typeof(CompilerChild), "child", "child", new[] { nameof(CompilerChild.Value) }, "root.Id=child.ParentId")
                },
                Relations = new List<Relation> {
                    new Relation {
                        ParentIndex = 0,
                        ChildIndex = 1,
                        ParentKey = nameof(CompilerRoot.Id),
                        ChildKey = nameof(CompilerChild.ParentId),
                        AggregateBuildOption = AggregateBuildOptions.AggregateObject,
                        NewName = nameof(CompilerRoot.Child),
                        Fields = new List<string> { nameof(CompilerChild.Value) }
                    }
                }
            };
        }

        private static void AddRootChildRelation(JoinDefinition definition) {
            definition.Relations.Add(new Relation {
                ParentIndex = 0,
                ChildIndex = 1,
                ParentKey = nameof(CompilerRoot.Id),
                ChildKey = nameof(CompilerChild.ParentId),
                AggregateBuildOption = AggregateBuildOptions.AggregateObject,
                NewName = nameof(CompilerRoot.Child),
                Fields = new List<string> { nameof(CompilerChild.Value) }
            });
        }

        private static JoinDefinition CreateThreeTableDefinition() {
            JoinDefinition definition = CreateDefinition();
            definition.Joins.Add(Table(typeof(CompilerChild), "grand", "grand", new[] { nameof(CompilerChild.Value) }, "root.Id=grand.ParentId"));
            definition.Relations.Add(new Relation {
                ParentIndex = 0,
                ChildIndex = 2,
                ParentKey = nameof(CompilerRoot.Id),
                ChildKey = nameof(CompilerChild.ParentId),
                AggregateBuildOption = AggregateBuildOptions.AggregateObject,
                NewName = nameof(CompilerRoot.OtherChild),
                Fields = new List<string> { nameof(CompilerChild.Value) }
            });
            return definition;
        }

        private static JoiningTable Table(Type type, string alias, string prefix, IEnumerable<string> columns, string args = "") {
            return new JoiningTable {
                ValueObject = type,
                TableName = type.Name,
                Alias = alias,
                Prefix = prefix,
                Args = args,
                Type = JoinType.LEFT,
                Columns = new List<string>(columns)
            };
        }

        private sealed class FreshDefinitionBuilder : IJoinBuilder {
            private readonly Func<JoinDefinition> _factory;

            public FreshDefinitionBuilder(Func<JoinDefinition> factory) {
                _factory = factory;
            }

            public int GetJoinCalls { get; private set; }

            public JoinDefinition GetJoin() {
                GetJoinCalls++;
                return _factory();
            }

            public IQueryBuilder GenerateQuery(IQueryGenerator generator, IQueryBuilder conditions, MemberInfo orderingMember = null!, OrderingType otype = OrderingType.Asc, int? p = null, int? limit = null, IQueryBuilder conditionsRoot = null!) {
                throw new NotSupportedException();
            }
        }

        private class CapturingQueryGenerator : DispatchProxy {
            public DefinitiveJoinPlan? Received { get; private set; }

            public static IQueryGenerator Create(out CapturingQueryGenerator capture) {
                IQueryGenerator generator = DispatchProxy.Create<IQueryGenerator, CapturingQueryGenerator>();
                capture = (CapturingQueryGenerator)(object)generator;
                return generator;
            }

            protected override object? Invoke(MethodInfo? targetMethod, object?[]? args) {
                if (targetMethod?.Name == nameof(IQueryGenerator.GenerateJoinQuery) && args![0] is DefinitiveJoinPlan plan) {
                    Received = plan;
                    return null;
                }
                throw new NotSupportedException(targetMethod?.Name);
            }
        }

        private class RecordingQueryGenerator : DispatchProxy {
            public ConcurrentBag<DefinitiveJoinPlan> Received { get; } = new ConcurrentBag<DefinitiveJoinPlan>();

            public static IQueryGenerator Create(out RecordingQueryGenerator capture) {
                IQueryGenerator generator = DispatchProxy.Create<IQueryGenerator, RecordingQueryGenerator>();
                capture = (RecordingQueryGenerator)(object)generator;
                return generator;
            }

            protected override object? Invoke(MethodInfo? targetMethod, object?[]? args) {
                if (targetMethod?.Name == nameof(IQueryGenerator.GenerateJoinQuery) && args![0] is DefinitiveJoinPlan plan) {
                    Received.Add(plan);
                    return null;
                }
                throw new NotSupportedException(targetMethod?.Name);
            }
        }

        private sealed class CompilerRoot : IDataObject<Guid> {
            public Guid Id { get; set; }
            object IDataObject.Id { get => Id; set => Id = (Guid)value; }
            public DateTime CreatedAt { get; set; }
            public DateTime? UpdatedAt { get; set; }
            public string Name { get; set; } = String.Empty;
            public string JoinKey { get; set; } = String.Empty;
            public string LateColumn { get; set; } = String.Empty;
            public CompilerChild? Child { get; set; }
            public CompilerChild? OtherChild { get; set; }
        }

        private sealed class CompilerChild : IDataObject<long> {
            public long Id { get; set; }
            object IDataObject.Id { get => Id; set => Id = (long)value; }
            public DateTime CreatedAt { get; set; }
            public DateTime? UpdatedAt { get; set; }
            public Guid ParentId { get; set; }
            public string ForeignJoinKey { get; set; } = String.Empty;
            public string SourcePayload { get; set; } = String.Empty;
            public string Value { get; set; } = String.Empty;
        }

        private sealed class LegacyCompilerRoot : ILegacyDataObject {
            public long Id { get; set; }
            object IDataObject.Id { get => Id; set => Id = Convert.ToInt64(value); }
            public string RID { get; set; } = String.Empty;
            public bool IsPersisted { get; set; }
            public int PersistedHash { get; set; }
            public ulong AlteredBy { get; set; }
            public ulong CreatedBy { get; set; }
            public bool IsReceivedFromSync { get; set; }
            public DateTime CreatedAt { get; set; }
            public DateTime? UpdatedAt { get; set; }
            public string Name { get; set; } = String.Empty;
        }

        private sealed class NoIdentifierCompilerRoot : IDataObject {
            object IDataObject.Id { get => null!; set { } }
            public DateTime CreatedAt { get; set; }
            public DateTime? UpdatedAt { get; set; }
            public string Name { get; set; } = String.Empty;
        }
    }
}
