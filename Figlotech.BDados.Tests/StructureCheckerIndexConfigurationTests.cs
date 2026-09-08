using System.Reflection;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using Figlotech.BDados.MySqlDataAccessor;
using Figlotech.BDados.PgSQLDataAccessor;
using Figlotech.Data;
using Xunit;

namespace Figlotech.BDados.Tests {
    public class StructureCheckerIndexConfigurationTests {
        [Theory]
        [InlineData(0, true)]
        [InlineData(1, false)]
        public void NonUniqueMetadataHasDatabaseSemantics(int nonUnique, bool isUnique) {
            var link = new ScStructuralLink { NON_UNIQUE = nonUnique };
            Assert.Equal(isUnique, link.IsUnique);
            Assert.Equal(nonUnique, new ScStructuralLink { IsUnique = isUnique }.NON_UNIQUE);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task ExistingNonUniqueCompositeIsReplacedAndThenMatches(bool postgres) {
            var accessor = DispatchProxy.Create<IRdbmsDataAccessor, SchemaAccessor>();
            var state = (SchemaAccessor)accessor;
            state.Generator = postgres ? new PgSQLQueryGenerator() : new MySqlQueryGenerator();
            state.Indexes = Rows("Embalagem,UnidadeArmazenagem,Id", false);
            var checker = new StructureChecker(accessor, new[] { typeof(IndexModel) });
            var desired = ReadLinks(checker, "GetNecessaryLinks").Single(l => l.KeyName == "ix_epu_embalagem_unidade_id");
            Assert.True(desired.IsUnique);
            Assert.Equal("Embalagem,UnidadeArmazenagem,Id", desired.Column);

            var actual = ReadLinks(checker, "GetInfoSchemaKeys");
            Assert.Equal(desired.Column, Assert.Single(actual).Column);
            Assert.False(actual[0].IsUnique);
            var actions = Evaluate(checker, desired, actual);
            Assert.Collection(actions, a => Assert.IsType<DropIdxScAction>(a), a => Assert.IsType<CreateIndexScAction>(a));
            foreach (var action in actions) {
                await action.Execute(null!, accessor);
            }
            Assert.Contains(postgres ? "CREATE UNIQUE INDEX" : "ADD UNIQUE KEY", state.ExecutedSql[1]);
            Assert.Contains("(embalagem,unidadearmazenagem,id)", state.ExecutedSql[1]);

            // Model a fresh metadata read after successful DDL, including MySQL's constraint representation.
            if (postgres) {
                state.Indexes = Rows(desired.Column, true);
            } else {
                state.Indexes.Clear();
                state.Constraints = Rows(desired.Column, true, "UNIQUE");
            }
            Assert.Empty(Evaluate(checker, desired, ReadLinks(checker, "GetInfoSchemaKeys")));
        }

        [Theory]
        [InlineData("Embalagem,UnidadeArmazenagem", true)]
        [InlineData("Embalagem,UnidadeArmazenagem,Id,Extra", true)]
        [InlineData("Id,UnidadeArmazenagem,Embalagem", true)]
        [InlineData("Embalagem,Other,Id", true)]
        [InlineData("Embalagem,UnidadeArmazenagem,Id", false)]
        public void SameNameWithWrongConfigurationRequiresReplacement(string actualColumns, bool actualUnique) {
            var checker = new StructureChecker(null!, Array.Empty<Type>());
            var desired = Index("Embalagem,UnidadeArmazenagem,Id", true);
            var actual = Index(actualColumns, actualUnique);
            var actions = Evaluate(checker, desired, new List<ScStructuralLink> { actual });

            Assert.Collection(actions, a => Assert.IsType<DropIdxScAction>(a), a => Assert.IsType<CreateIndexScAction>(a));
            Assert.Empty(Evaluate(checker, desired, new List<ScStructuralLink> { Index(desired.Column, true) }));
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void MatchingIndexIsIdempotentIgnoringCaseAndWhitespace(bool isUnique) {
            var checker = new StructureChecker(null!, Array.Empty<Type>());
            var desired = Index("Embalagem, UnidadeArmazenagem, Id", isUnique);
            var actual = Index("embalagem,unidadearmazenagem,id", isUnique);
            actual.Table = actual.Table.ToLowerInvariant();
            actual.KeyName = actual.KeyName.ToUpperInvariant();

            Assert.Empty(Evaluate(checker, desired, new List<ScStructuralLink> { actual }));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task UniqueDowngradeDropsTheCorrectDatabaseObject(bool constraintBacked) {
            var accessor = DispatchProxy.Create<IRdbmsDataAccessor, SchemaAccessor>();
            var state = (SchemaAccessor)accessor;
            state.Generator = new PgSQLQueryGenerator();
            var checker = new StructureChecker(accessor, Array.Empty<Type>());
            var actual = Index("Embalagem,Id", true);
            actual.CONSTRAINT_TYPE = constraintBacked ? "UNIQUE" : null!;
            var actions = Evaluate(checker, Index(actual.Column, false), new List<ScStructuralLink> { actual });

            Assert.Equal(2, actions.Count);
            await actions[0].Execute(null!, accessor);
            Assert.Contains(constraintBacked ? "DROP CONSTRAINT" : "DROP INDEX", state.ExecutedSql.Single());
            await actions[1].Execute(null!, accessor);
            Assert.Contains("CREATE INDEX", state.ExecutedSql[1]);
        }

        [Fact]
        public void MetadataDeduplicationUsesTableAndRetainsEveryOrderedColumn() {
            var accessor = DispatchProxy.Create<IRdbmsDataAccessor, SchemaAccessor>();
            var state = (SchemaAccessor)accessor;
            state.Constraints = Rows("Embalagem,UnidadeArmazenagem,Id", true, "UNIQUE");
            state.Indexes = Rows("Embalagem,UnidadeArmazenagem,Id", true);
            var otherTable = Rows("Embalagem,Id", false);
            otherTable.ForEach(r => r.Table = nameof(OtherIndexModel));
            state.Indexes.AddRange(otherTable);
            var checker = new StructureChecker(accessor, new[] { typeof(IndexModel), typeof(OtherIndexModel) });

            var keys = ReadLinks(checker, "GetInfoSchemaKeys");
            Assert.Equal(2, keys.Count);
            Assert.Contains(keys, k => k.Table == nameof(IndexModel) && k.Column == "Embalagem,UnidadeArmazenagem,Id" && k.IsUnique);
            Assert.Contains(keys, k => k.Table == nameof(OtherIndexModel) && k.Column == "Embalagem,Id" && !k.IsUnique);
        }

        [Fact]
        public void ForeignKeyWithSameNameDoesNotHideItsSupportingIndex() {
            var accessor = DispatchProxy.Create<IRdbmsDataAccessor, SchemaAccessor>();
            var state = (SchemaAccessor)accessor;
            state.Constraints = Rows("Embalagem", false, "FOREIGN KEY");
            state.Indexes = Rows("Embalagem", false);
            var checker = new StructureChecker(accessor, new[] { typeof(IndexModel) });

            var keys = ReadLinks(checker, "GetInfoSchemaKeys");
            Assert.Equal(2, keys.Count);
            Assert.Contains(keys, k => k.Type == ScStructuralKeyType.Index);
            Assert.Contains(keys, k => k.Type == ScStructuralKeyType.ForeignKey);
        }

        private static ScStructuralLink Index(string columns, bool unique) {
            return new ScStructuralLink {
                Table = nameof(IndexModel), Column = columns,
                KeyName = "ix_epu_embalagem_unidade_id", IsUnique = unique
            };
        }

        private static List<ScStructuralLink> Rows(string columns, bool unique, string? constraintType = null) {
            return columns.Split(',').Select((column, position) => {
                var row = Index(column, unique);
                row.SEQ_IN_INDEX = position + 1;
                row.CONSTRAINT_TYPE = constraintType!;
                return row;
            }).Reverse().ToList();
        }

        private static List<ScStructuralLink> ReadLinks(StructureChecker checker, string methodName) {
            var method = typeof(StructureChecker).GetMethod(methodName, BindingFlags.NonPublic | BindingFlags.Instance)!;
            return ((IEnumerable<ScStructuralLink>)method.Invoke(checker, null)!).ToList();
        }

        private static List<IStructureCheckNecessaryAction> Evaluate(StructureChecker checker, ScStructuralLink desired, List<ScStructuralLink> actual) {
            var needed = new List<ScStructuralLink> { desired };
            return checker.EvaluateLegacyKeys(needed, actual)
                .Concat(checker.EvaluateMissingKeysCreation(new List<FieldAttribute>(), needed, actual)).ToList();
        }

        public class SchemaAccessor : DispatchProxy {
            public IQueryGenerator Generator = new MySqlQueryGenerator();
            public List<ScStructuralLink> Constraints = new List<ScStructuralLink>();
            public List<ScStructuralLink> Indexes = new List<ScStructuralLink>();
            public List<string> ExecutedSql = new List<string>();

            protected override object? Invoke(MethodInfo? targetMethod, object?[]? args) {
                switch (targetMethod!.Name) {
                    case "get_SchemaName": return "schema_test";
                    case "get_QueryGenerator": return Generator;
                    case "Query":
                        var sql = ((IQueryBuilder)args![0]!).GetCommandText();
                        var rows = sql.Contains("information_schema.table_constraints") ? Constraints : Indexes;
                        return rows.Select(r => new ScStructuralLink {
                            Table = r.Table, Column = r.Column, KeyName = r.KeyName, NON_UNIQUE = r.NON_UNIQUE,
                            CONSTRAINT_TYPE = r.CONSTRAINT_TYPE, ORDINAL_POSITION = r.ORDINAL_POSITION
                        }).ToList();
                    case "ExecuteAsync":
                        ExecutedSql.Add(((IQueryBuilder)args![1]!).GetCommandText());
                        return Task.FromResult(0);
                    default: throw new NotSupportedException(targetMethod.Name);
                }
            }
        }

        public sealed class IndexModel : PlanDataObject<long> {
            [Field]
            [Index(nameof(UnidadeArmazenagem), nameof(Id), Name = "ix_epu_embalagem_unidade_id", IsUnique = true)]
            public long Embalagem { get; set; }
            [Field]
            public long UnidadeArmazenagem { get; set; }
        }

        public sealed class OtherIndexModel : PlanDataObject<long> {
        }
    }
}
