using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.SqliteDataAccessor;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Figlotech.BDados.Tests {
    public sealed class AggregateBuilderPlanTests {
        [Fact]
        public async Task FrozenSyncAsyncAndCoroutineBuildersMaterializeOrderedTypedGuidListGraphs() {
            DefinitiveJoinPlan plan = FullGraphPlan();
            CompiledAggregateMaterializerPlan materializer = CompiledAggregateMaterializerPlan.GetOrCreate(plan);
            using var accessor = CreateAccessor();

            List<GuidRoot> sync;
            using (BDadosTransaction transaction = accessor.CreateNewTransaction(CancellationToken.None, null)) {
                using IDbCommand command = CreateRowsCommand(transaction.Connection, plan, ValidRows(plan));
                sync = accessor.BuildAggregateListDirect<GuidRoot>(transaction, command, plan, materializer, overrideContext: null);
            }

            List<GuidRoot> asynchronous;
            await using (BDadosTransaction transaction = await accessor.CreateNewTransactionAsync(CancellationToken.None, null)) {
                await using DbCommand command = (DbCommand)CreateRowsCommand(transaction.Connection, plan, ValidRows(plan));
                asynchronous = await accessor.BuildAggregateListDirectAsync<GuidRoot>(transaction, command, plan, materializer, overrideContext: null);
            }

            List<GuidRoot> coroutine;
            await using (BDadosTransaction transaction = await accessor.CreateNewTransactionAsync(CancellationToken.None, null)) {
                await using DbCommand command = (DbCommand)CreateRowsCommand(transaction.Connection, plan, ValidRows(plan));
                coroutine = new List<GuidRoot>();
                await foreach (GuidRoot root in accessor.BuildAggregateListDirectCoroutinely<GuidRoot>(transaction, command, plan, materializer, CancellationToken.None)) {
                    coroutine.Add(root);
                }
            }

            AssertGraph(sync);
            AssertGraph(asynchronous);
            AssertGraph(coroutine);
        }

        [Fact]
        public void FrozenSyncBuilderRejectsReorderedSchemaBeforeCreatingAnyRoot() {
            DefinitiveJoinPlan plan = FullGraphPlan();
            CompiledAggregateMaterializerPlan materializer = CompiledAggregateMaterializerPlan.GetOrCreate(plan);
            using var accessor = CreateAccessor();
            string[] aliases = plan.Projection.Select(column => column.ResultAlias).ToArray();
            (aliases[0], aliases[1]) = (aliases[1], aliases[0]);

            using BDadosTransaction transaction = accessor.CreateNewTransaction(CancellationToken.None, null);
            using IDbCommand command = CreateRowsCommand(transaction.Connection, plan, ValidRows(plan), aliases);
            ArgumentException exception = Assert.Throws<ArgumentException>(() => accessor.BuildAggregateListDirect<GuidRoot>(transaction, command, plan, materializer, overrideContext: null));

            Assert.Contains("order mismatch", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("Expected ordered aliases", exception.Message);
        }

        [Fact]
        public async Task FrozenCoroutineBuilderRejectsAppendedSchemaBeforeYieldingAnyRoot() {
            DefinitiveJoinPlan plan = FullGraphPlan();
            CompiledAggregateMaterializerPlan materializer = CompiledAggregateMaterializerPlan.GetOrCreate(plan);
            using var accessor = CreateAccessor();
            var yielded = new List<GuidRoot>();

            await using BDadosTransaction transaction = await accessor.CreateNewTransactionAsync(CancellationToken.None, null);
            await using DbCommand command = (DbCommand)CreateRowsCommand(transaction.Connection, plan, ValidRows(plan), plan.Projection.Select(column => column.ResultAlias).Append("provider_sentinel").ToArray(), includeSentinel: true);
            ArgumentException exception = await Assert.ThrowsAsync<ArgumentException>(async () => {
                await foreach (GuidRoot root in accessor.BuildAggregateListDirectCoroutinely<GuidRoot>(transaction, command, plan, materializer, CancellationToken.None)) {
                    yielded.Add(root);
                }
            });

            Assert.Empty(yielded);
            Assert.Contains("unexpected", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("provider_sentinel", exception.Message);
        }

        [Fact]
        public void FrozenBuilderRejectsMaterializerFromEquivalentButDifferentPlanBeforeReaderExecution() {
            DefinitiveJoinPlan source = FullGraphPlan();
            DefinitiveJoinPlan equivalentButDistinct = FullGraphPlan();
            CompiledAggregateMaterializerPlan materializer = CompiledAggregateMaterializerPlan.GetOrCreate(source);
            using var accessor = CreateAccessor();

            using BDadosTransaction transaction = accessor.CreateNewTransaction(CancellationToken.None, null);
            using IDbCommand command = transaction.Connection.CreateCommand();
            command.CommandText = "this SQL must not be executed";
            ArgumentException exception = Assert.Throws<ArgumentException>(() => accessor.BuildAggregateListDirect<GuidRoot>(transaction, command, equivalentButDistinct, materializer, overrideContext: null));

            Assert.Contains("source", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("same", exception.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public void FrozenBuilderRejectsMismatchedRootTypeBeforeReaderExecution() {
            DefinitiveJoinPlan plan = FullGraphPlan();
            CompiledAggregateMaterializerPlan materializer = CompiledAggregateMaterializerPlan.GetOrCreate(plan);
            using var accessor = CreateAccessor();

            using BDadosTransaction transaction = accessor.CreateNewTransaction(CancellationToken.None, null);
            using IDbCommand command = transaction.Connection.CreateCommand();
            command.CommandText = "this SQL must not be executed";
            ArgumentException exception = Assert.Throws<ArgumentException>(() => accessor.BuildAggregateListDirect<LongRoot>(transaction, command, plan, materializer, overrideContext: null));

            Assert.Contains(nameof(GuidRoot), exception.Message);
            Assert.Contains(nameof(LongRoot), exception.Message);
        }

        [Fact]
        public async Task FrozenCoroutineBuilderPropagatesPreCanceledAndMidstreamCancellation() {
            DefinitiveJoinPlan plan = FullGraphPlan();
            CompiledAggregateMaterializerPlan materializer = CompiledAggregateMaterializerPlan.GetOrCreate(plan);
            using var accessor = CreateAccessor();

            using (var preCanceled = new CancellationTokenSource()) {
                preCanceled.Cancel();
                await using BDadosTransaction transaction = await accessor.CreateNewTransactionAsync(CancellationToken.None, null);
                await using DbCommand command = (DbCommand)CreateRowsCommand(transaction.Connection, plan, ValidRows(plan));
                await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => {
                    await foreach (GuidRoot _ in accessor.BuildAggregateListDirectCoroutinely<GuidRoot>(transaction, command, plan, materializer, preCanceled.Token)) {
                    }
                });
            }

            using (var midstream = new CancellationTokenSource()) {
                await using BDadosTransaction transaction = await accessor.CreateNewTransactionAsync(CancellationToken.None, null);
                await using DbCommand command = (DbCommand)CreateRowsCommand(transaction.Connection, plan, ValidRows(plan));
                int seen = 0;
                await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => {
                    await foreach (GuidRoot _ in accessor.BuildAggregateListDirectCoroutinely<GuidRoot>(transaction, command, plan, materializer, midstream.Token)) {
                        seen++;
                        midstream.Cancel();
                    }
                });
                Assert.Equal(1, seen);
            }
        }

        [Fact]
        public async Task FrozenListBuildersPreserveHooksWhileDirectCoroutineOwnsNone() {
            DefinitiveJoinPlan plan = DefinitiveJoinPlanCompiler.Compile(typeof(HookedGuidRoot), AggregateJoinShape.FullGraph);
            CompiledAggregateMaterializerPlan materializer = CompiledAggregateMaterializerPlan.GetOrCreate(plan);
            using var accessor = CreateAccessor();

            var syncLog = new System.Collections.Concurrent.ConcurrentQueue<string>();
            using (BDadosTransaction transaction = accessor.CreateNewTransaction(CancellationToken.None, null)) {
                using IDbCommand command = CreateRowsCommand(transaction.Connection, plan, HookedRows(plan));
                accessor.BuildAggregateListDirect<HookedGuidRoot>(transaction, command, plan, materializer, syncLog);
            }
            AssertListHookOrder(syncLog.ToArray());

            var asyncLog = new System.Collections.Concurrent.ConcurrentQueue<string>();
            await using (BDadosTransaction transaction = await accessor.CreateNewTransactionAsync(CancellationToken.None, null)) {
                await using DbCommand command = (DbCommand)CreateRowsCommand(transaction.Connection, plan, HookedRows(plan));
                await accessor.BuildAggregateListDirectAsync<HookedGuidRoot>(transaction, command, plan, materializer, asyncLog);
            }
            AssertListHookOrder(asyncLog.ToArray());

            var coroutineLog = new System.Collections.Concurrent.ConcurrentQueue<string>();
            await using (BDadosTransaction transaction = await accessor.CreateNewTransactionAsync(CancellationToken.None, null)) {
                transaction.ContextTransferObject = coroutineLog;
                await using DbCommand command = (DbCommand)CreateRowsCommand(transaction.Connection, plan, HookedRows(plan));
                await foreach (HookedGuidRoot _ in accessor.BuildAggregateListDirectCoroutinely<HookedGuidRoot>(transaction, command, plan, materializer, CancellationToken.None)) {
                }
            }
            Assert.Empty(coroutineLog);
        }

        [Fact]
        public async Task FrozenAsyncBuilderPropagatesPerItemHookFailure() {
            DefinitiveJoinPlan plan = DefinitiveJoinPlanCompiler.Compile(typeof(ThrowingHookGuidRoot), AggregateJoinShape.FullGraph);
            CompiledAggregateMaterializerPlan materializer = CompiledAggregateMaterializerPlan.GetOrCreate(plan);
            using var accessor = CreateAccessor();

            await using BDadosTransaction transaction = await accessor.CreateNewTransactionAsync(CancellationToken.None, null);
            await using DbCommand command = (DbCommand)CreateRowsCommand(transaction.Connection, plan, new[] {
                Row(plan, (plan.RootTableIndex, nameof(ThrowingHookGuidRoot.Id), Guid.Parse("88888888-8888-8888-8888-888888888888").ToString("D")))
            });
            Exception exception = await Assert.ThrowsAnyAsync<Exception>(() => accessor.BuildAggregateListDirectAsync<ThrowingHookGuidRoot>(transaction, command, plan, materializer, overrideContext: null));

            Assert.Contains("async aggregate hook failure", exception.ToString());
        }

        [Fact]
        public async Task FrozenAsyncBuilderSupportsStandaloneCommandWithoutTransaction() {
            DefinitiveJoinPlan plan = FullGraphPlan();
            CompiledAggregateMaterializerPlan materializer = CompiledAggregateMaterializerPlan.GetOrCreate(plan);
            using var accessor = CreateAccessor();
            await using var connection = new SqliteConnection("Data Source=:memory:");
            await connection.OpenAsync();

            List<GuidRoot> sync;
            using (IDbCommand command = CreateRowsCommand(connection, plan, ValidRows(plan))) {
                sync = accessor.BuildAggregateListDirect<GuidRoot>(null!, command, plan, materializer, overrideContext: null);
            }

            List<GuidRoot> asynchronous;
            await using (DbCommand command = (DbCommand)CreateRowsCommand(connection, plan, ValidRows(plan))) {
                asynchronous = await accessor.BuildAggregateListDirectAsync<GuidRoot>(null!, command, plan, materializer, overrideContext: null);
            }

            AssertGraph(sync);
            AssertGraph(asynchronous);
        }

        [Fact]
        public void FrozenBuilderOverloadsArePublicAndUseDefinitiveInputs() {
            Type accessorType = typeof(RdbmsDataAccessor);
            Assert.Contains(accessorType.GetMethods(), method => IsFrozen(method, nameof(RdbmsDataAccessor.BuildAggregateListDirect), typeof(IDbCommand)));
            Assert.Contains(accessorType.GetMethods(), method => IsFrozen(method, nameof(RdbmsDataAccessor.BuildAggregateListDirectAsync), typeof(DbCommand)));
            Assert.Contains(accessorType.GetMethods(), method => IsFrozen(method, nameof(RdbmsDataAccessor.BuildAggregateListDirectCoroutinely), typeof(DbCommand)));
        }

        private static bool IsFrozen(System.Reflection.MethodInfo method, string name, Type commandType) {
            if (!method.IsPublic || method.Name != name || !method.IsGenericMethodDefinition) {
                return false;
            }
            Type[] parameters = method.GetParameters().Select(parameter => parameter.ParameterType).ToArray();
            return parameters.Length >= 4
                && parameters.Contains(commandType)
                && parameters.Contains(typeof(DefinitiveJoinPlan))
                && parameters.Contains(typeof(CompiledAggregateMaterializerPlan))
                && !parameters.Contains(typeof(JoinDefinition));
        }

        private static RdbmsDataAccessor CreateAccessor() {
            return new RdbmsDataAccessor(new TestSqlitePlugin());
        }

        private static DefinitiveJoinPlan FullGraphPlan() {
            return DefinitiveJoinPlanCompiler.Compile(typeof(GuidRoot), AggregateJoinShape.FullGraph);
        }

        private static IReadOnlyList<object[]> HookedRows(DefinitiveJoinPlan plan) {
            Guid firstRoot = Guid.Parse("55555555-5555-5555-5555-555555555555");
            Guid secondRoot = Guid.Parse("66666666-6666-6666-6666-666666666666");
            int root = plan.RootTableIndex;
            int list = TableIndex(plan, typeof(ListAggregate));
            return new[] {
                Row(plan, (root, nameof(HookedGuidRoot.Id), firstRoot.ToString("D")), (list, nameof(ListAggregate.Id), Guid.Parse("77777777-7777-7777-7777-777777777777").ToString("D")), (list, nameof(ListAggregate.ParentId), firstRoot.ToString("D"))),
                Row(plan, (root, nameof(HookedGuidRoot.Id), secondRoot.ToString("D")))
            };
        }

        private static void AssertListHookOrder(string[] log) {
            Assert.Equal("list:2", log[0]);
            foreach (string id in new[] { "55555555-5555-5555-5555-555555555555", "66666666-6666-6666-6666-666666666666" }) {
                int aggregate = Array.IndexOf(log, "aggregate:" + id);
                int load = Array.IndexOf(log, "load:" + id);
                Assert.True(aggregate > 0, "Missing aggregate hook for " + id + ".");
                Assert.True(load > aggregate, "Normal load must follow aggregate hook for " + id + ".");
            }
        }

        private static IReadOnlyList<object[]> ValidRows(DefinitiveJoinPlan plan) {
            Guid rootA = Guid.Parse("11111111-1111-1111-1111-111111111111");
            Guid rootB = Guid.Parse("22222222-2222-2222-2222-222222222222");
            Guid firstChild = Guid.Parse("33333333-3333-3333-3333-333333333333");
            Guid secondChild = Guid.Parse("44444444-4444-4444-4444-444444444444");
            int root = plan.RootTableIndex;
            int list = TableIndex(plan, typeof(ListAggregate));
            return new[] {
                Row(plan, (root, nameof(GuidRoot.Id), rootA.ToString("D")), (list, nameof(ListAggregate.Id), firstChild.ToString("D")), (list, nameof(ListAggregate.ParentId), rootA.ToString("D")), (list, nameof(ListAggregate.Name), "first")),
                Row(plan, (root, nameof(GuidRoot.Id), rootA.ToString("D")), (list, nameof(ListAggregate.Id), secondChild.ToString("D")), (list, nameof(ListAggregate.ParentId), rootA.ToString("D")), (list, nameof(ListAggregate.Name), "second")),
                Row(plan, (root, nameof(GuidRoot.Id), rootB.ToString("D")))
            };
        }

        private static int TableIndex(DefinitiveJoinPlan plan, Type entityType) {
            return Array.FindIndex(plan.Tables.ToArray(), table => table.EntityType == entityType);
        }

        private static object[] Row(DefinitiveJoinPlan plan, params (int TableIndex, string Column, object Value)[] values) {
            object[] row = Enumerable.Repeat<object>(DBNull.Value, plan.Projection.Length).ToArray();
            foreach ((int tableIndex, string column, object value) in values) {
                row[plan.Projection.Single(candidate => candidate.TableIndex == tableIndex && candidate.SourceColumn == column).Ordinal] = value;
            }
            return row;
        }

        private static IDbCommand CreateRowsCommand(IDbConnection connection, DefinitiveJoinPlan plan, IReadOnlyList<object[]> rows, string[]? aliases = null, bool includeSentinel = false) {
            aliases ??= plan.Projection.Select(column => column.ResultAlias).ToArray();
            if (aliases.Length != plan.Projection.Length && !includeSentinel) {
                throw new ArgumentException("Aliases must match the frozen projection unless a sentinel was requested.", nameof(aliases));
            }
            IDbCommand command = connection.CreateCommand();
            var selects = new List<string>();
            int parameterIndex = 0;
            for (int rowIndex = 0; rowIndex < rows.Count; rowIndex++) {
                var columns = new List<string>();
                for (int ordinal = 0; ordinal < aliases.Length; ordinal++) {
                    string parameterName = "@p" + parameterIndex++;
                    object value = ordinal < plan.Projection.Length ? rows[rowIndex][ordinal] : "sentinel";
                    IDbDataParameter parameter = command.CreateParameter();
                    parameter.ParameterName = parameterName;
                    parameter.Value = value;
                    command.Parameters.Add(parameter);
                    columns.Add(parameterName + " AS \"" + aliases[ordinal].Replace("\"", "\"\"") + "\"");
                }
                selects.Add("SELECT " + String.Join(", ", columns));
            }
            command.CommandText = String.Join(" UNION ALL ", selects);
            return command;
        }

        private static void AssertGraph(IReadOnlyList<GuidRoot> roots) {
            Assert.Equal(2, roots.Count);
            Assert.Equal(Guid.Parse("11111111-1111-1111-1111-111111111111"), roots[0].Id);
            Assert.Equal(Guid.Parse("22222222-2222-2222-2222-222222222222"), roots[1].Id);
            Assert.Equal(new[] {
                Guid.Parse("33333333-3333-3333-3333-333333333333"),
                Guid.Parse("44444444-4444-4444-4444-444444444444")
            }, roots[0].AggregateList.Select(child => child.Id));
            Assert.Equal(new[] { "first", "second" }, roots[0].AggregateList.Select(child => child.Name));
            Assert.Empty(roots[1].AggregateList);
        }

        private sealed class TestSqlitePlugin : IRdbmsPluginAdapter {
            public IDbConnection GetNewConnection() => new SqliteConnection("Data Source=:memory:");
            public IDbConnection GetNewSchemalessConnection() => GetNewConnection();
            public IQueryGenerator QueryGenerator { get; } = new SqliteQueryGenerator();
            public void SetConfiguration(IDictionary<string, object> settings) { }
            public bool ContinuousConnection => true;
            public int CommandTimeout => 30;
            public int ConnectTimeout => 30;
            public int PoolSize => 1;
            public string SchemaName => "main";
            public string DatabaseHost => ":memory:";
            public string ConnectionString => "Data Source=:memory:";
            public Dictionary<string, string> InfoSchemaColumnsMap { get; } = new Dictionary<string, string>();
            public object ProcessParameterValue(object value) => value;
        }
    }
}
