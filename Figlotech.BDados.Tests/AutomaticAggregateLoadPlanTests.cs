using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.SqliteDataAccessor;
using Figlotech.Core.Interfaces;
using Figlotech.Data;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Figlotech.BDados.Tests {
    public sealed class AutomaticAggregateLoadPlanTests {
        [Fact]
        public void AggregateLoadLinearUsesCanonicalScalarPlanAndExcludesLists() {
            using var accessor = CreateAccessor(out CapturingGenerator capture);
            using BDadosTransaction transaction = accessor.CreateNewTransaction(CancellationToken.None, null);
            Seed(transaction.Connection);

            List<RuntimePlanRoot> roots = accessor.AggregateLoad(transaction,
                new LoadAllArgs<RuntimePlanRoot>().NoLists().Where(root => root.Id == RootId));
            DefinitiveJoinPlan expected = AutomaticJoinPlanCache.GetOrAdd(typeof(RuntimePlanRoot), AggregateJoinShape.ScalarAggregatesOnly);

            Assert.Same(expected, capture.Plan);
            Assert.Equal(AggregateJoinShape.ScalarAggregatesOnly, capture.Plan!.Shape);
            Assert.DoesNotContain(capture.Plan.Tables, table => table.EntityType == typeof(RuntimeList));
            Assert.Single(roots);
            Assert.Equal("scalar", roots[0].ScalarName);
            Assert.Empty(roots[0].Items);
            Assert.Contains(capture.Plan.Tables[capture.Plan.RootTableIndex].Prefix + ".Id", capture.Sql!);
        }

        [Fact]
        public async Task AggregateLoadAsyncUsesCanonicalFullPlanAndMaterializesOrderedLists() {
            using var accessor = CreateAccessor(out CapturingGenerator capture);
            await using BDadosTransaction transaction = await accessor.CreateNewTransactionAsync(CancellationToken.None, null);
            Seed(transaction.Connection);

            List<RuntimePlanRoot> roots = await accessor.AggregateLoadAsync(transaction,
                new LoadAllArgs<RuntimePlanRoot>().Full().Where(root => root.Id == RootId));
            DefinitiveJoinPlan expected = AutomaticJoinPlanCache.GetOrAdd(typeof(RuntimePlanRoot), AggregateJoinShape.FullGraph);

            Assert.Same(expected, capture.Plan);
            Assert.Equal(AggregateJoinShape.FullGraph, capture.Plan!.Shape);
            Assert.Single(roots);
            Assert.Equal("scalar", roots[0].ScalarName);
            Assert.Equal(new[] { "first", "second" }, roots[0].Items.Select(item => item.Name));
        }

        [Fact]
        public async Task AggregateLoadCoroutineUsesCanonicalFullPlanAndClientRootPaging() {
            using var accessor = CreateAccessor(out CapturingGenerator capture);
            await using BDadosTransaction transaction = await accessor.CreateNewTransactionAsync(CancellationToken.None, null);
            Seed(transaction.Connection);

            var roots = new List<RuntimePlanRoot>();
            await foreach (RuntimePlanRoot root in accessor.AggregateLoadAsyncCoroutinely(transaction,
                new LoadAllArgs<RuntimePlanRoot>().Full().Skip(0).Limit(1))) {
                roots.Add(root);
            }

            Assert.Same(AutomaticJoinPlanCache.GetOrAdd(typeof(RuntimePlanRoot), AggregateJoinShape.FullGraph), capture.Plan);
            Assert.Single(roots);
            Assert.Equal(new[] { "first", "second" }, roots[0].Items.Select(item => item.Name));
        }

        [Fact]
        public async Task AutomaticPublicAsyncAndCoroutinePreserveTheirDistinctHookLifecycles() {
            using var accessor = CreateAccessor(out _);
            await using BDadosTransaction transaction = await accessor.CreateNewTransactionAsync(CancellationToken.None, null);
            SeedHooked(transaction.Connection);

            var asyncLog = new System.Collections.Concurrent.ConcurrentQueue<string>();
            await accessor.AggregateLoadAsync(transaction, new LoadAllArgs<HookedGuidRoot>().Full().WithContext(asyncLog));
            Assert.Equal("list:1", asyncLog.First());
            Assert.True(Array.IndexOf(asyncLog.ToArray(), "aggregate:" + RootId) < Array.IndexOf(asyncLog.ToArray(), "load:" + RootId));

            var coroutineLog = new System.Collections.Concurrent.ConcurrentQueue<string>();
            await foreach (HookedGuidRoot _ in accessor.AggregateLoadAsyncCoroutinely(transaction, new LoadAllArgs<HookedGuidRoot>().Full().WithContext(coroutineLog))) {
            }
            Assert.DoesNotContain(coroutineLog, value => value.StartsWith("list:", StringComparison.Ordinal));
            Assert.True(Array.IndexOf(coroutineLog.ToArray(), "aggregate:" + RootId) < Array.IndexOf(coroutineLog.ToArray(), "load:" + RootId));
        }

        [Fact]
        public async Task AggregateLoadAsyncFallbackPreservesNormalLoadThenListHookWithoutAggregateHook() {
            using var accessor = CreateAccessor(out _);
            await using BDadosTransaction transaction = await accessor.CreateNewTransactionAsync(CancellationToken.None, null);
            SeedNonAggregate(transaction.Connection);

            var log = new System.Collections.Concurrent.ConcurrentQueue<string>();
            List<NonAggregateHookLongRoot> roots = await accessor.AggregateLoadAsync(transaction,
                new LoadAllArgs<NonAggregateHookLongRoot>().Full().WithContext(log));

            Assert.Single(roots);
            Assert.Equal(42L, roots[0].Id);

            string[] events = log.ToArray();
            Assert.DoesNotContain(events, value => value.StartsWith("aggregate:", StringComparison.Ordinal));
            Assert.Equal(2, events.Length);
            Assert.Equal("load:42", events[0]);
            Assert.Equal("list:1", events[1]);
        }

        [Fact]
        public void AggregateLoadMaterializesListRowsUsingChildRemoteFieldWhenItMatchesParentIdentifierName() {
            using var accessor = CreateAccessor(out _);
            using BDadosTransaction transaction = accessor.CreateNewTransaction(CancellationToken.None, null);
            SeedCollidingListKeys(transaction.Connection);

            List<CollidingListKeyRoot> roots = accessor.AggregateLoad(transaction,
                new LoadAllArgs<CollidingListKeyRoot>().Full().Where(root => root.ParentReference == RootId));

            CollidingListKeyRoot root = Assert.Single(roots);
            Assert.Equal(RootId, root.ParentReference);
            Assert.Equal(new[] { "first", "second" }, root.Children.Select(child => child.Name));
            Assert.All(root.Children, child => Assert.Equal(RootId, child.ParentReference));
        }

        private static readonly Guid RootId = Guid.Parse("11111111-1111-1111-1111-111111111111");
        private static readonly Guid ScalarId = Guid.Parse("22222222-2222-2222-2222-222222222222");

        private static RdbmsDataAccessor CreateAccessor(out CapturingGenerator capture) {
            IQueryGenerator generator = CapturingGenerator.Create(out capture);
            return new RdbmsDataAccessor(new SqlitePlugin(generator));
        }

        private static void Seed(IDbConnection connection) {
            Execute(connection, "CREATE TABLE RuntimePlanRoot (Id TEXT NOT NULL, ScalarId TEXT NOT NULL)");
            Execute(connection, "CREATE TABLE RuntimeScalar (Id TEXT NOT NULL, Name TEXT NULL)");
            Execute(connection, "CREATE TABLE RuntimeList (Id TEXT NOT NULL, RootId TEXT NOT NULL, Name TEXT NULL)");
            Execute(connection, "INSERT INTO RuntimePlanRoot (Id, ScalarId) VALUES ('" + RootId + "', '" + ScalarId + "')");
            Execute(connection, "INSERT INTO RuntimeScalar (Id, Name) VALUES ('" + ScalarId + "', 'scalar')");
            Execute(connection, "INSERT INTO RuntimeList (Id, RootId, Name) VALUES ('33333333-3333-3333-3333-333333333333', '" + RootId + "', 'first')");
            Execute(connection, "INSERT INTO RuntimeList (Id, RootId, Name) VALUES ('44444444-4444-4444-4444-444444444444', '" + RootId + "', 'second')");
        }

        private static void SeedHooked(IDbConnection connection) {
            Execute(connection, "CREATE TABLE HookedGuidRoot (Id TEXT NOT NULL)");
            Execute(connection, "CREATE TABLE ListAggregate (Id TEXT NOT NULL, ParentId TEXT NOT NULL, Name TEXT NULL)");
            Execute(connection, "INSERT INTO HookedGuidRoot (Id) VALUES ('" + RootId + "')");
            Execute(connection, "INSERT INTO ListAggregate (Id, ParentId, Name) VALUES ('33333333-3333-3333-3333-333333333333', '" + RootId + "', 'child')");
        }

        private static void SeedNonAggregate(IDbConnection connection) {
            Execute(connection, "CREATE TABLE NonAggregateHookLongRoot (Id INTEGER NOT NULL)");
            Execute(connection, "INSERT INTO NonAggregateHookLongRoot (Id) VALUES (42)");
        }

        private static void SeedCollidingListKeys(IDbConnection connection) {
            Execute(connection, "CREATE TABLE CollidingListKeyRoot (ParentReference TEXT NOT NULL)");
            Execute(connection, "CREATE TABLE CollidingListKeyChild (ChildIdentifier TEXT NOT NULL, ParentReference TEXT NOT NULL, Name TEXT NULL)");
            Execute(connection, "INSERT INTO CollidingListKeyRoot (ParentReference) VALUES ('" + RootId + "')");
            Execute(connection, "INSERT INTO CollidingListKeyChild (ChildIdentifier, ParentReference, Name) VALUES ('33333333-3333-3333-3333-333333333333', '" + RootId + "', 'first')");
            Execute(connection, "INSERT INTO CollidingListKeyChild (ChildIdentifier, ParentReference, Name) VALUES ('44444444-4444-4444-4444-444444444444', '" + RootId + "', 'second')");
        }

        private static void Execute(IDbConnection connection, string sql) {
            using IDbCommand command = connection.CreateCommand();
            command.CommandText = sql;
            command.ExecuteNonQuery();
        }

        private class CapturingGenerator : DispatchProxy {
            private readonly IQueryGenerator _inner = new SqliteQueryGenerator();
            public DefinitiveJoinPlan? Plan { get; private set; }
            public string? Sql { get; private set; }

            public static IQueryGenerator Create(out CapturingGenerator capture) {
                IQueryGenerator result = DispatchProxy.Create<IQueryGenerator, CapturingGenerator>();
                capture = (CapturingGenerator)(object)result;
                return result;
            }

            protected override object? Invoke(MethodInfo? targetMethod, object?[]? args) {
                if (targetMethod == null) {
                    throw new InvalidOperationException("Query generator method was not supplied.");
                }
                object? result = targetMethod.Invoke(_inner, args);
                if (targetMethod.Name == nameof(IQueryGenerator.GenerateJoinQuery) && args![0] is DefinitiveJoinPlan plan) {
                    Plan = plan;
                    Sql = ((IQueryBuilder)result!).GetCommandText();
                }
                return result;
            }
        }

        private sealed class SqlitePlugin : IRdbmsPluginAdapter {
            public SqlitePlugin(IQueryGenerator generator) { QueryGenerator = generator; }
            public IDbConnection GetNewConnection() => new SqliteConnection("Data Source=:memory:");
            public IDbConnection GetNewSchemalessConnection() => GetNewConnection();
            public IQueryGenerator QueryGenerator { get; }
            public void SetConfiguration(IDictionary<string, object> settings) { }
            public bool ContinuousConnection => true;
            public TimeSpan CommandTimeout => TimeSpan.FromSeconds(30);
            public TimeSpan ConnectTimeout => TimeSpan.FromSeconds(30);
            public int PoolSize => 1;
            public string SchemaName => "main";
            public string DatabaseHost => ":memory:";
            public string ConnectionString => "Data Source=:memory:";
            public IReadOnlyDictionary<string, string> InfoSchemaColumnsMap { get; } = new Dictionary<string, string>();
            public object ProcessParameterValue(object value) => value;
        }
    }
}
