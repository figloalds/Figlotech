using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Runtime.ExceptionServices;
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
        public void AggregateLoadLinearMaterializesNestedObjectsWithoutQueryingTheirLists() {
            using var accessor = CreateAccessor(out CapturingGenerator capture);
            using BDadosTransaction transaction = accessor.CreateNewTransaction(CancellationToken.None, null);
            SeedLinearNestedObject(transaction.Connection);

            List<SharedNestedObjectRoot> roots = accessor.AggregateLoad(transaction,
                new LoadAllArgs<SharedNestedObjectRoot>().NoLists().Where(root => root.Id == RootId));

            SharedNestedObjectRoot root = Assert.Single(roots);
            Assert.NotNull(root.First);
            Assert.NotNull(root.Second);
            Assert.Equal("nested scalar", root.First!.NestedName);
            Assert.Equal("nested scalar", root.Second!.NestedName);
            Assert.Equal("nested object", root.First.NestedObject!.Name);
            Assert.Equal("nested object", root.Second.NestedObject!.Name);
            Assert.Empty(root.First.NestedList);
            Assert.Empty(root.Second.NestedList);
            Assert.DoesNotContain(capture.Plan!.Tables, table => table.EntityType == typeof(SharedNestedListItem));
            Assert.DoesNotContain(capture.Plan.Relations, relation => relation.BuildKind == AggregateBuildOptions.AggregateList);
            Assert.DoesNotContain(nameof(SharedNestedListItem), capture.Sql!, StringComparison.OrdinalIgnoreCase);
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

        [Theory]
        [InlineData(false, OrderingType.Asc, 1, 2)]
        [InlineData(false, OrderingType.Desc, 2, 1)]
        [InlineData(true, OrderingType.Asc, 2, 1)]
        [InlineData(true, OrderingType.Desc, 1, 2)]
        public async Task AggregateLoadAsyncOrdersLegacyAggregateRootsByRequestedProjectedMember(
            bool orderByRid, OrderingType orderingType, long firstId, long secondId) {
            using var accessor = CreateAccessor(out _);
            await using BDadosTransaction transaction = await accessor.CreateNewTransactionAsync(CancellationToken.None, null);
            SeedLegacyOrderedAggregates(transaction.Connection);

            DefinitiveJoinPlan plan = AutomaticJoinPlanCache.GetOrAdd(typeof(LegacyOrderedAggregateRoot), AggregateJoinShape.FullGraph);
            Assert.Equal(nameof(LegacyOrderedAggregateRoot.RID), plan.RootOrdering.ColumnName);

            var args = new LoadAllArgs<LegacyOrderedAggregateRoot>().Full();
            if (orderByRid) {
                args.OrderBy(root => root.RID, orderingType);
            } else {
                args.OrderBy(root => root.Id, orderingType);
            }

            List<LegacyOrderedAggregateRoot> roots = await accessor.AggregateLoadAsync(transaction, args);

            Assert.Equal(new[] { firstId, secondId }, roots.Select(root => root.Id));
            Assert.All(roots, root => Assert.Equal(2, root.Children.Count));
            Assert.Equal(new[] { "one-a", "one-b" }, roots.Single(root => root.Id == 1).Children.Select(child => child.Name));
            Assert.Equal(new[] { "two-a", "two-b" }, roots.Single(root => root.Id == 2).Children.Select(child => child.Name));
        }

        [Fact]
        public async Task AggregateLoadAsyncRejectsOrderingByAggregateMember() {
            using var accessor = CreateAccessor(out _);
            await using BDadosTransaction transaction = await accessor.CreateNewTransactionAsync(CancellationToken.None, null);
            SeedLegacyOrderedAggregates(transaction.Connection);

            ArgumentException exception = await Assert.ThrowsAsync<ArgumentException>(() => accessor.AggregateLoadAsync(transaction,
                new LoadAllArgs<LegacyOrderedAggregateRoot>().Full().OrderBy(root => root.Children)));

            Assert.Contains("projected member", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("root table", exception.Message, StringComparison.OrdinalIgnoreCase);
        }

        [Fact]
        public async Task AggregateLoadAsyncRejectsShadowedBaseOrderingMember() {
            using var accessor = CreateAccessor(out _);
            await using BDadosTransaction transaction = await accessor.CreateNewTransactionAsync(CancellationToken.None, null);
            SeedShadowedOrderingAggregate(transaction.Connection);

            ArgumentException exception = await Assert.ThrowsAsync<ArgumentException>(() => accessor.AggregateLoadAsync(transaction,
                new LoadAllArgs<ShadowedOrderingAggregateRoot>().Full()
                    .OrderBy(root => ((ShadowedOrderingAggregateRootBase)root).ShadowedColumn)));

            Assert.Contains("projected member", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("root table", exception.Message, StringComparison.OrdinalIgnoreCase);
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

        [Theory]
        [InlineData("sync", true, AggregateJoinShape.ScalarAggregatesOnly, false)]
        [InlineData("sync", false, AggregateJoinShape.FullGraph, true)]
        [InlineData("async", true, AggregateJoinShape.ScalarAggregatesOnly, false)]
        [InlineData("async", false, AggregateJoinShape.FullGraph, true)]
        [InlineData("coroutine", true, AggregateJoinShape.ScalarAggregatesOnly, false)]
        [InlineData("coroutine", false, AggregateJoinShape.FullGraph, true)]
        public async Task AggregateHooksReceiveTheExecutedAggregationQueryShape(
            string loadPath,
            bool linear,
            AggregateJoinShape expectedShape,
            bool includesOneToManyAggregations) {
            using var accessor = CreateAccessor(out _);
            using BDadosTransaction transaction = accessor.CreateNewTransaction(CancellationToken.None, null);
            SeedContextAwareHook(transaction.Connection);
            var probe = new AggregateLoadContextProbe();
            LoadAllArgs<ContextAwareHookRoot> args = new LoadAllArgs<ContextAwareHookRoot>()
                .LinearIf(linear)
                .Where(root => root.Id == RootId)
                .WithContext(probe);

            ContextAwareHookRoot root;
            switch (loadPath) {
                case "sync":
                    root = Assert.Single(accessor.AggregateLoad(transaction, args));
                    break;
                case "async":
                    root = Assert.Single(await accessor.AggregateLoadAsync(transaction, args));
                    break;
                case "coroutine":
                    var roots = new List<ContextAwareHookRoot>();
                    await foreach (ContextAwareHookRoot item in accessor.AggregateLoadAsyncCoroutinely(transaction, args)) {
                        roots.Add(item);
                    }
                    root = Assert.Single(roots);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(loadPath), loadPath, "Unknown aggregate load path.");
            }

            Assert.NotNull(root.AggregateObject);
            Assert.Equal(linear ? 0 : 1, root.AggregateList.Count);
            Assert.NotEmpty(probe.Contexts);
            Assert.All(probe.Contexts, context => {
                Assert.True(context.IsAggregateLoad);
                Assert.Equal(expectedShape, context.AggregateShape);
                Assert.Equal(linear, context.IsLinearAggregateLoad);
                Assert.Equal(includesOneToManyAggregations, context.IncludesOneToManyAggregations);
                Assert.Same(accessor, context.DataAccessor);
                Assert.Same(transaction, context.Transaction);
            });
        }

        [Theory]
        [InlineData("sync")]
        [InlineData("async")]
        [InlineData("coroutine")]
        public async Task LinearListOnlyAggregateFallbackPreservesAggregateQueryShapeForHooks(string loadPath) {
            using var accessor = CreateAccessor(out _);
            using BDadosTransaction transaction = accessor.CreateNewTransaction(CancellationToken.None, null);
            SeedContextAwareListOnlyHook(transaction.Connection);
            var probe = new AggregateLoadContextProbe();
            LoadAllArgs<ContextAwareListOnlyHookRoot> args = new LoadAllArgs<ContextAwareListOnlyHookRoot>()
                .NoLists()
                .Where(root => root.Id == 42L)
                .WithContext(probe);

            ContextAwareListOnlyHookRoot root;
            switch (loadPath) {
                case "sync":
                    root = Assert.Single(accessor.AggregateLoad(transaction, args));
                    break;
                case "async":
                    root = Assert.Single(await accessor.AggregateLoadAsync(transaction, args));
                    break;
                case "coroutine":
                    var roots = new List<ContextAwareListOnlyHookRoot>();
                    await foreach (ContextAwareListOnlyHookRoot item in accessor.AggregateLoadAsyncCoroutinely(transaction, args)) {
                        roots.Add(item);
                    }
                    root = Assert.Single(roots);
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(loadPath), loadPath, "Unknown aggregate load path.");
            }

            Assert.Empty(root.AggregateList);
            Assert.NotEmpty(probe.Contexts);
            Assert.All(probe.Contexts, context => {
                Assert.True(context.IsAggregateLoad);
                Assert.Equal(AggregateJoinShape.ScalarAggregatesOnly, context.AggregateShape);
                Assert.True(context.IsLinearAggregateLoad);
                Assert.False(context.IncludesOneToManyAggregations);
                Assert.Same(accessor, context.DataAccessor);
                Assert.Same(transaction, context.Transaction);
            });
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

        private static void SeedLinearNestedObject(IDbConnection connection) {
            Guid parentId = Guid.Parse("55555555-5555-5555-5555-555555555555");
            Guid nestedObjectId = Guid.Parse("66666666-6666-6666-6666-666666666666");
            Execute(connection, "CREATE TABLE SharedNestedObjectRoot (Id TEXT NOT NULL, SharedId TEXT NOT NULL)");
            Execute(connection, "CREATE TABLE SharedNestedParent (Id TEXT NOT NULL, RootId TEXT NOT NULL, NestedScalarId TEXT NOT NULL, NestedObjectId TEXT NOT NULL)");
            Execute(connection, "CREATE TABLE ScalarAggregate (Id TEXT NOT NULL, Name TEXT NULL)");
            Execute(connection, "CREATE TABLE ObjectAggregate (Id TEXT NOT NULL, Name TEXT NULL)");
            Execute(connection, "INSERT INTO SharedNestedObjectRoot (Id, SharedId) VALUES ('" + RootId + "', '" + parentId + "')");
            Execute(connection, "INSERT INTO SharedNestedParent (Id, RootId, NestedScalarId, NestedObjectId) VALUES ('" + parentId + "', '" + RootId + "', '" + ScalarId + "', '" + nestedObjectId + "')");
            Execute(connection, "INSERT INTO ScalarAggregate (Id, Name) VALUES ('" + ScalarId + "', 'nested scalar')");
            Execute(connection, "INSERT INTO ObjectAggregate (Id, Name) VALUES ('" + nestedObjectId + "', 'nested object')");
        }

        private static void SeedContextAwareHook(IDbConnection connection) {
            Guid objectId = Guid.Parse("77777777-7777-7777-7777-777777777777");
            Execute(connection, "CREATE TABLE ContextAwareHookRoot (Id TEXT NOT NULL, ObjectAggregateId TEXT NOT NULL)");
            Execute(connection, "CREATE TABLE ObjectAggregate (Id TEXT NOT NULL, Name TEXT NULL)");
            Execute(connection, "CREATE TABLE ListAggregate (Id TEXT NOT NULL, ParentId TEXT NOT NULL, Name TEXT NULL)");
            Execute(connection, "INSERT INTO ContextAwareHookRoot (Id, ObjectAggregateId) VALUES ('" + RootId + "', '" + objectId + "')");
            Execute(connection, "INSERT INTO ObjectAggregate (Id, Name) VALUES ('" + objectId + "', 'object')");
            Execute(connection, "INSERT INTO ListAggregate (Id, ParentId, Name) VALUES ('33333333-3333-3333-3333-333333333333', '" + RootId + "', 'child')");
        }

        private static void SeedContextAwareListOnlyHook(IDbConnection connection) {
            Execute(connection, "CREATE TABLE ContextAwareListOnlyHookRoot (Id INTEGER NOT NULL)");
            Execute(connection, "INSERT INTO ContextAwareListOnlyHookRoot (Id) VALUES (42)");
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

        private static void SeedLegacyOrderedAggregates(IDbConnection connection) {
            Execute(connection, "CREATE TABLE LegacyOrderedAggregateRoot (Id INTEGER NOT NULL, UpdatedAt TEXT NULL, CreatedAt TEXT NOT NULL, RID TEXT NOT NULL, IsActive INTEGER NOT NULL, AlteredBy INTEGER NOT NULL, CreatedBy INTEGER NOT NULL)");
            Execute(connection, "CREATE TABLE LegacyOrderedAggregateChild (Id INTEGER NOT NULL, RootRID TEXT NOT NULL, Name TEXT NULL)");
            Execute(connection, "INSERT INTO LegacyOrderedAggregateRoot (Id, UpdatedAt, CreatedAt, RID, IsActive, AlteredBy, CreatedBy) VALUES (1, NULL, '2026-01-01', 'z-rid', 1, 0, 0)");
            Execute(connection, "INSERT INTO LegacyOrderedAggregateRoot (Id, UpdatedAt, CreatedAt, RID, IsActive, AlteredBy, CreatedBy) VALUES (2, NULL, '2026-01-01', 'a-rid', 1, 0, 0)");
            Execute(connection, "INSERT INTO LegacyOrderedAggregateChild (Id, RootRID, Name) VALUES (11, 'z-rid', 'one-a')");
            Execute(connection, "INSERT INTO LegacyOrderedAggregateChild (Id, RootRID, Name) VALUES (12, 'z-rid', 'one-b')");
            Execute(connection, "INSERT INTO LegacyOrderedAggregateChild (Id, RootRID, Name) VALUES (21, 'a-rid', 'two-a')");
            Execute(connection, "INSERT INTO LegacyOrderedAggregateChild (Id, RootRID, Name) VALUES (22, 'a-rid', 'two-b')");
        }

        private static void SeedShadowedOrderingAggregate(IDbConnection connection) {
            Execute(connection, "CREATE TABLE ShadowedOrderingAggregateRoot (Id INTEGER NOT NULL, ShadowedColumn TEXT NULL)");
            Execute(connection, "CREATE TABLE ShadowedOrderingAggregateChild (Id INTEGER NOT NULL, RootId INTEGER NOT NULL)");
            Execute(connection, "INSERT INTO ShadowedOrderingAggregateRoot (Id, ShadowedColumn) VALUES (1, 'derived')");
            Execute(connection, "INSERT INTO ShadowedOrderingAggregateChild (Id, RootId) VALUES (2, 1)");
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
                object? result;
                try {
                    result = targetMethod.Invoke(_inner, args);
                } catch (TargetInvocationException exception) when (exception.InnerException != null) {
                    ExceptionDispatchInfo.Capture(exception.InnerException).Throw();
                    throw;
                }
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
