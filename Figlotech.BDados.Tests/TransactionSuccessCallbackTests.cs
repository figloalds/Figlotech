using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.Exceptions;
using Figlotech.BDados.SqliteDataAccessor;
using Figlotech.Core.Interfaces;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Figlotech.BDados.Tests {
    public sealed class TransactionSuccessCallbackTests {
        [Theory]
        [InlineData(false, false, "empty")]
        [InlineData(false, true, "empty")]
        [InlineData(true, false, "empty")]
        [InlineData(true, true, "empty")]
        [InlineData(false, false, "read")]
        [InlineData(false, true, "read")]
        [InlineData(true, false, "read")]
        [InlineData(true, true, "read")]
        [InlineData(false, false, "write")]
        [InlineData(false, true, "write")]
        [InlineData(true, false, "write")]
        [InlineData(true, true, "write")]
        public async Task AccessCallbacksRunAfterCleanupAndCanAcquireTheOnlyConnectionSlot(bool asynchronous, bool databaseTransaction, string operation) {
            using var accessor = CreateAccessor();
            var observations = new List<(bool ConnectionCleared, bool TransactionCleared, ConnectionState State, int ActiveConnections)>();
            var results = new List<long>();
            var errors = new List<Exception>();
            IsolationLevel? isolation = databaseTransaction ? IsolationLevel.Serializable : null;

            void RegisterCallbacks(BDadosTransaction transaction) {
                IDbConnection connection = transaction.Connection;
                void Observe() => observations.Add((transaction.Connection == null, !transaction.IsUsingRdbmsTransaction,
                    connection.State, accessor.ActiveConnectionStatus().Count));

                transaction.ExecuteWhenSuccess(() => {
                    Observe();
                    results.Add(accessor.Access(nested => {
                        using IDbCommand command = nested.CreateCommand();
                        command.CommandText = "SELECT 1";
                        return Convert.ToInt64(command.ExecuteScalar());
                    }, null));
                }, errors.Add);
                transaction.ExecuteWhenSuccess(async () => {
                    await Task.Yield();
                    Observe();
                    results.Add(await accessor.AccessAsync(async nested => {
                        using DbCommand command = (DbCommand)await nested.CreateCommandAsync();
                        command.CommandText = "SELECT 2";
                        return Convert.ToInt64(await command.ExecuteScalarAsync());
                    }, CancellationToken.None, null));
                }, error => {
                    errors.Add(error);
                    return Task.CompletedTask;
                });
            }

            if (asynchronous) {
                await accessor.AccessAsync(async transaction => {
                    RegisterCallbacks(transaction);
                    await ExecuteOperationAsync(transaction, operation);
                }, CancellationToken.None, isolation);
            } else {
                accessor.Access(transaction => {
                    RegisterCallbacks(transaction);
                    ExecuteOperationAsync(transaction, operation).GetAwaiter().GetResult();
                }, isolation);
            }

            Assert.Empty(errors);
            Assert.Equal(new long[] { 1, 2 }, results);
            Assert.Equal(2, observations.Count);
            Assert.All(observations, observation => {
                Assert.True(observation.ConnectionCleared);
                Assert.True(observation.TransactionCleared);
                Assert.Equal(ConnectionState.Closed, observation.State);
                Assert.Equal(0, observation.ActiveConnections);
            });
        }

        [Theory]
        [InlineData(false, false, false)]
        [InlineData(false, false, true)]
        [InlineData(false, true, false)]
        [InlineData(false, true, true)]
        [InlineData(true, false, false)]
        [InlineData(true, false, true)]
        [InlineData(true, true, false)]
        [InlineData(true, true, true)]
        public async Task ManualCommitDefersCallbacksUntilEndOrDisposeAndRunsThemOnce(bool asynchronous, bool databaseTransaction, bool explicitEnd) {
            using var accessor = CreateAccessor();
            BDadosTransaction transaction = await CreateTransactionAsync(accessor, asynchronous, databaseTransaction);
            var target = new MutationTarget { Name = "before" };
            int calls = 0;
            bool callbackSawMutation = false;
            transaction.AddMutateTarget(typeof(MutationTarget).GetProperty(nameof(MutationTarget.Name))!, target, "after");
            transaction.ExecuteWhenSuccess(() => {
                calls++;
                callbackSawMutation = target.Name == "after" && target.UpdatedAt.HasValue;
            });

            try {
                await CommitAsync(transaction, asynchronous);
                Assert.Equal("after", target.Name);
                Assert.NotNull(target.UpdatedAt);
                Assert.True(transaction.IsCommited);
                Assert.Equal(0, calls);
                await CommitAsync(transaction, asynchronous);
                if (explicitEnd) {
                    await EndAsync(transaction, asynchronous);
                    await EndAsync(transaction, asynchronous);
                } else {
                    await DisposeAsync(transaction, asynchronous);
                }
                Assert.Equal(1, calls);
                Assert.True(callbackSawMutation);
                Assert.Null(transaction.Connection);
                Assert.Throws<BDadosException>(() => transaction.ExecuteWhenSuccess(() => { }));
            } finally {
                await DisposeAsync(transaction, asynchronous);
                await DisposeAsync(transaction, asynchronous);
            }
            Assert.Equal(1, calls);
        }

        [Theory]
        [InlineData(false, false)]
        [InlineData(false, true)]
        [InlineData(true, false)]
        [InlineData(true, true)]
        public async Task DisposalAutoCommitsBeforeRunningCallbacks(bool asynchronous, bool databaseTransaction) {
            using var accessor = CreateAccessor();
            BDadosTransaction transaction = await CreateTransactionAsync(accessor, asynchronous, databaseTransaction);
            bool completed = false;
            transaction.ExecuteWhenSuccess(() => completed = transaction.IsCommited && transaction.Connection == null);
            await DisposeAsync(transaction, asynchronous);
            Assert.True(completed);
        }

        [Theory]
        [InlineData(false, false)]
        [InlineData(false, true)]
        [InlineData(true, false)]
        [InlineData(true, true)]
        public async Task RollbackDiscardsCallbacksIncludingWithoutDatabaseTransaction(bool asynchronous, bool databaseTransaction) {
            using var accessor = CreateAccessor();
            BDadosTransaction transaction = await CreateTransactionAsync(accessor, asynchronous, databaseTransaction);
            int calls = 0;
            transaction.ExecuteWhenSuccess(() => calls++);
            if (asynchronous) {
                await transaction.RollbackAsync();
            } else {
                transaction.Rollback();
            }
            await CommitAsync(transaction, asynchronous);
            await DisposeAsync(transaction, asynchronous);
            Assert.True(transaction.IsRolledBack);
            Assert.False(transaction.IsCommited);
            Assert.Equal(0, calls);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task FailedAccessDiscardsCallbacks(bool asynchronous) {
            using var accessor = CreateAccessor();
            int calls = 0;
            if (asynchronous) {
                await Assert.ThrowsAsync<BDadosException>(() => accessor.AccessAsync<int>(transaction => {
                    transaction.ExecuteWhenSuccess(() => calls++);
                    throw new InvalidOperationException("user code failed");
                }, CancellationToken.None, null));
            } else {
                Assert.Throws<BDadosException>(() => accessor.Access(transaction => {
                    transaction.ExecuteWhenSuccess(() => calls++);
                    throw new InvalidOperationException("user code failed");
                }, null));
            }
            Assert.Equal(0, calls);
            Assert.Empty(accessor.ActiveConnectionStatus());
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task FailedCommitDiscardsCallbacksAndIsNotRetriedByDisposal(bool asynchronous) {
            using var accessor = CreateAccessor();
            BDadosTransaction transaction = await CreateTransactionAsync(accessor, asynchronous, false);
            int calls = 0;
            transaction.ExecuteWhenSuccess(() => calls++);
            using (IDbCommand setup = transaction.CreateCommand()) {
                setup.CommandText = "PRAGMA foreign_keys = ON; CREATE TABLE parent (id INTEGER PRIMARY KEY); " +
                    "CREATE TABLE child (parent_id INTEGER REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED);";
                setup.ExecuteNonQuery();
            }
            transaction.BeginTransaction(IsolationLevel.Serializable);
            using (IDbCommand insert = transaction.CreateCommand()) {
                insert.CommandText = "INSERT INTO child (parent_id) VALUES (42)";
                insert.ExecuteNonQuery();
                transaction.NotifyWriteOperation();
            }

            await Assert.ThrowsAsync<SqliteException>(() => CommitAsync(transaction, asynchronous));
            await DisposeAsync(transaction, asynchronous);
            Assert.False(transaction.IsCommited);
            Assert.True(transaction.IsRolledBack);
            Assert.Equal(0, calls);
            Assert.Empty(accessor.ActiveConnectionStatus());
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task CallbackFailuresUseHandlersAndContinueInOrder(bool asynchronous) {
            using var accessor = CreateAccessor();
            BDadosTransaction transaction = await CreateTransactionAsync(accessor, asynchronous, true);
            var order = new List<int>();
            var failure = new InvalidOperationException("callback failed");
            Exception? handled = null;
            transaction.ExecuteWhenSuccess((Action)(() => { order.Add(1); throw failure; }), error => {
                handled = error;
                order.Add(2);
            });
            transaction.ExecuteWhenSuccess(async () => {
                await Task.Yield();
                order.Add(3);
                throw failure;
            }, async error => {
                await Task.Yield();
                order.Add(4);
                throw new InvalidOperationException("handler failed", error);
            });
            transaction.ExecuteWhenSuccess((Action)(() => { order.Add(5); throw failure; }));
            transaction.ExecuteWhenSuccess(() => order.Add(6));

            await CommitAsync(transaction, asynchronous);
            await EndAsync(transaction, asynchronous);
            await DisposeAsync(transaction, asynchronous);
            Assert.Same(failure, handled);
            Assert.Equal(new[] { 1, 2, 3, 4, 5, 6 }, order);
            Assert.True(transaction.IsCommited);
            Assert.False(transaction.IsRolledBack);
        }

        [Fact]
        public async Task AsyncCleanupAwaitsCallbacksBeforeReturning() {
            using var accessor = CreateAccessor();
            BDadosTransaction transaction = await CreateTransactionAsync(accessor, true, true);
            var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            transaction.ExecuteWhenSuccess(async () => {
                started.SetResult();
                await release.Task;
            });
            await transaction.CommitAsync();
            Task cleanup = transaction.DisposeAsync().AsTask();
            try {
                await started.Task.WaitAsync(TimeSpan.FromSeconds(5));
                Assert.False(cleanup.IsCompleted);
                Assert.Null(transaction.Connection);
                Assert.Empty(accessor.ActiveConnectionStatus());
            } finally {
                release.TrySetResult();
                await cleanup;
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task ThrowingEndingHookStillClosesResourcesAndDiscardsCallbacks(bool asynchronous) {
            using var accessor = CreateAccessor();
            BDadosTransaction transaction = await CreateTransactionAsync(accessor, asynchronous, true);
            IDbConnection connection = transaction.Connection;
            int calls = 0;
            transaction.ExecuteWhenSuccess(() => calls++);
            typeof(BDadosTransaction).GetProperty(nameof(BDadosTransaction.OnTransactionEnding))!
                .SetValue(transaction, (Action)(() => throw new InvalidOperationException("ending hook failed")));
            await CommitAsync(transaction, asynchronous);
            await Assert.ThrowsAsync<InvalidOperationException>(() => EndAsync(transaction, asynchronous));
            await DisposeAsync(transaction, asynchronous);
            Assert.Equal(0, calls);
            Assert.Null(transaction.Connection);
            Assert.Equal(ConnectionState.Closed, connection.State);
            Assert.Empty(accessor.ActiveConnectionStatus());
            using BDadosTransaction next = accessor.CreateNewTransaction(CancellationToken.None, null);
        }

        private static RdbmsDataAccessor CreateAccessor() {
            return new RdbmsDataAccessor(new SqlitePlugin(new SqlitePluginConfiguration {
                DataSource = ":memory:", Schema = "main", PoolSize = 1
            }));
        }

        private static async Task<BDadosTransaction> CreateTransactionAsync(RdbmsDataAccessor accessor, bool asynchronous, bool databaseTransaction) {
            IsolationLevel? isolation = databaseTransaction ? IsolationLevel.Serializable : null;
            return asynchronous
                ? await accessor.CreateNewTransactionAsync(CancellationToken.None, isolation)
                : accessor.CreateNewTransaction(CancellationToken.None, isolation);
        }

        private static async Task CommitAsync(BDadosTransaction transaction, bool asynchronous) {
            if (asynchronous) { await transaction.CommitAsync(); } else { transaction.Commit(); }
        }

        private static async Task EndAsync(BDadosTransaction transaction, bool asynchronous) {
            if (asynchronous) { await transaction.EndTransactionAsync(); } else { transaction.EndTransaction(); }
        }

        private static async Task DisposeAsync(BDadosTransaction transaction, bool asynchronous) {
            if (asynchronous) { await transaction.DisposeAsync(); } else { transaction.Dispose(); }
        }

        private static async Task ExecuteOperationAsync(BDadosTransaction transaction, string operation) {
            if (operation == "empty") { return; }
            using DbCommand command = (DbCommand)await transaction.CreateCommandAsync();
            command.CommandText = operation == "read" ? "SELECT 1" : "CREATE TABLE sample (id INTEGER)";
            await command.ExecuteNonQueryAsync();
            if (operation == "write") { transaction.NotifyWriteOperation(); }
        }

        private sealed class MutationTarget : IDataObject {
            public object Id { get; set; } = 0;
            public DateTime CreatedAt { get; set; }
            public DateTime? UpdatedAt { get; set; }
            public string Name { get; set; } = string.Empty;
        }
    }
}
