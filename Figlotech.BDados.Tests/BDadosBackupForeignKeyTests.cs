using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.Helpers;
using Figlotech.BDados.PgSQLDataAccessor;
using Figlotech.BDados.SqliteDataAccessor;
using Xunit;
using static Figlotech.BDados.Tests.BDadosBackupTests;

namespace Figlotech.BDados.Tests {
    public class BDadosBackupForeignKeyTests {
        [Fact]
        public async Task PostgreSqlRestoreUsesTransactionLocalSettingWithoutSessionReset() {
            var target = AccessorProxy.Create();
            target.QueryGenerator = new PgSQLQueryGenerator();
            using var input = new MemoryStream(await CreateArchiveAsync());

            await new BDadosBackup(target.Accessor).RestoreAsync(input, new[] { typeof(Row) });

            Assert.Equal("SET LOCAL session_replication_role = 'replica';", Assert.Single(target.ExecutedCommands));
            Assert.Single(target.Saved);
            Assert.Equal(1, target.Commits);
        }

        [Fact]
        public async Task PostgreSqlRestorePreservesOriginalFailureInAbortedTransaction() {
            var target = AccessorProxy.Create();
            target.QueryGenerator = new PgSQLQueryGenerator();
            var original = new InvalidOperationException("Original save failure");
            bool aborted = false;
            target.AfterSaveBatch = () => { aborted = true; throw original; };
            target.ExecuteFailure = _ => aborted ? new InvalidOperationException("25P02: transaction aborted") : null;
            using var input = new MemoryStream(await CreateArchiveAsync());

            var error = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                new BDadosBackup(target.Accessor).RestoreAsync(input, new[] { typeof(Row) }));

            Assert.Same(original, error);
            Assert.Single(target.ExecutedCommands);
            Assert.Equal(1, target.Rollbacks);
            Assert.Empty(target.Saved);
        }

        [Fact]
        public async Task PostgreSqlPermissionFailureStopsBeforeSaving() {
            var target = AccessorProxy.Create();
            target.QueryGenerator = new PgSQLQueryGenerator();
            var denied = new InvalidOperationException("42501: permission denied to set parameter session_replication_role");
            target.ExecuteFailure = _ => denied;
            using var input = new MemoryStream(await CreateArchiveAsync());

            var error = await Assert.ThrowsAsync<InvalidOperationException>(() =>
                new BDadosBackup(target.Accessor).RestoreAsync(input, new[] { typeof(Row) }));

            Assert.Same(denied, error);
            Assert.Single(target.ExecutedCommands);
            Assert.Empty(target.SavedBatches);
            Assert.Equal(1, target.Rollbacks);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task SessionScopedProvidersStillEnableChecks(bool failSave) {
            var target = AccessorProxy.Create();
            target.FailSave = failSave;
            using var input = new MemoryStream(await CreateArchiveAsync());
            var backup = new BDadosBackup(target.Accessor);

            if (failSave) {
                await Assert.ThrowsAsync<InvalidDataException>(() => backup.RestoreAsync(input, new[] { typeof(Row) }));
            } else {
                await backup.RestoreAsync(input, new[] { typeof(Row) });
            }

            Assert.Equal(new[] { "PRAGMA foreign_keys = OFF;", "PRAGMA foreign_keys = ON;" }, target.ExecutedCommands);
        }

        [Fact]
        public void ExistingSessionCommandsRemainAvailable() {
            IQueryGenerator postgres = new PgSQLQueryGenerator();
            Assert.Equal("SET session_replication_role = 'replica';", postgres.DisableForeignKeys().GetCommandText());
            Assert.Equal("SET session_replication_role = 'origin';", postgres.EnableForeignKeys().GetCommandText());
            IQueryGenerator sqlite = new SqliteQueryGenerator();
            Assert.Null(sqlite.DisableForeignKeysUntilTransactionEnd());
        }

        private static async Task<byte[]> CreateArchiveAsync() {
            var source = AccessorProxy.Create();
            source.Rows[typeof(Row)] = new[] { new Row { Id = 1, Text = "restored" } };
            using var output = new MemoryStream();
            await new BDadosBackup(source.Accessor).BackupAsync(output, new[] { typeof(Row) });
            return output.ToArray();
        }
    }
}
