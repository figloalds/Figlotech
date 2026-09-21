using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.Helpers;
using Figlotech.BDados.MySqlDataAccessor;
using Figlotech.BDados.PgSQLDataAccessor;
using Figlotech.BDados.SqliteDataAccessor;
using Microsoft.Data.Sqlite;
using Xunit;
using static Figlotech.BDados.Tests.BDadosBackupTests;
using static Figlotech.BDados.Tests.BDadosBackupChunkTests;

namespace Figlotech.BDados.Tests {
    public class BDadosBackupIdentityTests {
        [Theory]
        [InlineData("SQLite")]
        [InlineData("MySQL")]
        [InlineData("PostgreSQL")]
        public void BulkInsertBindsDistinctValuesForEveryRow(string engine) {
            IQueryGenerator generator = engine switch {
                "SQLite" => new SqliteQueryGenerator(),
                "MySQL" => new MySqlQueryGenerator(),
                _ => new PgSQLQueryGenerator()
            };
            var rows = Enumerable.Range(1, 1000).Select(i => new LegacyRow {
                Id = i, RID = $"row-{i}", Text = $"value-{i}"
            }).ToList();
            var query = generator.GenerateMultiInsert(rows, false);
            var parameters = query.GetParameters();
            Assert.Equal(Enumerable.Range(1, 1000).Select(i => (long)i), parameters.Values.OfType<long>().OrderBy(id => id));
            Assert.Equal(rows.Select(r => r.RID).OrderBy(rid => rid), parameters.Values.OfType<string>().Where(v => v.StartsWith("row-")).OrderBy(rid => rid));
            Assert.Equal(rows.Select(r => r.Text).OrderBy(text => text), parameters.Values.OfType<string>().Where(v => v.StartsWith("value-")).OrderBy(text => text));
        }

        [Fact]
        public async Task DuplicateTableRegistrationFailsBeforeReadingOrWriting() {
            var accessor = AccessorProxy.Create();
            var backup = new BDadosBackup(accessor.Accessor);
            using var output = new ForwardStream(false);
            await Assert.ThrowsAsync<ArgumentException>(() => backup.BackupAsync(output, new[] { typeof(Row), typeof(Row) }));
            Assert.Empty(output.Bytes);
            using var input = new ForwardStream(true);
            await Assert.ThrowsAsync<ArgumentException>(() => backup.RestoreAsync(input, new[] { typeof(Row), typeof(Row) }));
            Assert.Equal(0, accessor.Transactions);
        }

        [Fact]
        public async Task SameIdInDifferentTablesIsSavedOnceForEachTable() {
            var source = AccessorProxy.Create();
            source.Rows[typeof(Row)] = new Row[] { new Row { Id = 1, Text = "first" } };
            source.Rows[typeof(OtherRow)] = new OtherRow[] { new OtherRow { Id = 1, Text = "second" } };
            using var output = new ForwardStream(false);
            await new BDadosBackup(source.Accessor).BackupAsync(output, new[] { typeof(Row), typeof(OtherRow) });
            Assert.Equal(2, source.Fetches);
            var target = AccessorProxy.Create();
            using var input = new ForwardStream(true, output.Bytes);
            await new BDadosBackup(target.Accessor).RestoreAsync(input, new[] { typeof(Row), typeof(OtherRow) });
            Assert.Equal(2, target.Saved.Count);
            Assert.Equal("first", Assert.IsType<Row>(target.Saved[0]).Text);
            Assert.Equal("second", Assert.IsType<OtherRow>(target.Saved[1]).Text);
            Assert.All(target.SavedBatches, batch => Assert.Single(batch.Rows));
        }

        [Theory]
        [InlineData(1)]
        [InlineData(1000)]
        [InlineData(2501)]
        public async Task RealLegacyBackupAndRestoreVisitEveryRecordOnceIncludingAcrossBatches(int chunkSize) {
            string sourceConnection = $"Data Source=identity-source-{Guid.NewGuid():N};Mode=Memory;Cache=Shared";
            string targetConnection = $"Data Source=identity-target-{Guid.NewGuid():N};Mode=Memory;Cache=Shared";
            await using var sourceKeeper = new SqliteConnection(sourceConnection);
            await using var targetKeeper = new SqliteConnection(targetConnection);
            await sourceKeeper.OpenAsync();
            await targetKeeper.OpenAsync();
            const string ddl = "CREATE TABLE LegacyRow (Id INTEGER PRIMARY KEY, RID TEXT UNIQUE, Text TEXT, CreatedAt TEXT, UpdatedAt TEXT)";
            await using var sourceCommand = sourceKeeper.CreateCommand();
            sourceCommand.CommandText = ddl + "; WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM seq WHERE n < 2501) "
                + "INSERT INTO LegacyRow SELECT n, 'row-' || n, 'value-' || n, '2024-01-02 03:04:05', '2024-01-02 03:04:05' FROM seq";
            await sourceCommand.ExecuteNonQueryAsync();
            await using var targetCommand = targetKeeper.CreateCommand();
            targetCommand.CommandText = ddl + "; CREATE TABLE SaveAudit (Id INTEGER); "
                + "CREATE TRIGGER TrackInsert AFTER INSERT ON LegacyRow BEGIN INSERT INTO SaveAudit VALUES (NEW.Id); END; "
                + "CREATE TRIGGER TrackUpdate AFTER UPDATE ON LegacyRow BEGIN INSERT INTO SaveAudit VALUES (NEW.Id); END";
            await targetCommand.ExecuteNonQueryAsync();
            using var source = new RdbmsDataAccessor(new SharedSqlitePlugin(sourceConnection));
            using var target = new RdbmsDataAccessor(new SharedSqlitePlugin(targetConnection));
            using var output = new ForwardStream(false);
            await new BDadosBackup(source).BackupAsync(output, new[] { typeof(LegacyRow) });
            // Inspect every archived row through an independent accessor before the real restore.
            var inspected = AccessorProxy.Create();
            using var inspection = new ForwardStream(true, output.Bytes);
            await new BDadosBackup(inspected.Accessor).RestoreAsync(inspection, new[] { typeof(LegacyRow) });
            Assert.Equal(2501, inspected.Saved.Count);
            Assert.Equal(Enumerable.Range(1, 2501).Select(i => (long)i), inspected.Saved.Cast<LegacyRow>().Select(r => r.Id).OrderBy(id => id));
            var backup = new BDadosBackup(target, new BDadosBackupOptions { RestoreChunkSize = chunkSize });
            // Audit writes, not only final row counts: an upsert could otherwise hide repeated saves.
            for (int attempt = 0; attempt < 2; attempt++) {
                using var input = new ForwardStream(true, output.Bytes);
                await backup.RestoreAsync(input, new[] { typeof(LegacyRow) });
                targetCommand.CommandText = "SELECT COUNT(*) FROM SaveAudit";
                Assert.Equal(2501L, await targetCommand.ExecuteScalarAsync());
                targetCommand.CommandText = "SELECT COUNT(DISTINCT Id) FROM SaveAudit";
                Assert.Equal(2501L, await targetCommand.ExecuteScalarAsync());
                targetCommand.CommandText = "SELECT COUNT(*) FROM LegacyRow WHERE RID = 'row-' || Id AND Text = 'value-' || Id";
                Assert.Equal(2501L, await targetCommand.ExecuteScalarAsync());
                targetCommand.CommandText = "DELETE FROM SaveAudit";
                await targetCommand.ExecuteNonQueryAsync();
            }
        }
    }
}
