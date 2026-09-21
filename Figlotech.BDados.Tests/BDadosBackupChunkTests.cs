using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using Figlotech.BDados.Helpers;
using Figlotech.Core.Interfaces;
using Microsoft.Data.Sqlite;
using Xunit;
using static Figlotech.BDados.Tests.BDadosBackupTests;

namespace Figlotech.BDados.Tests {
    public class BDadosBackupChunkTests {
        [Fact]
        public void OptionsDefaultPerInstanceAndConstructorAcceptsSuppliedOptions() {
            var accessor = AccessorProxy.Create().Accessor;
            var first = new BDadosBackup(accessor);
            var second = new BDadosBackup(accessor);
            Assert.Equal(1000, first.Options.RestoreChunkSize);
            first.Options.RestoreChunkSize = 25;
            Assert.Equal(1000, second.Options.RestoreChunkSize);
            var options = new BDadosBackupOptions { RestoreChunkSize = 50 };
            var configured = new BDadosBackup(accessor, options, maxHeaderBytes: 256, maxRowBytes: 128);
            Assert.Same(options, configured.Options);
            Assert.Equal(256, configured.MaxHeaderBytes);
            Assert.Equal(128, configured.MaxRowBytes);
            Assert.Equal(1000, new BDadosBackup(accessor, options: null!).Options.RestoreChunkSize);
        }

        [Theory]
        [InlineData(0, new int[0])]
        [InlineData(1, new[] { 1 })]
        [InlineData(1000, new[] { 1000 })]
        [InlineData(1001, new[] { 1000, 1 })]
        [InlineData(2501, new[] { 1000, 1000, 501 })]
        public async Task DefaultRestoreSavesFullChunksAndFinalRemainder(int count, int[] expected) {
            byte[] archive = await MakeRows(count);
            var target = AccessorProxy.Create();
            using var input = new ForwardStream(true, archive);
            await new BDadosBackup(target.Accessor).RestoreAsync(input, new[] { typeof(Row) });
            Assert.Equal(expected, target.SavedBatches.Select(b => b.Rows.Length));
            Assert.Equal(Enumerable.Range(1, count).Select(i => (long)i), target.Saved.Cast<Row>().Select(r => r.Id));
            Assert.Equal(1, target.Transactions);
            Assert.Equal(1, target.Commits);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task CustomChunkSizeCanBeSetByPropertyOrConstructor(bool constructor) {
            var target = AccessorProxy.Create();
            var backup = constructor
                ? new BDadosBackup(target.Accessor, new BDadosBackupOptions { RestoreChunkSize = 2 })
                : new BDadosBackup(target.Accessor);
            if (!constructor) backup.Options.RestoreChunkSize = 2;
            using var input = new ForwardStream(true, await MakeRows(5));
            await backup.RestoreAsync(input, new[] { typeof(Row) });
            Assert.Equal(new[] { 2, 2, 1 }, target.SavedBatches.Select(b => b.Rows.Length));
        }

        [Theory]
        [InlineData(0)]
        [InlineData(-1)]
        public async Task InvalidChunkSizeFailsBeforeReadingOrStartingTransaction(int size) {
            var target = AccessorProxy.Create();
            var backup = new BDadosBackup(target.Accessor);
            backup.Options.RestoreChunkSize = size;
            using var input = new ForwardStream(true);
            var exception = await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => backup.RestoreAsync(input, new[] { typeof(Row) }));
            Assert.Equal("RestoreChunkSize", exception.ParamName);
            Assert.Equal(0, target.Transactions);
        }

        [Fact]
        public async Task ChunkSizeChangesDuringRestoreApplyToNextOperation() {
            var target = AccessorProxy.Create();
            var backup = new BDadosBackup(target.Accessor, new BDadosBackupOptions { RestoreChunkSize = 2 });
            target.AfterSaveBatch = () => backup.Options.RestoreChunkSize = 3;
            byte[] archive = await MakeRows(5);
            using var first = new ForwardStream(true, archive);
            await backup.RestoreAsync(first, new[] { typeof(Row) });
            Assert.Equal(new[] { 2, 2, 1 }, target.SavedBatches.Select(b => b.Rows.Length));
            target.SavedBatches.Clear();
            using var second = new ForwardStream(true, archive);
            await backup.RestoreAsync(second, new[] { typeof(Row) });
            Assert.Equal(new[] { 3, 2 }, target.SavedBatches.Select(b => b.Rows.Length));
        }

        [Fact]
        public async Task BatchesKeepConcreteTableTypesAndFlushBeforeNextTable() {
            var source = AccessorProxy.Create();
            source.Rows[typeof(Row)] = new Row[] { new Row { Id = 1 }, new Row { Id = 2 }, new Row { Id = 3 } };
            source.Rows[typeof(OtherRow)] = new OtherRow[] { new OtherRow { Id = 4 } };
            using var output = new ForwardStream(false);
            await new BDadosBackup(source.Accessor).BackupAsync(output, new[] { typeof(Row), typeof(OtherRow) });
            var target = AccessorProxy.Create();
            using var input = new ForwardStream(true, output.Bytes);
            await new BDadosBackup(target.Accessor, new BDadosBackupOptions { RestoreChunkSize = 2 })
                .RestoreAsync(input, new[] { typeof(OtherRow), typeof(Row) });
            Assert.Equal(new[] { typeof(Row), typeof(Row), typeof(OtherRow) }, target.SavedBatches.Select(b => b.Type));
            Assert.Equal(new[] { 2, 1, 1 }, target.SavedBatches.Select(b => b.Rows.Length));
        }

        [Theory]
        [InlineData(2)]
        [InlineData(3)]
        public async Task FailedFullOrPartialBatchRollsBackPreviouslySavedChunks(int failingBatch) {
            var target = AccessorProxy.Create();
            target.FailBatch = failingBatch;
            using var input = new ForwardStream(true, await MakeRows(5));
            await Assert.ThrowsAsync<InvalidDataException>(() => new BDadosBackup(target.Accessor, new BDadosBackupOptions { RestoreChunkSize = 2 })
                .RestoreAsync(input, new[] { typeof(Row) }));
            Assert.Equal(failingBatch, target.SavedBatches.Count);
            Assert.Equal(1, target.Rollbacks);
            Assert.Empty(target.Saved);
            Assert.Equal(0, target.Commits);
        }

        [Fact]
        public async Task CorruptFooterRollsBackAllSavedChunks() {
            byte[] archive = await MakeRows(2001);
            archive[^1] ^= 1;
            var target = AccessorProxy.Create();
            using var input = new ForwardStream(true, archive);
            await Assert.ThrowsAsync<InvalidDataException>(() => new BDadosBackup(target.Accessor).RestoreAsync(input, new[] { typeof(Row) }));
            Assert.Equal(new[] { 1000, 1000, 1 }, target.SavedBatches.Select(b => b.Rows.Length));
            Assert.Empty(target.Saved);
            Assert.Equal(1, target.Rollbacks);
        }

        [Fact]
        public async Task CancellationAfterFirstBatchStopsFurtherSavesAndRollsBack() {
            using var cancellation = new CancellationTokenSource();
            var target = AccessorProxy.Create();
            target.AfterSaveBatch = cancellation.Cancel;
            using var input = new ForwardStream(true, await MakeRows(2001));
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => new BDadosBackup(target.Accessor)
                .RestoreAsync(input, new[] { typeof(Row) }, cancellation.Token));
            Assert.Single(target.SavedBatches);
            Assert.Empty(target.Saved);
            Assert.Equal(1, target.Rollbacks);
        }

        private static async Task<byte[]> MakeRows(int count) {
            var source = AccessorProxy.Create();
            source.Rows[typeof(Row)] = Enumerable.Range(1, count).Select(i => new Row { Id = i, Text = $"Row {i}" }).ToArray();
            using var output = new ForwardStream(false);
            await new BDadosBackup(source.Accessor).BackupAsync(output, new[] { typeof(Row) });
            return output.Bytes;
        }

        [Fact]
        public async Task LegacyModelsUseBulkSaveAndPreserveRestoredTimestampsInSqlite() {
            string connectionString = $"Data Source=bulk-restore-{Guid.NewGuid():N};Mode=Memory;Cache=Shared";
            await using var keeper = new SqliteConnection(connectionString);
            await keeper.OpenAsync();
            await using var command = keeper.CreateCommand();
            command.CommandText = "CREATE TABLE LegacyRow (Id INTEGER PRIMARY KEY, RID TEXT, Text TEXT, CreatedAt TEXT, UpdatedAt TEXT); INSERT INTO LegacyRow (Id, RID, Text) VALUES (1, 'row-1', 'old')";
            await command.ExecuteNonQueryAsync();
            var timestamp = new DateTime(2024, 1, 2, 3, 4, 5, DateTimeKind.Utc);
            var source = AccessorProxy.Create();
            source.Rows[typeof(LegacyRow)] = Enumerable.Range(1, 3).Select(i => new LegacyRow {
                Id = i, RID = $"row-{i}", Text = $"restored-{i}", CreatedAt = timestamp, UpdatedAt = timestamp
            }).ToArray();
            using var output = new ForwardStream(false);
            await new BDadosBackup(source.Accessor).BackupAsync(output, new[] { typeof(LegacyRow) });
            // Verify the restore binding passes the concrete legacy type and sync flag to the bulk API.
            var proxy = AccessorProxy.Create();
            using var proxyInput = new ForwardStream(true, output.Bytes);
            await new BDadosBackup(proxy.Accessor).RestoreAsync(proxyInput, new[] { typeof(LegacyRow) });
            Assert.Equal(typeof(LegacyRow), Assert.Single(proxy.SavedBatches).Type);
            Assert.All(proxy.Saved.Cast<LegacyRow>(), row => {
                Assert.True(row.IsReceivedFromSync);
                Assert.Equal(timestamp, row.UpdatedAt);
            });
            using var target = new RdbmsDataAccessor(new SharedSqlitePlugin(connectionString));
            using var input = new ForwardStream(true, output.Bytes);
            await new BDadosBackup(target, new BDadosBackupOptions { RestoreChunkSize = 2 })
                .RestoreAsync(input, new[] { typeof(LegacyRow) });
            command.CommandText = "SELECT Id, Text, UpdatedAt FROM LegacyRow ORDER BY Id";
            await using var reader = await command.ExecuteReaderAsync();
            for (int i = 1; i <= 3; i++) {
                Assert.True(await reader.ReadAsync());
                Assert.Equal(i, reader.GetInt64(0));
                Assert.Equal($"restored-{i}", reader.GetString(1));
                Assert.Equal(timestamp.Ticks, reader.GetDateTime(2).Ticks);
            }
            Assert.False(await reader.ReadAsync());
        }

        public class LegacyRow : Row, ILegacyDataObject {
            [Field, ReliableId] public string RID { get; set; } = "";
            [Field] public override DateTime CreatedAt { get; set; }
            [Field] public override DateTime? UpdatedAt { get; set; }
            public bool IsPersisted { get; set; }
            public int PersistedHash { get; set; }
            public ulong AlteredBy { get; set; }
            public ulong CreatedBy { get; set; }
            public bool IsReceivedFromSync { get; set; }
        }
    }
}
