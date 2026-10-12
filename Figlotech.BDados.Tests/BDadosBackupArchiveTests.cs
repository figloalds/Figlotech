using System.Buffers.Binary;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using Figlotech.BDados.Helpers;
using Figlotech.Core.Interfaces;
using Microsoft.Data.Sqlite;
using Xunit;
using static Figlotech.BDados.Tests.BDadosBackupTests;

namespace Figlotech.BDados.Tests {
    public class BDadosBackupArchiveTests {
        private static readonly Type[] Tables = { typeof(ArchiveRow) };
        private static readonly DateTime Cutoff = new(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        public class ArchiveRow : BaseDataObject<long> {
            [Field, PrimaryKey] public override long Id { get; set; }
            [Field] public override DateTime CreatedAt { get; set; }
            [Field(AllowNull = true)] public override DateTime? UpdatedAt { get; set; }
            [Field(AllowNull = true)] public byte[]? Payload { get; set; }
        }
        private static AccessorProxy Source(int size = 13) {
            var source = AccessorProxy.Create();
            source.Rows[typeof(ArchiveRow)] = new IDataObject[] { new ArchiveRow { Id = 1, CreatedAt = Cutoff, Payload = Enumerable.Range(0, size).Select(i => (byte)(i % 251)).ToArray() } };
            return source;
        }
        private static async Task<(byte[] Base, byte[] All)> Fixture(int size = 13) {
            var source = Source(size); var backup = new BDadosBackup(source.Accessor);
            using var archive = new MemoryStream();
            await backup.BackupAppendableAsync(archive, Tables);
            byte[] original = archive.ToArray();
            source.Rows[typeof(ArchiveRow)] = new IDataObject[] { new ArchiveRow { Id = 2, CreatedAt = Cutoff.AddSeconds(1), Payload = "FGCOMT02FGBDONE2"u8.ToArray() } };
            await backup.AppendIncrementalAsync(archive, Tables, Cutoff);
            return (original, archive.ToArray());
        }

        [Theory]
        [InlineData(0)] [InlineData(1)] [InlineData(65535)] [InlineData(65536)] [InlineData(81920)] [InlineData(2097152)]
        public async Task PayloadSizesRoundTripAndKeepPreviousCheckpointImmutable(int size) {
            var fixture = await Fixture(size);
            Assert.Equal(fixture.Base, fixture.All[..fixture.Base.Length]);
            var target = AccessorProxy.Create();
            using var input = new MemoryStream(fixture.All);
            var status = await new BDadosBackup(target.Accessor).RestoreAppendableAsync(input, Tables);
            Assert.Equal(2, status.BlockCount); Assert.False(status.HasIncompleteTail);
            Assert.Equal(1, target.Commits); Assert.Equal(1, target.Transactions);
            Assert.Equal(size, ((ArchiveRow)target.Saved[0]).Payload!.Length);
            Assert.Equal(2, target.Saved.Count);
        }

        [Fact]
        public async Task EveryTruncatedAppendRecoversOnlyWithExplicitPermissionAndCanAppendAgain() {
            var (original, all) = await Fixture();
            for (int length = original.Length + 1; length < all.Length; length++) {
                using var archive = new MemoryStream(); await archive.WriteAsync(all.AsMemory(0, length));
                var target = AccessorProxy.Create(); var backup = new BDadosBackup(target.Accessor);
                var state = await backup.InspectAppendableAsync(archive);
                Assert.Equal(1, state.BlockCount); Assert.Equal(original.Length, state.CommittedLength);
                Assert.Equal(length - original.Length, state.IncompleteTailBytes);
                await Assert.ThrowsAsync<InvalidDataException>(() => backup.RestoreAppendableAsync(archive, Tables));
                Assert.Equal(0, target.Transactions);
                await backup.RestoreAppendableAsync(archive, Tables, allowIncompleteTail: true);
                Assert.Single(target.Saved);
                var source = Source(); var writer = new BDadosBackup(source.Accessor);
                await Assert.ThrowsAsync<InvalidDataException>(() => writer.AppendIncrementalAsync(archive, Tables, Cutoff));
                await writer.AppendIncrementalAsync(archive, Tables, Cutoff, recoverIncompleteTail: true);
                Assert.Equal(original, archive.ToArray()[..original.Length]);
                Assert.Equal(2, (await writer.InspectAppendableAsync(archive)).BlockCount);
            }
        }

        [Fact]
        public async Task EveryTruncatedBaseIsRejectedWithoutDatabaseAccess() {
            var (original, _) = await Fixture();
            for (int length = 0; length < original.Length; length++) {
                var target = AccessorProxy.Create();
                using var input = new MemoryStream(original[..length]);
                await Assert.ThrowsAsync<InvalidDataException>(() => new BDadosBackup(target.Accessor).RestoreAppendableAsync(input, Tables, allowIncompleteTail: true));
                Assert.Equal(0, target.Transactions);
            }
        }

        [Fact]
        public async Task EverySingleByteCorruptionOfCommittedArchiveFailsBeforeDatabaseAccess() {
            var (_, all) = await Fixture();
            for (int offset = 0; offset < all.Length; offset++) {
                byte[] corrupt = (byte[])all.Clone(); corrupt[offset] ^= 1;
                var target = AccessorProxy.Create(); using var input = new MemoryStream(corrupt);
                await Assert.ThrowsAsync<InvalidDataException>(() => new BDadosBackup(target.Accessor).RestoreAppendableAsync(input, Tables, allowIncompleteTail: true));
                Assert.Equal(0, target.Transactions);
            }
        }

        [Fact]
        public async Task WriteFailuresAtEveryByteIncludingBackfillPreserveCommittedBytes() {
            var (original, all) = await Fixture();
            // The seal is written twice: zero reservation and CRC/length backfill.
            int totalWrites = all.Length - original.Length + 24;
            for (int budget = 0; budget < totalWrites; budget++) {
                using var archive = new FaultStream(original) { RemainingWriteBytes = budget };
                var source = Source();
                source.Rows[typeof(ArchiveRow)] = new IDataObject[] { new ArchiveRow { Id = 2, CreatedAt = Cutoff.AddSeconds(1), Payload = "FGCOMT02FGBDONE2"u8.ToArray() } };
                var backup = new BDadosBackup(source.Accessor);
                await Assert.ThrowsAnyAsync<Exception>(() => backup.AppendIncrementalAsync(archive, Tables, Cutoff).WaitAsync(TimeSpan.FromSeconds(5)));
                Assert.Equal(original, archive.ToArray()[..original.Length]);
                archive.RemainingWriteBytes = null;
                var state = await backup.InspectAppendableAsync(archive);
                Assert.Equal(1, state.BlockCount);
                await backup.AppendIncrementalAsync(archive, Tables, Cutoff, recoverIncompleteTail: true);
                Assert.Equal(2, (await backup.InspectAppendableAsync(archive)).BlockCount);
            }
        }

        [Theory]
        [InlineData(1, false)] [InlineData(1, true)] [InlineData(2, false)] [InlineData(2, true)]
        public async Task FlushFailuresNeverDamagePriorCommit(int flush, bool after) {
            var (original, _) = await Fixture();
            using var archive = new FaultStream(original) { FailFlush = flush, ThrowAfterFlush = after };
            var source = Source(); var backup = new BDadosBackup(source.Accessor);
            await Assert.ThrowsAsync<IOException>(() => backup.AppendIncrementalAsync(archive, Tables, Cutoff));
            Assert.Equal(original, archive.ToArray()[..original.Length]);
            archive.FailFlush = 0;
            var state = await backup.InspectAppendableAsync(archive);
            // A trailer flush failure has uncertain outcome; valid committed bytes remain usable.
            Assert.Equal(flush == 1 ? 1 : 2, state.BlockCount);
            await backup.AppendIncrementalAsync(archive, Tables, Cutoff, recoverIncompleteTail: true);
            Assert.Equal(flush == 1 ? 2 : 3, (await backup.InspectAppendableAsync(archive)).BlockCount);
        }

        [Theory]
        [InlineData(1)] [InlineData(2)]
        public async Task BackfillAndReturnSeekFailuresKeepPreviousCheckpoint(int failedSeek) {
            var (original, _) = await Fixture();
            using var archive = new FaultStream(original) { FailWritePhaseSeek = failedSeek };
            var backup = new BDadosBackup(Source().Accessor);
            await Assert.ThrowsAsync<IOException>(() => backup.AppendIncrementalAsync(archive, Tables, Cutoff));
            Assert.Equal(original, archive.ToArray()[..original.Length]);
            archive.FailWritePhaseSeek = 0;
            Assert.Equal(1, (await backup.InspectAppendableAsync(archive)).BlockCount);
            await backup.AppendIncrementalAsync(archive, Tables, Cutoff, recoverIncompleteTail: true);
            Assert.Equal(2, (await backup.InspectAppendableAsync(archive)).BlockCount);
        }

        [Fact]
        public async Task SealAndPayloadAreFlushedBeforeTrailerAndCrcMatchesIndependentImplementation() {
            var (original, _) = await Fixture();
            using var archive = new FaultStream(original);
            int flushes = 0;
            archive.BeforeFlush = () => {
                flushes++;
                byte[] bytes = archive.ToArray(); int start = original.Length;
                long length = BinaryPrimitives.ReadInt64LittleEndian(bytes.AsSpan(start + 48));
                Assert.Equal(Crc(bytes.AsSpan(start + 72, checked((int)length))), BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(start + 56)));
                Assert.Equal(start + 72 + length + (flushes == 1 ? 0 : 64), archive.Length);
                Assert.Equal(archive.Length, archive.Position);
                Assert.Equal(original, bytes[..original.Length]);
            };
            await new BDadosBackup(Source().Accessor).AppendIncrementalAsync(archive, Tables, Cutoff);
            Assert.Equal(2, flushes);
            Assert.Equal(0xCBF43926u, Crc("123456789"u8));
        }

        [Theory]
        [InlineData(1, false)] [InlineData(1, true)] [InlineData(2, false)] [InlineData(2, true)]
        public async Task DurableFileFlushFailuresCanBeInspectedAfterReopening(int failedFlush, bool after) {
            var (original, _) = await Fixture();
            string path = Path.Combine(Path.GetTempPath(), "figlotech-durable-test-" + Guid.NewGuid().ToString("N") + ".fgb");
            try {
                await File.WriteAllBytesAsync(path, original);
                var backup = new BDadosBackup(Source().Accessor);
                await using (var archive = new FaultFileStream(path, failedFlush, after)) {
                    await Assert.ThrowsAsync<IOException>(() => backup.AppendIncrementalAsync(archive, Tables, Cutoff));
                }
                Assert.Equal(original, (await File.ReadAllBytesAsync(path))[..original.Length]);
                await using var reopened = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
                var state = await backup.InspectAppendableAsync(reopened);
                Assert.Equal(failedFlush == 1 ? 1 : 2, state.BlockCount);
                await backup.AppendIncrementalAsync(reopened, Tables, Cutoff, recoverIncompleteTail: true);
                Assert.Equal(failedFlush == 1 ? 2 : 3, (await backup.InspectAppendableAsync(reopened)).BlockCount);
            } finally { File.Delete(path); }
        }

        [Fact]
        public async Task CancellationDuringPayloadIsRecoverable() {
            var (original, _) = await Fixture();
            using var cancel = new CancellationTokenSource();
            using var archive = new FaultStream(original);
            archive.AfterWrite = () => { if (archive.Length > original.Length + 72) cancel.Cancel(); };
            var backup = new BDadosBackup(Source(2 * 1024 * 1024).Accessor);
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => backup.AppendIncrementalAsync(archive, Tables, Cutoff, cancel.Token).WaitAsync(TimeSpan.FromSeconds(5)));
            archive.AfterWrite = null;
            Assert.Equal(original, archive.ToArray()[..original.Length]);
            Assert.Equal(1, (await backup.InspectAppendableAsync(archive)).BlockCount);
            await backup.AppendIncrementalAsync(archive, Tables, Cutoff, recoverIncompleteTail: true);
            Assert.Equal(2, (await backup.InspectAppendableAsync(archive)).BlockCount);
        }

        [Fact]
        public async Task WaitingWriterCancellationDoesNotInterruptCurrentOwner() {
            var (original, _) = await Fixture();
            using var archive = new FaultStream(original);
            var arrived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            archive.BeforeFlush = () => arrived.TrySetResult();
            archive.PauseFlush = release.Task;
            var backup = new BDadosBackup(Source().Accessor);
            var first = backup.AppendIncrementalAsync(archive, Tables, Cutoff);
            await arrived.Task.WaitAsync(TimeSpan.FromSeconds(5));
            using var cancellation = new CancellationTokenSource();
            var second = backup.AppendIncrementalAsync(archive, Tables, Cutoff, cancellation.Token);
            cancellation.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => second.WaitAsync(TimeSpan.FromSeconds(5)));
            release.TrySetResult();
            await first.WaitAsync(TimeSpan.FromSeconds(5));
            Assert.Equal(2, (await backup.InspectAppendableAsync(archive)).BlockCount);
        }

        [Fact]
        public async Task RemovedOrDuplicatedMiddleBlockNeverFallsBackToEarlierCheckpoint() {
            var (original, all) = await Fixture();
            using var archive = new MemoryStream(); await archive.WriteAsync(all);
            var backup = new BDadosBackup(Source().Accessor);
            await backup.AppendIncrementalAsync(archive, Tables, Cutoff);
            byte[] three = archive.ToArray();
            byte[][] corruptions = {
                three[..original.Length].Concat(three[all.Length..]).ToArray(),
                three[..all.Length].Concat(three[original.Length..all.Length]).Concat(three[all.Length..]).ToArray()
            };
            foreach (var bytes in corruptions) {
                using var corrupt = new MemoryStream(bytes);
                await Assert.ThrowsAsync<InvalidDataException>(() => backup.RestoreAppendableAsync(corrupt, Tables, allowIncompleteTail: true));
            }
        }

        [Theory]
        [InlineData(0)] [InlineData(1)] [InlineData(2)] [InlineData(3)]
        public async Task LaterCommitProvesCorruptionWhenMiddleSealAndTrailerAreDamaged(int laterDamage) {
            var (original, all) = await Fixture();
            using var archive = new MemoryStream(); await archive.WriteAsync(all);
            var backup = new BDadosBackup(Source().Accessor);
            await backup.AppendIncrementalAsync(archive, Tables, Cutoff);
            byte[] corrupt = archive.ToArray();
            corrupt.AsSpan(original.Length + 48, 24).Clear();
            corrupt[all.Length - 64] ^= 1;
            if (laterDamage == 1) corrupt[all.Length + 72 + 20] ^= 1;
            if (laterDamage == 2) corrupt[all.Length] ^= 1;
            if (laterDamage == 3) corrupt.AsSpan(all.Length + 48, 24).Clear();
            using var input = new MemoryStream(); await input.WriteAsync(corrupt);
            var target = AccessorProxy.Create();
            var restore = new BDadosBackup(target.Accessor);
            await Assert.ThrowsAsync<InvalidDataException>(() => restore.InspectAppendableAsync(input));
            await Assert.ThrowsAsync<InvalidDataException>(() => restore.RestoreAppendableAsync(input, Tables, allowIncompleteTail: true));
            Assert.Equal(0, target.Transactions);
            await Assert.ThrowsAsync<InvalidDataException>(() => backup.AppendIncrementalAsync(input, Tables, Cutoff, recoverIncompleteTail: true));
            Assert.Equal(corrupt, input.ToArray());
        }

        [Fact]
        public async Task TornCompletePrefixWithPersistedBodyFailsClosed() {
            var (original, all) = await Fixture();
            byte[] torn = all[..^64];
            // A device may persist the payload before some prefix bytes on the first durable flush.
            torn[original.Length] ^= 1;
            using var input = new MemoryStream(torn);
            var target = AccessorProxy.Create(); var backup = new BDadosBackup(target.Accessor);
            await Assert.ThrowsAsync<InvalidDataException>(() => backup.RestoreAppendableAsync(input, Tables, allowIncompleteTail: true));
            Assert.Equal(0, target.Transactions);
        }

        [Fact]
        public async Task ProducerAndCopyFailuresCancelAllPipelineParticipants() {
            var (original, _) = await Fixture();
            var source = Source(3 * 1024 * 1024);
            using var archive = new FaultStream(original) { RemainingWriteBytes = 100_000 };
            await Assert.ThrowsAnyAsync<Exception>(() => new BDadosBackup(source.Accessor).AppendIncrementalAsync(archive, Tables, Cutoff).WaitAsync(TimeSpan.FromSeconds(5)));
            Assert.Equal(original, archive.ToArray()[..original.Length]);
            using var second = new MemoryStream(); await second.WriteAsync(original);
            await Assert.ThrowsAnyAsync<Exception>(() => new BDadosBackup(source.Accessor, maxRowBytes: 1024).AppendIncrementalAsync(second, Tables, Cutoff).WaitAsync(TimeSpan.FromSeconds(5)));
            Assert.Equal(original, second.ToArray()[..original.Length]);
            using var canceled = new CancellationTokenSource(); canceled.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => new BDadosBackup(source.Accessor).AppendIncrementalAsync(second, Tables, Cutoff, canceled.Token));
        }

        [Fact]
        public async Task LastDeltaFailureRollsBackBaseInOneTransaction() {
            var (_, all) = await Fixture();
            var target = AccessorProxy.Create(); target.FailBatch = 2;
            using var archive = new MemoryStream(all);
            await Assert.ThrowsAsync<InvalidDataException>(() => new BDadosBackup(target.Accessor).RestoreAppendableAsync(archive, Tables));
            Assert.Equal(1, target.Transactions); Assert.Equal(0, target.Commits); Assert.Equal(1, target.Rollbacks); Assert.Empty(target.Saved);
        }

        [Fact]
        public async Task RealSqliteRequiresEmptyOnlyForBaseAndRollsBackFailedDelta() {
            var (_, all) = await Fixture();
            string connectionString = $"Data Source=archive-{Guid.NewGuid():N};Mode=Memory;Cache=Shared";
            await using var keeper = new SqliteConnection(connectionString); await keeper.OpenAsync();
            await using var command = keeper.CreateCommand();
            command.CommandText = "CREATE TABLE ArchiveRow (Id INTEGER PRIMARY KEY, CreatedAt TEXT, UpdatedAt TEXT, Payload BLOB); CREATE TRIGGER RejectDelta BEFORE INSERT ON ArchiveRow WHEN NEW.Id=2 BEGIN SELECT RAISE(ABORT,'synthetic delta failure'); END";
            await command.ExecuteNonQueryAsync();
            using var target = new RdbmsDataAccessor(new SharedSqlitePlugin(connectionString));
            var backup = new BDadosBackup(target, new BDadosBackupOptions { RequireEmptyTables = true });
            using var archive = new MemoryStream(all);
            await Assert.ThrowsAnyAsync<Exception>(() => backup.RestoreAppendableAsync(archive, Tables));
            command.CommandText = "SELECT COUNT(*) FROM ArchiveRow";
            Assert.Equal(0L, await command.ExecuteScalarAsync());
            command.CommandText = "DROP TRIGGER RejectDelta"; await command.ExecuteNonQueryAsync();
            await backup.RestoreAppendableAsync(archive, Tables);
            command.CommandText = "SELECT COUNT(*) FROM ArchiveRow"; Assert.Equal(2L, await command.ExecuteScalarAsync());
        }

        [Fact]
        public async Task RealSqliteIncrementalMergesUpdatesAndInsertsButDoesNotInventDeletes() {
            string sourceConnection = $"Data Source=archive-source-{Guid.NewGuid():N};Mode=Memory;Cache=Shared";
            string targetConnection = $"Data Source=archive-target-{Guid.NewGuid():N};Mode=Memory;Cache=Shared";
            await using var sourceKeeper = new SqliteConnection(sourceConnection); await sourceKeeper.OpenAsync();
            await using var targetKeeper = new SqliteConnection(targetConnection); await targetKeeper.OpenAsync();
            await using var command = sourceKeeper.CreateCommand();
            await using var targetCommand = targetKeeper.CreateCommand();
            command.CommandText = targetCommand.CommandText = "CREATE TABLE ArchiveRow (Id INTEGER PRIMARY KEY, CreatedAt TEXT, UpdatedAt TEXT, Payload BLOB)";
            await command.ExecuteNonQueryAsync(); await targetCommand.ExecuteNonQueryAsync();
            command.CommandText = "INSERT INTO ArchiveRow VALUES (1,@before,NULL,X'01'),(3,@before,NULL,X'03')";
            command.Parameters.AddWithValue("@before", Cutoff.AddSeconds(-1)); await command.ExecuteNonQueryAsync();
            using var source = new RdbmsDataAccessor(new SharedSqlitePlugin(sourceConnection));
            using var target = new RdbmsDataAccessor(new SharedSqlitePlugin(targetConnection));
            using var archive = new MemoryStream(); var backup = new BDadosBackup(source);
            await backup.BackupAppendableAsync(archive, Tables);
            command.Parameters.Clear(); command.Parameters.AddWithValue("@after", Cutoff.AddSeconds(1));
            command.CommandText = "UPDATE ArchiveRow SET Payload=X'09',UpdatedAt=@after WHERE Id=1; DELETE FROM ArchiveRow WHERE Id=3; INSERT INTO ArchiveRow VALUES(2,@after,NULL,X'02')";
            await command.ExecuteNonQueryAsync();
            await backup.AppendIncrementalAsync(archive, Tables, Cutoff);
            await new BDadosBackup(target).RestoreAppendableAsync(archive, Tables);
            targetCommand.CommandText = "SELECT COUNT(*) FROM ArchiveRow"; Assert.Equal(3L, await targetCommand.ExecuteScalarAsync());
            targetCommand.CommandText = "SELECT Payload FROM ArchiveRow WHERE Id=1"; Assert.Equal(new byte[] { 9 }, (byte[])(await targetCommand.ExecuteScalarAsync())!);
            targetCommand.CommandText = "SELECT COUNT(*) FROM ArchiveRow WHERE Id=3"; Assert.Equal(1L, await targetCommand.ExecuteScalarAsync());
        }

        [Fact]
        public async Task SameStreamConcurrentAppendsSerializeAndRepeatedRecoveryHasContiguousChain() {
            var source = Source(); var backup = new BDadosBackup(source.Accessor);
            using var archive = new MemoryStream(); await backup.BackupAppendableAsync(archive, Tables);
            await Task.WhenAll(Enumerable.Range(0, 12).Select(_ => backup.AppendIncrementalAsync(archive, Tables, Cutoff)));
            Assert.Equal(13, (await backup.InspectAppendableAsync(archive)).BlockCount);
            for (int i = 0; i < 5; i++) {
                archive.Position = archive.Length; await archive.WriteAsync("FGB"u8.ToArray());
                await backup.AppendIncrementalAsync(archive, Tables, Cutoff, recoverIncompleteTail: true);
            }
            Assert.Equal(18, (await backup.InspectAppendableAsync(archive)).BlockCount);
        }

        [Fact]
        public async Task InvalidLengthsLinksReorderedBlocksAndSchemaChangesAreRejected() {
            var (original, all) = await Fixture(); int start = original.Length;
            foreach (long value in new[] { -1L, long.MaxValue }) {
                byte[] bytes = (byte[])all.Clone(); BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(start + 48), value);
                BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(start + 60), Crc(bytes.AsSpan(start + 48, 12)));
                using var corrupt = new MemoryStream(bytes);
                await Assert.ThrowsAsync<InvalidDataException>(() => new BDadosBackup(Source().Accessor).InspectAppendableAsync(corrupt));
            }
            foreach (long link in new[] { 0L, (long)start, (long)all.Length, -1L }) {
                byte[] bytes = (byte[])all.Clone(); BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(start + 24), link);
                BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(start + 44), Crc(bytes.AsSpan(start, 44)));
                using var corrupt = new MemoryStream(bytes);
                await Assert.ThrowsAsync<InvalidDataException>(() => new BDadosBackup(Source().Accessor).InspectAppendableAsync(corrupt));
            }
            using var reordered = new MemoryStream(all[..64].Concat(all[start..]).Concat(all[64..start]).ToArray());
            await Assert.ThrowsAsync<InvalidDataException>(() => new BDadosBackup(Source().Accessor).InspectAppendableAsync(reordered));
            using var archive = new MemoryStream(); await archive.WriteAsync(all);
            await Assert.ThrowsAsync<InvalidDataException>(() => new BDadosBackup(Source().Accessor).AppendIncrementalAsync(archive, Array.Empty<Type>(), Cutoff));
            Assert.Equal(all, archive.ToArray());
        }

        [Fact]
        public async Task FileApisRestoreLegacyAndV2AndEnforceExclusiveWriter() {
            string directory = Path.Combine(Path.GetTempPath(), "figlotech-archive-test-" + Guid.NewGuid().ToString("N")); Directory.CreateDirectory(directory);
            try {
                string path = Path.Combine(directory, "archive.fgb");
                var backup = new BDadosBackup(Source().Accessor);
                await backup.BackupAppendableFileAsync(path, Tables);
                await Assert.ThrowsAsync<IOException>(() => backup.BackupAppendableFileAsync(path, Tables));
                using (var held = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None)) {
                    await Assert.ThrowsAsync<IOException>(() => backup.AppendIncrementalFileAsync(path, Tables, Cutoff));
                }
                await backup.AppendIncrementalFileAsync(path, Tables, Cutoff);
                var target = AccessorProxy.Create(); await new BDadosBackup(target.Accessor).RestoreFileAsync(path, Tables);
                Assert.Equal(1, target.Commits); Assert.Equal(2, target.Saved.Count);
                string legacy = Path.Combine(directory, "legacy.fgb"); await backup.BackupFileAsync(legacy, Tables);
                var other = AccessorProxy.Create(); await new BDadosBackup(other.Accessor).RestoreFileAsync(legacy, Tables); Assert.Single(other.Saved);
                await Assert.ThrowsAsync<InvalidDataException>(() => backup.AppendIncrementalFileAsync(legacy, Tables, Cutoff));
            } finally { Directory.Delete(directory, true); }
        }

        private static uint Crc(ReadOnlySpan<byte> bytes) {
            uint crc = uint.MaxValue;
            foreach (byte value in bytes) { crc ^= value; for (int bit = 0; bit < 8; bit++) crc = (crc >> 1) ^ ((crc & 1) != 0 ? 0xEDB88320u : 0); }
            return ~crc;
        }
        private sealed class FaultFileStream(string path, int failedFlush, bool after)
            : FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None, 65536, FileOptions.Asynchronous) {
            private int _durableFlushes;
            public override void Flush(bool flushToDisk) {
                bool fail = flushToDisk && ++_durableFlushes == failedFlush;
                if (fail && !after) throw new IOException("synthetic durable flush failure");
                base.Flush(flushToDisk);
                if (fail) throw new IOException("synthetic durable flush failure");
            }
        }
        private sealed class FaultStream : MemoryStream {
            internal int? RemainingWriteBytes;
            internal int FailFlush;
            internal bool ThrowAfterFlush;
            internal int FailWritePhaseSeek;
            internal Action? BeforeFlush;
            internal Action? AfterWrite;
            internal Task? PauseFlush;
            private bool _writing;
            private int _writeSeeks;
            private int _flushes;
            internal FaultStream(byte[] initial) { base.Write(initial); }
            public override long Position {
                get => base.Position;
                set {
                    if (_writing && ++_writeSeeks == FailWritePhaseSeek) throw new IOException("synthetic seek failure");
                    base.Position = value;
                }
            }
            public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default) {
                cancellationToken.ThrowIfCancellationRequested();
                _writing = true;
                if (RemainingWriteBytes is int remaining && remaining < buffer.Length) {
                    base.Write(buffer.Span[..remaining]); RemainingWriteBytes = 0; throw new IOException("synthetic torn write");
                }
                base.Write(buffer.Span);
                if (RemainingWriteBytes.HasValue) RemainingWriteBytes -= buffer.Length;
                AfterWrite?.Invoke();
                return ValueTask.CompletedTask;
            }
            public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) => WriteAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();
            public override async Task FlushAsync(CancellationToken cancellationToken) {
                BeforeFlush?.Invoke();
                if (PauseFlush != null) await PauseFlush.WaitAsync(cancellationToken);
                bool fail = ++_flushes == FailFlush;
                if (fail && !ThrowAfterFlush) throw new IOException("synthetic flush failure");
                await base.FlushAsync(cancellationToken);
                if (fail) throw new IOException("synthetic flush failure");
            }
        }
    }
}
