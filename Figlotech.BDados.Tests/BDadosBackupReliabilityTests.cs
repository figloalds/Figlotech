using System.Collections;
using System.Reflection;
using Figlotech.BDados.Business;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using Figlotech.BDados.Helpers;
using Figlotech.Core.BusinessModel;
using Figlotech.Core.Interfaces;
using Microsoft.Data.Sqlite;
using Xunit;
using static Figlotech.BDados.Tests.BDadosBackupTests;
using static Figlotech.BDados.Tests.BDadosBackupChunkTests;

namespace Figlotech.BDados.Tests {
    public class BDadosBackupReliabilityTests {
        internal static async Task<byte[]> ArchiveAsync(params IDataObject[] rows) {
            var source = AccessorProxy.Create();
            foreach (var group in rows.GroupBy(row => row.GetType())) source.Rows[group.Key] = group.ToArray();
            using var output = new MemoryStream();
            await new BDadosBackup(source.Accessor).BackupAsync(output, source.Rows.Keys);
            return output.ToArray();
        }

        [Fact]
        public async Task GenericRestorePreservesGeneratedIdAndTimestampsOnInsertAndUpdate() {
            await using var database = await SqliteDatabase.CreateAsync("CREATE TABLE TimedRow (Id INTEGER PRIMARY KEY AUTOINCREMENT, Text TEXT, CreatedAt TEXT, UpdatedAt TEXT)");
            var timestamp = new DateTime(2001, 2, 3, 4, 5, 6, DateTimeKind.Utc);
            var row = new TimedRow { Id = 42, Text = "original", CreatedAt = timestamp, UpdatedAt = timestamp };
            for (int attempt = 0; attempt < 2; attempt++) {
                using var archive = new MemoryStream(await ArchiveAsync(row));
                await new BDadosBackup(database.Accessor).RestoreAsync(archive, new[] { typeof(TimedRow) });
                Assert.Equal(42L, await database.ScalarAsync("SELECT Id FROM TimedRow"));
                Assert.Equal(timestamp, DateTime.Parse((string)(await database.ScalarAsync("SELECT UpdatedAt FROM TimedRow"))!));
                Assert.Equal(row.Text, await database.ScalarAsync("SELECT Text FROM TimedRow"));
                row.Text = "updated";
            }
            await database.ExecuteAsync("INSERT INTO TimedRow (Text) VALUES ('normal insert')");
            Assert.Equal(43L, await database.ScalarAsync("SELECT Id FROM TimedRow WHERE Text = 'normal insert'"));
            var normal = new TimedRow { Text = "normal save", UpdatedAt = timestamp };
            await database.Accessor.AccessAsync(async transaction => { await database.Accessor.SaveItemAsync(transaction, normal); }, default);
            Assert.True((long)(await database.ScalarAsync("SELECT Id FROM TimedRow WHERE Text = 'normal save'"))! > 43);
            Assert.True(normal.UpdatedAt > timestamp);
        }

        [Fact]
        public async Task MissingColumnsUseDatabaseDefaultsAndStayUnchangedOnMerge() {
            await using var database = await SqliteDatabase.CreateAsync("CREATE TABLE WithDefaults (Id INTEGER PRIMARY KEY, Text TEXT, Added TEXT NOT NULL DEFAULT 'database', Number INTEGER NOT NULL DEFAULT 7)");
            var bytes = await ArchiveAsync(new Row { Id = 42, Text = "old" });
            var backup = new BDadosBackup(database.Accessor);
            var report = await backup.RestoreWithReportAsync(new MemoryStream(bytes), new[] { typeof(WithDefaults) });
            Assert.Equal("database", await database.ScalarAsync("SELECT Added FROM WithDefaults"));
            Assert.Equal(7L, await database.ScalarAsync("SELECT Number FROM WithDefaults"));
            Assert.Equal(new[] { "Added", "Number" }, report.Tables.Single().DefaultedColumns.OrderBy(n => n));
            await database.ExecuteAsync("UPDATE WithDefaults SET Added = 'existing', Number = 99");
            await backup.RestoreAsync(new MemoryStream(bytes), new[] { typeof(WithDefaults) });
            Assert.Equal("existing", await database.ScalarAsync("SELECT Added FROM WithDefaults"));
            Assert.Equal(99L, await database.ScalarAsync("SELECT Number FROM WithDefaults"));
        }

        [Fact]
        public async Task MissingRequiredColumnWithoutDefaultRollsBack() {
            await using var database = await SqliteDatabase.CreateAsync("CREATE TABLE WithDefaults (Id INTEGER PRIMARY KEY, Text TEXT, Added TEXT NOT NULL, Number INTEGER NOT NULL DEFAULT 7)");
            using var archive = new MemoryStream(await ArchiveAsync(new Row { Id = 42 }));
            await Assert.ThrowsAnyAsync<Exception>(() => new BDadosBackup(database.Accessor).RestoreAsync(archive, new[] { typeof(WithDefaults) }));
            Assert.Equal(0L, await database.ScalarAsync("SELECT COUNT(*) FROM WithDefaults"));
        }

        [Theory]
        [InlineData(1)]
        [InlineData(1000)]
        public async Task DuplicateKeysFailAndRollBackAcrossChunkBoundaries(int chunkSize) {
            await using var database = await SqliteDatabase.CreateAsync("CREATE TABLE Row (Id INTEGER PRIMARY KEY, Text TEXT)");
            using var archive = new MemoryStream(await ArchiveAsync(new Row { Id = 42, Text = "first" }, new Row { Id = 42, Text = "second" }));
            var error = await Assert.ThrowsAnyAsync<Exception>(() => new BDadosBackup(database.Accessor, new BDadosBackupOptions { RestoreChunkSize = chunkSize })
                .RestoreAsync(archive, new[] { typeof(Row) }));
            Assert.Contains("Duplicate primary key", error.ToString());
            Assert.Equal(0L, await database.ScalarAsync("SELECT COUNT(*) FROM Row"));
        }

        [Fact]
        public async Task UnknownTablesFailByDefaultAndExplicitSkipsAreReported() {
            var bytes = await ArchiveAsync(new Row { Id = 1 });
            var target = AccessorProxy.Create();
            var backup = new BDadosBackup(target.Accessor);
            await Assert.ThrowsAsync<InvalidDataException>(() => backup.RestoreAsync(new MemoryStream(bytes), Array.Empty<Type>()));
            Assert.Equal(0, target.Transactions);
            backup.Options.IgnoredTables.Add(nameof(Row));
            var report = await backup.RestoreWithReportAsync(new MemoryStream(bytes), Array.Empty<Type>());
            var table = Assert.Single(report.Tables);
            Assert.True(table.Skipped);
            Assert.Equal(1, table.RowsRead);
            Assert.Equal(0, table.RowsSaved);
            bytes[^1] ^= 1;
            await Assert.ThrowsAsync<InvalidDataException>(() => backup.RestoreAsync(new MemoryStream(bytes), Array.Empty<Type>()));
        }

        [Fact]
        public async Task ByteBudgetFlushesEarlyAndIdentityLimitFailsClosed() {
            var bytes = await ArchiveAsync(new Row { Id = 1, Text = "123456" }, new Row { Id = 2, Text = "123456" });
            var target = AccessorProxy.Create();
            var backup = new BDadosBackup(target.Accessor, new BDadosBackupOptions { RestoreChunkBytes = 20 });
            await backup.RestoreAsync(new MemoryStream(bytes), new[] { typeof(Row) });
            Assert.Equal(new[] { 1, 1 }, target.SavedBatches.Select(b => b.Rows.Length));
            backup.Options.MaxTrackedKeys = 1;
            await Assert.ThrowsAsync<InvalidDataException>(() => backup.RestoreAsync(new MemoryStream(bytes), new[] { typeof(Row) }));
        }

        [Fact]
        public async Task RequireEmptyModeRejectsExistingRows() {
            await using var database = await SqliteDatabase.CreateAsync("CREATE TABLE Row (Id INTEGER PRIMARY KEY, Text TEXT); INSERT INTO Row VALUES (99, 'keep')");
            using var archive = new MemoryStream(await ArchiveAsync(new Row { Id = 1 }));
            await Assert.ThrowsAnyAsync<Exception>(() => new BDadosBackup(database.Accessor, new BDadosBackupOptions { RequireEmptyTables = true })
                .RestoreAsync(archive, new[] { typeof(Row) }));
            Assert.Equal(99L, await database.ScalarAsync("SELECT Id FROM Row"));
        }

        [Fact]
        public async Task CollationChangesCannotSilentlyMergeDifferentKeys() {
            await using var database = await SqliteDatabase.CreateAsync("CREATE TABLE StringRow (Id TEXT COLLATE NOCASE PRIMARY KEY, Text TEXT)");
            using var archive = new MemoryStream(await ArchiveAsync(new StringRow { Id = "a", Text = "first" }, new StringRow { Id = "A", Text = "second" }));
            var failure = await Assert.ThrowsAnyAsync<Exception>(() => new BDadosBackup(database.Accessor)
                .RestoreAsync(archive, new[] { typeof(StringRow) }));
            Assert.Contains("different identities", failure.ToString());
            Assert.Equal(0L, await database.ScalarAsync("SELECT COUNT(*) FROM StringRow"));
        }

        [Fact]
        public async Task TriggerValueChangesAreDetectedAndRolledBack() {
            await using var database = await SqliteDatabase.CreateAsync("CREATE TABLE Row (Id INTEGER PRIMARY KEY, Text TEXT); CREATE TRIGGER Mutate AFTER INSERT ON Row BEGIN UPDATE Row SET Text = 'mutated' WHERE Id = NEW.Id; END;");
            using var archive = new MemoryStream(await ArchiveAsync(new Row { Id = 42, Text = "stored" }));
            var failure = await Assert.ThrowsAnyAsync<Exception>(() => new BDadosBackup(database.Accessor).RestoreAsync(archive, new[] { typeof(Row) }));
            Assert.Contains("Database changed restored value", failure.ToString());
            Assert.Equal(0L, await database.ScalarAsync("SELECT COUNT(*) FROM Row"));
        }

        [Fact]
        public async Task MissingPrimaryKeyFailsBeforeStartingTransaction() {
            using var archive = new MemoryStream(await ArchiveAsync(new Row { Id = 42 }));
            var target = AccessorProxy.Create();
            await Assert.ThrowsAsync<InvalidDataException>(() => new BDadosBackup(target.Accessor).RestoreAsync(archive, new[] { typeof(ChangedKey) }));
            Assert.Equal(0, target.Transactions);
        }

        [Fact]
        public void AmbiguousColumnCasingCannotProduceAnUnrestorableArchive() {
            Assert.Throws<InvalidDataException>(() => BDadosBackup.CreateSchema<CaseAmbiguity>());
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task ForeignKeyCyclesRestoreAndOrphansRollBack(bool enforcement) {
            await using var database = await SqliteDatabase.CreateAsync("CREATE TABLE Parent (Id INTEGER PRIMARY KEY, Text TEXT, ChildId INTEGER REFERENCES Child(Id)); CREATE TABLE Child (Id INTEGER PRIMARY KEY, Text TEXT, ParentId INTEGER REFERENCES Parent(Id))", enforcement);
            var backup = new BDadosBackup(database.Accessor);
            using var archive = new MemoryStream(await ArchiveAsync(new Child { Id = 8, ParentId = 42 }, new Parent { Id = 42, ChildId = 8 }));
            await backup.RestoreAsync(archive, new[] { typeof(Parent), typeof(Child) });
            Assert.Equal(0L, await database.ScalarAsync("SELECT COUNT(*) FROM pragma_foreign_key_check"));
            using var broken = new MemoryStream(await ArchiveAsync(new Child { Id = 9, ParentId = 404 }));
            await Assert.ThrowsAnyAsync<Exception>(() => backup.RestoreAsync(broken, new[] { typeof(Child) }));
            Assert.Equal(1L, await database.ScalarAsync("SELECT COUNT(*) FROM Child"));
        }

        [Fact]
        public async Task BackupBypassesLoadHooksButNormalFetchStillUsesThem() {
            await using var database = await SqliteDatabase.CreateAsync("CREATE TABLE HookRow (Id INTEGER PRIMARY KEY, Text TEXT); INSERT INTO HookRow VALUES (42, 'stored')");
            using var archive = new MemoryStream();
            await new BDadosBackup(database.Accessor).BackupAsync(archive, new[] { typeof(HookRow) });
            var target = AccessorProxy.Create();
            archive.Position = 0;
            await new BDadosBackup(target.Accessor).RestoreAsync(archive, new[] { typeof(HookRow) });
            Assert.Equal("stored", Assert.IsType<HookRow>(Assert.Single(target.Saved)).Text);
            await database.Accessor.AccessAsync(async transaction => {
                var rows = await database.Accessor.LoadAllAsync<HookRow>(transaction);
                Assert.Equal("hook mutated", Assert.Single(rows).Text);
            }, default);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task RestoreDoesNotRetainRowsOrEmitSaveEventsEvenOnCorruption(bool legacy) {
            string table = legacy ? nameof(LegacyRow) : nameof(TimedRow);
            await using var database = await SqliteDatabase.CreateAsync($"CREATE TABLE {table} (Id INTEGER PRIMARY KEY, Text TEXT, CreatedAt TEXT, UpdatedAt TEXT{(legacy ? ", RID TEXT" : "")})");
            var rows = Enumerable.Range(1, 40).Select(i => legacy
                ? (IDataObject)new LegacyRow { Id = i, RID = $"r{i}", Text = new string('x', 1000) }
                : new TimedRow { Id = i, Text = new string('x', 1000) }).ToArray();
            var bytes = await ArchiveAsync(rows);
            bytes[^1] ^= 1;
            int events = 0, inspected = 0;
            database.Accessor.OnSuccessfulSave += (_, _) => Interlocked.Increment(ref events);
            using var stream = new InspectFooterStream(bytes, () => {
                inspected++;
                var transaction = Assert.Single(database.Accessor.ActiveConnections.Values);
                Assert.Empty(transaction.Commands);
                Assert.Null(transaction.OnTransactionEnding);
                var field = typeof(BDadosTransaction).GetField("AutoMutateRollbacks", BindingFlags.Instance | BindingFlags.NonPublic)!;
                Assert.Empty((IEnumerable)field.GetValue(transaction)!);
                var property = typeof(BDadosTransaction).GetProperty("ObjectsToNotify", BindingFlags.Instance | BindingFlags.NonPublic)!;
                Assert.Empty((IEnumerable)property.GetValue(transaction)!);
            });
            await Assert.ThrowsAnyAsync<Exception>(() => new BDadosBackup(database.Accessor, new BDadosBackupOptions { RestoreChunkSize = 2 })
                .RestoreAsync(stream, new[] { rows[0].GetType() }));
            Assert.Equal(1, inspected);
            Assert.Equal(0, events);
            Assert.Equal(0L, await database.ScalarAsync($"SELECT COUNT(*) FROM {table}"));
        }

        [Fact]
        public async Task CancellationAfterWritesRollsBackDatabase() {
            await using var database = await SqliteDatabase.CreateAsync("CREATE TABLE Row (Id INTEGER PRIMARY KEY, Text TEXT)");
            using var cancellation = new CancellationTokenSource();
            using var stream = new InspectFooterStream(await ArchiveAsync(new Row { Id = 42 }), cancellation.Cancel);
            await Assert.ThrowsAnyAsync<Exception>(() => new BDadosBackup(database.Accessor).RestoreAsync(stream, new[] { typeof(Row) }, cancellation.Token));
            Assert.Equal(0L, await database.ScalarAsync("SELECT COUNT(*) FROM Row"));
        }

        [Fact]
        public async Task MultipleHistoricalNamesRestoreWithoutChangingDdlAliases() {
            using var archive = new MemoryStream(await ArchiveAsync(new Row { Id = 42, Text = "historic" }));
            var target = AccessorProxy.Create();
            await new BDadosBackup(target.Accessor).RestoreAsync(archive, new[] { typeof(RenamedAgain) });
            Assert.Equal("historic", Assert.IsType<RenamedAgain>(Assert.Single(target.Saved)).DisplayName);
        }

        [Fact]
        public void NullDoesNotBecomeAnInitializer() {
            var schema = BDadosBackup.CreateSchema<BDadosBackupMigrationTests.Scalar<int?>>();
            var bytes = BDadosBackup.Export(new BDadosBackupMigrationTests.Scalar<int?>(), schema);
            Assert.Throws<InvalidDataException>(() => BDadosBackup.Import<BDadosBackupMigrationTests.Scalar<int>>(schema, bytes));
        }

        [Fact]
        public async Task FileBackupPublishesOnlyCompleteArchivesAndPreservesExistingFileOnFailure() {
            string directory = Path.Combine(Path.GetTempPath(), "bdados-file-test-" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(directory);
            string path = Path.Combine(directory, "backup.bin");
            try {
                var source = AccessorProxy.Create();
                source.Rows[typeof(Row)] = new[] { new Row { Id = 42, Text = "complete" } };
                await new BDadosBackup(source.Accessor).BackupFileAsync(path, new[] { typeof(Row) });
                byte[] original = await File.ReadAllBytesAsync(path);
                await Assert.ThrowsAsync<InvalidDataException>(() => new BDadosBackup(source.Accessor, maxRowBytes: 4)
                    .BackupFileAsync(path, new[] { typeof(Row) }, overwrite: true));
                Assert.Equal(original, await File.ReadAllBytesAsync(path));
                Assert.Equal(new[] { path }, Directory.GetFiles(directory));
                using var input = File.OpenRead(path);
                var target = AccessorProxy.Create();
                await new BDadosBackup(target.Accessor).RestoreAsync(input, new[] { typeof(Row) });
                Assert.Equal(42, Assert.IsType<Row>(Assert.Single(target.Saved)).Id);
            } finally {
                File.Delete(path);
                Directory.Delete(directory);
            }
        }

        [Fact]
        public async Task HistoricalV2ArchiveRestoresIntoRenamedModelWithDatabaseDefaults() {
            // Produced by commit 51600ff5f06476e165b1c9bcf8ca6d504b481389, not the current writer.
            await using var database = await SqliteDatabase.CreateAsync("CREATE TABLE WithDefaults (Id INTEGER PRIMARY KEY, Text TEXT, Added TEXT NOT NULL DEFAULT 'database', Number INTEGER NOT NULL DEFAULT 7)");
            using var archive = File.OpenRead(Path.Combine(AppContext.BaseDirectory, "Fixtures", "BDadosBackup", "v2-row.fgb"));
            await new BDadosBackup(database.Accessor).RestoreAsync(archive, new[] { typeof(WithDefaults) });
            Assert.Equal(42L, await database.ScalarAsync("SELECT Id FROM WithDefaults"));
            Assert.Equal("pre-restoration-mode", await database.ScalarAsync("SELECT Text FROM WithDefaults"));
            Assert.Equal(7L, await database.ScalarAsync("SELECT Number FROM WithDefaults"));
        }

        public class TimedRow : Row {
            [Field] public override DateTime CreatedAt { get; set; }
            [Field(AllowNull = true)] public override DateTime? UpdatedAt { get; set; }
        }
        [OldName(nameof(Row))]
        public class WithDefaults : Row {
            [Field] public string? Added { get; set; } = "CLR initializer must not replace database default";
            [Field] public int Number { get; set; } = 123;
        }
        public class Parent : Row { [Field] public long? ChildId { get; set; } }
        public class Child : Row { [Field] public long? ParentId { get; set; } }
        public class StringRow : BaseDataObject<string> {
            [Field, PrimaryKey] public override string Id { get; set; } = "";
            [Field] public string? Text { get; set; }
        }
        public class CaseAmbiguity {
            [Field] public int Value { get; set; }
            [Field] public int value { get; set; }
        }
        [OldName(nameof(Row))]
        public class ChangedKey : BaseDataObject<long> {
            public override long Id { get => NewKey; set => NewKey = value; }
            [Field, PrimaryKey] public long NewKey { get; set; }
        }
        [OldName("Intermediate"), BackupAlias(nameof(Row))]
        public class RenamedAgain : BaseDataObject<long> {
            [Field, PrimaryKey] public override long Id { get; set; }
            [Field, OldName("IntermediateText"), BackupAlias("Text")] public string? DisplayName { get; set; }
        }
        public class HookRow : Row, IBusinessObject {
            public void OnAfterLoad(DataLoadContext ctx) => Text = "hook mutated";
            public Task<ValidationErrors> ValidateInput() => throw new NotSupportedException();
            public Task<ValidationErrors> ValidateBusiness() => throw new NotSupportedException();
            public Task<string> RunValidations() => throw new NotSupportedException();
            public Task<bool> ValidateAndPersistAsync(string token) => throw new NotSupportedException();
            public Task OnBeforePersistAsync() => throw new NotSupportedException();
            public Task OnAfterPersistAsync() => throw new NotSupportedException();
        }
        private sealed class InspectFooterStream(byte[] bytes, Action inspect) : MemoryStream(bytes) {
            public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) {
                if (Position == Length - 32) inspect();
                return base.ReadAsync(buffer, cancellationToken);
            }
        }
        internal sealed class SqliteDatabase : IAsyncDisposable {
            private SqliteConnection _keeper = null!;
            public RdbmsDataAccessor Accessor { get; private set; } = null!;
            internal static async Task<SqliteDatabase> CreateAsync(string ddl, bool foreignKeys = true) {
                var cs = $"Data Source=reliability-{Guid.NewGuid():N};Mode=Memory;Cache=Shared;Foreign Keys={foreignKeys}";
                var database = new SqliteDatabase { _keeper = new SqliteConnection(cs), Accessor = new RdbmsDataAccessor(new SharedSqlitePlugin(cs)) };
                await database._keeper.OpenAsync();
                await database.ExecuteAsync(ddl);
                return database;
            }
            internal async Task ExecuteAsync(string sql) {
                await using var command = _keeper.CreateCommand();
                command.CommandText = sql;
                await command.ExecuteNonQueryAsync();
            }
            internal async Task<object?> ScalarAsync(string sql) {
                await using var command = _keeper.CreateCommand();
                command.CommandText = sql;
                return await command.ExecuteScalarAsync();
            }
            public async ValueTask DisposeAsync() {
                Accessor.Dispose();
                await _keeper.DisposeAsync();
            }
        }
    }
}
