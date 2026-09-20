using System.Buffers.Binary;
using System.Text;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using Figlotech.BDados.Helpers;
using Figlotech.BDados.MySqlDataAccessor;
using Figlotech.BDados.PgSQLDataAccessor;
using Figlotech.BDados.SqliteDataAccessor;
using Figlotech.Core.Interfaces;
using Microsoft.Data.Sqlite;
using Xunit;
using static Figlotech.BDados.Tests.BDadosBackupTests;

namespace Figlotech.BDados.Tests {
    public class BDadosBackupIncrementalTests {
        [Fact]
        public async Task SqliteIncrementalFiltersBothTimestampsAndRestoresOnForwardOnlyStreams() {
            var since = new DateTime(2026, 8, 1, 12, 0, 0, DateTimeKind.Utc);
            var before = since.AddSeconds(-1);
            var after = since.AddSeconds(1);
            var rows = new[] {
                new IncrementalRow { Id = 1, CreatedAt = before, UpdatedAt = before },
                new IncrementalRow { Id = 2, CreatedAt = since, UpdatedAt = since },
                new IncrementalRow { Id = 3, CreatedAt = after, UpdatedAt = null },
                new IncrementalRow { Id = 4, CreatedAt = before, UpdatedAt = after },
                new IncrementalRow { Id = 5, CreatedAt = after, UpdatedAt = after },
                new IncrementalRow { Id = 6, CreatedAt = before, UpdatedAt = null },
                new IncrementalRow { Id = 7, CreatedAt = since, UpdatedAt = before },
                new IncrementalRow { Id = 8, CreatedAt = before, UpdatedAt = since },
                new IncrementalRow { Id = 9, CreatedAt = after, UpdatedAt = before },
                new IncrementalRow { Id = 10, CreatedAt = since, UpdatedAt = after }
            };
            string connectionString = $"Data Source=incremental-{Guid.NewGuid():N};Mode=Memory;Cache=Shared";
            await using var keeper = new SqliteConnection(connectionString);
            await keeper.OpenAsync();
            await using var command = keeper.CreateCommand();
            command.CommandText = "CREATE TABLE IncrementalRow (Id INTEGER PRIMARY KEY, CreatedAt TEXT, UpdatedAt TEXT); CREATE TABLE NoChangesRow (Id INTEGER PRIMARY KEY, CreatedAt TEXT, UpdatedAt TEXT)";
            await command.ExecuteNonQueryAsync();
            command.CommandText = "INSERT INTO IncrementalRow VALUES (@id, @created, @updated)";
            foreach (var row in rows) {
                command.Parameters.Clear();
                command.Parameters.AddWithValue("@id", row.Id);
                command.Parameters.AddWithValue("@created", row.CreatedAt);
                command.Parameters.AddWithValue("@updated", (object?)row.UpdatedAt ?? DBNull.Value);
                await command.ExecuteNonQueryAsync();
            }
            command.Parameters.Clear();
            command.CommandText = "INSERT INTO NoChangesRow SELECT * FROM IncrementalRow WHERE Id = 1";
            await command.ExecuteNonQueryAsync();
            using var source = new RdbmsDataAccessor(new SharedSqlitePlugin(connectionString)) { DefaultQueryLimit = 1 };
            using var output = new ForwardStream(false);
            var tables = new[] { typeof(IncrementalRow), typeof(NoChangesRow) };
            await new BDadosBackup(source).BackupIncrementalAsync(output, tables, since);
            Assert.False(output.WasDisposed);

            // Every selected table gets one header schema, even if its filtered data set is empty.
            byte[] bytes = output.Bytes;
            int headerLength = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(12));
            using var header = new MemoryStream(bytes, 16, headerLength, false);
            using var reader = new BinaryReader(header, Encoding.UTF8);
            Assert.Equal(2, reader.ReadInt32());
            foreach (var type in tables) {
                Assert.Equal(type.Name, Encoding.UTF8.GetString(reader.ReadBytes(reader.ReadInt32())));
                var schema = BDadosBackupSchema.Deserialize(reader.ReadBytes(reader.ReadInt32()));
                Assert.Equal(type.Name, schema.TableName);
                Assert.Equal(3, schema.Columns.Count);
            }
            Assert.Equal(header.Length, header.Position);

            var target = AccessorProxy.Create();
            using var input = new ForwardStream(true, bytes);
            await new BDadosBackup(target.Accessor).RestoreAsync(input, tables);
            Assert.False(input.WasDisposed);
            Assert.Equal(new long[] { 3, 4, 5, 9, 10 }, target.Saved.Cast<IncrementalRow>().Select(r => r.Id).OrderBy(id => id));
            Assert.Null(target.Saved.Cast<IncrementalRow>().Single(r => r.Id == 3).UpdatedAt);
            Assert.Equal(after, target.Saved.Cast<IncrementalRow>().Single(r => r.Id == 4).UpdatedAt);
            Assert.Equal(1, target.Commits);
        }

        [Theory]
        [InlineData("MySQL", false)]
        [InlineData("PostgreSQL", false)]
        [InlineData("SQLite", false)]
        [InlineData("MySQL", true)]
        [InlineData("PostgreSQL", true)]
        [InlineData("SQLite", true)]
        public async Task IncrementalUsesParameterizedStrictOrPredicateAndTimestampAttributes(string engine, bool renamed) {
            var since = new DateTime(2026, 8, 1, 12, 34, 56, DateTimeKind.Unspecified).AddTicks(1234);
            var source = AccessorProxy.Create();
            Type type = renamed ? typeof(RenamedTimestamps) : typeof(IncrementalRow);
            source.Rows[type] = Array.Empty<IDataObject>();
            using var output = new ForwardStream(false);
            await new BDadosBackup(source.Accessor).BackupIncrementalAsync(output, new[] { type }, since);
            var predicate = Assert.Single(source.FetchConditions)!;
            IQueryGenerator generator = engine switch {
                "MySQL" => new MySqlQueryGenerator(),
                "PostgreSQL" => new PgSQLQueryGenerator(),
                _ => new SqliteQueryGenerator()
            };
            var select = renamed ? generator.GenerateSelect<RenamedTimestamps>(predicate, null, null, null, OrderingType.Asc)
                : generator.GenerateSelect<IncrementalRow>(predicate, null, null, null, OrderingType.Asc);
            string sql = select.GetCommandText();
            Assert.Contains(renamed ? "BornOn>" : "CreatedAt>", sql);
            Assert.Contains(renamed ? "EditedOn>" : "UpdatedAt>", sql);
            Assert.Contains("OR", sql);
            Assert.DoesNotContain(">=", sql);
            Assert.DoesNotContain("2026", sql);
            Assert.Equal(2, select.GetParameters().Count);
            Assert.All(select.GetParameters(), parameter => {
                var cutoff = Assert.IsType<DateTime>(parameter.Value);
                Assert.Equal(since, cutoff);
                Assert.Equal(since.Kind, cutoff.Kind);
            });
            var target = AccessorProxy.Create();
            using var input = new ForwardStream(true, output.Bytes);
            await new BDadosBackup(target.Accessor).RestoreAsync(input, new[] { type });
            Assert.Empty(target.Saved);
        }

        [Theory]
        [InlineData(typeof(Row))]
        [InlineData(typeof(InvalidTimestamp))]
        [InlineData(typeof(AmbiguousTimestamp))]
        public async Task InvalidTimestampMappingsFailBeforeWritingOrQuerying(Type type) {
            var source = AccessorProxy.Create();
            using var output = new ForwardStream(false);
            await Assert.ThrowsAsync<ArgumentException>(() => new BDadosBackup(source.Accessor)
                .BackupIncrementalAsync(output, new[] { type }, DateTime.UtcNow));
            Assert.Empty(output.Bytes);
            Assert.Equal(0, source.Transactions);
        }

        [Fact]
        public async Task FullBackupRemainsUnfilteredAndIncrementalsRespectCancellation() {
            var source = AccessorProxy.Create();
            source.Rows[typeof(Row)] = Array.Empty<IDataObject>();
            using var output = new ForwardStream(false);
            await new BDadosBackup(source.Accessor).BackupAsync(output, new[] { typeof(Row) });
            Assert.Null(Assert.Single(source.FetchConditions));
            using var cancelled = new CancellationTokenSource();
            cancelled.Cancel();
            using var cancelledOutput = new ForwardStream(false);
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => new BDadosBackup(source.Accessor)
                .BackupIncrementalAsync(cancelledOutput, new[] { typeof(IncrementalRow) }, DateTime.UtcNow, cancelled.Token));
            Assert.Empty(cancelledOutput.Bytes);
        }

        public class IncrementalRow : BaseDataObject<long> {
            [Field, PrimaryKey] public override long Id { get; set; }
            [Field] public override DateTime CreatedAt { get; set; }
            [Field(AllowNull = true)] public override DateTime? UpdatedAt { get; set; }
        }
        public class NoChangesRow : IncrementalRow { }
        public class RenamedTimestamps : BaseDataObject<long> {
            [Field, PrimaryKey] public override long Id { get; set; }
            [Field, CreationTimeStamp] public DateTime BornOn { get; set; }
            [Field, UpdateTimeStamp] public DateTime? EditedOn { get; set; }
        }
        public class InvalidTimestamp : IncrementalRow {
            [Field, UpdateTimeStamp] public string Invalid { get; set; } = "";
        }
        public class AmbiguousTimestamp : IncrementalRow {
            [Field, UpdateTimeStamp] public DateTime First { get; set; }
            [Field, UpdateTimeStamp] public DateTime Second { get; set; }
        }
    }
}
