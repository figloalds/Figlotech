using System.Buffers.Binary;
using System.Data;
using System.Reflection;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using Figlotech.BDados.Helpers;
using Figlotech.BDados.SqliteDataAccessor;
using Figlotech.Core.Interfaces;
using Figlotech.Data;
using Microsoft.Data.Sqlite;
using Xunit;

namespace Figlotech.BDados.Tests {
    public class BDadosBackupTests {
        [Fact]
        public void SchemaAndRawRowsRoundTripPersistedFieldsAndAllSupportedTypes() {
            var original = new Values {
                Id = 42, Text = "héllo 世界\0🙂", Blob = new byte[] { 0, 255, 4 },
                Flag = true, Byte = byte.MaxValue, SByte = sbyte.MinValue,
                Short = short.MinValue, UShort = ushort.MaxValue, Int = int.MinValue,
                UInt = uint.MaxValue, Long = long.MinValue, ULong = ulong.MaxValue,
                Single = float.NegativeInfinity, Double = double.NaN, Decimal = decimal.MaxValue,
                Character = '\ud800', Guid = Guid.NewGuid(), Optional = 6,
                Date = new DateTime(638000000000000123, DateTimeKind.Local),
                Offset = new DateTimeOffset(638000000000000123, TimeSpan.FromHours(5.5)),
                Duration = TimeSpan.MinValue, Day = DateOnly.MaxValue, Time = TimeOnly.MaxValue,
                Enum = WideEnum.High, NullableEnum = WideEnum.High, Ignored = "not persisted"
            };
            var schema = BDadosBackupSchema.Deserialize(BDadosBackup.CreateSchema<Values>().Serialize());
            var restored = BDadosBackup.Import<Values>(schema, BDadosBackup.Export(original, schema));

            Assert.Equal(nameof(Values), schema.TableName);
            Assert.DoesNotContain(schema.Columns, c => c.Name == nameof(Values.Ignored));
            Assert.Equal(schema.Columns.Select(c => c.Name).OrderBy(n => n, StringComparer.Ordinal), schema.Columns.Select(c => c.Name));
            Assert.True(schema.Columns.Single(c => c.Name == nameof(Values.Id)).IsPrimaryKey);
            var textColumn = schema.Columns.Single(c => c.Name == nameof(Values.Text));
            Assert.True(textColumn.IsReliableId);
            Assert.Equal(1000, textColumn.Size);
            Assert.Equal("PreviousText", textColumn.OldName);
            Assert.Equal(nameof(OtherRow), textColumn.ForeignTable);
            foreach (var member in typeof(Values).GetMembers().Where(m => m.IsDefined(typeof(FieldAttribute)))) {
                object? Get(object instance) => member is PropertyInfo p ? p.GetValue(instance) : ((FieldInfo)member).GetValue(instance);
                Assert.Equal(Get(original), Get(restored));
            }
            Assert.Equal(original.Date.Kind, restored.Date.Kind);
            Assert.Equal(original.Offset.Offset, restored.Offset.Offset);
            Assert.Equal("default", restored.Ignored);
        }

        [Fact]
        public void NullAndEmptyValuesRemainDistinctAndSchemaOrderControlsRows() {
            var schema = BDadosBackup.CreateSchema<Values>();
            schema.Columns.Reverse();
            var row = new Values { Text = null, Blob = Array.Empty<byte>(), NullableEnum = null, Optional = null };
            var restored = BDadosBackup.Import<Values>(schema, BDadosBackup.Export(row, schema));
            Assert.Null(restored.Text);
            Assert.Empty(restored.Blob!);
            Assert.Null(restored.Optional);
            Assert.Null(restored.NullableEnum);
            row.Text = "";
            row.Blob = null;
            restored = BDadosBackup.Import<Values>(schema, BDadosBackup.Export(row, schema));
            Assert.Equal("", restored.Text);
            Assert.Null(restored.Blob);
        }

        [Fact]
        public void RawRowsHaveOnlyLengthsAndValuesAndRejectInvalidLengths() {
            var schema = BDadosBackup.CreateSchema<OneValue>();
            byte[] bytes = BDadosBackup.Export(new OneValue { Number = 42 }, schema);
            Assert.Equal(new byte[] { 4, 0, 0, 0, 42, 0, 0, 0 }, bytes);
            Assert.Throws<InvalidDataException>(() => BDadosBackup.Import<OneValue>(schema, bytes[..^1]));
            Assert.Throws<InvalidDataException>(() => BDadosBackup.Import<OneValue>(schema, bytes.Concat(new byte[1]).ToArray()));
            foreach (int length in new[] { -1, -2, 0, 3, int.MaxValue }) {
                BinaryPrimitives.WriteInt32LittleEndian(bytes, length);
                Assert.Throws<InvalidDataException>(() => BDadosBackup.Import<OneValue>(schema, bytes));
            }
        }

        [Fact]
        public void InvalidSchemasAndUnsupportedMembersFailExplicitly() {
            Assert.Throws<NotSupportedException>(() => BDadosBackup.CreateSchema<Unsupported>());
            Assert.Throws<NotSupportedException>(() => BDadosBackup.CreateSchema<ReadOnlyModel>());
            Assert.Throws<InvalidDataException>(() => BDadosBackupSchema.Deserialize("null"u8.ToArray()));
            Assert.Throws<InvalidDataException>(() => BDadosBackupSchema.Deserialize("{"u8.ToArray()));
            var schema = BDadosBackup.CreateSchema<OneValue>();
            schema.Columns[0].DataType = BDadosBackupDataType.Int64;
            Assert.Throws<InvalidDataException>(() => BDadosBackup.Import<OneValue>(schema, new byte[8]));
            schema.Columns.Add(schema.Columns[0]);
            Assert.Throws<InvalidDataException>(() => schema.Serialize());
        }

        [Fact]
        public async Task BackupRestoreStreamsMultipleTablesWithPartialReadsAndLeavesStreamsOpen() {
            var source = AccessorProxy.Create();
            source.Rows[typeof(Row)] = new IDataObject[] { new Row { Id = 1, Text = "one" }, new Row { Id = 2, Text = null } };
            source.Rows[typeof(OtherRow)] = Array.Empty<IDataObject>();
            byte[] bytes;
            using (var output = new ForwardStream(false)) {
                await new BDadosBackup(source.Accessor).BackupAsync(output, new[] { typeof(Row), typeof(OtherRow) });
                Assert.False(output.WasDisposed);
                bytes = output.Bytes;
            }
            var target = AccessorProxy.Create();
            using var input = new ForwardStream(true, bytes);
            // Registration order is irrelevant; header order determines which table follows.
            await new BDadosBackup(target.Accessor).RestoreAsync(input, new[] { typeof(OtherRow), typeof(Row) });
            Assert.False(input.WasDisposed);
            Assert.Equal(2, target.Saved.Count);
            Assert.Equal("one", Assert.IsType<Row>(target.Saved[0]).Text);
            Assert.Null(Assert.IsType<Row>(target.Saved[1]).Text);
            Assert.Equal(2, ((Row)target.Saved[1]).Id);
            Assert.Equal(1, target.Commits);
            Assert.Equal(2, source.Fetches);
        }

        [Fact]
        public async Task EmptyBackupRoundTrips() {
            var accessor = AccessorProxy.Create();
            using var output = new ForwardStream(false);
            await new BDadosBackup(accessor.Accessor).BackupAsync(output, Array.Empty<Type>());
            using var input = new ForwardStream(true, output.Bytes);
            await new BDadosBackup(accessor.Accessor).RestoreAsync(input, Array.Empty<Type>());
            Assert.Empty(accessor.Saved);
        }

        [Fact]
        public async Task TruncationAtEveryByteBoundaryNeverCommits() {
            byte[] bytes = await MakeBackup();
            for (int length = 0; length < bytes.Length; length++) {
                var target = AccessorProxy.Create();
                using var input = new ForwardStream(true, bytes[..length]);
                await Assert.ThrowsAnyAsync<Exception>(() => new BDadosBackup(target.Accessor).RestoreAsync(input, new[] { typeof(Row) }));
                Assert.Equal(0, target.Commits);
                Assert.Empty(target.Saved);
            }
        }

        [Theory]
        [InlineData("magic")]
        [InlineData("version")]
        [InlineData("headerLength")]
        [InlineData("header")]
        [InlineData("rowLength")]
        [InlineData("row")]
        [InlineData("count")]
        [InlineData("footer")]
        [InlineData("hash")]
        public async Task CorruptionIsRejectedAndRollsBack(string part) {
            byte[] bytes = await MakeBackup();
            int dataStart = 16 + BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(12)) + 32;
            int rowLength = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(dataStart));
            int endOfRows = dataStart + 4 + rowLength;
            switch (part) {
                case "magic": bytes[0] ^= 1; break;
                case "version": bytes[8] = 2; break;
                case "headerLength": BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(12), int.MaxValue); break;
                case "header": bytes[20] ^= 1; break;
                case "rowLength": BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(dataStart), -2); break;
                case "row": bytes[dataStart + 4 + rowLength - 1] ^= 1; break;
                case "count": bytes[endOfRows + 4] ^= 1; break;
                case "footer": bytes[endOfRows + 12] ^= 1; break;
                case "hash": bytes[^1] ^= 1; break;
            }
            var target = AccessorProxy.Create();
            using var input = new ForwardStream(true, bytes);
            await Assert.ThrowsAsync<InvalidDataException>(() => new BDadosBackup(target.Accessor).RestoreAsync(input, new[] { typeof(Row) }));
            Assert.Equal(0, target.Commits);
            Assert.Empty(target.Saved);
        }

        [Fact]
        public async Task MissingTypeFailsBeforeTransactionAndSaveFailureRollsBack() {
            byte[] bytes = await MakeBackup();
            var target = AccessorProxy.Create();
            using var input = new ForwardStream(true, bytes);
            await Assert.ThrowsAsync<InvalidDataException>(() => new BDadosBackup(target.Accessor).RestoreAsync(input, Array.Empty<Type>()));
            Assert.Equal(0, target.Transactions);
            target.FailSave = true;
            using var failingInput = new ForwardStream(true, bytes);
            await Assert.ThrowsAsync<InvalidDataException>(() => new BDadosBackup(target.Accessor).RestoreAsync(failingInput, new[] { typeof(Row) }));
            Assert.Equal(1, target.Rollbacks);
            Assert.Empty(target.Saved);
        }

        [Fact]
        public async Task LimitsAndCancellationAreEnforced() {
            var source = AccessorProxy.Create();
            source.Rows[typeof(Row)] = new IDataObject[] { new Row { Text = new string('a', 100) } };
            using var output = new ForwardStream(false);
            await Assert.ThrowsAsync<InvalidDataException>(() => new BDadosBackup(source.Accessor, maxRowBytes: 20).BackupAsync(output, new[] { typeof(Row) }));
            await Assert.ThrowsAsync<InvalidDataException>(() => new BDadosBackup(source.Accessor, maxHeaderBytes: 4).BackupAsync(output, new[] { typeof(Row) }));
            using var cancelled = new CancellationTokenSource();
            cancelled.Cancel();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => new BDadosBackup(source.Accessor).BackupAsync(output, new[] { typeof(Row) }, cancelled.Token));
            using var input = new ForwardStream(true, await MakeBackup());
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => new BDadosBackup(source.Accessor).RestoreAsync(input, new[] { typeof(Row) }, cancelled.Token));
        }

        [Fact]
        public async Task RealSqliteAccessorRestoresRowsAndRollsBackCorruptArchive() {
            string sourceConnection = $"Data Source=backup-{Guid.NewGuid():N};Mode=Memory;Cache=Shared";
            string targetConnection = $"Data Source=restore-{Guid.NewGuid():N};Mode=Memory;Cache=Shared";
            await using var sourceKeeper = new SqliteConnection(sourceConnection);
            await using var targetKeeper = new SqliteConnection(targetConnection);
            await sourceKeeper.OpenAsync();
            await targetKeeper.OpenAsync();
            await using var sourceCommand = sourceKeeper.CreateCommand();
            sourceCommand.CommandText = "CREATE TABLE SqlRow (Id INTEGER PRIMARY KEY, Text TEXT); INSERT INTO SqlRow VALUES (42, 'hello'), (71, NULL)";
            await sourceCommand.ExecuteNonQueryAsync();
            await using var targetCommand = targetKeeper.CreateCommand();
            targetCommand.CommandText = "CREATE TABLE SqlRow (Id INTEGER PRIMARY KEY, Text TEXT)";
            await targetCommand.ExecuteNonQueryAsync();
            using var source = new RdbmsDataAccessor(new SharedSqlitePlugin(sourceConnection)) { DefaultQueryLimit = 1 };
            using var target = new RdbmsDataAccessor(new SharedSqlitePlugin(targetConnection));
            using var output = new ForwardStream(false);
            await new BDadosBackup(source).BackupAsync(output, new[] { typeof(SqlRow) });
            byte[] bytes = output.Bytes;
            using var input = new ForwardStream(true, bytes);
            await new BDadosBackup(target).RestoreAsync(input, new[] { typeof(SqlRow) });
            targetCommand.CommandText = "SELECT COUNT(*) FROM SqlRow WHERE (Id = 42 AND Text = 'hello') OR (Id = 71 AND Text IS NULL)";
            Assert.Equal(2L, await targetCommand.ExecuteScalarAsync());
            targetCommand.CommandText = "DELETE FROM SqlRow";
            await targetCommand.ExecuteNonQueryAsync();
            bytes[^1] ^= 1;
            using var corrupt = new ForwardStream(true, bytes);
            await Assert.ThrowsAnyAsync<Exception>(() => new BDadosBackup(target).RestoreAsync(corrupt, new[] { typeof(SqlRow) }));
            targetCommand.CommandText = "SELECT COUNT(*) FROM SqlRow";
            Assert.Equal(0L, await targetCommand.ExecuteScalarAsync());
        }

        private static async Task<byte[]> MakeBackup() {
            var source = AccessorProxy.Create();
            source.Rows[typeof(Row)] = new IDataObject[] { new Row { Id = 7, Text = "sample" } };
            using var output = new ForwardStream(false);
            await new BDadosBackup(source.Accessor).BackupAsync(output, new[] { typeof(Row) });
            return output.Bytes;
        }

        public class Row : BaseDataObject<long> {
            [Field, PrimaryKey] public override long Id { get; set; }
            [Field(AllowNull = true)] public string? Text { get; set; }
        }
        public class OtherRow : Row { }
        public class SqlRow : Row, IApplicationGeneratedId<long> {
            public long GenerateId() => 123;
        }
        public class Values : Row {
            [Field(Size = 1000), ReliableId, OldName("PreviousText"), ForeignKey(typeof(OtherRow), "Text")]
            public new string? Text { get; set; }
            [Field] public byte[]? Blob { get; set; }
            [Field] public bool Flag { get; set; }
            [Field] public byte Byte { get; set; }
            [Field] public sbyte SByte { get; set; }
            [Field] public short Short { get; set; }
            [Field] public ushort UShort { get; set; }
            [Field] public int Int { get; set; }
            [Field] public uint UInt { get; set; }
            [Field] public long Long { get; set; }
            [Field] public ulong ULong { get; set; }
            [Field] public float Single { get; set; }
            [Field] public double Double { get; set; }
            [Field] public decimal Decimal { get; set; }
            [Field] public char Character { get; set; }
            [Field] public Guid Guid { get; set; }
            [Field] public int? Optional { get; set; }
            [Field] public DateTime Date { get; set; }
            [Field] public DateTimeOffset Offset { get; set; }
            [Field] public TimeSpan Duration { get; set; }
            [Field] public DateOnly Day { get; set; }
            [Field] public TimeOnly Time { get; set; }
            [Field] public WideEnum Enum;
            [Field] public WideEnum? NullableEnum { get; set; }
            public string Ignored { get; set; } = "default";
        }
        public enum WideEnum : ulong { High = ulong.MaxValue }
        public class OneValue { [Field] public int Number { get; set; } }
        public class Unsupported { [Field] public object? Value { get; set; } }
        public class ReadOnlyModel { [Field] public int Value => 1; }

        internal sealed class SharedSqlitePlugin : IRdbmsPluginAdapter {
            public SharedSqlitePlugin(string connectionString) { ConnectionString = connectionString; }
            public IQueryGenerator QueryGenerator { get; } = new SqliteQueryGenerator();
            public bool ContinuousConnection => true;
            public TimeSpan CommandTimeout => TimeSpan.FromSeconds(30);
            public TimeSpan ConnectTimeout => TimeSpan.FromSeconds(5);
            public int PoolSize => 2;
            public string SchemaName => "main";
            public string DatabaseHost => "memory";
            public string ConnectionString { get; }
            public IReadOnlyDictionary<string, string> InfoSchemaColumnsMap { get; } = new Dictionary<string, string>();
            public IDbConnection GetNewConnection() => new SqliteConnection(ConnectionString);
            public IDbConnection GetNewSchemalessConnection() => GetNewConnection();
            public void SetConfiguration(IDictionary<string, object> settings) { }
            public object ProcessParameterValue(object value) => value;
        }

        public class AccessorProxy : DispatchProxy {
            public IRdbmsDataAccessor Accessor => (IRdbmsDataAccessor)(object)this;
            public Dictionary<Type, IDataObject[]> Rows { get; } = new();
            public List<IDataObject> Saved { get; } = new();
            public int Fetches { get; private set; }
            public List<IQueryBuilder?> FetchConditions { get; } = new();
            public int Transactions { get; private set; }
            public int Commits { get; private set; }
            public int Rollbacks { get; private set; }
            public bool FailSave { get; set; }
            public static AccessorProxy Create() => (AccessorProxy)(object)DispatchProxy.Create<IRdbmsDataAccessor, AccessorProxy>();

            protected override object? Invoke(MethodInfo? targetMethod, object?[]? args) {
                switch (targetMethod!.Name) {
                    case "AccessAsync":
                        Assert.Equal(IsolationLevel.Serializable, args![2]);
                        return RunTransaction((Func<BDadosTransaction, ValueTask>)args[0]!);
                    case "FetchAsync":
                        Fetches++;
                        FetchConditions.Add((IQueryBuilder?)args![1]);
                        Assert.Null(args![3]); // No default query limit.
                        var type = targetMethod.GetGenericArguments()[0];
                        return typeof(AccessorProxy).GetMethod(nameof(Enumerate), BindingFlags.Static | BindingFlags.NonPublic)!
                            .MakeGenericMethod(type).Invoke(null, new object[] { Rows[type] });
                    case "SaveItemAsync":
                        Saved.Add((IDataObject)args![1]!);
                        return Task.FromResult(!FailSave);
                    default: throw new NotSupportedException(targetMethod.Name);
                }
            }

            private async ValueTask RunTransaction(Func<BDadosTransaction, ValueTask> action) {
                Transactions++;
                try {
                    await action(null!);
                    Commits++;
                } catch {
                    Saved.Clear();
                    Rollbacks++;
                    throw;
                }
            }

            private static async IAsyncEnumerable<T> Enumerate<T>(IDataObject[] rows) {
                await Task.Yield();
                foreach (var row in rows) yield return (T)row;
            }
        }

        // Deliberately forbids all synchronous I/O and all seeking, including Length/Position.
        internal sealed class ForwardStream : Stream {
            private readonly MemoryStream _buffer;
            private readonly bool _readable;
            public bool WasDisposed { get; private set; }
            public byte[] Bytes => _buffer.ToArray();
            public ForwardStream(bool readable, byte[]? bytes = null) {
                _readable = readable;
                _buffer = bytes == null ? new MemoryStream() : new MemoryStream(bytes, false);
            }
            public override bool CanRead => _readable;
            public override bool CanSeek => false;
            public override bool CanWrite => !_readable;
            public override long Length => throw new NotSupportedException();
            public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
            public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default) {
                Assert.True(_readable);
                return _buffer.ReadAsync(buffer[..Math.Min(3, buffer.Length)], cancellationToken);
            }
            public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default) {
                Assert.False(_readable);
                return _buffer.WriteAsync(buffer, cancellationToken);
            }
            public override void Flush() => throw new NotSupportedException();
            public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
            public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
            public override void SetLength(long value) => throw new NotSupportedException();
            public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
            protected override void Dispose(bool disposing) {
                if (disposing) _buffer.Dispose();
                WasDisposed = true;
                base.Dispose(disposing);
            }
        }
    }
}
