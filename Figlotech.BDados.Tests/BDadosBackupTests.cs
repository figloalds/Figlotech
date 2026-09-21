using System.Buffers.Binary;
using System.Data;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;
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
        public void FixedSizeRawRowsOmitLengthsAndRejectTruncationOrTrailingBytes() {
            var schema = BDadosBackup.CreateSchema<OneValue>();
            byte[] bytes = BDadosBackup.Export(new OneValue { Number = 42 }, schema);
            Assert.Equal(2, schema.Version);
            Assert.Equal(4, schema.Columns[0].FixedLength);
            Assert.Equal(new byte[] { 42, 0, 0, 0 }, bytes);
            Assert.Throws<InvalidDataException>(() => BDadosBackup.Import<OneValue>(schema, bytes[..^1]));
            Assert.Throws<InvalidDataException>(() => BDadosBackup.Import<OneValue>(schema, bytes.Concat(new byte[1]).ToArray()));
        }

        [Fact]
        public void LegacySchemaStillReadsAndWritesLengthPrefixedRows() {
            var schema = BDadosBackupSchema.Deserialize("""
                {"Version":1,"TableName":"OneValue","Columns":[{"Name":"Number","DataType":6,"IsNullable":false}]}
                """u8.ToArray());
            byte[] bytes = { 4, 0, 0, 0, 42, 0, 0, 0 };
            Assert.Equal(42, BDadosBackup.Import<OneValue>(schema, bytes).Number);
            Assert.Equal(bytes, BDadosBackup.Export(new OneValue { Number = 42 }, schema));
            foreach (int length in new[] { -1, -2, 0, 3, int.MaxValue }) {
                BinaryPrimitives.WriteInt32LittleEndian(bytes, length);
                Assert.Throws<InvalidDataException>(() => BDadosBackup.Import<OneValue>(schema, bytes));
            }
        }

        [Fact]
        public void AllBuiltinFixedSizesAreStoredOnceInSchemaIncludingNullableValues() {
            (object Value, int Length)[] samples = {
                (true, 1), ((byte)7, 1), ((sbyte)-7, 1), ((short)-7, 2), ((ushort)7, 2), ('x', 2),
                (42, 4), (42U, 4), (1.25f, 4), (DateOnly.MaxValue, 4),
                (42L, 8), (42UL, 8), (1.25d, 8), (TimeSpan.MinValue, 8), (TimeOnly.MaxValue, 8),
                (decimal.MaxValue, 16), (Guid.NewGuid(), 16), (WideEnum.High, 8),
                (new DateTime(638000000000000123, DateTimeKind.Local), 9),
                (new DateTimeOffset(638000000000000123, TimeSpan.FromHours(5.5)), 10)
            };
            foreach (var sample in samples) {
                foreach (bool nullable in new[] { false, true }) {
                    Type valueType = nullable ? typeof(Nullable<>).MakeGenericType(sample.Value.GetType()) : sample.Value.GetType();
                    Type modelType = typeof(BDadosBackupBinarySerializationTests.Scalar<>).MakeGenericType(valueType);
                    var schema = BDadosBackup.CreateSchema(modelType);
                    Assert.Equal(sample.Length, schema.Columns[0].FixedLength);
                    object instance = Activator.CreateInstance(modelType)!;
                    var property = modelType.GetProperty("Value")!;
                    property.SetValue(instance, sample.Value);
                    byte[] bytes = BDadosBackup.Export(instance, schema);
                    Assert.Equal(sample.Length + (nullable ? 1 : 0), bytes.Length);
                    Assert.Equal(sample.Value, property.GetValue(BDadosBackup.Import(modelType, schema, bytes)));
                    if (nullable) {
                        Assert.Equal(1, bytes[0]);
                        property.SetValue(instance, null);
                        Assert.Equal(new byte[] { 0 }, BDadosBackup.Export(instance, schema));
                        Assert.Null(property.GetValue(BDadosBackup.Import(modelType, schema, new byte[] { 0 })));
                    }
                }
            }
        }

        [Fact]
        public async Task LegacyArchiveRestoresWithOriginalSchemaAndFieldFraming() {
            // Build an independent v1 fixture without calling the current schema or row writer.
            byte[] schema = """
                {"Version":1,"TableName":"Row","Columns":[
                    {"Name":"Id","DataType":8,"IsNullable":false},
                    {"Name":"Text","DataType":14,"IsNullable":true}]}
                """u8.ToArray();
            using var header = new MemoryStream();
            using (var writer = new BinaryWriter(header, Encoding.UTF8, true)) {
                writer.Write(1);
                writer.Write(3);
                writer.Write("Row"u8);
                writer.Write(schema.Length);
                writer.Write(schema);
            }
            byte[] headerBytes = header.ToArray();
            using var archive = new MemoryStream();
            using (var writer = new BinaryWriter(archive, Encoding.UTF8, true)) {
                writer.Write("FGBACKUP"u8);
                writer.Write(1);
                writer.Write(headerBytes.Length);
                writer.Write(headerBytes);
                writer.Write(SHA256.HashData(headerBytes));
                writer.Write(18); // Row: (4 + 8) ID bytes, (4 + 2) text bytes.
                writer.Write(8);
                writer.Write(42L);
                writer.Write(2);
                writer.Write("hi"u8);
                writer.Write(-1);
                writer.Write(1L);
                writer.Write("FGBEND01"u8);
                writer.Write(SHA256.HashData(archive.ToArray()));
            }
            using var input = new ForwardStream(true, archive.ToArray());
            var target = AccessorProxy.Create();
            await new BDadosBackup(target.Accessor).RestoreAsync(input, new[] { typeof(Row) });
            var row = Assert.IsType<Row>(Assert.Single(target.Saved));
            Assert.Equal(42, row.Id);
            Assert.Equal("hi", row.Text);
            Assert.Equal(1, target.Commits);
        }

        [Fact]
        public void ReadonlyFieldsAndGetterOnlyPropertiesAreOmittedFromBackup() {
            var schema = BDadosBackup.CreateSchema<ReadOnlyModel>();
            Assert.Equal(nameof(ReadOnlyModel.Mutable), Assert.Single(schema.Columns).Name);
            byte[] bytes = BDadosBackup.Export(new ReadOnlyModel { Mutable = 42 }, schema);
            var restored = BDadosBackup.Import<ReadOnlyModel>(schema, bytes);
            Assert.Equal(42, restored.Mutable);
            Assert.Equal(1, restored.Value);
            Assert.NotNull(restored.Frozen);
        }

        [Fact]
        public void RestoreSkipsArchivedColumnsThatAreNowReadonly() {
            var schema = BDadosBackup.CreateSchema<PreviouslyWritableModel>();
            byte[] bytes = BDadosBackup.Export(new PreviouslyWritableModel { Mutable = 42, Value = 99, Frozen = 10 }, schema);
            var restored = BDadosBackup.Import<ReadOnlyModel>(schema, bytes);
            Assert.Equal(42, restored.Mutable);
            Assert.Equal(1, restored.Value);
            Assert.NotNull(restored.Frozen);
        }

        [Fact]
        public void InvalidSchemasAndUnsupportedMembersFailExplicitly() {
            Assert.Throws<NotSupportedException>(() => BDadosBackup.CreateSchema<Unsupported>());
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
            await Assert.ThrowsAsync<InvalidDataException>(() => new BDadosBackup(target.Accessor).RestoreAsync(input, Array.Empty<Type>(), ignoreMissingTables: false));
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
        public class ReadOnlyModelBase {
            [Field(DefaultValue = 7)] public int Value => 1;
            [Field] public readonly object Frozen = new object();
        }
        public class ReadOnlyModel : ReadOnlyModelBase {
            [Field] public int Mutable { get; set; }
        }
        public class PreviouslyWritableModel {
            [Field] public int Value { get; set; }
            [Field] public int Frozen;
            [Field] public int Mutable { get; set; }
        }

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
            public List<(Type Type, IDataObject[] Rows)> SavedBatches { get; } = new();
            public int FailBatch { get; set; }
            public Action? AfterSaveBatch { get; set; }
            public IQueryGenerator QueryGenerator { get; set; } = new SqliteQueryGenerator();
            public List<string> ExecutedCommands { get; } = new();
            public Func<string, Exception?>? ExecuteFailure { get; set; }
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
                    case "get_QueryGenerator":
                        return QueryGenerator;
                    case "ExecuteAsync":
                        var command = ((IQueryBuilder)args![1]!).GetCommandText();
                        ExecutedCommands.Add(command);
                        var failure = ExecuteFailure?.Invoke(command);
                        if (failure != null) return Task.FromException<int>(failure);
                        return Task.FromResult(0);
                    case "SaveListAsync":
                        var rows = ((System.Collections.IEnumerable)args![1]!).Cast<IDataObject>().ToArray();
                        var savedType = targetMethod.GetGenericArguments()[0];
                        Assert.All(rows, row => Assert.Equal(savedType, row.GetType()));
                        Assert.False((bool)args[2]!);
                        SavedBatches.Add((savedType, rows));
                        Saved.AddRange(rows);
                        AfterSaveBatch?.Invoke();
                        return Task.FromResult(!FailSave && SavedBatches.Count != FailBatch);
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
