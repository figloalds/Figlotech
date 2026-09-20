using System.Buffers.Binary;
using System.Globalization;
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
    public class BDadosBackupMigrationTests {
        [Fact]
        public void OlderRowsMapRenamesSkipDeletedColumnsAndRetainNewDefaults() {
            var old = new OldCustomer { Id = 45, Name = "customer", Deleted = new byte[] { 1, 2, 3 }, Count = 72, Optional = null };
            var schema = BDadosBackup.CreateSchema<OldCustomer>();
            var bytes = BDadosBackup.Export(old, schema);
            var current = BDadosBackup.Import<Customer>(BDadosBackupSchema.Deserialize(schema.Serialize()), bytes);
            Assert.Equal(45, current.Id);
            Assert.Equal("customer", current.DisplayName);
            Assert.Equal(72L, current.Count);
            Assert.Equal(99, current.Optional);
            Assert.Equal(17, current.Added);
            Assert.Equal("new default", current.NewText);
            old.Optional = 32;
            current = BDadosBackup.Import<Customer>(schema, BDadosBackup.Export(old, schema));
            Assert.Equal(32, current.Optional);
        }

        [Fact]
        public void CurrentNameWinsOverOldNameRegardlessOfStoredColumnOrder() {
            var schema = BDadosBackup.CreateSchema<BothNames>();
            foreach (bool reverse in new[] { false, true }) {
                if (reverse) schema.Columns.Reverse();
                var restored = BDadosBackup.Import<Renamed>(schema, BDadosBackup.Export(new BothNames(), schema));
                Assert.Equal("current", restored.New);
            }
        }

        [Fact]
        public void CasingChangesAndNullableWideningWork() {
            var schema = BDadosBackup.CreateSchema<Scalar<int>>();
            byte[] bytes = BDadosBackup.Export(new Scalar<int> { Value = 123 }, schema);
            schema.Columns[0].Name = "value";
            Assert.Equal(123, BDadosBackup.Import<Scalar<int?>>(schema, bytes).Value);
        }

        [Fact]
        public void AmbiguousAliasesFailRatherThanChoosingByReflectionOrder() {
            var schema = BDadosBackup.CreateSchema<Scalar<int>>();
            byte[] bytes = BDadosBackup.Export(new Scalar<int> { Value = 123 }, schema);
            Assert.Throws<InvalidDataException>(() => BDadosBackup.Import<Ambiguous>(schema, bytes));
        }

        [Theory]
        [MemberData(nameof(CompatibleValues))]
        public void CompatibleTypeChangesPreserveValues(object original, Type target, object expected) {
            Assert.Equal(expected, ConvertScalar(original, target));
        }

        public static IEnumerable<object[]> CompatibleValues() {
            yield return new object[] { byte.MaxValue, typeof(long), 255L };
            yield return new object[] { uint.MaxValue, typeof(long), 4294967295L };
            yield return new object[] { ulong.MaxValue, typeof(decimal), (decimal)ulong.MaxValue };
            yield return new object[] { 42L, typeof(short), (short)42 };
            yield return new object[] { 1.25f, typeof(double), 1.25d };
            yield return new object[] { 1.25d, typeof(float), 1.25f };
            yield return new object[] { 1.25m, typeof(double), 1.25d };
            yield return new object[] { 1.25d, typeof(decimal), 1.25m };
            yield return new object[] { true, typeof(int), 1 };
            yield return new object[] { 0L, typeof(bool), false };
            yield return new object[] { 1m, typeof(bool), true };
            yield return new object[] { Guid.Empty, typeof(string), Guid.Empty.ToString() };
            yield return new object[] { Guid.Empty.ToString(), typeof(Guid), Guid.Empty };
            yield return new object[] { "123.75", typeof(decimal), 123.75m };
            yield return new object[] { "1", typeof(bool), true };
            yield return new object[] { OldStatus.Ready, typeof(NewStatus), NewStatus.RenamedReady };
            yield return new object[] { (OldStatus)200, typeof(NewStatus), (NewStatus)200 };
            yield return new object[] { 5, typeof(NewStatus), NewStatus.RenamedReady };
            yield return new object[] { NewStatus.RenamedReady, typeof(int), 5 };
            yield return new object[] { "renamedready", typeof(NewStatus), NewStatus.RenamedReady };
        }

        [Theory]
        [MemberData(nameof(IncompatibleValues))]
        public void OverflowLossyAndInvalidConversionsNameTheColumn(object original, Type target) {
            var exception = Assert.Throws<InvalidDataException>(() => ConvertScalar(original, target));
            Assert.Contains("Value", exception.Message);
        }

        public static IEnumerable<object[]> IncompatibleValues() {
            yield return new object[] { ulong.MaxValue, typeof(long) };
            yield return new object[] { -1L, typeof(uint) };
            yield return new object[] { 1.5d, typeof(int) };
            yield return new object[] { 16777217, typeof(float) };
            yield return new object[] { 9007199254740993L, typeof(double) };
            yield return new object[] { 0.1m, typeof(double) };
            yield return new object[] { 0.1d, typeof(decimal) };
            yield return new object[] { 2, typeof(bool) };
            yield return new object[] { "invalid", typeof(Guid) };
            yield return new object[] { "invalid", typeof(int) };
        }

        [Fact]
        public void StringConversionsAreCultureIndependentAndDatesKeepTheirKind() {
            var previous = CultureInfo.CurrentCulture;
            try {
                CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("pt-BR");
                Assert.Equal(123.75m, ConvertScalar("123.75", typeof(decimal)));
                var date = new DateTime(638000000000000123, DateTimeKind.Utc);
                string text = (string)ConvertScalar(date, typeof(string));
                var restored = (DateTime)ConvertScalar(text, typeof(DateTime));
                Assert.Equal(date, restored);
                Assert.Equal(DateTimeKind.Utc, restored.Kind);
            } finally {
                CultureInfo.CurrentCulture = previous;
            }
        }

        [Fact]
        public void ExportStillRequiresAnExactSchema() {
            var schema = BDadosBackup.CreateSchema<Scalar<long>>();
            Assert.Throws<InvalidDataException>(() => BDadosBackup.Export(new Scalar<int> { Value = 1 }, schema));
        }

        [Fact]
        public async Task OldTableNamesRestoreAndDeliberatelyRemovedTablesCanBeSkipped() {
            var source = AccessorProxy.Create();
            source.Rows[typeof(OtherRow)] = new IDataObject[] { new OtherRow { Id = 1, Text = "removed table" } };
            source.Rows[typeof(OldCustomer)] = new IDataObject[] { new OldCustomer { Id = 42, Name = "kept" } };
            using var output = new ForwardStream(false);
            await new BDadosBackup(source.Accessor).BackupAsync(output, new[] { typeof(OtherRow), typeof(OldCustomer) });
            byte[] bytes = output.Bytes;
            var target = AccessorProxy.Create();
            using var input = new ForwardStream(true, bytes);
            await new BDadosBackup(target.Accessor).RestoreAsync(input, new[] { typeof(Customer) }, ignoreMissingTables: true);
            var restored = Assert.IsType<Customer>(Assert.Single(target.Saved));
            Assert.Equal("kept", restored.DisplayName);
            Assert.Equal(42, restored.Id);
            // Skipping a table must not skip integrity validation of its bytes.
            int firstRowOffset = 16 + BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(12)) + 32;
            int firstRowLength = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(firstRowOffset));
            bytes[firstRowOffset + 4 + firstRowLength - 1] ^= 1;
            var failingTarget = AccessorProxy.Create();
            using var corrupt = new ForwardStream(true, bytes);
            await Assert.ThrowsAsync<InvalidDataException>(() => new BDadosBackup(failingTarget.Accessor)
                .RestoreAsync(corrupt, new[] { typeof(Customer) }, ignoreMissingTables: true));
            Assert.Equal(1, failingTarget.Rollbacks);
        }

        [Fact]
        public async Task AmbiguousTableAliasesFailBeforeSaving() {
            var source = AccessorProxy.Create();
            source.Rows[typeof(OldCustomer)] = Array.Empty<IDataObject>();
            using var output = new ForwardStream(false);
            await new BDadosBackup(source.Accessor).BackupAsync(output, new[] { typeof(OldCustomer) });
            var target = AccessorProxy.Create();
            using var input = new ForwardStream(true, output.Bytes);
            await Assert.ThrowsAsync<InvalidDataException>(() => new BDadosBackup(target.Accessor)
                .RestoreAsync(input, new[] { typeof(Customer), typeof(ConflictingCustomer) }));
            Assert.Equal(0, target.Transactions);
        }

        [Fact]
        public async Task SqliteRestoresOlderSchemaIntoRenamedTableWithAddedRequiredColumn() {
            string connectionString = $"Data Source=migration-{Guid.NewGuid():N};Mode=Memory;Cache=Shared";
            await using var keeper = new SqliteConnection(connectionString);
            await keeper.OpenAsync();
            await using var command = keeper.CreateCommand();
            command.CommandText = "CREATE TABLE Customer (Id INTEGER PRIMARY KEY, DisplayName TEXT, Count INTEGER, Optional INTEGER NOT NULL, Added INTEGER NOT NULL, NewText TEXT)";
            await command.ExecuteNonQueryAsync();
            var source = AccessorProxy.Create();
            source.Rows[typeof(OldCustomer)] = new IDataObject[] { new OldCustomer { Id = 89, Name = "migrated", Count = 123, Optional = null } };
            using var output = new ForwardStream(false);
            await new BDadosBackup(source.Accessor).BackupAsync(output, new[] { typeof(OldCustomer) });
            using var target = new RdbmsDataAccessor(new SharedSqlitePlugin(connectionString));
            using var input = new ForwardStream(true, output.Bytes);
            await new BDadosBackup(target).RestoreAsync(input, new[] { typeof(Customer) });
            command.CommandText = "SELECT COUNT(*) FROM Customer WHERE Id = 89 AND DisplayName = 'migrated' AND Count = 123 AND Optional = 99 AND Added = 17 AND NewText = 'new default'";
            Assert.Equal(1L, await command.ExecuteScalarAsync());
        }

        [Theory]
        [InlineData("MySQL")]
        [InlineData("PostgreSQL")]
        [InlineData("SQLite")]
        public void DestinationGeneratorUsesCurrentModelAndClrValues(string engine) {
            var schema = BDadosBackup.CreateSchema<OldCustomer>();
            schema.Columns.Single(c => c.Name == "Name").DatabaseType = "MYSQL_SPECIFIC_TYPE";
            var restored = BDadosBackup.Import<Customer>(schema, BDadosBackup.Export(new OldCustomer { Id = 42, Count = uint.MaxValue }, schema));
            IQueryGenerator generator = engine switch {
                "MySQL" => new MySqlQueryGenerator(),
                "PostgreSQL" => new PgSQLQueryGenerator(),
                _ => new SqliteQueryGenerator()
            };
            var insert = generator.GenerateSingleInsertQuery(restored);
            Assert.Contains("Customer", insert.GetCommandText());
            Assert.Contains("DisplayName", insert.GetCommandText());
            Assert.DoesNotContain("Deleted", insert.GetCommandText());
            Assert.DoesNotContain("MYSQL_SPECIFIC_TYPE", insert.GetCommandText());
            Assert.Contains(insert.GetParameters(), p => Equals(p.Value, (long)uint.MaxValue));
            Assert.Contains(insert.GetParameters(), p => Equals(p.Value, 42L));
        }

        private static object ConvertScalar(object value, Type target) {
            Type sourceType = typeof(Scalar<>).MakeGenericType(value.GetType());
            object instance = Activator.CreateInstance(sourceType)!;
            sourceType.GetProperty("Value")!.SetValue(instance, value);
            var schema = BDadosBackup.CreateSchema(sourceType);
            Type targetType = typeof(Scalar<>).MakeGenericType(target);
            var restored = BDadosBackup.Import(targetType, schema, BDadosBackup.Export(instance, schema));
            return targetType.GetProperty("Value")!.GetValue(restored)!;
        }

        public class Scalar<T> { [Field] public T Value { get; set; } = default!; }
        public enum OldStatus : byte { Ready = 5 }
        public enum NewStatus : long { RenamedReady = 5 }
        public class BothNames {
            [Field] public string Old { get; set; } = "old";
            [Field] public string New { get; set; } = "current";
        }
        public class Renamed { [Field, OldName("Old")] public string New { get; set; } = ""; }
        public class Ambiguous {
            [Field, OldName("Value")] public int First { get; set; }
            [Field, OldName("Value")] public int Second { get; set; }
        }
        public class OldCustomer : BaseDataObject<long> {
            [Field, PrimaryKey] public override long Id { get; set; }
            [Field] public string Name { get; set; } = "";
            [Field] public byte[]? Deleted { get; set; }
            [Field] public uint Count { get; set; }
            [Field] public int? Optional { get; set; }
        }
        [OldName(nameof(OldCustomer))]
        public class Customer : BaseDataObject<long>, IApplicationGeneratedId<long> {
            [Field, PrimaryKey] public override long Id { get; set; }
            [Field, OldName("Name")] public string DisplayName { get; set; } = "";
            [Field] public long Count { get; set; }
            [Field] public int Optional { get; set; } = 99;
            [Field(DefaultValue = 17)] public int Added { get; set; }
            [Field(DefaultValue = "new default")] public string NewText { get; set; } = "";
            public long GenerateId() => 123;
        }
        [OldName(nameof(OldCustomer))]
        public class ConflictingCustomer : Customer { }
    }
}
