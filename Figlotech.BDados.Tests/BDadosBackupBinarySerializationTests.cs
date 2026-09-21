using System.Buffers.Binary;
using System.Text;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using Figlotech.BDados.Helpers;
using Figlotech.Core.Interfaces;
using Xunit;
using static Figlotech.BDados.Tests.BDadosBackupTests;

namespace Figlotech.BDados.Tests {
    public class BDadosBackupBinarySerializationTests {
        [Fact]
        public void FixedStructUsesSchemaLengthWithoutPrefix() {
            var schema = BDadosBackupSchema.Deserialize(BDadosBackup.CreateSchema<Scalar<FixedValue>>().Serialize());
            Assert.Equal(2, schema.Version);
            Assert.Equal(BDadosBackupDataType.BinarySerializable, schema.Columns[0].DataType);
            Assert.Equal(typeof(FixedValue).FullName, schema.Columns[0].BinaryType);
            Assert.Equal(4, schema.Columns[0].FixedLength);
            byte[] bytes = BDadosBackup.Export(new Scalar<FixedValue> { Value = new FixedValue { Number = 42 } }, schema);
            Assert.Equal(new byte[] { 42, 0, 0, 0 }, bytes);
            Assert.Equal(42, BDadosBackup.Import<Scalar<FixedValue>>(schema, bytes).Value.Number);
        }

        [Fact]
        public void NullableFixedStructUsesOneByteMarkerAndPreservesZero() {
            var schema = BDadosBackup.CreateSchema<Scalar<FixedValue?>>();
            byte[] bytes = BDadosBackup.Export(new Scalar<FixedValue?> { Value = new FixedValue() }, schema);
            Assert.Equal(new byte[] { 1, 0, 0, 0, 0 }, bytes);
            Assert.Equal(0, BDadosBackup.Import<Scalar<FixedValue?>>(schema, bytes).Value!.Value.Number);
            bytes = BDadosBackup.Export(new Scalar<FixedValue?>(), schema);
            Assert.Equal(new byte[] { 0 }, bytes);
            Assert.Null(BDadosBackup.Import<Scalar<FixedValue?>>(schema, bytes).Value);
            Assert.Equal(0, BDadosBackup.Import<Scalar<FixedValue>>(schema, bytes).Value.Number);
        }

        [Fact]
        public void ExplicitStaticMetadataSupportsFixedClassesAndFreshInstances() {
            var schema = BDadosBackup.CreateSchema<Scalar<FixedClass>>();
            byte[] bytes = BDadosBackup.Export(new Scalar<FixedClass> { Value = new FixedClass { Number = 7 } }, schema);
            Assert.Equal(new byte[] { 1, 7 }, bytes);
            var first = BDadosBackup.Import<Scalar<FixedClass>>(schema, bytes);
            var second = BDadosBackup.Import<Scalar<FixedClass>>(schema, bytes);
            Assert.Equal(7, first.Value.Number);
            Assert.NotSame(first.Value, second.Value);
            Assert.Null(BDadosBackup.Import<Scalar<FixedClass>>(schema, new byte[] { 0 }).Value);
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("héllo 世界")]
        public void VariableTypesDefaultToLengthPrefixAndPreserveNullAndEmpty(string? text) {
            var schema = BDadosBackup.CreateSchema<Scalar<VariableValue>>();
            Assert.Null(schema.Columns[0].FixedLength);
            byte[] bytes = BDadosBackup.Export(new Scalar<VariableValue> {
                Value = text == null ? null! : new VariableValue { Text = text }
            }, schema);
            Assert.Equal(text == null ? -1 : Encoding.UTF8.GetByteCount(text), BinaryPrimitives.ReadInt32LittleEndian(bytes));
            Assert.Equal(text, BDadosBackup.Import<Scalar<VariableValue>>(schema, bytes).Value?.Text);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void RemovedCustomColumnsAreSkippedUsingOnlyArchiveMetadata(bool nulls) {
            var schema = BDadosBackup.CreateSchema<BinaryRow>();
            var row = new BinaryRow {
                Id = 19, Text = "after", Fixed = new FixedValue { Number = 42 },
                Optional = nulls ? null : new FixedValue { Number = 5 },
                Variable = nulls ? null : new VariableValue { Text = "payload" }
            };
            byte[] bytes = BDadosBackup.Export(row, schema);
            // Removed types need not exist locally; metadata is never resolved through assembly loading.
            foreach (var column in schema.Columns.Where(c => c.BinaryType != null)) column.BinaryType = "Removed.Type";
            var restored = BDadosBackup.Import<Row>(schema, bytes);
            Assert.Equal(19, restored.Id);
            Assert.Equal("after", restored.Text);
        }

        [Fact]
        public void MalformedFixedValuesAreRejected() {
            var schema = BDadosBackup.CreateSchema<Scalar<FixedValue?>>();
            byte[] valid = { 1, 42, 0, 0, 0 };
            for (int i = 0; i < valid.Length; i++) {
                Assert.Throws<InvalidDataException>(() => BDadosBackup.Import<Scalar<FixedValue?>>(schema, valid[..i]));
            }
            Assert.Throws<InvalidDataException>(() => BDadosBackup.Import<Scalar<FixedValue?>>(schema, new byte[] { 2, 42, 0, 0, 0 }));
            Assert.Throws<InvalidDataException>(() => BDadosBackup.Import<Scalar<FixedValue?>>(schema, new byte[] { 0, 42 }));
            var variableSchema = BDadosBackup.CreateSchema<Scalar<VariableValue>>();
            Assert.Throws<InvalidDataException>(() => BDadosBackup.Import<Scalar<VariableValue>>(variableSchema, new byte[] { 5, 0, 0, 0, 1 }));
        }

        [Fact]
        public void CustomDecoderErrorsIncludeColumnContext() {
            var schema = BDadosBackup.CreateSchema<Scalar<FixedValue>>();
            var exception = Assert.Throws<InvalidDataException>(() => BDadosBackup.Import<Scalar<FixedValue>>(schema, new byte[] { 255, 255, 255, 255 }));
            Assert.Contains("Value", exception.Message);
            Assert.IsType<InvalidDataException>(exception.InnerException);
        }

        [Fact]
        public void InvalidPayloadsAndUnconstructibleTypesFailExplicitly() {
            var schema = BDadosBackup.CreateSchema<Scalar<WrongLength>>();
            Assert.Throws<InvalidDataException>(() => BDadosBackup.Export(new Scalar<WrongLength> { Value = new WrongLength() }, schema));
            var nullSchema = BDadosBackup.CreateSchema<Scalar<NullPayload>>();
            Assert.Throws<InvalidDataException>(() => BDadosBackup.Export(new Scalar<NullPayload> { Value = new NullPayload() }, nullSchema));
            Assert.Throws<InvalidDataException>(() => BDadosBackup.CreateSchema<Scalar<NegativeLength>>());
            Assert.Throws<NotSupportedException>(() => BDadosBackup.CreateSchema<Scalar<NoConstructor>>());
            Assert.Throws<NotSupportedException>(() => BDadosBackup.CreateSchema<Scalar<IBinarySerializable>>());
        }

        [Fact]
        public void BinaryIdentityLengthAndSchemaVersionMustMatch() {
            var schema = BDadosBackup.CreateSchema<Scalar<FixedValue>>();
            Assert.Throws<InvalidDataException>(() => BDadosBackup.Import<Scalar<FixedClass>>(schema, new byte[4]));
            Assert.Throws<InvalidDataException>(() => BDadosBackup.Import<Scalar<string>>(schema, new byte[4]));
            schema.Columns[0].FixedLength = 8;
            Assert.Throws<InvalidDataException>(() => BDadosBackup.Import<Scalar<FixedValue>>(schema, new byte[8]));
            Assert.Throws<InvalidDataException>(() => BDadosBackup.Export(new Scalar<FixedValue>(), schema));
            schema.Columns[0].FixedLength = -1;
            Assert.Throws<InvalidDataException>(() => schema.Serialize());
            schema.Columns[0].FixedLength = 4;
            schema.Version = 1;
            Assert.Throws<InvalidDataException>(() => schema.Serialize());
            schema.Version = 2;
            schema.Columns[0].BinaryType = null;
            Assert.Throws<InvalidDataException>(() => schema.Serialize());
            var primitiveSchema = BDadosBackup.CreateSchema<OneValue>();
            primitiveSchema.Columns[0].FixedLength = 8;
            Assert.Throws<InvalidDataException>(() => primitiveSchema.Serialize());
            var stringSchema = BDadosBackup.CreateSchema<Scalar<string>>();
            Assert.Throws<InvalidDataException>(() => BDadosBackup.Import<Scalar<VariableValue>>(stringSchema, new byte[4]));
        }

        [Fact]
        public void ZeroLengthFixedPayloadIsDistinctFromNull() {
            var schema = BDadosBackup.CreateSchema<Scalar<EmptyValue?>>();
            byte[] bytes = BDadosBackup.Export(new Scalar<EmptyValue?> { Value = new EmptyValue() }, schema);
            Assert.Equal(new byte[] { 1 }, bytes);
            Assert.True(BDadosBackup.Import<Scalar<EmptyValue?>>(schema, bytes).Value.HasValue);
            Assert.Null(BDadosBackup.Import<Scalar<EmptyValue?>>(schema, new byte[] { 0 }).Value);
            var requiredSchema = BDadosBackup.CreateSchema<Scalar<EmptyValue>>();
            Assert.Empty(BDadosBackup.Export(new Scalar<EmptyValue>(), requiredSchema));
            BDadosBackup.Import<Scalar<EmptyValue>>(requiredSchema, Array.Empty<byte>());
        }

        [Fact]
        public async Task StreamingArchiveRoundTripsCustomTypesAndRollsBackCorruption() {
            var source = AccessorProxy.Create();
            source.Rows[typeof(BinaryRow)] = new IDataObject[] {
                new BinaryRow { Id = 1, Fixed = new FixedValue { Number = 42 }, Optional = new FixedValue(), Variable = new VariableValue { Text = "hello" } },
                new BinaryRow { Id = 2 }
            };
            using var output = new ForwardStream(false);
            await new BDadosBackup(source.Accessor).BackupAsync(output, new[] { typeof(BinaryRow) });
            byte[] bytes = output.Bytes;
            var target = AccessorProxy.Create();
            using var input = new ForwardStream(true, bytes);
            await new BDadosBackup(target.Accessor).RestoreAsync(input, new[] { typeof(BinaryRow) });
            Assert.Equal(1, target.Commits);
            var rows = target.Saved.Cast<BinaryRow>().ToArray();
            Assert.Equal(42, rows[0].Fixed.Number);
            Assert.Equal(0, rows[0].Optional!.Value.Number);
            Assert.Equal("hello", rows[0].Variable!.Text);
            Assert.Null(rows[1].Optional);
            Assert.Null(rows[1].Variable);
            target = AccessorProxy.Create();
            bytes[^1] ^= 1;
            using var corrupt = new ForwardStream(true, bytes);
            await Assert.ThrowsAsync<InvalidDataException>(() => new BDadosBackup(target.Accessor).RestoreAsync(corrupt, new[] { typeof(BinaryRow) }));
            Assert.Equal(1, target.Rollbacks);
            Assert.Empty(target.Saved);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task CustomPayloadsRespectRowLimits(bool fixedLength) {
            var source = AccessorProxy.Create();
            Type rowType = fixedLength ? typeof(FixedRow) : typeof(VariableRow);
            source.Rows[rowType] = new IDataObject[] { fixedLength ? new FixedRow() : new VariableRow { Value = new VariableValue { Text = new string('x', 100) } } };
            using var output = new ForwardStream(false);
            await Assert.ThrowsAsync<InvalidDataException>(() => new BDadosBackup(source.Accessor, maxRowBytes: 11).BackupAsync(output, new[] { rowType }));
        }

        public class Scalar<T> { [Field] public T Value { get; set; } = default!; }
        public class BinaryRow : Row {
            [Field] public FixedValue Fixed { get; set; }
            [Field] public FixedValue? Optional { get; set; }
            [Field] public VariableValue? Variable { get; set; }
        }
        public class FixedRow : BaseDataObject<long> {
            [Field] public override long Id { get; set; }
            [Field] public FixedValue Value { get; set; }
        }
        public class VariableRow : BaseDataObject<long> {
            [Field] public override long Id { get; set; }
            [Field] public VariableValue? Value { get; set; }
        }
        public struct FixedValue : IBinarySerializable {
            public int Number { get; set; }
            public static bool IsFixedLength => true;
            public static int Length => 4;
            public byte[] ToBytes() {
                var bytes = new byte[4];
                BinaryPrimitives.WriteInt32LittleEndian(bytes, Number);
                return bytes;
            }
            public void FromBytes(byte[] bytes) {
                Number = BinaryPrimitives.ReadInt32LittleEndian(bytes);
                if (Number < 0) throw new InvalidDataException("Negative values are not supported.");
            }
        }
        public class FixedClass : IBinarySerializable {
            public byte Number { get; set; }
            static bool IBinarySerializable.IsFixedLength => true;
            static int IBinarySerializable.Length => 1;
            public byte[] ToBytes() => new byte[] { Number };
            public void FromBytes(byte[] bytes) => Number = bytes[0];
        }
        public class VariableValue : IBinarySerializable {
            public string Text { get; set; } = "";
            public byte[] ToBytes() => Encoding.UTF8.GetBytes(Text);
            public void FromBytes(byte[] bytes) => Text = Encoding.UTF8.GetString(bytes);
        }
        public class WrongLength : IBinarySerializable {
            public static bool IsFixedLength => true;
            public static int Length => 4;
            public byte[] ToBytes() => new byte[3];
            public void FromBytes(byte[] bytes) { }
        }
        public class NullPayload : IBinarySerializable {
            public byte[] ToBytes() => null!;
            public void FromBytes(byte[] bytes) { }
        }
        public class NegativeLength : IBinarySerializable {
            public static bool IsFixedLength => true;
            public static int Length => -1;
            public byte[] ToBytes() => Array.Empty<byte>();
            public void FromBytes(byte[] bytes) { }
        }
        public class NoConstructor : IBinarySerializable {
            public NoConstructor(int value) { }
            public byte[] ToBytes() => Array.Empty<byte>();
            public void FromBytes(byte[] bytes) { }
        }
        public struct EmptyValue : IBinarySerializable {
            public static bool IsFixedLength => true;
            public static int Length => 0;
            public byte[] ToBytes() => Array.Empty<byte>();
            public void FromBytes(byte[] bytes) { }
        }
    }
}
