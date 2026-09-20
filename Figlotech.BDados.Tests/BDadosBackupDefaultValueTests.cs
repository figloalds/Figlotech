using System.Text;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using Figlotech.BDados.Helpers;
using Figlotech.Core.Interfaces;
using Xunit;
using static Figlotech.BDados.Tests.BDadosBackupTests;

namespace Figlotech.BDados.Tests {
    public class BDadosBackupDefaultValueTests {
        [Fact]
        public void MissingMembersUseCurrentAttributeDefaultsWithoutSerializingThem() {
            var schema = BDadosBackupSchema.Deserialize(BDadosBackup.CreateSchema<OldRow>().Serialize());
            byte[] bytes = BDadosBackup.Export(new OldRow { Id = 42, OldName = "saved" }, schema);
            var restored = BDadosBackup.Import<CurrentRow>(schema, bytes);
            Assert.Equal(42, restored.Id);
            Assert.Equal("saved", restored.Name);
            Assert.Equal(7, restored.Required);
            Assert.False(restored.Enabled);
            Assert.Equal(12.5m, restored.Amount);
            Assert.Equal(Status.Ready, restored.State);
            Assert.Equal(123L, restored.Optional);
            Assert.Equal(31, restored.InheritedDefault);
            Assert.Null(restored.ExplicitNull);
            Assert.Equal("initializer", restored.NoDefault);
            Assert.DoesNotContain("\"DefaultValue\":", Encoding.UTF8.GetString(BDadosBackup.CreateSchema<CurrentRow>().Serialize()));
        }

        [Fact]
        public void ExistingZeroAndRenamedNullValuesAreNotReplacedByAttributeDefaults() {
            var schema = BDadosBackup.CreateSchema<OldRow>();
            var restored = BDadosBackup.Import<CurrentRow>(schema, BDadosBackup.Export(new OldRow { Id = 0, OldName = null }, schema));
            Assert.Equal(0, restored.Id);
            Assert.Null(restored.Name);
        }

        [Fact]
        public void InvalidDefaultOnAPresentColumnIsNotEvaluated() {
            var schema = BDadosBackup.CreateSchema<OldRow>();
            var restored = BDadosBackup.Import<InvalidButPresent>(schema, BDadosBackup.Export(new OldRow { Id = 45 }, schema));
            Assert.Equal(45, restored.Id);
        }

        [Theory]
        [InlineData(typeof(InvalidDefault))]
        [InlineData(typeof(InvalidNullDefault))]
        public async Task InvalidMissingDefaultsFailBeforeStartingRestoreTransaction(Type targetType) {
            var source = AccessorProxy.Create();
            source.Rows[typeof(OldRow)] = Array.Empty<IDataObject>();
            using var output = new ForwardStream(false);
            await new BDadosBackup(source.Accessor).BackupAsync(output, new[] { typeof(OldRow) });
            var target = AccessorProxy.Create();
            using var input = new ForwardStream(true, output.Bytes);
            var exception = await Assert.ThrowsAsync<InvalidDataException>(() => new BDadosBackup(target.Accessor)
                .RestoreAsync(input, new[] { targetType }));
            Assert.Contains("Field.DefaultValue", exception.Message);
            Assert.Contains("Added", exception.Message);
            Assert.Equal(0, target.Transactions);
        }

        [Fact]
        public async Task RestoredRowsDoNotShareMutableArrayDefaults() {
            var source = AccessorProxy.Create();
            source.Rows[typeof(OldRow)] = new IDataObject[] { new OldRow { Id = 1 }, new OldRow { Id = 2 } };
            using var output = new ForwardStream(false);
            await new BDadosBackup(source.Accessor).BackupAsync(output, new[] { typeof(OldRow) });
            var target = AccessorProxy.Create();
            using var input = new ForwardStream(true, output.Bytes);
            await new BDadosBackup(target.Accessor).RestoreAsync(input, new[] { typeof(CurrentRow) });
            var first = Assert.IsType<CurrentRow>(target.Saved[0]);
            var second = Assert.IsType<CurrentRow>(target.Saved[1]);
            Assert.Equal(new byte[] { 1, 2 }, first.Bytes);
            Assert.NotSame(first.Bytes, second.Bytes);
            first.Bytes[0] = 99;
            Assert.Equal(1, second.Bytes[0]);
        }

        public enum Status { Ready = 2 }
        public class OldRow : BaseDataObject<long> {
            [Field, PrimaryKey] public override long Id { get; set; }
            [Field] public string? OldName { get; set; }
        }
        public class DefaultBase : BaseDataObject<long> {
            [Field(DefaultValue = 99), PrimaryKey] public override long Id { get; set; }
            [Field(DefaultValue = 31)] public virtual int InheritedDefault { get; set; }
        }
        [OldName(nameof(OldRow))]
        public class CurrentRow : DefaultBase {
            [Field(DefaultValue = "fallback"), OldName("OldName")] public string? Name { get; set; } = "initial";
            [Field(DefaultValue = 7)] public int Required { get; set; } = 100;
            [Field(DefaultValue = false)] public bool Enabled = true;
            [Field(DefaultValue = "12.5")] public decimal Amount { get; set; }
            [Field(DefaultValue = "Ready")] public Status State { get; set; }
            [Field(DefaultValue = 123)] public long? Optional { get; set; }
            [Field(DefaultValue = null)] public string? ExplicitNull { get; set; } = "initial";
            [Field] public string NoDefault { get; set; } = "initializer";
            [Field(DefaultValue = new byte[] { 1, 2 })] public byte[] Bytes { get; set; } = Array.Empty<byte>();
            public override int InheritedDefault { get; set; }
        }
        public class InvalidButPresent {
            [Field(DefaultValue = "invalid")] public long Id { get; set; }
        }
        [OldName(nameof(OldRow))]
        public class InvalidDefault : OldRow {
            [Field(DefaultValue = "not an integer")] public int Added { get; set; }
        }
        [OldName(nameof(OldRow))]
        public class InvalidNullDefault : OldRow {
            [Field(DefaultValue = null)] public int Added { get; set; }
        }
    }
}
