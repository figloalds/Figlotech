using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using Figlotech.BDados.DataAccessAbstractions;
using Xunit;

namespace Figlotech.BDados.Tests {
    public class ReaderSchemaValidatorTests {
        [Fact]
        public void ValidateAcceptsExactOrderedFullGraphProjectionAliases() {
            DefinitiveJoinPlan plan = FullGraphPlan();
            string[] aliases = ExpectedAliases(plan);

            ReaderSchemaValidator.Validate(new SchemaOnlyDataRecord(aliases), plan);
        }

        [Fact]
        public void ValidateAcceptsCaseFoldedAliasesWhenTheirOrderIsUnchanged() {
            DefinitiveJoinPlan plan = FullGraphPlan();
            string[] aliases = ExpectedAliases(plan).Select(alias => alias.ToLowerInvariant()).ToArray();

            ReaderSchemaValidator.Validate(new SchemaOnlyDataRecord(aliases), plan);
        }

        [Fact]
        public void ValidateRejectsReorderedAliasesWithOrderMismatchAndOrdinalDiagnostics() {
            DefinitiveJoinPlan plan = FullGraphPlan();
            string[] aliases = ExpectedAliases(plan);
            (aliases[0], aliases[1]) = (aliases[1], aliases[0]);

            ArgumentException exception = Assert.Throws<ArgumentException>(() => ReaderSchemaValidator.Validate(new SchemaOnlyDataRecord(aliases), plan));

            Assert.Contains("order mismatch", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("index 0", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("Expected ordered aliases", exception.Message);
            Assert.Contains("Actual ordered aliases", exception.Message);
        }

        [Fact]
        public void ValidateRejectsMissingAliasAndNamesTheMissingExpectedAlias() {
            DefinitiveJoinPlan plan = FullGraphPlan();
            string[] aliases = ExpectedAliases(plan);
            string missing = aliases[^1];

            ArgumentException exception = Assert.Throws<ArgumentException>(() => ReaderSchemaValidator.Validate(new SchemaOnlyDataRecord(aliases.Take(aliases.Length - 1)), plan));

            Assert.Contains("missing", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Contains(missing, exception.Message);
            Assert.Contains("Expected ordered aliases", exception.Message);
            Assert.Contains("Actual ordered aliases", exception.Message);
        }

        [Fact]
        public void ValidateRejectsUnexpectedAliasAndNamesTheUnexpectedAlias() {
            DefinitiveJoinPlan plan = FullGraphPlan();
            string unexpected = "provider_sentinel";
            string[] aliases = ExpectedAliases(plan).Append(unexpected).ToArray();

            ArgumentException exception = Assert.Throws<ArgumentException>(() => ReaderSchemaValidator.Validate(new SchemaOnlyDataRecord(aliases), plan));

            Assert.Contains("unexpected", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Contains(unexpected, exception.Message);
            Assert.Contains("Expected ordered aliases", exception.Message);
            Assert.Contains("Actual ordered aliases", exception.Message);
        }

        [Fact]
        public void ValidateRejectsDuplicateAliasAndIncludesBothOrderedSchemas() {
            DefinitiveJoinPlan plan = FullGraphPlan();
            string[] aliases = ExpectedAliases(plan);
            string duplicate = aliases[0];
            aliases[^1] = duplicate;

            ArgumentException exception = Assert.Throws<ArgumentException>(() => ReaderSchemaValidator.Validate(new SchemaOnlyDataRecord(aliases), plan));

            Assert.Contains("duplicate", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Contains(duplicate, exception.Message);
            Assert.Contains("Expected ordered aliases", exception.Message);
            Assert.Contains("Actual ordered aliases", exception.Message);
        }

        [Fact]
        public void ValidateIgnoresProviderFieldTypesAndLeavesPlanContentUnchanged() {
            DefinitiveJoinPlan plan = FullGraphPlan();
            string signature = plan.StructuralSignature;
            string[] projection = ExpectedAliases(plan);
            var first = new SchemaOnlyDataRecord(projection, Enumerable.Repeat(typeof(string), projection.Length));
            var second = new SchemaOnlyDataRecord(projection, Enumerable.Repeat(typeof(DateTime), projection.Length));

            ReaderSchemaValidator.Validate(first, plan);
            ReaderSchemaValidator.Validate(second, plan);

            Assert.Equal(0, first.GetFieldTypeCallCount);
            Assert.Equal(0, second.GetFieldTypeCallCount);
            Assert.Equal(signature, plan.StructuralSignature);
            Assert.Equal(projection, ExpectedAliases(plan));
        }

        [Fact]
        public void ValidateGuardsNullRecordAndPlan() {
            DefinitiveJoinPlan plan = FullGraphPlan();
            var record = new SchemaOnlyDataRecord(ExpectedAliases(plan));

            ArgumentNullException recordException = Assert.Throws<ArgumentNullException>(() => ReaderSchemaValidator.Validate(null!, plan));
            ArgumentNullException planException = Assert.Throws<ArgumentNullException>(() => ReaderSchemaValidator.Validate(record, null!));

            Assert.Equal("record", recordException.ParamName);
            Assert.Equal("plan", planException.ParamName);
        }

        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData(" ")]
        public void ValidateRejectsNullOrBlankActualAliasActionably(string? invalidAlias) {
            DefinitiveJoinPlan plan = FullGraphPlan();
            string?[] aliases = ExpectedAliases(plan).Cast<string?>().ToArray();
            aliases[2] = invalidAlias;

            ArgumentException exception = Assert.Throws<ArgumentException>(() => ReaderSchemaValidator.Validate(new SchemaOnlyDataRecord(aliases), plan));

            Assert.Contains("actual result alias", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("index 2", exception.Message, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("Expected ordered aliases", exception.Message);
            Assert.Contains("Actual ordered aliases", exception.Message);
        }

        private static DefinitiveJoinPlan FullGraphPlan() {
            return DefinitiveJoinPlanCompiler.Compile(typeof(GuidRoot), AggregateJoinShape.FullGraph);
        }

        private static string[] ExpectedAliases(DefinitiveJoinPlan plan) {
            return plan.Projection.Select(column => column.ResultAlias).ToArray();
        }

        private sealed class SchemaOnlyDataRecord : IDataRecord {
            private readonly string?[] _names;
            private readonly Type[] _fieldTypes;

            public SchemaOnlyDataRecord(IEnumerable<string?> names, IEnumerable<Type>? fieldTypes = null) {
                _names = names.ToArray();
                _fieldTypes = (fieldTypes ?? Enumerable.Repeat(typeof(object), _names.Length)).ToArray();
                if (_fieldTypes.Length != _names.Length) {
                    throw new ArgumentException("Field types must have one entry for each schema name.", nameof(fieldTypes));
                }
            }

            public int GetFieldTypeCallCount { get; private set; }
            public int FieldCount => _names.Length;
            public object this[int i] => throw new NotSupportedException();
            public object this[string name] => throw new NotSupportedException();

            public string GetName(int i) {
                return _names[i]!;
            }

            public Type GetFieldType(int i) {
                GetFieldTypeCallCount++;
                return _fieldTypes[i];
            }

            public bool GetBoolean(int i) => throw new NotSupportedException();
            public byte GetByte(int i) => throw new NotSupportedException();
            public long GetBytes(int i, long fieldOffset, byte[]? buffer, int bufferoffset, int length) => throw new NotSupportedException();
            public char GetChar(int i) => throw new NotSupportedException();
            public long GetChars(int i, long fieldoffset, char[]? buffer, int bufferoffset, int length) => throw new NotSupportedException();
            public IDataReader GetData(int i) => throw new NotSupportedException();
            public string GetDataTypeName(int i) => throw new NotSupportedException();
            public DateTime GetDateTime(int i) => throw new NotSupportedException();
            public decimal GetDecimal(int i) => throw new NotSupportedException();
            public double GetDouble(int i) => throw new NotSupportedException();
            public float GetFloat(int i) => throw new NotSupportedException();
            public Guid GetGuid(int i) => throw new NotSupportedException();
            public short GetInt16(int i) => throw new NotSupportedException();
            public int GetInt32(int i) => throw new NotSupportedException();
            public long GetInt64(int i) => throw new NotSupportedException();
            public int GetOrdinal(string name) => throw new NotSupportedException();
            public string GetString(int i) => throw new NotSupportedException();
            public object GetValue(int i) => throw new NotSupportedException();
            public int GetValues(object[] values) => throw new NotSupportedException();
            public bool IsDBNull(int i) => throw new NotSupportedException();
        }
    }
}
