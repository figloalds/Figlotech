using System.Reflection;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using Figlotech.BDados.PgSQLDataAccessor;
using Xunit;

namespace Figlotech.BDados.Tests {
    public class PgSQLEnumMappingTests {
        public enum ConversionType {
            Multiplication = 1,
            Division = 2
        }

        public class EnumModel {
            [Field]
            public ConversionType RequiredProperty { get; set; }

            [Field(AllowNull = true)]
            public ConversionType? NullableProperty { get; set; }

            [Field]
            public ConversionType RequiredField;

            [Field(AllowNull = true)]
            public ConversionType? NullableField;
        }

        [Theory]
        [InlineData(nameof(EnumModel.RequiredProperty), false)]
        [InlineData(nameof(EnumModel.NullableProperty), true)]
        [InlineData(nameof(EnumModel.RequiredField), false)]
        [InlineData(nameof(EnumModel.NullableField), true)]
        public void EnumColumnsUseInt4AndPreserveNullability(string memberName, bool nullable) {
            var member = typeof(EnumModel).GetMember(memberName).Single();
            var attribute = member.GetCustomAttribute<FieldAttribute>()!;
            var generator = new PgSQLQueryGenerator();

            Assert.Equal("INT4", generator.GetDatabaseType(member));
            Assert.Equal("INT4", generator.GetDatabaseTypeWithLength(member, attribute));
            Assert.Equal($"{memberName} INT4  {(nullable ? "DEFAULT NULL" : "NOT NULL")}",
                generator.GetColumnDefinition(member, attribute));
        }
    }
}
