
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Figlotech.Core.Helpers;
using Xunit;

namespace Figlotech.Core.Tests {
    public class ReflectionToolTests {
        [Theory]
        [InlineData(1, 1L)]
        [InlineData(2L, 2L)]
        [InlineData(3.0, 3L)]
        public void BuildObjectToTargetConversionExpression_ConvertsNumericObjectToValueBox(object input, long expected) {
            var parameter = Expression.Parameter(typeof(object), "value");
            var conversion = ReflectionTool.BuildObjectToTargetConversionExpression(parameter, typeof(ValueBox<long>));
            var converter = Expression.Lambda<Func<object, ValueBox<long>>>(conversion, parameter).Compile();

            var result = converter(input);

            Assert.NotNull(result);
            Assert.Equal(expected, result.Value);
        }

        [Fact]
        public void BuildObjectToTargetConversionExpression_ConvertsDbNullToDefaultValueBox() {
            var parameter = Expression.Parameter(typeof(object), "value");
            var conversion = ReflectionTool.BuildObjectToTargetConversionExpression(parameter, typeof(ValueBox<long>));
            var converter = Expression.Lambda<Func<object, ValueBox<long>>>(conversion, parameter).Compile();

            var result = converter(DBNull.Value);

            Assert.Null(result);
        }

        [Fact]
        public void BuildObjectToTargetConversionExpression_ConvertsDbNullToNullObject() {
            var parameter = Expression.Parameter(typeof(object), "value");
            var conversion = ReflectionTool.BuildObjectToTargetConversionExpression(parameter, typeof(object));
            var converter = Expression.Lambda<Func<object, object>>(conversion, parameter).Compile();

            var result = converter(DBNull.Value);

            Assert.Null(result);
        }

        [Fact]
        public void BuildObjectToTargetConversionExpression_ConvertsByteArrayToUtf8String() {
            var parameter = Expression.Parameter(typeof(object), "value");
            var conversion = ReflectionTool.BuildObjectToTargetConversionExpression(parameter, typeof(string));
            var converter = Expression.Lambda<Func<object, string>>(conversion, parameter).Compile();

            var result = converter(new byte[] { 0xC3, 0xA7, 0xC3, 0xA3, 0x6F });

            Assert.Equal("ção", result);
        }

        class SampleForDbNullAssignment {
            public int Value = 42;
        }

        [Fact]
        public void SetValue_DbNullForNonNullableValue_DoesNotAssignDefault() {
            var target = new SampleForDbNullAssignment();

            ReflectionTool.SetValue(target, "Value", DBNull.Value);

            Assert.Equal(42, target.Value);
        }

        // ==== Fix #1 (cleanup): CollectMembers has 4 loops; 2 are dead (BindingFlags.Instance
        // without an access flag returns 0), 2 are duplicates of each other. Result: no actual
        // duplication today, but 2 wasted reflection calls per type init. Test guards against
        // future regression to true duplication.
        [Fact]
        public void FieldsAndPropertiesOf_DoesNotReturnDuplicateMembers() {
            var members = ReflectionTool.FieldsAndPropertiesOf(typeof(SampleForMembers));
            var names = members.Select(m => m.Name).ToList();
            var duplicates = names.GroupBy(n => n).Where(g => g.Count() > 1).Select(g => g.Key).ToList();
            Assert.Empty(duplicates);
        }

        [Fact]
        public void FieldsAndPropertiesOf_IncludesPublicInstanceMembers_ExcludesPrivateAndStatic() {
            var members = ReflectionTool.FieldsAndPropertiesOf(typeof(SampleForMembers));
            var names = members.Select(m => m.Name).ToHashSet();
            Assert.Contains("PublicField", names);
            Assert.Contains("PublicProperty", names);
            Assert.DoesNotContain("PrivateField", names);
            Assert.DoesNotContain("StaticField", names);
        }

        // ==== Fix #2: EnumerateList must yield element VALUES, not the PropertyInfo of Current.
        // Previously it yielded EnumeratorPropCurrentCache[type] (a PropertyInfo) every iteration.
        [Fact]
        public void EnumerateList_YieldsElementValues_NotPropertyInfo() {
            var list = new List<object> { "a", "b", "c" };
            var items = ReflectionTool.EnumerateList(list).ToList();
            Assert.Equal(3, items.Count);
            Assert.Equal("a", items[0]);
            Assert.Equal("b", items[1]);
            Assert.Equal("c", items[2]);
        }

        [Fact]
        public void EnumerateList_NullInput_YieldsNothing() {
            var items = ReflectionTool.EnumerateList(null).ToList();
            Assert.Empty(items);
        }

        // ==== Fix #3: SetMemberValue bool-from-string branch must not capture the first-call
        // string in its cached closure. The cached lambda must use the runtime argument.
        class SampleForBoolCapture {
            public bool Flag;
        }

        [Fact]
        public void SetMemberValue_BoolFromString_DoesNotStaleCapture() {
            var first = new SampleForBoolCapture();
            ReflectionTool.SetValue(first, "Flag", "yes");
            Assert.True(first.Flag);

            // Second call with a DIFFERENT string must evaluate THAT string, not the first.
            var second = new SampleForBoolCapture();
            ReflectionTool.SetValue(second, "Flag", "no");
            Assert.False(second.Flag);
        }

        // ==== Sibling of #3: long -> ulong conversion must use the runtime argument v,
        // not the captured first-call value `l`.
        class SampleForUlongCapture {
            public ulong Value;
        }

        [Fact]
        public void SetMemberValue_LongToUlong_DoesNotStaleCapture() {
            var first = new SampleForUlongCapture();
            ReflectionTool.SetValue(first, "Value", 10L);
            Assert.Equal(10UL, first.Value);

            // Second call with a DIFFERENT long must convert THAT value, not the first.
            var second = new SampleForUlongCapture();
            ReflectionTool.SetValue(second, "Value", 99L);
            Assert.Equal(99UL, second.Value);
        }

        // ==== Fix #4: TypeAsDerivingFromGeneric must not NRE on null input.
        [Fact]
        public void TypeAsDerivingFromGeneric_NullInput_ReturnsNull_NotNre() {
            var result = ReflectionTool.TypeAsDerivingFromGeneric(null, typeof(List<>));
            Assert.Null(result);
        }

#pragma warning disable CS0169, CS0649 // unused fields, fine for reflection tests
        class SampleForMembers {
            public int PublicField;
            private int PrivateField;
            public static int StaticField;
            public int PublicProperty { get; set; }
            public int ReadOnlyProperty => 42;
            private int PrivateProperty { get; set; }
        }
#pragma warning restore CS0169, CS0649
    }
}
