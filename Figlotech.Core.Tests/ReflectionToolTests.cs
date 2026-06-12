using System;
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
    }
}
