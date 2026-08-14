using System.Net.Http;
using System.Reflection;
using Figlotech.Core;
using Xunit;

namespace Figlotech.Core.Tests {
    public sealed class FiHttpResultHeadersTests {
        [Fact]
        public void Headers_RetrievesValueUsingCaseInsensitiveKey() {
            var result = CreateFiHttpResult();

            result.ResponseHeaders["adminsecuritykey"] = "expected-value";

            Assert.Equal("expected-value", result.ResponseHeaders["AdminSecurityKey"]);
        }

        private static FiHttpResult CreateFiHttpResult() {
            var constructor = typeof(FiHttpResult).GetConstructor(
                BindingFlags.NonPublic | BindingFlags.Instance,
                null,
                new[] { typeof(FiHttp), typeof(HttpRequestMessage) },
                null);

            Assert.NotNull(constructor);

            return (FiHttpResult)constructor!.Invoke(new object?[] { null, null });
        }
    }
}
