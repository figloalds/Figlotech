using Figlotech.Core.Extensions;
using Xunit;
using System.Collections.Generic;

namespace Figlotech.Core.Tests {
    public class IEnumerableExtensionsTests {
        [Fact]
        public void ToDictionaryIgnoreDuplicates_LastKeyWinsOnDuplicate() {
            var items = new List<(int Key, string Val)> {
                (1, "first"), (2, "a"), (1, "second"), (3, "b"), (2, "c")
            };
            var dict = items.ToDictionaryIgnoreDuplicates(x => x.Key, x => x.Val);
            Assert.Equal("second", dict[1]);
            Assert.Equal("c", dict[2]);
            Assert.Equal("b", dict[3]);
            Assert.Equal(3, dict.Count);
        }

        [Fact]
        public void ToDictionaryIgnoreDuplicates_EmptySource_ReturnsEmpty() {
            var dict = new List<(int, string)>()
                .ToDictionaryIgnoreDuplicates(x => x.Item1, x => x.Item2);
            Assert.Empty(dict);
        }
    }
}
