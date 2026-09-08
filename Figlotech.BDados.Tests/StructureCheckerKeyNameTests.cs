using System.Globalization;
using System.Reflection;
using System.Text;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using Xunit;

namespace Figlotech.BDados.Tests {
    public class StructureCheckerKeyNameTests {
        [Theory]
        [InlineData(false, 62)]
        [InlineData(false, 63)]
        [InlineData(false, 64)]
        [InlineData(true, 62)]
        [InlineData(true, 63)]
        [InlineData(true, 64)]
        public void OnlyOversizedGeneratedNamesUseHash(bool isUnique, int naiveLength) {
            var prefix = isUnique ? "uk_" : "idx_";
            var link = new ScStructuralLink {
                Table = "T",
                Column = new string('x', naiveLength - prefix.Length - 2),
                IsUnique = isUnique
            };
            var naiveName = $"{prefix}t_{link.Column}";

            if (naiveLength <= 63) {
                Assert.Equal(naiveName, link.KeyName);
            } else {
                Assert.NotEqual(naiveName, link.KeyName);
                Assert.Matches($"^{prefix}[0-9a-f]{{56}}$", link.KeyName);
                Assert.True(Encoding.UTF8.GetByteCount(link.KeyName) <= 63);
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void UnicodeNamesRespectPostgresByteLimit(bool isUnique) {
            var link = new ScStructuralLink { Table = "T", Column = new string('é', 30), IsUnique = isUnique };

            Assert.Matches(isUnique ? "^uk_[0-9a-f]{56}$" : "^idx_[0-9a-f]{56}$", link.KeyName);
            Assert.True(Encoding.UTF8.GetByteCount(link.KeyName) <= 63);
        }

        [Fact]
        public void LongNamesAreStableAcrossInstancesAndCultures() {
            var table = "INVENTORY_" + new string('x', 60);
            var originalCulture = CultureInfo.CurrentCulture;
            try {
                CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("en-US");
                var name = new ScStructuralLink { Table = table, Column = "ITEM_ID,LOCATION_ID" }.KeyName;
                Assert.Equal("idx_c40133b852e448d1f2c873131fb25b773993e096dfd5c52c5ba5c363", name);
                CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("tr-TR");

                Assert.Equal(name, new ScStructuralLink { Table = table, Column = "ITEM_ID,LOCATION_ID" }.KeyName);
                Assert.Equal(name, new ScStructuralLink { Table = table.ToLowerInvariant(), Column = "item_id,location_id" }.KeyName);
            } finally {
                CultureInfo.CurrentCulture = originalCulture;
            }
        }

        [Fact]
        public void HashIncludesEntireTableAndColumnsWithoutLosingBoundaries() {
            var table = new string('t', 70);
            var configurations = new[] {
                (table, "a_b,c"), (table, "a,b_c"), (table, "a_b,d"),
                (table + "_a", "b,c"), (table + "x", "a_b,c")
            };
            var names = configurations.Select(c => new ScStructuralLink { Table = c.Item1, Column = c.Item2 }.KeyName);

            Assert.Equal(configurations.Length, names.Distinct().Count());
        }

        [Fact]
        public void CompositeAttributesUseBoundedStableNamesAndPreserveColumnOrder() {
            var checker = new StructureChecker(null!, new[] { typeof(CompositeIndexModel) });
            var method = typeof(StructureChecker).GetMethod("GetNecessaryLinks", BindingFlags.NonPublic | BindingFlags.Instance)!;
            var links = ((IEnumerable<ScStructuralLink>)method.Invoke(checker, null)!).ToList();
            var composites = links.Where(l => l.Column.Contains(',')).ToList();

            Assert.Equal(3, composites.Count);
            Assert.Equal(composites[0].KeyName, composites[1].KeyName);
            Assert.NotEqual(composites[0].Column, composites[1].Column);
            Assert.Matches("^idx_[0-9a-f]{56}$", composites[0].KeyName);
            Assert.Matches("^uk_[0-9a-f]{56}$", composites[2].KeyName);
            Assert.Contains(links, l => l.KeyName == "idx_compositeindexmodel_short");
            Assert.Contains(links, l => l.KeyName == "explicit_index");
        }

        public sealed class CompositeIndexModel : PlanDataObject<long> {
            [Field]
            [Index(nameof(SecondLongCompositeColumn), nameof(ThirdLongCompositeColumn))]
            [Index(nameof(ThirdLongCompositeColumn), nameof(SecondLongCompositeColumn))]
            [Index(nameof(SecondLongCompositeColumn), nameof(ThirdLongCompositeColumn), IsUnique = true)]
            public long FirstLongCompositeColumn { get; set; }

            [Field]
            public long SecondLongCompositeColumn { get; set; }

            [Field]
            public long ThirdLongCompositeColumn { get; set; }

            [Field]
            [Index]
            [Index(Name = "Explicit_Index")]
            public long Short { get; set; }
        }
    }
}
