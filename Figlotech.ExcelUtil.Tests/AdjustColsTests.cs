using System;
using System.Drawing;
using System.IO;
using OfficeOpenXml;
using Xunit;

namespace Figlotech.ExcelUtil.Tests {
    public class WindowsFactAttribute : FactAttribute {
        public WindowsFactAttribute() {
            if (!OperatingSystem.IsWindows()) {
                Skip = "GDI integration checks require Windows.";
            }
        }
    }

    public class AdjustColsTests {
        [Theory]
        // Stored widths from the reported workbook, compared with a 96-DPI refit.
        [InlineData(78.41949462890625, 39.567)]
        [InlineData(37.904075622558594, 19.309)]
        [InlineData(37.910213470458984, 19.312)]
        [InlineData(249.0544891357422, 124.884)]
        [InlineData(171.8990020751953, 86.307)]
        public void ReportedWidthsMatch96DpiRefit(double storedWidth, double expectedWidth) {
            var actual = ExcelColumnWidth.NormalizeDpi(storedWidth, 9.140625, 7, 192, false);
            Assert.InRange(actual, expectedWidth - 0.001, expectedWidth + 0.001);
        }

        [Fact]
        public void ShortColumnsKeepTheDefaultMinimum() {
            Assert.Equal(9.140625, ExcelColumnWidth.NormalizeDpi(9.853668212890625,
                9.140625, 7, 192, false));
        }

        [WindowsFact]
        public void NormalizedGdiMeasurementsMatch96DpiIncludingFilterPadding() {
            foreach (var style in new[] { FontStyle.Regular, FontStyle.Bold }) {
                using var font = new Font("Calibri", 11, style);
                var expected = (Measure("Código Fornecedor", font, 96) + 5) / 7d;
                foreach (var dpi in new[] { 96, 144, 192, 288 }) {
                    var rawWidth = (Measure("Código Fornecedor", font, dpi) + 5) / 7d;
                    var actual = ExcelColumnWidth.NormalizeDpi(rawWidth, 0, 7, dpi, false);
                    Assert.InRange(actual, expected - 0.005, expected + 0.005);
                    var filtered = ExcelColumnWidth.NormalizeDpi(rawWidth + 2.25, 0, 7, dpi, true);
                    Assert.InRange(filtered, expected + 2.245, expected + 2.255);
                }
            }
        }

        [WindowsFact]
        public void AdjustColsUsesOneBasedBoundsAndIsStableAcrossRepeatedCalls() {
            foreach (var startColumn in new[] { 1, 4, ExcelPackage.MaxColumns }) {
                using var input = new MemoryStream();
                using (var source = new ExcelPackage()) {
                    var sheet = source.Workbook.Worksheets.Add("Data");
                    sheet.Cells[3, startColumn].Value = "A sufficiently long supplier identifier";
                    source.SaveAs(input);
                }
                input.Position = 0;
                using var excelator = new Excelator(input);
                excelator.AdjustCols();
                var firstWidth = ReadWidth(excelator, startColumn);
                Assert.InRange(firstWidth, 25, 45);
                excelator.AdjustCols();
                Assert.Equal(firstWidth, ReadWidth(excelator, startColumn), 6);
            }
        }

        [Fact]
        public void EmptyWorksheetIsANoOpAndMissingWorksheetReportsAnError() {
            using var excelator = new Excelator();
            Assert.Throws<InvalidOperationException>(() => excelator.AdjustCols());
            excelator.NewWorkSheet("Empty");
            excelator.AdjustCols();
        }

        private static float Measure(string text, Font font, float dpi) {
            using var bitmap = new Bitmap(1, 1);
            bitmap.SetResolution(dpi, dpi);
            using var graphics = Graphics.FromImage(bitmap);
            graphics.PageUnit = GraphicsUnit.Pixel;
            return graphics.MeasureString(text, font, 10000, StringFormat.GenericDefault).Width;
        }

        private static double ReadWidth(Excelator excelator, int column) {
            using var output = new MemoryStream();
            excelator.Save(output);
            output.Position = 0;
            using var package = new ExcelPackage(output);
            var sheet = package.Workbook.Worksheets[0];
            Assert.Equal(column, sheet.Dimension.Start.Column);
            Assert.Equal(column, sheet.Dimension.End.Column);
            return sheet.Column(column).Width;
        }
    }
}
