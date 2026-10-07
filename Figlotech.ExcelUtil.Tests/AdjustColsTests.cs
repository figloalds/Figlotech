using System;
using System.Drawing;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Xml.Linq;
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

        [Theory]
        [InlineData(0f)]
        [InlineData(-96f)]
        [InlineData(float.NaN)]
        [InlineData(float.PositiveInfinity)]
        public void InvalidDpiPreservesTheUnscaledWidth(float dpi) {
            Assert.Equal(40, ExcelColumnWidth.NormalizeDpi(40, 9.140625, 7, dpi, false));
        }

        [Theory]
        [InlineData(0d)]
        [InlineData(-7d)]
        [InlineData(double.NaN)]
        [InlineData(double.PositiveInfinity)]
        public void InvalidFontMetricsPreserveTheUnscaledWidth(double fontWidth) {
            Assert.Equal(40, ExcelColumnWidth.NormalizeDpi(40, 9.140625, fontWidth, 192, false));
        }

        [Theory]
        [InlineData(double.NaN, 9.140625, 96f)]
        [InlineData(double.PositiveInfinity, 9.140625, 192f)]
        [InlineData(0d, 0d, 96f)]
        [InlineData(-10d, double.NaN, 192f)]
        [InlineData(double.NaN, double.PositiveInfinity, 96f)]
        [InlineData(40d, 9.140625, float.Epsilon)]
        public void InvalidOrExtremeWidthsAlwaysProduceFiniteVisibleColumns(
            double width, double minimumWidth, float dpi) {
            var actual = ExcelColumnWidth.NormalizeDpi(width, minimumWidth, 7, dpi, false);
            Assert.True(double.IsFinite(actual));
            Assert.InRange(actual, 1, 255);
        }

        [Fact]
        public void ZeroDpiCannotSerializeInvalidWidthsAcrossUsedColumns() {
            using var package = new ExcelPackage();
            var sheet = package.Workbook.Worksheets.Add("Data");
            for (int column = 1; column <= 34; column++) {
                sheet.Cells[1, column].Value = "Visible data";
                sheet.Column(column).Width = ExcelColumnWidth.NormalizeDpi(40, 9.140625, 7, 0, false);
            }
            using var output = new MemoryStream();
            package.SaveAs(output);
            output.Position = 0;
            using var zip = new ZipArchive(output, ZipArchiveMode.Read);
            using var xmlStream = zip.GetEntry("xl/worksheets/sheet1.xml").Open();
            var xml = XDocument.Load(xmlStream);
            XNamespace ns = "http://schemas.openxmlformats.org/spreadsheetml/2006/main";
            foreach (var column in xml.Root.Element(ns + "cols").Elements(ns + "col")) {
                var width = double.Parse(column.Attribute("width").Value, CultureInfo.InvariantCulture);
                Assert.Equal(40, width);
            }
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
