using Figlotech.Core.FileAcessAbstractions;
using OfficeOpenXml;
using OfficeOpenXml.Style;
using OfficeOpenXml.Style.Dxf;
using System;
using System.Drawing;
using System.IO;
using System.Threading.Tasks;

namespace Figlotech.ExcelUtil
{
    public class ExcelatorLineReader {
        int line;
        Excelator excelator;

        public int LineNumber => line;

        public bool IsLast => line == excelator.NumRows;

        public ExcelatorLineReader(int ln, Excelator parent) {
            line = ln;
            excelator = parent;
        }
        public object Get(int column) {
            return excelator.Get(line, column);
        }
        public T Get<T>(int column) {
            return excelator.Get<T>(line, column);
        }
    }
    public class ExcelatorLineWriter {
        int line;
        Excelator excelator;
        public ExcelatorLineWriter(int ln, Excelator parent) {
            line = ln;
            excelator = parent;
        }
        public void Set(int column, object value, Action<ExcelStyle> es = null) {
            excelator.Set(line, column, column, value);
            if (es != null) {
                Style(column, column, es);
            }
        }

        public void Set(int columnFrom, int columnTo, object value, Action<ExcelStyle> es = null) {
            excelator.Set(line, columnFrom, columnTo, value);
            if (es != null) {
                Style(columnFrom, columnTo, es);
            }
        }

        public void Style(int col, Action<ExcelStyle> es) {
            Style(col, col, es);
        }
        public void Style(int colf, int colt, Action<ExcelStyle> es) {
            excelator.Style(line, line, colf, colt, es);
        }
        public void SetFormat(int column, string value) {
            excelator.SetFormat(line, column, value);
        }

        public void ConditionalFormat(int col, String statement, Action<ExcelDxfStyleConditionalFormatting> fn) {
            ConditionalFormat(col, col, statement, fn);
        }
        public void ConditionalFormat(int fc, int tc, String statement, Action<ExcelDxfStyleConditionalFormatting> fn) {
            excelator.ConditionalFormat(line, line, fc, tc, statement, fn);
        }

        public void EndLine() {
            line++;
        }
    }

    public class Excelator {
        ExcelPackage pack;
        ExcelWorksheet ws;
        String filePath;
        Stream _stream;
        

        public Excelator(Stream stream, string name = "Untitled") {
            _stream = stream;
            pack = new ExcelPackage();
            if (stream.Length > 0) {
                pack.Load(stream);
                ws = pack.Workbook.Worksheets[1];
            } else {
                pack.Workbook.Worksheets.Add(name);
                ws = pack.Workbook.Worksheets[1];
            }
        }

        public void AdjustCols() {
            for (int c = 0; c < ws.Dimension.Columns + 1; c++) {
                ws.Column(c + 1).AutoFit();
            }
        }

        public Excelator(String path, string name = "Untitled") {
            filePath = path;
            pack = new ExcelPackage();
            if (File.Exists(path)) {
                pack.Load(new FileStream(path, FileMode.Open));
                ws = pack.Workbook.Worksheets[1];
            } else {
                pack.Workbook.Worksheets.Add(name);
                ws = pack.Workbook.Worksheets[1];
            }
        }

        public void Style(int fr, int tr, int fc, int tc, Action<ExcelStyle> fn) {
            fn(ws.Cells[fr, fc, tr, tc].Style);
        }
        public void ConditionalFormat(int fr, int tr, int fc, int tc, String statement, Action<ExcelDxfStyleConditionalFormatting> fn) {
            var formatRangeAddress = new ExcelAddress(fr, fc, tr, tc);
            var _cond4 = ws.ConditionalFormatting.AddExpression(formatRangeAddress);
            fn(_cond4.Style);
            statement = statement.Replace("$???", formatRangeAddress.Address);
            _cond4.Formula = statement;
        }

        public T Get<T>(int line, int column) {
            var o = Get(line, column);
            try {
                if(o == null && o is T obj) {
                    return obj;
                }
                if(typeof(T).IsAssignableFrom(o.GetType())) {
                    return (T)o;
                }
            } catch(Exception) {

            }
            return default(T);
        }
        public object Get(int line, int column) {
            if(line > NumRows + 1 || column > NumCols +1) {
                return null;
            }
            return ws.Cells[line, column].Value;
        }
        public void SetFormat(int line, int column, String value) {
            if ((ws.Dimension?.Rows ?? 0) < line - 1) {
                ws.InsertRow(line, 4);
            }
            if ((ws.Dimension?.Columns ?? 0) < column - 1) {
                ws.InsertColumn(column, 4);
            }
            try {
                ws.Cells[line, column].Style.Numberformat.Format = value;
            } catch (Exception x) {
                //Console.WriteLine(x.Message);
            }
        }
        public void Set(int line, int columnFrom, int columnTo, object value) {
            if ((ws.Dimension?.Rows ?? 0) < line - 1) {
                ws.InsertRow(line, 4);
            }
            if ((ws.Dimension?.Columns ?? 0) < columnTo - 1) {
                ws.InsertColumn(columnTo, 4);
            }
            try {
                ws.Cells[line, columnFrom, line, columnTo].Value = value;
                if (columnFrom != columnTo) {
                    ws.Cells[line, columnFrom, line, columnTo].Merge = true;
                }
            } catch (Exception x) {
                //Console.WriteLine(x.Message);
            }
        }

        public void Save(Stream stream) {
            pack.SaveAs(stream);
        }
        public void Save() {
            pack.SaveAs(_stream);
        }

        public int NumRows {
            get {
                return ws?.Dimension?.Rows ?? 0;
            }
        }
        public int NumCols {
            get {
                return ws?.Dimension?.Columns ?? 0;
            }
        }

        public void Write(Action<ExcelatorLineWriter> lineFun, int skipLines = 0) {
            lineFun(new ExcelatorLineWriter(1 + skipLines, this));
        }

        public async Task WriteAsync(Func<ExcelatorLineWriter, Task> lineFun, int skipLines = 0) {
            await lineFun(new ExcelatorLineWriter(1 + skipLines, this));
        }

        public void ReadAll(Action<ExcelatorLineReader> lineFun, int skipLines = 0, Action<Exception> handler = null) {
            for (var i = 1 + skipLines; i < ws.Dimension.Rows + 1; i++) {
                try {
                    lineFun(new ExcelatorLineReader(i, this));
                } catch (Exception x) {
                    handler?.Invoke(x);
                }
            }
        }
    }

}
