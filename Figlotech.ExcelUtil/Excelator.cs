using Figlotech.Core;
using Figlotech.Core.FileAcessAbstractions;
using OfficeOpenXml;
using OfficeOpenXml.Style;
using OfficeOpenXml.Style.Dxf;
using System;
using System.Collections.Generic;
using System.Diagnostics;
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
    
    public class ExcelatorRowRangeWriter {
        int row;
        int ColFrom;
        int ColTo;

        Excelator Excelator;
        ExcelatorRowWriter rowWriter;

        //public ExcelatorRowWriter Row => rowWriter ?? new ExcelatorRowWriter(row, Excelator);

        /// <summary>
        /// Short for "Back to Row"
        /// </summary>
        /// <returns>The ExcelatorRowWriter which created this RangeWriter</returns>
        public ExcelatorRowWriter ToRow() {
            return rowWriter;
        }

        public ExcelatorRowRangeWriter(Excelator excelator, int row, int cf, int ct) {
            this.Excelator = excelator;
            this.row = row;
            this.ColFrom = cf;
            this.ColFrom = ct;
        }
        internal ExcelatorRowRangeWriter(ExcelatorRowWriter lw, int cf, int ct) {
            this.Excelator = lw.excelator;
            this.row = lw.Row;
            this.ColFrom = cf;
            this.ColTo = ct;
            rowWriter = lw;
        }

        public ExcelatorRowRangeWriter RowSum(int columnFrom, int columnTo) {
            var cf = new IntEx(columnFrom-1).ToString(IntEx.Base26);
            var ct = new IntEx(columnTo-1).ToString(IntEx.Base26);
            Excelator.SetFormula(row, ColFrom, ColTo, $"SUM({cf}{row}:{ct}{row})");
            return this;
        }
        public ExcelatorRowRangeWriter ColSum(int rowFrom, int rowTo) {
            var cf = new IntEx(ColFrom - 1).ToString(IntEx.Base26);
            var ct = new IntEx(ColTo - 1).ToString(IntEx.Base26);
            Excelator.SetFormula(row, ColFrom, ColTo, $"SUM({cf}{rowFrom}:{ct}{rowTo})");
            return this;
        }
        public ExcelatorRowRangeWriter Value(object value) {
            Excelator.Set(row, ColFrom, ColTo, value);
            //ColTo = ColFrom;
            return this;
        }
        public ExcelatorRowRangeWriter Formula(string formula) {
            Excelator.SetFormula(row, ColFrom, ColTo, formula);
            return this;
        }
        public ExcelatorRowRangeWriter Style(Action<ExcelStyle> es) {
            Excelator.Style(row, row, ColFrom, ColTo, es);
            return this;
        }
        public ExcelatorRowRangeWriter Bold() {
            Excelator.Style(row, row, ColFrom, ColTo, es => es.Font.Bold = true);
            return this;
        }
        public ExcelatorRowRangeWriter CenterH() {
            Excelator.Style(row, row, ColFrom, ColTo, es => es.HorizontalAlignment = ExcelHorizontalAlignment.Center);
            return this;
        }

    }

    public class ExcelatorRowWriter {
        internal int Row;
        internal Excelator excelator;
        internal int offsetCols = 0;
        public ExcelatorRowWriter(int ln, Excelator parent) {
            Row = ln;
            excelator = parent;
        }

        public ExcelatorRowWriter SetFormula(int column, string formula) {
            excelator.SetFormula(Row, column + offsetCols, column + offsetCols, formula);
            return this;
        }
        public ExcelatorRowWriter Set(int column, object value, Action<ExcelStyle> es = null) {
            excelator.Set(Row, column + offsetCols, column + offsetCols, value);
            if (es != null) {
                Style(column, column, es);
            }
            return this;
        }

        public ExcelatorRowWriter Set(int columnFrom, int columnTo, object value, Action<ExcelStyle> es = null) {
            excelator.Set(Row, columnFrom + offsetCols, columnTo + offsetCols, value);
            if (es != null) {
                Style(columnFrom, columnTo, es);
            }
            return this;
        }
        
        public ExcelatorRowWriter IterateThrough<T>(IEnumerable<T> values, Action<T> fn, bool autoNextRow = true) {
            foreach(var a in values) {
                fn(a);
                if (autoNextRow) {
                    NextRow();
                }
            }

            return this;
        }

        public ExcelatorRowWriter PutValues<T>(int startingColumn, T[,] values, Action<int> ctrlFn) {
            for (int l = 0; l < values.GetLength(0); l++) {
                for (int c = 0; c < values.GetLength(1); c++) {
                    Set(c + startingColumn, values[l, c]);
                }
                ctrlFn?.Invoke(l);
                NextRow();
            }
            return this;
        }

        public ExcelatorRowWriter OffsetCols(int offset) {
            this.offsetCols = offset;
            return this;
        }

        public ExcelatorRowRangeWriter PutValues(int startingColumn, IEnumerable<object> values) {
            int c = startingColumn;
            foreach (var value in values) {
                Set(c++, value);
            }
            return Range(startingColumn, c);
        }

        public ExcelatorRowRangeWriter Cell(int column) {
            return new ExcelatorRowRangeWriter(this, column + offsetCols, column + offsetCols);
        }
        public ExcelatorRowRangeWriter ForCellsInRange(int start, int end, Action<ExcelatorRowRangeWriter> cellAction) {
            for(int i = start; i <= end; i++) {
                cellAction(new ExcelatorRowRangeWriter(this, i + offsetCols, i + offsetCols));
            }
            return new ExcelatorRowRangeWriter(this, start + offsetCols, end + offsetCols);
        }
        public ExcelatorRowRangeWriter Range(int columnFrom, int columnTo) {
            return new ExcelatorRowRangeWriter(this, columnFrom + offsetCols, columnTo + offsetCols);
        }
         
        public ExcelatorRowWriter Style(int column, Action<ExcelStyle> es) {
            Style(column, column, es);
            return this;
        }
        public ExcelatorRowWriter Style(int colf, int colt, Action<ExcelStyle> es) {
            excelator.Style(Row, Row, colf + offsetCols, colt + offsetCols, es);
            return this;
        }
        public ExcelatorRowWriter SetFormat(int column, string value) {
            excelator.SetFormat(Row, column + offsetCols, value);
            return this;
        }

        public ExcelatorRowWriter ConditionalFormat(int col, String statement, Action<ExcelDxfStyleConditionalFormatting> fn) {
            ConditionalFormat(col, col, statement, fn);
            return this;
        }
        public ExcelatorRowWriter ConditionalFormat(int fc, int tc, String statement, Action<ExcelDxfStyleConditionalFormatting> fn) {
            excelator.ConditionalFormat(Row, Row, fc + offsetCols, tc + offsetCols, statement, fn);
            return this;
        }

        public ExcelatorRowWriter NextRow() {
            Row++;
            return this;
        }

        public ExcelatorRowWriter Rewind() {
            Row = 1;
            return this;
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
                var en = pack.Workbook.Worksheets.GetEnumerator();
                en.MoveNext();
                ws = en.Current;
            } else {
                pack.Workbook.Worksheets.Add(name);
                var en = pack.Workbook.Worksheets.GetEnumerator();
                en.MoveNext();
                ws = en.Current;
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
                var en = pack.Workbook.Worksheets.GetEnumerator();
                en.MoveNext();
                ws = en.Current;
            } else {
                pack.Workbook.Worksheets.Add(name);
                var en = pack.Workbook.Worksheets.GetEnumerator();
                en.MoveNext();
                ws = en.Current;
            }
        }

        public void FillMatrix(int column, int row, object[,] values) {
            for (int x = 0; x < values.GetLength(1); x++) {
                for (int y = 0; y < values.GetLength(0); y++) {
                    Set(row + y, column + x, column + x, values[x, y]);
                }
            }
        }

        public void Style(int fr, int tr, int fc, int tc, Action<ExcelStyle> fn) {
            EnsureColumnRange(fc, tc);
            EnsureRowRange(fr, tr);
            try {
                fn(ws.Cells[fr, fc, tr, tc].Style);
            } catch(Exception x) {
                if(Debugger.IsAttached) {
                    Debugger.Break();
                }
                throw x;
            }
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
                if(Nullable.GetUnderlyingType(typeof(T)) != null) {
                    return (T)Convert.ChangeType(o, Nullable.GetUnderlyingType(typeof(T)));
                }
                return (T) Convert.ChangeType(o, typeof(T));
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
            EnsureColumnRange(line, line);
            EnsureRowRange(column, column);
            try {
                ws.Cells[line, column].Style.Numberformat.Format = value;
            } catch (Exception x) {
                //Console.WriteLine(x.Message);
            }
        }
        public void Set(int line, int columnFrom, int columnTo, object value) {
            EnsureColumnRange(line, line);
            EnsureRowRange(columnFrom, columnTo);
            try {
                ws.Cells[line, columnFrom, line, columnTo].Value = value;
                if (columnFrom != columnTo) {
                    ws.Cells[line, columnFrom, line, columnTo].Merge = true;
                }
            }
            catch (Exception x) {
                //Console.WriteLine(x.Message);
            }
        }

        public void EnsureColumnRange(int columnFrom, int columnTo) {
            var wdc = (ws.Dimension?.Columns ?? 0);
            if (wdc < columnFrom - 1) {
                ws.InsertColumn(columnFrom, columnFrom - wdc);
            }
            if(columnTo > columnFrom) {
                if (wdc < columnTo - 1) {
                    ws.InsertColumn(columnTo, columnTo - wdc);
                }
            }
        }
        public void EnsureRowRange(int rowFrom, int rowTo) {
            var wdr = (ws.Dimension?.Rows ?? 0);
            if (wdr < rowFrom - 1) {
                ws.InsertRow(rowFrom, rowFrom - wdr);
            }
            if(rowTo > rowFrom) {
                if (wdr < rowTo - 1) {
                    ws.InsertRow(rowTo, rowTo - wdr);
                }
            }
        }

        public void SetFormula(int line, int columnFrom, int columnTo, string value) {
            EnsureRowRange(line, line);
            EnsureColumnRange(columnFrom, columnTo);
            try {
                ws.Cells[line, columnFrom, line, columnTo].Formula = value;
                if (columnFrom != columnTo) {
                    ws.Cells[line, columnFrom, line, columnTo].Merge = true;
                }
            }
            catch (Exception x) {
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

        public void Write(Action<ExcelatorRowWriter> lineFun, int skipLines = 0) {
            lineFun(new ExcelatorRowWriter(1 + skipLines, this));
        }

        public ExcelatorRowWriter Row(int r) {
            return new ExcelatorRowWriter(r, this);
        }
        public ExcelatorRowRangeWriter Cell(int r, int c) {
            return new ExcelatorRowWriter(r, this).Cell(c);
        }

        public async Task WriteAsync(Func<ExcelatorRowWriter, Task> lineFun, int skipLines = 0) {
            await lineFun(new ExcelatorRowWriter(1 + skipLines, this));
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
