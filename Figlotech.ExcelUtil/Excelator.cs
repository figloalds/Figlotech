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
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.ExcelUtil {
    public sealed class ReadingStoppedException : Exception {
        
    }
    public sealed class ExcelatorLineReader {
        int line;
        internal bool stopReadingIssued;
        Excelator excelator;

        public int LineNumber => line;

        internal int CurrentRow {
            get => line;
            set => line = value;
        }

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

        public void SkipRemainder() {
            stopReadingIssued = true;
        }

        public void StopReading() {
            stopReadingIssued = true;
            throw new ReadingStoppedException();
        }
    }
    
    public sealed class ExcelatorRowRangeWriter {
        int row;
        int ColFrom;
        int ColTo;

        Excelator Excelator;
        ExcelatorRowWriter rowWriter;

        public int LineNumber => row;
        public int ColStart => ColFrom;
        public int ColEnd => ColTo;
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
            var cf = IntEx.Int64ToString(columnFrom-1, IntEx.Base26);
            var ct = IntEx.Int64ToString(columnTo-1, IntEx.Base26);
            Excelator.SetFormula(row, ColFrom, ColTo, $"SUM({cf}{row}:{ct}{row})");
            return this;
        }

        public ExcelatorRowRangeWriter ColSum(int rowFrom, int rowTo) {
            //var cf = new IntEx(ColFrom - 1).ToString(IntEx.Base26);
            //var ct = new IntEx(ColTo - 1).ToString(IntEx.Base26);
            Excelator.SetFormulaR1C1(row, ColFrom, ColTo, $"SUM(R{rowFrom}C{ColFrom}:R{rowTo}C{ColTo})");
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
        public ExcelatorRowRangeWriter FormulaR1C1(string formula) {
            Excelator.SetFormulaR1C1(row, ColFrom, ColTo, formula);
            return this;
        }
        public ExcelatorRowRangeWriter Style(Action<ExcelStyle> es) {
            Excelator.Style(row, row, ColFrom, ColTo, es);
            return this;
        }
        public ExcelatorRowRangeWriter SolidFill(Color c) {
            Excelator.Style(row, row, ColFrom, ColTo, es=> {
                es.Fill.PatternType = ExcelFillStyle.Solid;
                es.Fill.BackgroundColor.SetColor(c);
            });
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

    public sealed class ExcelatorRowWriter {
        internal int Row;
        internal Excelator excelator;
        internal int offsetCols = 0;
        public ExcelatorRowWriter(int ln, Excelator parent) {
            Row = ln;
            excelator = parent;
        }

        public int LineNumber => Row;

        public ExcelatorRowRangeWriter this[int i] {
            get {
                return this.Cell(i);
            }
        }
        public ExcelatorRowRangeWriter this[int i, int j] {
            get {
                return this.Range(i, j);
            }
        }

        public ExcelatorRowWriter SetFormula(int column, string formula) {
            excelator.SetFormula(Row, column + offsetCols, column + offsetCols, formula);
            return this;
        }
        public ExcelatorRowWriter SetFormulaR1C1(int column, string formula) {
            excelator.SetFormulaR1C1(Row, column + offsetCols, column + offsetCols, formula);
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
        
        public ExcelatorRowWriter IterateThrough<T>(IEnumerable<T> values, Action<T, int> fn, bool autoNextRow = true) {
            int i = 0;
            foreach(var a in values) {
                fn(a, i++);
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

        public ExcelatorRowRangeWriter PutValues(int startingColumn, params object[] values) {
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
        public ExcelatorRowWriter PreviousRow() {
            Row--;
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

    public sealed class Excelator {
        ExcelPackage _pack { get; set; }
        ExcelWorksheet _currentWorkSheet { get; set; }
        String _filePath { get; set; }
        Stream _stream { get; set; }
        List<ExcelWorksheet> _workSheets { get; set; } = new List<ExcelWorksheet>();

        public Excelator(Stream stream, string name = "Untitled") {
            _stream = stream;
            _pack = new ExcelPackage();
            if (stream.Length > 0) {
                _pack.Load(stream);
                var en = _pack.Workbook.Worksheets.GetEnumerator();
                en.MoveNext();
                _currentWorkSheet = en.Current;
            } else {
                _pack.Workbook.Worksheets.Add(name);
                var en = _pack.Workbook.Worksheets.GetEnumerator();
                en.MoveNext();
                _currentWorkSheet = en.Current;
            }
            _workSheets.Add(_currentWorkSheet);
        }

        public Excelator() {
            _pack = new ExcelPackage();
        }

        public int From0Idx(int idx) {
            var add = _pack.Compatibility.IsWorksheets1Based ? 1 : 0;
            return idx + add;
        }

        public void NewWorkSheet(string name) {
            _pack.Workbook.Worksheets.Add(name);
            var sub = _pack.Compatibility.IsWorksheets1Based ? 0 : 1;
            var en = _pack.Workbook.Worksheets[_pack.Workbook.Worksheets.Count - sub];
            _currentWorkSheet = en;
            _workSheets.Add(_currentWorkSheet);
        }
        public void SetWorkSheet(int idx) {
            var en = _pack.Workbook.Worksheets[idx];
            _currentWorkSheet = en;
        }

        public void AdjustCols() {
            try {
                for (int c = 0; c < _currentWorkSheet.Dimension.Columns + 1; c++) {
                    _currentWorkSheet.Column(c + 1).AutoFit();
                }
            } catch(Exception x) {
                Fi.Tech.WriteLine("EXCELATOR", $"AutoFit threw an exception {x.Message}");
            }
        }

        public Excelator(String path, string name = "Untitled") {
            _filePath = path;
            _pack = new ExcelPackage();
            if (File.Exists(path)) {
                _pack.Load(new FileStream(path, FileMode.Open));
                var en = _pack.Workbook.Worksheets.GetEnumerator();
                en.MoveNext();
                _currentWorkSheet = en.Current;
            } else {
                _pack.Workbook.Worksheets.Add(name);
                var en = _pack.Workbook.Worksheets.GetEnumerator();
                en.MoveNext();
                _currentWorkSheet = en.Current;
            }
        }

        public ExcelatorRowWriter this[int i] {
            get {
                return this.Row(i);
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
                fn(_currentWorkSheet.Cells[fr, fc, tr, tc].Style);
            } catch(Exception x) {
                if(Debugger.IsAttached) {
                    Debugger.Break();
                }
                throw x;
            }
        }

        public void ConditionalFormat(int fr, int tr, int fc, int tc, String statement, Action<ExcelDxfStyleConditionalFormatting> fn) {
            var formatRangeAddress = new ExcelAddress(fr, fc, tr, tc);
            var _cond4 = _currentWorkSheet.ConditionalFormatting.AddExpression(formatRangeAddress);
            fn(_cond4.Style);
            statement = statement.Replace("$???", formatRangeAddress.Address);
            _cond4.Formula = statement;
        }

        public T Get<T>(int line, int column) {
            try {
                var o = Get(line, column);
                if(o == null) {
                    return default(T);
                }
                if(o != null && o is T obj) {
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
            return _currentWorkSheet.Cells[line, column].Value;
        }
        public void SetFormat(int line, int column, String value) {
            EnsureColumnRange(column, column);
            EnsureRowRange(line, line);
            try {
                _currentWorkSheet.Cells[line, column].Style.Numberformat.Format = value;

            } catch (Exception x) {

                //Console.WriteLine(x.Message);
            }
        }
        public void Set(int line, int columnFrom, int columnTo, object value) {
            EnsureColumnRange(columnFrom, columnTo);
            EnsureRowRange(line, line);
            try {
                _currentWorkSheet.Cells[line, columnFrom, line, columnTo].Value = value;
                if (columnFrom != columnTo) {
                    _currentWorkSheet.Cells[line, columnFrom, line, columnTo].Merge = true;
                }
            }

            catch (Exception x) {

                //Console.WriteLine(x.Message);
            }
        }

        public void EnsureColumnRange(int columnFrom, int columnTo) {
            var wdc = (_currentWorkSheet.Dimension?.Columns ?? 0);
            if (wdc < columnFrom - 1) {
                _currentWorkSheet.InsertColumn(columnFrom, columnFrom - wdc);
            }
            if(columnTo > columnFrom) {
                if (wdc < columnTo - 1) {
                    _currentWorkSheet.InsertColumn(columnTo, columnTo - wdc);
                }
            }
        }
        public void EnsureRowRange(int rowFrom, int rowTo) {
            var wdr = (_currentWorkSheet.Dimension?.Rows ?? 0);
            if (wdr < rowFrom - 1) {
                _currentWorkSheet.InsertRow(rowFrom, rowFrom - wdr);
            }
            if(rowTo > rowFrom) {
                if (wdr < rowTo - 1) {
                    _currentWorkSheet.InsertRow(rowTo, rowTo - wdr);
                }
            }
        }

        public void SetFormula(int line, int columnFrom, int columnTo, string value) {
            EnsureRowRange(line, line);
            EnsureColumnRange(columnFrom, columnTo);
            try {
                _currentWorkSheet.Cells[line, columnFrom, line, columnTo].Formula = value;
                if (columnFrom != columnTo) {
                    _currentWorkSheet.Cells[line, columnFrom, line, columnTo].Merge = true;
                }
            } catch (Exception x) {

                //Console.WriteLine(x.Message);
            }
        }
        public void SetFormulaR1C1(int line, int columnFrom, int columnTo, string value) {
            EnsureRowRange(line, line);
            EnsureColumnRange(columnFrom, columnTo);
            try {
                _currentWorkSheet.Cells[line, columnFrom, line, columnTo].FormulaR1C1 = value;
                if (columnFrom != columnTo) {
                    _currentWorkSheet.Cells[line, columnFrom, line, columnTo].Merge = true;
                }
            } catch (Exception x) {

                //Console.WriteLine(x.Message);
            }
        }

        public void Save(Stream stream) {
            _pack.SaveAs(stream);
        }
        public void Save() {
            _pack.SaveAs(_stream);
        }

        public int NumRows {
            get {
                return _currentWorkSheet?.Dimension?.Rows ?? 0;
            }
        }
        public int NumCols {
            get {
                return _currentWorkSheet?.Dimension?.Columns ?? 0;
            }
        }

        public void Write(Action<ExcelatorRowWriter> lineFun, int skipLines = 0) {
            lineFun(new ExcelatorRowWriter(1 + skipLines, this));
        }
        public async Task Write(Func<ExcelatorRowWriter, Task> lineFun, int skipLines = 0) {
            await lineFun(new ExcelatorRowWriter(1 + skipLines, this)).ConfigureAwait(false);
        }

        public ExcelatorRowWriter Row(int r) {
            return new ExcelatorRowWriter(r, this);
        }
        public ExcelatorRowRangeWriter Cell(int r, int c) {
            return new ExcelatorRowWriter(r, this).Cell(c);
        }

        public async Task WriteAsync(Func<ExcelatorRowWriter, Task> lineFun, int skipLines = 0) {
            await lineFun(new ExcelatorRowWriter(1 + skipLines, this)).ConfigureAwait(false);
        }

        public async IAsyncEnumerable<ExcelatorLineReader> ReadAllLines(int skipLines = 0) {
            ExcelatorLineReader xcl = new ExcelatorLineReader(0, this);
            for (var i = 1 + skipLines; i < _currentWorkSheet.Dimension.Rows + 1; i++) {
                xcl.CurrentRow = i;
                yield return xcl;
                if (xcl.stopReadingIssued) {
                    break;
                }
            }
        }

        public async Task ReadAllAsync(CancellationToken cancellation, Func<CancellationToken, ExcelatorLineReader, Task> lineFun, int skipLines = 0, Func<Exception, Task> handler = null) {
            ExcelatorLineReader xcl = new ExcelatorLineReader(0, this);
            for (var i = 1 + skipLines; i < _currentWorkSheet.Dimension.Rows + 1; i++) {
                try {
                    xcl.CurrentRow = i;
                    using var tcsFork = CancellationTokenSource.CreateLinkedTokenSource(cancellation);
                    await lineFun(tcsFork.Token, xcl).ConfigureAwait(false);
                    if(xcl.stopReadingIssued || cancellation.IsCancellationRequested) {
                        break;
                    }
                } catch (Exception x) {
                    if(x is ReadingStoppedException) {
                        break;
                    }
                    await handler(x).ConfigureAwait(false);
                }
            }
        }
        public async Task ReadAllAsync(Func<ExcelatorLineReader, Task> lineFun, int skipLines = 0, Func<Exception, Task> handler = null) {
            await ReadAllAsync(CancellationToken.None, async(cancellation, xcl) => {
                await lineFun(xcl).ConfigureAwait(false);
            }, skipLines, handler).ConfigureAwait(false);
        }
    }

}
