using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;

namespace Figlotech.Core {
    public interface IExpressionFilter {
        object Transform(object input);
    }

    public interface IDataExpressionFormatter {
        string Format(object input, string expression);
    }

    public interface IDataFormatter {
        string Name { get; }
        object Apply(object value, string[] args, CultureInfo culture);
    }

    public sealed class NumberFormatter : IDataFormatter {
        public string Name => "Number";
        public object Apply(object value, string[] args, CultureInfo culture) {
            if (!TryToDecimal(value, culture, out var dec)) return string.Empty;
            // args: minDec, maxDec
            int min = 0, max = 2;
            if (args.Length > 0) int.TryParse(args[0], out min);
            if (args.Length > 1) int.TryParse(args[1], out max);
            if (min < 0) min = 0;
            if (max < min) max = min;

            // Build a numeric custom format like "0.00###" (min = 2, max = 5)
            var sb = new StringBuilder("0");
            if (max > 0) {
                sb.Append('.');
                sb.Append(new string('0', min));
                sb.Append(new string('#', Math.Max(0, max - min)));
            }
            return dec.ToString(sb.ToString(), culture);
        }

        private static bool TryToDecimal(object value, CultureInfo culture, out decimal dec) {
            switch (value) {
                case null:
                    dec = 0; return false;
                case decimal d:
                    dec = d; return true;
                case double dd:
                    dec = (decimal)dd; return true;
                case float ff:
                    dec = (decimal)ff; return true;
                case int i:
                    dec = i; return true;
                case long l:
                    dec = l; return true;
                case string s:
                    return decimal.TryParse(s, NumberStyles.Any, culture, out dec);
                default:
                    try {
                        dec = Convert.ToDecimal(value, culture);
                        return true;
                    } catch {
                        dec = 0; return false;
                    }
            }
        }
    }

    public sealed class DateFormatter : IDataFormatter {
        public string Name => "Date";
        public object Apply(object value, string[] args, CultureInfo culture) {
            if (!TemplatingHelpers.TryToDateTime(value, culture, out var dt)) return string.Empty;
            var fmt = args.Length > 0 ? TemplatingHelpers.Unquote(args[0]) : "d";
            return dt.ToString(fmt, culture);
        }
    }

    public sealed class DateTimeFormatter : IDataFormatter {
        public string Name => "DateTime";
        public object Apply(object value, string[] args, CultureInfo culture) {
            if (!TemplatingHelpers.TryToDateTime(value, culture, out var dt)) return string.Empty;
            var fmt = args.Length > 0 ? TemplatingHelpers.Unquote(args[0]) : "g";
            return dt.ToString(fmt, culture);
        }
    }

    internal static class TemplatingHelpers {

        internal static string Unquote(string s) {
            if (s.Length >= 2 && ((s[0] == '"' && s[^1] == '"') || (s[0] == '\'' && s[^1] == '\'')))
                return s.Substring(1, s.Length - 2).Replace(s[0].ToString() + s[0], s[0].ToString());
            return s;
        }
        internal static bool TryToDateTime(object value, CultureInfo culture, out DateTime dt) {
            switch (value) {
                case DateTime d: dt = d; return true;
                // case DateOnly o: dt = o.ToDateTime(TimeOnly.MinValue); return true;
                case string s: return DateTime.TryParse(s, culture, DateTimeStyles.None, out dt);
                default:
                    try { dt = Convert.ToDateTime(value, culture); return true; } catch { dt = default; return false; }
            }
        }
    }

    public sealed class DataExpressionFormatter : IDataExpressionFormatter {
        private readonly Dictionary<string, IExpressionFilter> _filterDictionary;
        private readonly Dictionary<string, IDataFormatter> _formatterRegistry;
        private readonly CultureInfo _culture;
        private readonly int _maxDepth;
        private readonly int _maxCollection;

        public DataExpressionFormatter(
            IEnumerable<IExpressionFilter> filters = null,
            IEnumerable<IDataFormatter> formatters = null,
            CultureInfo culture = null,
            int maxDepth = 64,
            int maxCollection = 10000
        ) {
            _culture = culture ?? CultureInfo.InvariantCulture;
            _maxDepth = Math.Max(8, maxDepth);
            _maxCollection = Math.Max(100, maxCollection);

            _filterDictionary = (filters ?? Enumerable.Empty<IExpressionFilter>())
                .ToDictionary(
                    f => {
                        var name = f.GetType().Name;
                        return name.EndsWith("Filter", StringComparison.OrdinalIgnoreCase)
                            ? name.Substring(0, name.Length - "Filter".Length)
                            : name;
                    },
                    f => f,
                    StringComparer.OrdinalIgnoreCase);

            // Built-ins + user-provided
            _formatterRegistry = new Dictionary<string, IDataFormatter>(StringComparer.OrdinalIgnoreCase);
            void add(IDataFormatter df) { if (df != null) _formatterRegistry[df.Name] = df; }

            add(new NumberFormatter());
            add(new DateFormatter());
            add(new DateTimeFormatter());

            if (formatters != null)
                foreach (var df in formatters) add(df);
        }

        #region Public API

        public string Format(object input, string expression) {
            try {
                if (string.IsNullOrEmpty(expression)) return string.Empty;
                var result = Render(input, expression.AsSpan(), 0);
                return result ?? string.Empty;
            } catch {
                return string.Empty;
            }
        }

        private string Render(object context, ReadOnlySpan<char> tpl, int depth) {
            if (depth > _maxDepth) return string.Empty;

            var sb = new StringBuilder();
            int i = 0;

            while (i < tpl.Length) {
                int next = -1; string opener = null; string closer = null;

                int j1 = IndexOf(tpl, "{{", i);
                int j2 = IndexOf(tpl, "[[", i);
                int j3 = IndexOf(tpl, "<<", i);
                int j4 = IndexOf(tpl, "<?", i);  // NEW: conditional opener

                if (j1 >= 0 && (next < 0 || j1 < next)) { next = j1; opener = "{{"; closer = "}}"; }
                if (j2 >= 0 && (next < 0 || j2 < next)) { next = j2; opener = "[["; closer = "]]"; }
                if (j3 >= 0 && (next < 0 || j3 < next)) { next = j3; opener = "<<"; closer = ">>"; }
                if (j4 >= 0 && (next < 0 || j4 < next)) { next = j4; opener = "<?"; closer = "?>"; } // NEW

                if (next < 0) {
                    sb.Append(tpl.Slice(i).ToString());
                    break;
                }

                if (next > i) sb.Append(tpl.Slice(i, next - i).ToString());

                ReadOnlySpan<char> inner;
                int endAfter;
                FindBalanced(tpl, next, opener!, closer!, out inner, out endAfter);
                if (inner.Length == 0 && endAfter < 0) {
                    i = next + opener!.Length;
                    continue;
                }

                string replacement = string.Empty;
                try {
                    if (opener == "{{") {
                        replacement = EvaluatePipedExpression(context, inner.ToString()) ?? string.Empty;
                    } else if (opener == "[[") {
                        var inside = inner.ToString();
                        string mathResult;
                        var pipeIdx = inside.IndexOf('|');
                        if (pipeIdx >= 0) {
                            var left = inside.Substring(0, pipeIdx);
                            var right = inside.Substring(pipeIdx + 1);
                            left = Render(context, left, depth + 1);
                            mathResult = RunMaths(left);
                            replacement = EvaluatePipedExpression(context, mathResult + "|" + right) ?? string.Empty;
                        } else {
                            inside = Render(context, inside, depth + 1);
                            mathResult = RunMaths(inside);
                            replacement = mathResult ?? string.Empty;
                        }
                    } else if (opener == "<<") { // repeat
                        var inside = inner.ToString();
                        var sep = inside.IndexOf('|');
                        if (sep >= 0) {
                            var path = inside.Substring(0, sep).Trim();
                            var itemTemplate = inside.Substring(sep + 1);
                            var target = SafeRead(context, path);
                            if (target is IEnumerable en && !(target is string)) {
                                int count = 0;
                                foreach (var item in en) {
                                    if (++count > _maxCollection) break;
                                    sb.Append(Render(item, itemTemplate.AsSpan(), depth + 1) ?? string.Empty);
                                }
                                replacement = string.Empty; // appended directly
                            } else {
                                replacement = Render(target, itemTemplate.AsSpan(), depth + 1) ?? string.Empty;
                            }
                        }
                    } else if (opener == "<?") { // NEW: conditional
                        var inside = inner.ToString();
                        var sep = inside.IndexOf('|');
                        if (sep >= 0) {
                            var condPath = inside.Substring(0, sep).Trim();
                            var body = inside.Substring(sep + 1);
                            var condValue = SafeRead(context, condPath);
                            if (IsTruthy(condValue)) {
                                replacement = Render(context, body.AsSpan(), depth + 1) ?? string.Empty;
                            } else {
                                replacement = string.Empty;
                            }
                        }
                    }
                } catch {
                    replacement = string.Empty;
                }

                // FIX: was "**" before; repeat is "<<"
                if (opener != "<<") sb.Append(replacement);

                i = endAfter;
            }

            return sb.ToString();
        }

        private void FindBalanced(ReadOnlySpan<char> tpl, int start, string opener, string closer, out ReadOnlySpan<char> inner, out int endAfter) {
            int oLen = opener.Length, cLen = closer.Length;
            int i = start + oLen, depth = 1;
            while (i <= tpl.Length - cLen) {
                if (StartsWith(tpl, i, opener)) { depth++; i += oLen; continue; }
                if (StartsWith(tpl, i, closer)) {
                    depth--;
                    if (depth == 0) {
                        inner = tpl.Slice(start + oLen, i - (start + oLen));
                        endAfter = i + cLen;
                        return;
                    }
                    i += cLen;
                    continue;
                }
                i++;
            }
            inner = ReadOnlySpan<char>.Empty;
            endAfter = -1;
        }

        private static bool StartsWith(ReadOnlySpan<char> s, int index, string token) {
            if (index + token.Length > s.Length) return false;
            for (int k = 0; k < token.Length; k++)
                if (s[index + k] != token[k]) return false;
            return true;
        }

        private static int IndexOf(ReadOnlySpan<char> s, string token, int startIndex) {
            if (startIndex < 0) startIndex = 0;
            var idx = s.Slice(startIndex).ToString().IndexOf(token, StringComparison.Ordinal);
            return idx < 0 ? -1 : startIndex + idx;
        }

        #endregion

        #region Expressions, Filters & Formatters

        private string EvaluatePipedExpression(object context, string expression) {
            // Split by '|' but respect parentheses in formatter calls
            var parts = SplitPipes(expression);
            if (parts.Count == 0) return string.Empty;

            object value;
            // first stage is a value path
            try {
                value = SafeRead(context, parts[0].Trim());
            } catch {
                return string.Empty;
            }

            for (int i = 1; i < parts.Count; i++) {
                var stage = parts[i].Trim();
                if (string.IsNullOrEmpty(stage)) continue;

                // Formatter with args? e.g. Name(arg1, arg2)
                if (TryParseCall(stage, out var name, out var args)) {
                    value = ApplyFormatter(value, name, args);
                } else {
                    // Legacy IExpressionFilter (no args)
                    var filter = FindFilter(stage);
                    if (filter == null) {
                        // Unknown filter -> swallow
                        return string.Empty;
                    }
                    try { value = filter.Transform(value); } catch { return string.Empty; }
                }

                if (value == null) return string.Empty;
            }

            return EnsureString(value);
        }

        private object ApplyFormatter(object value, string name, string[] args) {
            if (_formatterRegistry.TryGetValue(name, out var f)) {
                try { return f.Apply(value, args, _culture) ?? string.Empty; } catch { return string.Empty; }
            }
            // Fall back to existing filters if someone wrote Date(...) but registered as filter “Date”
            var fallback = FindFilter(name);
            if (fallback != null) {
                try { return fallback.Transform(value) ?? string.Empty; } catch { return string.Empty; }
            }
            return string.Empty;
        }

        private IExpressionFilter FindFilter(string name) {
            _filterDictionary.TryGetValue(name, out var filter);
            return filter;
        }

        private static List<string> SplitPipes(string s) {
            var list = new List<string>();
            if (string.IsNullOrEmpty(s)) return list;
            var sb = new StringBuilder();
            int paren = 0;
            for (int i = 0; i < s.Length; i++) {
                var ch = s[i];
                if (ch == '(') paren++;
                if (ch == ')') paren = Math.Max(0, paren - 1);

                if (ch == '|' && paren == 0) {
                    list.Add(sb.ToString());
                    sb.Clear();
                } else sb.Append(ch);
            }
            list.Add(sb.ToString());
            return list;
        }

        private static bool TryParseCall(string stage, out string name, out string[] args) {
            name = null; args = Array.Empty<string>();
            int par = stage.IndexOf('(');
            if (par < 0 || !stage.EndsWith(")", StringComparison.Ordinal)) return false;

            name = stage.Substring(0, par).Trim();
            var inside = stage.Substring(par + 1, stage.Length - par - 2);
            args = SplitArgs(inside);
            return true;

            static string[] SplitArgs(string s) {
                var res = new List<string>();
                var sb = new StringBuilder();
                bool inQuote = false;
                char q = '\0';

                for (int i = 0; i < s.Length; i++) {
                    var ch = s[i];
                    if (!inQuote && ch is '\'' or '\"') { inQuote = true; q = ch; sb.Append(ch); continue; }
                    if (inQuote && ch == q) {
                        // allow doubled quotes inside
                        if (i + 1 < s.Length && s[i + 1] == q) { sb.Append(q); i++; continue; }
                        inQuote = false;
                        sb.Append(ch);
                        continue;
                    }
                    if (!inQuote && ch == ',') {
                        res.Add(sb.ToString().Trim());
                        sb.Clear();
                    } else sb.Append(ch);
                }
                res.Add(sb.ToString().Trim());
                return res.ToArray();
            }
        }

        private static string EnsureString(object obj) => obj?.ToString() ?? string.Empty;

        private object SafeRead(object ctx, string path) {
            try {
                return ValuePathReader.Read(ctx, path);
            } catch {
                return null;
            }
        }

        #endregion
        #region Truthiness helper (NEW)
        private bool IsTruthy(object value) {
            try {
                if (value == null) return false;

                // bool
                if (value is bool b) return b;

                // string (empty/whitespace -> false)
                if (value is string s) return !string.IsNullOrWhiteSpace(s);

                // IEnumerable (empty -> false)
                if (value is IEnumerable en && !(value is string)) {
                    var e = en.GetEnumerator();
                    try { return e.MoveNext(); } finally {
                        var disp = e as IDisposable;
                        if (disp != null) disp.Dispose();
                    }
                }

                // numeric (0 -> false)
                // Try Convert.ToDecimal; if it fails, treat as truthy object
                try {
                    var dec = Convert.ToDecimal(value, _culture);
                    return dec != 0m;
                } catch {
                    // Non-numeric, non-bool, non-string, non-enumerable: any object is truthy
                    return true;
                }
            } catch {
                return false;
            }
        }

        #endregion

        #region Maths (kept, now silent)

        public string RunMaths(string expression) {
            try {
                if (string.IsNullOrWhiteSpace(expression))
                    return string.Empty;
                var parser = new BasicMathParser(expression);
                return parser.ParseExpression() ?? string.Empty;
            } catch {
                return string.Empty;
            }
        }

        #endregion
    }
}
