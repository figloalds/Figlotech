using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace Figlotech.Core
{
    public interface IExpressionFilter
    {
        object Transform(object input);
    }

    public interface IDataExpressionFormatter
    {
        string Format(object input, string expression);
    }

    public sealed class DataExpressionFormatter : IDataExpressionFormatter {
        private readonly Dictionary<string, IExpressionFilter> _filterDictionary;
        private static readonly Regex DoubleBracketRegex = new Regex(@"\[\[([^\[\]]*)\]\]", RegexOptions.Compiled);
        private static readonly Regex DoubleCurlyBraceRegex = new Regex(@"\{\{([^\{\}]*)\}\}", RegexOptions.Compiled);

        public DataExpressionFormatter(params IExpressionFilter[] filters) {
            _filterDictionary = filters.ToDictionary(
                filter => {
                    var name = filter.GetType().Name;
                    return name.EndsWith("Filter") ? name.Substring(0, name.Length - "Filter".Length) : name;
                },
                filter => filter,
                StringComparer.OrdinalIgnoreCase
            );
        }

        private string EnsureReturnIsString(object obj) => obj?.ToString() ?? string.Empty;

        private IExpressionFilter FindFilter(string name) {
            _filterDictionary.TryGetValue(name, out var filter);
            return filter;
        }

        private string Parse(object input, string expression) {
            object value = input;
            int iter = 0;
            ReadOnlySpan<char> exprSpan = expression.AsSpan();

            while (!exprSpan.IsEmpty) {
                int idx = exprSpan.IndexOf('|');
                ReadOnlySpan<char> current;
                if (idx >= 0) {
                    current = exprSpan.Slice(0, idx).Trim();
                    exprSpan = exprSpan.Slice(idx + 1).Trim();
                } else {
                    current = exprSpan.Trim();
                    exprSpan = ReadOnlySpan<char>.Empty;
                }

                string currentStr = current.ToString();
                if (iter++ == 0) {
                    value = ValuePathReader.Read(value, currentStr);
                } else {
                    var filter = FindFilter(currentStr);
                    if (filter == null) {
                        return null;
                    }
                    value = filter.Transform(value);
                }

                if (value == null) {
                    return null;
                }
            }

            return EnsureReturnIsString(value);
        }

        public string RunMaths(string expression) {
            if (string.IsNullOrWhiteSpace(expression))
                return string.Empty;

            var parser = new BasicMathParser(expression);
            return parser.ParseExpression();
        }

        public string Format(object input, string expression) {
            var sb = new StringBuilder(expression);
            // Handle [[...]] replacements
            foreach (Match match in DoubleBracketRegex.Matches(sb.ToString())) {
                var parsed = Format(input, match.Groups[1].Value);
                var mathablePart = parsed;
                string replacement;

                if (mathablePart.Contains('|')) {
                    var idx = mathablePart.IndexOf('|');
                    var mathable = mathablePart.Substring(0, idx);
                    var rest = mathablePart.Substring(idx + 1);
                    mathable = RunMaths(mathable);
                    replacement = Parse(input, mathable + "|" + rest) ?? string.Empty;
                } else {
                    replacement = RunMaths(mathablePart);
                }

                sb.Replace(match.Value, replacement);
            }

            // Handle {{...}} replacements
            foreach (Match match in DoubleCurlyBraceRegex.Matches(sb.ToString())) {
                var parsed = Parse(input, match.Groups[1].Value) ?? string.Empty;
                sb.Replace(match.Value, parsed);
            }

            return sb.ToString();
        }
    }
}
