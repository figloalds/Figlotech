using System;
using System.Collections.Generic;
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

    public sealed class DataExpressionFormatter : IDataExpressionFormatter
    {
        IExpressionFilter[] Filters { get; set; }
        public DataExpressionFormatter(params IExpressionFilter[] filters) {
            this.Filters = filters;
        }

        private string EnsureReturnIsString(object obj) {
            if(obj is string str) {
                return str;
            } else {
                return obj?.ToString() ?? "";
            }
        }

        private IExpressionFilter FindFilter(string name) {
            return Filters.FirstOrDefault(x => {
                var t = x.GetType();
                var tn = t.Name;
                var tnEx = tn.Substring(0, tn.Length - "Filter".Length);
                return name == tn || name == tnEx;
            });
        }

        private string Parse(object input, string expression) {
            object value = input;
            string current;
            int iter = 0;
            do {
                var idx = expression.IndexOf('|');
                if (idx >= 0) {
                    current = (expression.Substring(0, idx)).Trim();
                    expression = (expression.Substring(idx + 1, expression.Length - idx - 1)).Trim();
                } else {
                    current = expression;
                    expression = null;
                }
                if(iter++ == 0) {
                    value = ValuePathReader.Read(value, current);
                } else {
                    value = FindFilter(current)?.Transform(value);
                }
                if (value == null) {
                    return null;
                }
            } while (!string.IsNullOrEmpty(expression));
            return EnsureReturnIsString(value);
        }

        public string Format(object input, string expression) {
            var retv = expression;
            var RegExp = @"\{\{([^\{\}]*)\}\}";
            var match = Regex.Match(expression, RegExp);
            while(match.Success) {
                var capture = match.Captures[0];
                var parsed = Parse(input, match.Groups[1].Value);
                var capEnd = capture.Index + capture.Length;
                var next = expression.Substring(0, capture.Index) + parsed + expression.Substring(Math.Min(capEnd, expression.Length - 1), Math.Max(0, expression.Length - capEnd));

                expression = next;
                retv = expression;
                match = Regex.Match(expression, RegExp);
            }
            return retv;
        }
    }

}
