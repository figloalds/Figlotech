using Figlotech.Core.Helpers;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Figlotech.BDados.Builders {
    public sealed class Qh {
        public Qh() {

        }

        public static bool In<T>(object input, string column, List<T> o, Func<T, object> fn) {
            if (o == null) {
                return false;
            }
            var inputValue = ReflectionTool.GetValue(input, column);
            return o.Any(item => Object.Equals(fn(item), inputValue));
        }
        public static bool NotIn<T>(object input, string column, List<T> o, Func<T, object> fn) {
            return !In(input, column, o, fn);
        }
    }
}
