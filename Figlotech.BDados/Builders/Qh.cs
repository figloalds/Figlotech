using Figlotech.Core.Helpers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Figlotech.BDados.Builders
{
    public sealed class Qh
    {
        public Qh() {
            
        }

        public static bool In<T>(object input, string column, List<T> o, Func<T, object> fn) {
            if (o == null) {
                return false;
            }
            return o.Any(item => fn(item) == ReflectionTool.GetValue(input, column));
        }
        public static bool NotIn<T>(object input, string column, List<T> o, Func<T, object> fn) {
            if(o == null) {
                return true;
            }
            return o.Any(item => fn(item) == ReflectionTool.GetValue(input, column));
        }
    }
}
