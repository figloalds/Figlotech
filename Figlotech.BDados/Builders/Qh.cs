using Figlotech.Core.Helpers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Figlotech.BDados.Builders
{
    public class Qh
    {
        public Qh() {
            
        }

        public static bool In<T>(object input, string column, List<T> o, Func<T, object> fn) {
            var refl = new ObjectReflector(input);
            return o.Any(item=> fn(item) == refl[column]);
        }
    }
}
