using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Figlotech.Core.Extensions
{
    public static class ExceptionExtensions
    {

        public static IEnumerable<Exception> ToRecursiveInnerExceptions(this Exception ex)  {
            var x = ex;
            yield return x;
            if (x is AggregateException ag && (ag.InnerExceptions?.Any()??false)) {
                foreach(var a in ag.InnerExceptions) {
                    yield return a;
                }
            }
            if (x.InnerException != null) {
                yield return x.InnerException;
                foreach (var a in x.InnerException.ToRecursiveInnerExceptions()) {
                    yield return a;
                }
            }
        }
    }
}
