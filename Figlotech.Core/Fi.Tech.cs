using System;
using System.Collections.Generic;
using System.Globalization;
using System.Data;
using System.Reflection;
using System.Linq;
using System.Linq.Expressions;
using Newtonsoft.Json;
using System.Threading.Tasks;
using System.Diagnostics;
using Figlotech.Core.Helpers;
using Figlotech.Core.I18n;
using Figlotech.Core.Interfaces;
using System.Text;

namespace Figlotech.Core {
    /// <summary>
    /// This is an utilitarian class, it provides quick static logic 
    /// for the rest of Figlotech Tools to operate
    /// </summary>
    public class Fi {

        public static Fi Tech = new Fi();

        public static IEnumerable<int> Range(int min, int max, int skip = 1) {
            for(int i = min; i < max; i+=skip) {
                yield return i;
            }
        }
        
        public FnVal<T> V<T>(Func<T> fn) {
            return new FnVal<T>(fn);
        }

    }
}