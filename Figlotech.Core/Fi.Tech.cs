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
            if(skip < 0) {
                for (int i = max-1; i >= min; i += skip) {
                    yield return i;
                }
            } else {
                for (int i = min; i < max; i += skip) {
                    yield return i;
                }
            }
        }
                
        public FnVal<T> V<T>(Func<T> fn) {
            return new FnVal<T>(fn);
        }
        public static readonly String[] ByteNames = {
            "B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"
        };
        public static void Benchmark(params Action[] fn) {
            if (!FiTechCoreExtensions.EnableBenchMarkers) {
                fn.ForEach(a => a?.Invoke());
                return;
            }
            Benchmarker bm = new Benchmarker();
            var i = 0;
            foreach(var a in fn) {
                bm.Mark($"{++i}");
                a?.Invoke();
            }
            bm.FinalMark();
        }
    }
}