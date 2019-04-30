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
using System.Text.RegularExpressions;

namespace Figlotech.Core {
    public class With<T> {
        T value;
        public With(T value) {
            this.value = value;
        }
        public bool Predicate(Predicate<T> predicate) {
            return predicate.Invoke(value);
        }
    }

    /// <summary>
    /// This is an utilitarian class, it provides quick static logic 
    /// for the rest of Figlotech Tools to operate
    /// </summary>
    /// 
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
        
        /// <summary>
        /// This is here because ain't nobody got time to write
        /// FormattableString.Invariant(), it's also a shame this isn't the default for the $"" syntax
        /// Considering CurrentCulture is a HUGE mess when it goes from the compiled-at machine to a run-at machine
        /// And then it converts special characters that are already correct into garbage making 
        /// a huge mess. Seriously I wonder why the hell CurrentCulture is default for $"", it's terrible.
        /// </summary>
        /// <param name="fms"></param>
        /// <returns></returns>
        public static string S(FormattableString fms) {
            return fms.ToString(CultureInfo.InvariantCulture);
        }

        public static With<T> With<T>(T value) {
            return new With<T>(value);
        }
                
        public FnVal<T> V<T>(Func<T> fn) {
            return new FnVal<T>(fn);
        }

        public enum DataUnitType {
            SI, IEC
        }
        public enum DataUnitFormat {
            Short, Long
        }
        public enum DataUnitDimension {
            Byte, Bit
        }
        public enum DataUnitGeneralFormat {
            SIShortByte,
            SIShortBit,
            SILongByte,
            SILongBit,
            IECShortByte,
            IECShortBit,
            IECLongByte,
            IECLongBit,
        }

        public static Dictionary<DataUnitGeneralFormat, string[]> DataUnitNames = new Dictionary<DataUnitGeneralFormat, string[]> {
            { DataUnitGeneralFormat.SIShortByte, new string[] {
                "B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"
            }},
            { DataUnitGeneralFormat.SIShortBit, new string[] {
                "b", "Kb", "Mb", "Gb", "Tb", "Pb", "Eb", "Zb", "Yb"
            }},
            { DataUnitGeneralFormat.SILongByte, new string[] {
                "Byte", "Kilobyte", "Megabyte", "Gigabyte", "Terabyte", "Petabyte", "Exabyte", "Zetabyte", "Yottabyte"
            }},
            { DataUnitGeneralFormat.SILongBit, new string[] {
                "bit", "Kilobit", "Megabit", "Gigabit", "Terabit", "Petabit", "Exabit", "Zetabit", "Yottabit"
            }},
            { DataUnitGeneralFormat.IECShortByte, new string[] {
                "B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB", "ZiB", "YiB"
            }},
            { DataUnitGeneralFormat.IECShortBit, new string[] {
                "b", "Kib", "Mib", "Gib", "Tib", "Pib", "Eib", "Zib", "Yib"
            }},
            { DataUnitGeneralFormat.IECLongByte, new string[] {
                "byte", "Kibibyte", "Mebibyte", "Gibibyte", "Tibibyte", "Pebibyte", "Exbibyte", "Zebibyte", "yobibyte"
            }},
            { DataUnitGeneralFormat.IECLongBit, new string[] {
                "bit", "Kibibit", "Mebibit", "Gibibit", "Tibibit", "Pebibit", "Exbibit", "Zebibit", "yobibit"
            }},
        };

        public static Dictionary<DataUnitGeneralFormat, int> DataUnitFactors = new Dictionary<DataUnitGeneralFormat, int> {
            { DataUnitGeneralFormat.SIShortByte, 1000 },
            { DataUnitGeneralFormat.SIShortBit, 8000 },
            { DataUnitGeneralFormat.SILongByte, 1000},
            { DataUnitGeneralFormat.SILongBit, 8000 },
            { DataUnitGeneralFormat.IECShortByte, 1024 },
            { DataUnitGeneralFormat.IECShortBit, 8192 },
            { DataUnitGeneralFormat.IECLongByte, 1024},
            { DataUnitGeneralFormat.IECLongBit, 8192 },
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