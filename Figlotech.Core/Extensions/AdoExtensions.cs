using Figlotech.Core;
using Figlotech.Core.Helpers;
using System.Collections.Generic;
using System.Data;

namespace System.Data {
    public static class AdoExtensions {
        public static IEnumerable<T> Map<T>(this DataTable dt, Dictionary<string, string> mapReplacements = null) where T : new() {
            return Fi.Tech.Map<T>(dt, mapReplacements);
        }

        public static IEnumerable<T> ToEnumerable<T>(this DataColumn dc) {
            var dt = dc.Table;
            for (int i = 0; i < dt.Rows.Count; i++) {
                yield return (T)ReflectionTool.DbEvalValue(dt.Rows[i][dc], typeof(T));
            }
        }

        public static IEnumerable<DataRow> ToEnumerable(this DataRowCollection drc) {
            for (int i = 0; i < drc.Count; i++) {
                yield return drc[i];
            }
        }
    }
}
