using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core.Extensions
{
    public static class IEnumerableExtensions
    {
        public static DataTable ToDataTable<T>(this IEnumerable<T> me) {
            var dt = new DataTable();
            var enny = me.GetEnumerator();
            if (!enny.MoveNext())
                return dt;
            var refl = enny.Current.ToReflectable();
            foreach (var col in refl) {
                dt.Columns.Add(col.Key.Name);
            }
            Tuple<List<MemberInfo>, List<DataColumn>> meta = Fi.Tech.MapMeta(typeof(T), dt);
            do {
                var dr = dt.NewRow();
                enny.Current.ToDataRow(dr, meta);
            } while (enny.MoveNext());
            return dt;
        }
    }
}
