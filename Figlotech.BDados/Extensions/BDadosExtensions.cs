using Figlotech.BDados.Builders;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.Core;
using Figlotech.Core.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.DataAccessAbstractions {
    public static class IListBDadosExtensions {

        public static int DefaultPageSize = 200;
        public static int PageSize { get; set; } = DefaultPageSize;
        public static bool LinearLoad = false;
        
        public static Qb ListRids<T>(this IList<T> me, Func<T, String> fn) where T: IDataObject, new() {
            Qb retv = new Qb();
            var uni = IntEx.GenerateShortRid();
            for (int i = 0; i < me.Count; i++) {
                retv.Append($"@{uni}{i}", me[i].RID);
                if (i < me.Count - 1)
                    retv.Append(",");
            }
            return retv;
        }
        
        public static void LoadAll<T>(this IList<T> me, IDataAccessor DataAccessor, 
            Expression<Func<T, bool>> cnd = null, int? skip = null, int? limit = null,
            Expression<Func<T, object>> orderingMember = null, OrderingType otype = OrderingType.Asc,
            MemberInfo GroupingMember = null) where T : IDataObject, new() {
            var agl = DataAccessor.AggregateLoad<T>(cnd, skip, limit, orderingMember, otype, GroupingMember, false);
            agl.Iterate(a=> me.Add(a));
            agl.Clear();
        }

        public static void LoadAllLinear<T>(this IList<T> me, IDataAccessor DataAccessor, Expression<Func<T, bool>> cnd = null, int? skip = null, int? limit = null,
            Expression<Func<T, object>> orderingMember = null, OrderingType otype = OrderingType.Asc,
            MemberInfo GroupingMember = null) where T : IDataObject, new() {
            var agl = DataAccessor.AggregateLoad<T>(cnd, skip, limit, orderingMember, otype, GroupingMember, true);
            agl.Iterate(a => me.Add(a));
            agl.Clear();
        }

        public static bool Save<T>(this IList<T> me, IDataAccessor DataAccessor) where T : IDataObject, new() {
            return DataAccessor?.SaveList(me) ?? false;
        }


        public static void AddRange<T>(this IList<T> me, IEnumerable<T> range) {
            range.Iterate(i => me.Add(i));
        }
        public static void RemoveAll<T>(this IList<T> me, Predicate<T> predicate) {
            int i = me.Count-1;
            while(i-->0) {
                if (predicate(me[i])) {
                    me.RemoveAt(i);
                }
            }
        }

        public static List<T> ToList<T>(this IList<T> me) {
            if (me is List<T> li)
                return li;
            else
                return (me as IEnumerable<T>).ToList();
        }
    }

    public static class BDadosExtensions {
        public static RecordSet<T> ToRecordSet<T>(this IEnumerable<T> me) where T : IDataObject, new() {
            var rs = new RecordSet<T>();
            rs.AddRange(me);
            return rs;
        }
    }
}
