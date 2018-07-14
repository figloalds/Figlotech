using Figlotech.BDados.Builders;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using Figlotech.Core;
using Figlotech.Core.Extensions;
using Figlotech.Core.Helpers;
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
            agl.ForEach(a=> me.Add(a));
            agl.Clear();
        }

        public static void LoadAllLinear<T>(this IList<T> me, IDataAccessor DataAccessor, Expression<Func<T, bool>> cnd = null, int? skip = null, int? limit = null,
            Expression<Func<T, object>> orderingMember = null, OrderingType otype = OrderingType.Asc,
            MemberInfo GroupingMember = null) where T : IDataObject, new() {
            var agl = DataAccessor.AggregateLoad<T>(cnd, skip, limit, orderingMember, otype, GroupingMember, true);
            agl.ForEach(a => me.Add(a));
            agl.Clear();
        }

        public static bool Save<T>(this IList<T> me, IDataAccessor DataAccessor) where T : IDataObject, new() {
            return DataAccessor?.SaveList(me) ?? false;
        }


        public static void AddRange<T>(this IList<T> me, IEnumerable<T> range) {
            range.ForEach(i => {
                if(i != null) {
                    me.Add(i);
                }
            });
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
            if (me is List<T> li && me != null)
                return li;
            if (me is IEnumerable<T> ien) {
                ien.ToList();
            }
            return new List<T>();
        }
    }

    public static class BDadosExtensions {
        public static RecordSet<T> ToRecordSet<T>(this IEnumerable<T> me) where T : IDataObject, new() {
            var rs = new RecordSet<T>();
            rs.AddRange(me);
            return rs;
        }

        static SelfInitializerDictionary<Type, MemberInfo[]> membersList = new SelfInitializerDictionary<Type, MemberInfo[]>(
            (t) => ReflectionTool.FieldsAndPropertiesOf(t).Where(f => f.GetCustomAttribute<FieldAttribute>() != null).ToArray()
        );

        internal static int ComputeDataFieldsHash(this IDataObject self) {
            if(self == null) {
                return 0;
            }
            var retv = 0;
            var t = self.GetType();
            MemberInfo[] li = membersList[t];
            li.ForEach(i => retv ^= i?.GetHashCode() ?? 0);
            
            return retv;
        }

        public static void MakeUnique(this IDataObject self) {
            self.Id = 0;
            self.RID = null;
            self.IsPersisted = false;
        }
    }
}
