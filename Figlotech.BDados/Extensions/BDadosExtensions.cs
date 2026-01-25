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
using Figlotech.Core.Interfaces;
using System.Threading.Tasks;
using Figlotech.Data;
using Figlotech.BDados.Helpers;
using System.Threading;

namespace Figlotech.BDados.DataAccessAbstractions {
    


    public static class ListBDadosExtensions {

        public static int DefaultPageSize = 200;
        public static int PageSize { get; set; } = DefaultPageSize;
        public static bool LinearLoad = false;

        static uint upseq = 0;
        public static Qb ListRids<T>(this List<T> me) where T: ILegacyDataObject, new() {
            Qb retv = new Qb();
            for (int i = 0; i < me.Count; i++) {
                retv.Append($"@{++upseq}_{i}", me[i].RID);
                if (i < me.Count - 1)
                    retv.Append(",");
            }
            return retv;
        }
        
        public static void LoadAll<T>(this List<T> me, IDataAccessor DataAccessor,
            LoadAllArgs<T> args = null) where T : IDataObject, new() {
            var agl = DataAccessor.AggregateLoad<T>(args);
            agl.ForEach(a=> me.Add(a));
            agl.Clear();
        }

        public static void LoadAllLinear<T>(this List<T> me, IDataAccessor DataAccessor, LoadAllArgs<T> args = null) where T : IDataObject, new() {
            var agl = DataAccessor.AggregateLoad<T>(args.NoLists());
            agl.ForEach(a => me.Add(a));
            agl.Clear();
        }

        public static bool Save<T>(this List<T> me, IDataAccessor DataAccessor) where T : ILegacyDataObject, new() {
            return DataAccessor?.SaveList(me) ?? false;
        }


        public static void AddRange<T>(this List<T> me, IEnumerable<T> range) {
            range.ForEach(i => {
                if(i != null) {
                    me.Add(i);
                }
            });
        }

        public static List<T> ToList<T>(this List<T> me) {
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
                
        public static void MakeUnique(this ILegacyDataObject self) {
            self.Id = 0;
            self.RID = null;
            self.IsPersisted = false;
        }
    }
}
