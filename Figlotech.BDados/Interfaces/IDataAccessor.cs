using Figlotech.BDados.Builders;
using Figlotech.BDados.Entity;
using Figlotech.BDados.Requirements;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Interfaces {
    public interface IDataAccessor : IRequiresLogger {

        T Instantiate<T>() where T : IDataObject, new();

        T ForceExist<T>(Func<T> Default, Conditions<T> cnd) where T : IDataObject, new();

        RecordSet<T> LoadAll<T>(Expression<Func<T,bool>> condicoes, int? page = null, int? limit = 200) where T : IDataObject, new();

        T LoadFirstOrDefault<T>(Expression<Func<T, bool>> condicoes, int? page = null, int? limit = 200) where T : IDataObject, new();

        T LoadByRid<T>(object RID) where T : IDataObject, new();
        T LoadById<T>(object Id) where T : IDataObject, new();

        bool DeleteWhereRidNotIn<T>(Expression<Func<T, bool>> cnd, RecordSet<T> rids) where T : IDataObject, new();
        bool Delete<T>(Expression<Func<T, bool>> condition) where T : IDataObject, new();
        bool Delete(IDataObject obj);

        bool SaveRecordSet<T>(RecordSet<T> rs) where T : IDataObject, new();
        bool SaveItem(IDataObject objeto, Action funcaoPosSalvar = null);

        Type[] WorkingTypes { get; set; }

        RecordSet<T> AggregateLoad<T>(Expression<Func<T, bool>> cnd = null, int? limit = null, int? page = null, int PageSize = 200, MemberInfo OrderingMember = null, OrderingType Ordering = OrderingType.Asc, bool Linear = false) where T : IDataObject, new();
    }
}
