using Figlotech.BDados.Builders;
using Figlotech.Core.Interfaces;
using System;
using System.Linq.Expressions;
using System.Reflection;

namespace Figlotech.BDados.DataAccessAbstractions {
    public interface IDataAccessor
    {
        T ForceExist<T>(Func<T> Default, Conditions<T> cnd) where T : IDataObject, new();

        ILogger Logger { get; set; }

        RecordSet<T> LoadAll<T>(Expression<Func<T, bool>> condicoes, int? page = null, int? limit = 200) where T : IDataObject, new();

        T LoadFirstOrDefault<T>(Expression<Func<T, bool>> condicoes, int? page = null, int? limit = 200) where T : IDataObject, new();
        T LoadByRid<T>(String RID) where T : IDataObject, new();
        T LoadById<T>(long Id) where T : IDataObject, new();

        bool DeleteWhereRidNotIn<T>(Expression<Func<T, bool>> cnd, RecordSet<T> rids) where T : IDataObject, new();
        bool Delete<T>(Expression<Func<T, bool>> condition) where T : IDataObject, new();
        bool Delete(IDataObject obj);

        bool SaveRecordSet<T>(RecordSet<T> rs) where T : IDataObject, new();
        bool SaveItem(IDataObject objeto, Action funcaoPosSalvar = null);

        Type[] WorkingTypes { get; set; }

        RecordSet<T> AggregateLoad<T>(Expression<Func<T, bool>> cnd = null, int? limit = null, int? page = null, int PageSize = 200, MemberInfo OrderingMember = null, OrderingType Ordering = OrderingType.Asc, bool Linear = false) where T : IDataObject, new();
    }
}
