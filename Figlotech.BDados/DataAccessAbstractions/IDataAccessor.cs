using Figlotech.BDados.Builders;
using Figlotech.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace Figlotech.BDados.DataAccessAbstractions {
    public interface IDataAccessor
    {
        ILogger Logger { get; set; }

        T ForceExist<T>(Func<T> Default, Conditions<T> cnd) where T : IDataObject, new();
        RecordSet<T> LoadAll<T>(Expression<Func<T, bool>> condicoes, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new();
        IEnumerable<T> Fetch<T>(Expression<Func<T, bool>> condicoes, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new();

        T LoadFirstOrDefault<T>(Expression<Func<T, bool>> condicoes, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new();
        T LoadByRid<T>(String RID) where T : IDataObject, new();
        T LoadById<T>(long Id) where T : IDataObject, new();

        bool DeleteWhereRidNotIn<T>(Expression<Func<T, bool>> cnd, RecordSet<T> rids) where T : IDataObject, new();
        bool Delete<T>(Expression<Func<T, bool>> condition) where T : IDataObject, new();
        bool Delete(IDataObject obj);

        bool SaveRecordSet<T>(RecordSet<T> rs) where T : IDataObject, new();
        bool SaveItem(IDataObject objeto);

        Type[] WorkingTypes { get; set; }

        IEnumerable<T> AggregateLoad<T>(
            Expression<Func<T, bool>> cnd = null, int? skip = null, int? limit = null, 
            Expression<Func<T, object>> orderingMember = null, OrderingType otype = OrderingType.Asc, 
            MemberInfo GroupingMember = null, bool Linear = false) where T : IDataObject, new();

    }
}
