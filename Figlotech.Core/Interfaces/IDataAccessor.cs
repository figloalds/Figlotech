using Figlotech.Core.Helpers;
using Figlotech.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace Figlotech.Core.Interfaces {
    public enum OrderingType {
        Asc,
        Desc
    }

    public interface IDataAccessor
    {
        ILogger Logger { get; set; }

        T ForceExist<T>(Func<T> Default, Conditions<T> cnd) where T : IDataObject, new();
        IList<T> LoadAll<T>(Expression<Func<T, bool>> condicoes, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new();
        IList<T> Fetch<T>(Expression<Func<T, bool>> condicoes, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new();

        T LoadFirstOrDefault<T>(Expression<Func<T, bool>> condicoes, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new();
        T LoadByRid<T>(String RID) where T : IDataObject, new();
        T LoadById<T>(long Id) where T : IDataObject, new();

        bool DeleteWhereRidNotIn<T>(Expression<Func<T, bool>> cnd, IList<T> rids) where T : IDataObject, new();
        bool Delete<T>(Expression<Func<T, bool>> condition) where T : IDataObject, new();
        bool Delete(IDataObject obj);

        bool SaveList<T>(IList<T> rs, bool recoverIds = false) where T : IDataObject;
        bool SaveItem(IDataObject objeto);

        bool Test();

        Type[] WorkingTypes { get; set; }

        IList<T> AggregateLoad<T>(
            Expression<Func<T, bool>> cnd = null, int? skip = null, int? limit = null, 
            Expression<Func<T, object>> orderingMember = null, OrderingType otype = OrderingType.Asc, 
            MemberInfo GroupingMember = null, bool Linear = false) where T : IDataObject, new();

    }
}
