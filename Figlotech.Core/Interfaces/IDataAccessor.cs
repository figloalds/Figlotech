using Figlotech.Core.Helpers;
using Figlotech.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;

namespace Figlotech.Core.Interfaces {
    public enum OrderingType {
        Asc,
        Desc
    }

    public class LoadAllArgs<T> where T : IDataObject, new() {
        public Expression<Func<T, bool>> Conditions { get; private set; } = null;
        public int? RowSkip { get; private set; } = null;
        public int? RowLimit { get; private set; } = null;
        public Expression<Func<T, object>> OrderingMember { get; private set; } = null;
        public OrderingType OrderingType { get; private set; } = OrderingType.Asc;
        public MemberInfo GroupingMember { get; private set; } = null;
        public bool Linear { get; private set; } = false;
        public object ContextObject { get; set; }

        IDataAccessor DataAccessor { get; set; }

        public LoadAllArgs() {

        }

        public LoadAllArgs(IDataAccessor dataAccessor) {
            this.DataAccessor = dataAccessor;
        }

        public LoadAllArgs<T> Where(Expression<Func<T, bool>> conditions) {
            this.Conditions = conditions;
            return this;
        }
        public LoadAllArgs<T> Skip(int? skip) {
            RowSkip = skip;
            return this;
        }
        public LoadAllArgs<T> Limit(int? limit) {
            RowLimit = limit;
            return this;
        }
        public LoadAllArgs<T> OrderBy(Expression<Func<T, object>> orderingMember, OrderingType orderingType = OrderingType.Asc) {
            OrderingMember = orderingMember;
            OrderingType = orderingType;
            return this;
        }
        public LoadAllArgs<T> GroupBy(MemberInfo groupingMember) {
            GroupingMember = groupingMember;
            return this;
        }
        public LoadAllArgs<T> LinearIf(bool linearity) {
            Linear = linearity;
            return this;
        }
        public LoadAllArgs<T> Full() {
            Linear = false;
            return this;
        }
        public LoadAllArgs<T> NoLists() {
            Linear = true;
            return this;
        }
        public LoadAllArgs<T> WithContext(object ContextObject) {
            this.ContextObject = ContextObject;
            return this;
        }

        public IntermediateLoadAllArgs<T> Using(IDataAccessor dataAccessor) {
            return new IntermediateLoadAllArgs<T>(dataAccessor, this);
        }
    }

    public class IntermediateLoadAllArgs<T> where T : IDataObject, new() {
        public IntermediateLoadAllArgs(IDataAccessor dataAccessor, LoadAllArgs<T> largs) {
            DataAccessor = dataAccessor;
            LoadAllArgs = largs;
        }

        private IDataAccessor DataAccessor { get; set; }
        private LoadAllArgs<T> LoadAllArgs { get; set; }

        public List<T> Aggregate() {
            return DataAccessor.AggregateLoad(LoadAllArgs);
        }
        public IEnumerable<T> Fetch() {
            return DataAccessor.Fetch(LoadAllArgs);
        }
        public List<T> Load() {
            return DataAccessor.LoadAll(LoadAllArgs);
        }
    }

    public class LoadAll {

        public static LoadAllArgs<T> From<T>() where T : IDataObject, new() {
            return new LoadAllArgs<T>();
        }

        public static LoadAllArgs<T> Where<T>(Expression<Func<T, bool>> conditions)
            where T : IDataObject, new()
            => new LoadAllArgs<T>().Where(conditions);

        public static LoadAllArgs<T> Skip<T>(int? skip)
            where T : IDataObject, new()
            => new LoadAllArgs<T>().Skip(skip);

        public static LoadAllArgs<T> Limit<T>(int? limit)
            where T : IDataObject, new()
            => new LoadAllArgs<T>().Limit(limit);

        public static LoadAllArgs<T> OrderBy<T>(Expression<Func<T, object>> orderingMember, OrderingType orderingType = OrderingType.Asc)
            where T : IDataObject, new()
            => new LoadAllArgs<T>().OrderBy(orderingMember, orderingType);

        public static LoadAllArgs<T> GroupBy<T>(MemberInfo groupingMember)
            where T : IDataObject, new()
            => new LoadAllArgs<T>().GroupBy(groupingMember);
        public LoadAllArgs<T> LinearIf<T>(bool linearity)
            where T : IDataObject, new()
            => new LoadAllArgs<T>().LinearIf(linearity);
        public static LoadAllArgs<T> Full<T>()
            where T : IDataObject, new()
            => new LoadAllArgs<T>().Full();

        public static LoadAllArgs<T> NoLists<T>()
            where T : IDataObject, new()
            => new LoadAllArgs<T>().NoLists();

        public LoadAllArgs<T> WithContext<T>(object ContextObject)
            where T : IDataObject, new()
            => new LoadAllArgs<T>().WithContext(ContextObject);
    }

    public interface IDataAccessor
    {
        ILogger Logger { get; set; }

        T ForceExist<T>(Func<T> Default, Conditions<T> cnd) where T : IDataObject, new();
        List<T> LoadAll<T>(LoadAllArgs<T> args = null) where T : IDataObject, new();
        IEnumerable<T> Fetch<T>(LoadAllArgs<T> args = null) where T : IDataObject, new();

        T LoadFirstOrDefault<T>(LoadAllArgs<T> args = null) where T : IDataObject, new();
        T LoadByRid<T>(String RID) where T : IDataObject, new();
        T LoadById<T>(long Id) where T : IDataObject, new();

        bool DeleteWhereRidNotIn<T>(Expression<Func<T, bool>> cnd, List<T> rids) where T : IDataObject, new();
        bool Delete<T>(Expression<Func<T, bool>> condition) where T : IDataObject, new();
        bool Delete(IDataObject obj);

        bool SaveList<T>(List<T> rs, bool recoverIds = false) where T : IDataObject;
        bool SaveItem(IDataObject objeto);

        bool Test();

        Type[] WorkingTypes { get; set; }

        List<T> AggregateLoad<T>(LoadAllArgs<T> args = null) where T : IDataObject, new();

    }
}
