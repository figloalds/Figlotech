using Figlotech.BDados.Builders;
using Figlotech.BDados.DataAccessAbstractions;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Linq.Expressions;

namespace Figlotech.BDados.DataAccessAbstractions {
    public interface IRdbmsDataAccessor : IDataAccessor
    {
        event Action<Type, IDataObject> OnSuccessfulSave;
        event Action<Type, IDataObject, Exception> OnFailedSave;

        void Access(Action<IDbTransaction> tryFun, Action<Exception> catchFun = null);
        T Access<T>(Func<IDbTransaction, T> tryFun, Action<Exception> catchFun = null);

        RecordSet<T> LoadAll<T>(string where = "TRUE", params object[] args) where T : IDataObject, new();
        RecordSet<T> LoadAll<T>(IQueryBuilder condicoes = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new();

        IEnumerable<T> Fetch<T>(string where = "TRUE", params object[] args) where T : IDataObject, new();
        IEnumerable<T> Fetch<T>(IQueryBuilder condicoes = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new();

        //T ForceExist<T>(Func<T> Default, String query, params object[] args) where T : IDataObject, new();
        T ForceExist<T>(Func<T> Default, IQueryBuilder qb) where T : IDataObject, new();

        IEnumerable<T> Query<T>(IQueryBuilder Query = null) where T : new();
        IEnumerable<T> Query<T>(String Query, params object[] args) where T : new();

        DataTable Query(IQueryBuilder Query);
        DataTable Query(String Query, params object[] args);

        String SchemaName { get; }

        IQueryGenerator QueryGenerator { get; }

        IJoinBuilder MakeJoin(Action<JoinDefinition> fn);

        int Execute(IQueryBuilder Query);
        int Execute(String Query, params object[] args);

        void BeginTransaction();

        void EndTransaction();

        void Commit();
        void Rollback();

        DataTable Query(IDbTransaction transaction, IQueryBuilder Query);
        IEnumerable<T> Query<T>(IDbTransaction transaction, IQueryBuilder Query = null) where T : new();
        int Execute(IDbTransaction transaction, IQueryBuilder Query);
        IEnumerable<T> Fetch<T>(IDbTransaction transaction, IQueryBuilder condicoes = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new();
        RecordSet<T> LoadAll<T>(IDbTransaction transaction, IQueryBuilder condicoes = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new();

        bool SaveItem(IDbTransaction transaction, IDataObject objeto);
        bool SaveRecordSet<T>(IDbTransaction transaction, RecordSet<T> target) where T : IDataObject, new();

        T LoadFirstOrDefault<T>(IDbTransaction transaction, Expression<Func<T, bool>> condicoes, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new();
        T LoadByRid<T>(IDbTransaction transaction, String RID) where T : IDataObject, new();
        T LoadById<T>(IDbTransaction transaction, long Id) where T : IDataObject, new();

        //T ForceExist<T>(IDbTransaction transaction, Func<T> Default, Conditions<T> cnd) where T : IDataObject, new();
        RecordSet<T> LoadAll<T>(IDbTransaction transaction, Expression<Func<T, bool>> condicoes, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new();
        IEnumerable<T> Fetch<T>(IDbTransaction transaction, Expression<Func<T, bool>> condicoes, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new();

        bool DeleteWhereRidNotIn<T>(IDbTransaction transaction, Expression<Func<T, bool>> cnd, RecordSet<T> rids) where T : IDataObject, new();
        bool Delete<T>(IDbTransaction transaction, Expression<Func<T, bool>> condition) where T : IDataObject, new();
        bool Delete(IDbTransaction transaction, IDataObject obj);


        IEnumerable<T> AggregateLoad<T>(
            IDbTransaction transaction, 
            Expression<Func<T, bool>> cnd = null, int? skip = null, int? limit = null, 
            Expression<Func<T, object>> orderingMember = null, OrderingType otype = OrderingType.Asc,
            MemberInfo GroupingMember = null,
            bool Linear = false) where T : IDataObject, new();

    }
}
