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
        event Action<Type, List<IDataObject>> OnSuccessfulSave;
        event Action<Type, List<IDataObject>, Exception> OnFailedSave;

        int DefaultQueryLimit { get; set; }
        void Access(Action<ConnectionInfo> tryFun, Action<Exception> catchFun = null, bool useTransaction = false);
        T Access<T>(Func<ConnectionInfo, T> tryFun, Action<Exception> catchFun = null, bool useTransaction = false);

        RecordSet<T> LoadAll<T>(string where = "TRUE", params object[] args) where T : IDataObject, new();
        RecordSet<T> LoadAll<T>(IQueryBuilder condicoes = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new();

        List<T> Fetch<T>(string where = "TRUE", params object[] args) where T : IDataObject, new();
        List<T> Fetch<T>(IQueryBuilder condicoes = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new();

        //T ForceExist<T>(Func<T> Default, String query, params object[] args) where T : IDataObject, new();
        T ForceExist<T>(Func<T> Default, IQueryBuilder qb) where T : IDataObject, new();

        List<T> Query<T>(IQueryBuilder Query = null) where T : new();
        List<T> Query<T>(String Query, params object[] args) where T : new();

        DataTable Query(IQueryBuilder Query);
        DataTable Query(String Query, params object[] args);

        String SchemaName { get; }

        IQueryGenerator QueryGenerator { get; }

        IJoinBuilder MakeJoin(Action<JoinDefinition> fn);

        int Execute(IQueryBuilder Query);
        int Execute(String Query, params object[] args);

        void BeginTransaction(bool useRdbmsTransaction = false, IsolationLevel ilev = IsolationLevel.ReadUncommitted);

        void EndTransaction();

        void Commit();
        void Rollback();

        DataTable Query(ConnectionInfo transaction, IQueryBuilder Query);
        List<T> Query<T>(ConnectionInfo transaction, IQueryBuilder Query = null) where T : new();
        int Execute(ConnectionInfo transaction, IQueryBuilder Query);
        List<T> Fetch<T>(ConnectionInfo transaction, IQueryBuilder condicoes = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new();
        RecordSet<T> LoadAll<T>(ConnectionInfo transaction, IQueryBuilder condicoes = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new();

        bool SaveItem(ConnectionInfo transaction, IDataObject objeto);
        bool SaveRecordSet<T>(ConnectionInfo transaction, RecordSet<T> target, bool recoverIds = false) where T : IDataObject, new();

        T LoadFirstOrDefault<T>(ConnectionInfo transaction, Expression<Func<T, bool>> condicoes, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new();
        T LoadByRid<T>(ConnectionInfo transaction, String RID) where T : IDataObject, new();
        T LoadById<T>(ConnectionInfo transaction, long Id) where T : IDataObject, new();

        //T ForceExist<T>(ConnectionInfo transaction, Func<T> Default, Conditions<T> cnd) where T : IDataObject, new();
        RecordSet<T> LoadAll<T>(ConnectionInfo transaction, Expression<Func<T, bool>> condicoes, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new();
        List<T> Fetch<T>(ConnectionInfo transaction, Expression<Func<T, bool>> condicoes, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new();

        bool DeleteWhereRidNotIn<T>(ConnectionInfo transaction, Expression<Func<T, bool>> cnd, RecordSet<T> rids) where T : IDataObject, new();
        bool Delete<T>(ConnectionInfo transaction, Expression<Func<T, bool>> condition) where T : IDataObject, new();
        bool Delete(ConnectionInfo transaction, IDataObject obj);


        List<T> AggregateLoad<T>(
            ConnectionInfo transaction, 
            Expression<Func<T, bool>> cnd = null, int? skip = null, int? limit = null, 
            Expression<Func<T, object>> orderingMember = null, OrderingType otype = OrderingType.Asc,
            MemberInfo GroupingMember = null,
            bool Linear = false) where T : IDataObject, new();

    }
}
