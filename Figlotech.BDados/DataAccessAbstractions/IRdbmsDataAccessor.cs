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
using Figlotech.Core.Interfaces;
using System.IO;
using Figlotech.Core;
using Figlotech.BDados.DataAccessAbstractions.Attributes;

namespace Figlotech.BDados.DataAccessAbstractions {
    
    public interface IRdbmsDataAccessor : IDataAccessor
    {
        event Action<Type, IDataObject[]> OnSuccessfulSave;
        event Action<Type, IDataObject[], Exception> OnFailedSave;
        event Action<Type, IDataObject[]> OnDataObjectAltered;

        void EnsureDatabaseExists();

        int DefaultQueryLimit { get; set; }
        void Access(Action<ConnectionInfo> tryFun, Action<Exception> catchFun = null, IsolationLevel ilev = IsolationLevel.ReadUncommitted);
        T Access<T>(Func<ConnectionInfo, T> tryFun, Action<Exception> catchFun = null, IsolationLevel ilev = IsolationLevel.ReadUncommitted);

        List<T> LoadAll<T>(string where = "TRUE", params object[] args) where T : IDataObject, new();
        List<T> LoadAll<T>(IQueryBuilder condicoes = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc, object contextObject = null) where T : IDataObject, new();

        IEnumerable<T> Fetch<T>(string where = "TRUE", params object[] args) where T : IDataObject, new();
        IEnumerable<T> Fetch<T>(IQueryBuilder condicoes = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc, object contextObject = null) where T : IDataObject, new();

        //T ForceExist<T>(Func<T> Default, String query, params object[] args) where T : IDataObject, new();
        T ForceExist<T>(Func<T> Default, IQueryBuilder qb) where T : IDataObject, new();

        List<IDataObject> LoadUpdatedItemsSince(IEnumerable<Type> types, DateTime dt);

        List<T> Query<T>(IQueryBuilder Query = null) where T : new();
        List<T> Query<T>(String Query, params object[] args) where T : new();

        DataTable Query(IQueryBuilder Query);
        DataTable Query(String Query, params object[] args);

        String SchemaName { get; }

        IQueryGenerator QueryGenerator { get; }

        //IJoinBuilder MakeJoin(Action<JoinDefinition> fn);

        int Execute(IQueryBuilder Query);
        int Execute(String Query, params object[] args);

        IRdbmsDataAccessor Fork();

        IDbConnection BeginTransaction(IsolationLevel ilev = IsolationLevel.ReadUncommitted, Benchmarker bmark = null);

        void EndTransaction();

        void Commit();
        void Rollback();

        void SendLocalUpdates(ConnectionInfo transaction, IEnumerable<Type> types, DateTime dt, Stream stream);
        void ReceiveRemoteUpdatesAndPersist(ConnectionInfo transaction, IEnumerable<Type> types, Stream stream);
        void SendLocalUpdates(IEnumerable<Type> types, DateTime dt, Stream stream);
        void ReceiveRemoteUpdatesAndPersist(IEnumerable<Type> types, Stream stream);

        IEnumerable<IDataObject> ReceiveRemoteUpdates(IEnumerable<Type> types, Stream stream);


        List<IDataObject> LoadUpdatedItemsSince(ConnectionInfo transaction, IEnumerable<Type> types, DateTime dt);

        DataTable Query(ConnectionInfo transaction, IQueryBuilder Query);
        List<T> Query<T>(ConnectionInfo transaction, IQueryBuilder Query = null) where T : new();
        int Execute(ConnectionInfo transaction, IQueryBuilder Query);
        IEnumerable<T> Fetch<T>(ConnectionInfo transaction, IQueryBuilder condicoes = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc, object contextObject = null) where T : IDataObject, new();
        List<T> LoadAll<T>(ConnectionInfo transaction, IQueryBuilder condicoes = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc, object contextObject = null) where T : IDataObject, new();

        bool SaveItem(ConnectionInfo transaction, IDataObject objeto);
        bool SaveList<T>(ConnectionInfo transaction, List<T> target, bool recoverIds = false) where T : IDataObject;

        T LoadFirstOrDefault<T>(ConnectionInfo transaction, LoadAllArgs<T> args = null) where T : IDataObject, new();
        T LoadByRid<T>(ConnectionInfo transaction, String RID) where T : IDataObject, new();
        T LoadById<T>(ConnectionInfo transaction, long Id) where T : IDataObject, new();

        //T ForceExist<T>(ConnectionInfo transaction, Func<T> Default, Conditions<T> cnd) where T : IDataObject, new();
        List<T> LoadAll<T>(ConnectionInfo transaction, LoadAllArgs<T> args = null) where T : IDataObject, new();
        IEnumerable<T> Fetch<T>(ConnectionInfo transaction, LoadAllArgs<T> args = null) where T : IDataObject, new();

        bool DeleteWhereRidNotIn<T>(ConnectionInfo transaction, Expression<Func<T, bool>> cnd, List<T> rids) where T : IDataObject, new();
        bool Delete<T>(ConnectionInfo transaction, Expression<Func<T, bool>> condition) where T : IDataObject, new();
        bool Delete(ConnectionInfo transaction, IDataObject obj);
        bool Delete<T>(IEnumerable<T> obj) where T : IDataObject, new();
        bool Delete<T>(ConnectionInfo transaction, IEnumerable<T> obj) where T : IDataObject, new();

        List<T> AggregateLoad<T>(
            ConnectionInfo transaction, 
            LoadAllArgs<T> args = null) where T : IDataObject, new();

        List<FieldAttribute> GetInfoSchemaColumns();
    }
}
