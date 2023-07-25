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
using Figlotech.Data;
using System.Threading;

namespace Figlotech.BDados.DataAccessAbstractions {
    
    public interface IRdbmsDataAccessor : IDataAccessor
    {
        event Action<Type, IDataObject[]> OnSuccessfulSave;
        event Action<Type, IDataObject[], Exception> OnFailedSave;
        event Action<Type, IDataObject[]> OnDataObjectAltered;

        ValueTask EnsureDatabaseExistsAsync();

        int DefaultQueryLimit { get; set; }
        void Access(Action<BDadosTransaction> tryFun, IsolationLevel ilev = IsolationLevel.ReadUncommitted);
        T Access<T>(Func<BDadosTransaction, T> tryFun, IsolationLevel ilev = IsolationLevel.ReadUncommitted);
        ValueTask AccessAsync(Func<BDadosTransaction, ValueTask> tryFun, CancellationToken cancellationToken, IsolationLevel ilev = IsolationLevel.ReadUncommitted);
        ValueTask<T> AccessAsync<T>(Func<BDadosTransaction, ValueTask<T>> tryFun, CancellationToken cancellationToken, IsolationLevel ilev = IsolationLevel.ReadUncommitted);

        List<T> LoadAll<T>(IQueryBuilder condicoes = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc, object contextObject = null) where T : IDataObject, new();
        
        IEnumerable<T> Fetch<T>(IQueryBuilder condicoes = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc, object contextObject = null) where T : IDataObject, new();

        T ForceExist<T>(Func<T> Default, IQueryBuilder qb) where T : IDataObject, new();

        List<IDataObject> LoadUpdatedItemsSince(IEnumerable<Type> types, DateTime dt);

        List<T> Query<T>(IQueryBuilder Query = null) where T : new();

        DataTable Query(IQueryBuilder Query);

        String SchemaName { get; }

        IQueryGenerator QueryGenerator { get; }

        int Execute(IQueryBuilder Query);
        ValueTask<int> ExecuteAsync(BDadosTransaction transaction, IQueryBuilder query);
        ValueTask UpdateAsync<T>(BDadosTransaction transaction, T input, params (Expression<Func<T, object>> parameterExpression, object Value)[] updates) where T : IDataObject;
        ValueTask UpdateAndMutateAsync<T>(BDadosTransaction transaction, T input, params (Expression<Func<T, object>> parameterExpression, object Value)[] updates) where T : IDataObject;

        IRdbmsDataAccessor Fork();

        BDadosTransaction CreateNewTransaction(IsolationLevel ilev, CancellationToken cancellationToken, Benchmarker bmark = null);

        void SendLocalUpdates(BDadosTransaction transaction, IEnumerable<Type> types, DateTime dt, Stream stream);
        void ReceiveRemoteUpdatesAndPersist(BDadosTransaction transaction, IEnumerable<Type> types, Stream stream);
        void SendLocalUpdates(IEnumerable<Type> types, DateTime dt, Stream stream);
        void ReceiveRemoteUpdatesAndPersist(IEnumerable<Type> types, Stream stream);

        IEnumerable<IDataObject> ReceiveRemoteUpdates(IEnumerable<Type> types, Stream stream);

        List<IDataObject> LoadUpdatedItemsSince(BDadosTransaction transaction, IEnumerable<Type> types, DateTime dt);

        DataTable Query(BDadosTransaction transaction, IQueryBuilder Query);
        ValueTask<List<T>> QueryAsync<T>(BDadosTransaction transaction, IQueryBuilder Query = null) where T : new();
        List<T> Query<T>(BDadosTransaction transaction, IQueryBuilder Query = null) where T : new();
        IEnumerable<T> QueryCoroutinely<T>(BDadosTransaction transaction, IQueryBuilder query) where T : new();
        int Execute(BDadosTransaction transaction, IQueryBuilder Query);
        IAsyncEnumerable<T> FetchAsync<T>(BDadosTransaction transaction, IQueryBuilder condicoes = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc, object contextObject = null) where T : IDataObject, new();
        IEnumerable<T> Fetch<T>(BDadosTransaction transaction, IQueryBuilder condicoes = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc, object contextObject = null) where T : IDataObject, new();
        ValueTask<List<T>> LoadAllAsync<T>(BDadosTransaction transaction, IQueryBuilder condicoes = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc, object contextObject = null) where T : IDataObject, new();
        List<T> LoadAll<T>(BDadosTransaction transaction, IQueryBuilder condicoes = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc, object contextObject = null) where T : IDataObject, new();

        bool SaveItem(BDadosTransaction transaction, IDataObject input);
        ValueTask<bool> SaveItemAsync(BDadosTransaction transaction, IDataObject input);
        bool SaveList<T>(BDadosTransaction transaction, List<T> target, bool recoverIds = false) where T : IDataObject;
        ValueTask<bool> SaveListAsync<T>(BDadosTransaction transaction, List<T> target, bool recoverIds = false) where T : IDataObject;

        T LoadFirstOrDefault<T>(BDadosTransaction transaction, LoadAllArgs<T> args = null) where T : IDataObject, new();
        T LoadByRid<T>(BDadosTransaction transaction, String RID) where T : IDataObject, new();
        T LoadById<T>(BDadosTransaction transaction, long Id) where T : IDataObject, new();

        T ForceExist<T>(BDadosTransaction transaction, Func<T> Default, IQueryBuilder qb) where T : IDataObject, new();
        ValueTask<List<T>> LoadAllAsync<T>(BDadosTransaction transaction, LoadAllArgs<T> args = null) where T : IDataObject, new();
        List<T> LoadAll<T>(BDadosTransaction transaction, LoadAllArgs<T> args = null) where T : IDataObject, new();
        IAsyncEnumerable<T> FetchAsync<T>(BDadosTransaction transaction, LoadAllArgs<T> args = null) where T : IDataObject, new();
        IEnumerable<T> Fetch<T>(BDadosTransaction transaction, LoadAllArgs<T> args = null) where T : IDataObject, new();

        bool DeleteWhereRidNotIn<T>(BDadosTransaction transaction, Expression<Func<T, bool>> cnd, List<T> rids) where T : IDataObject, new();
        bool Delete<T>(BDadosTransaction transaction, Expression<Func<T, bool>> condition) where T : IDataObject, new();
        bool Delete(BDadosTransaction transaction, IDataObject obj);
        bool Delete<T>(IEnumerable<T> obj) where T : IDataObject, new();
        bool Delete<T>(BDadosTransaction transaction, IEnumerable<T> obj) where T : IDataObject, new();

        ValueTask<List<T>> AggregateLoadAsync<T>(
            BDadosTransaction transaction,
            LoadAllArgs<T> args = null) where T : IDataObject, new();
        List<T> AggregateLoad<T>(
            BDadosTransaction transaction, 
            LoadAllArgs<T> args = null) where T : IDataObject, new();

        List<FieldAttribute> GetInfoSchemaColumns();
    }
}
