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

        void Access(Action<IDbConnection> tryFun, Action<Exception> catchFun = null);
        T Access<T>(Func<IDbConnection, T> tryFun, Action<Exception> catchFun = null);

        RecordSet<T> LoadAll<T>(string where = "TRUE", params object[] args) where T : IDataObject, new();
        RecordSet<T> LoadAll<T>(IQueryBuilder condicoes = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new();

        IEnumerable<T> Fetch<T>(string where = "TRUE", params object[] args) where T : IDataObject, new();
        IEnumerable<T> Fetch<T>(IQueryBuilder condicoes = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new();

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

        DataTable Query(IDbConnection connection, IQueryBuilder Query);
        IEnumerable<T> Query<T>(IDbConnection connection, IQueryBuilder Query = null) where T : new();
        int Execute(IDbConnection connection, IQueryBuilder Query);
        IEnumerable<T> Fetch<T>(IDbConnection connection, IQueryBuilder condicoes = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new();
        RecordSet<T> LoadAll<T>(IDbConnection connection, IQueryBuilder condicoes = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new();

        bool SaveItem(IDbConnection connection, IDataObject objeto, Action funcaoPosSalvar = null);
    }
}
