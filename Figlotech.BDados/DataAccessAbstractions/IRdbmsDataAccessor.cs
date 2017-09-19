using Figlotech.BDados.Builders;
using Figlotech.BDados.DataAccessAbstractions;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.DataAccessAbstractions {
    public interface IRdbmsDataAccessor : IDataAccessor
    {

        Object Access(Action funcaoAcessar, Action<Exception> trataErros = null);

        RecordSet<T> LoadAll<T>(string where = "TRUE", params object[] args) where T : IDataObject, new();
        RecordSet<T> LoadAll<T>(IQueryBuilder condicoes) where T : IDataObject, new();

        T ForceExist<T>(Func<T> Default, String query, params object[] args) where T : IDataObject, new();
        T ForceExist<T>(Func<T> Default, IQueryBuilder qb) where T : IDataObject, new();

        List<T> Query<T>(IQueryBuilder Query) where T : new();
        List<T> Query<T>(String Query, params object[] args) where T : new();

        DataTable Query(IQueryBuilder Query);
        DataTable Query(String Query, params object[] args);

        String SchemaName { get; }

        IQueryGenerator QueryGenerator { get; }

        IJoinBuilder MakeJoin(Action<JoinDefinition> fn);

        int Execute(IQueryBuilder Query);
        int Execute(String Query, params object[] args);

    }
}
