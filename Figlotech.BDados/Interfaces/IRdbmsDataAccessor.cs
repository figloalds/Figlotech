using Figlotech.BDados.Builders;
using Figlotech.BDados.Entity;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Interfaces {
    public interface IRdbmsDataAccessor : IDataAccessor {
        bool CheckStructure(IEnumerable<Type> types, bool resetKeys);

        Object Access(Action<IRdbmsDataAccessor> funcaoAcessar, Action<Exception> trataErros = null);

        RecordSet<T> LoadAll<T>(string where = "TRUE", params object[] args) where T : IDataObject, new() ;
        RecordSet<T> LoadAll<T>(IQueryBuilder condicoes) where T : IDataObject, new() ;

        T ForceExist<T>(Func<T> Default, String query, params object[] args) where T : IDataObject, new();
        T ForceExist<T>(Func<T> Default, IQueryBuilder qb) where T : IDataObject, new();

        List<T> Query<T>(IQueryBuilder Query);
        List<T> Query<T>(String Query, params object[] args);

        DataTable Query(IQueryBuilder Query);
        DataTable Query(String Query, params object[] args);

        String SchemaName { get; }

        IJoinBuilder MakeJoin(Action<Entity.JoinDefinition> fn);

        IQueryBuilder GetPreferredQueryBuilder();
        IQueryGenerator GetQueryGenerator();

        int Execute(IQueryBuilder Query);
        int Execute(String Query, params object[] args);

    }
}
