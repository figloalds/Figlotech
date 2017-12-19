using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace Figlotech.BDados.DataAccessAbstractions
{
    public interface IRdbmsPluginAdapter {
        IDbConnection GetNewConnection();
        DataSet GetDataSet(IDbCommand command);
        IQueryGenerator QueryGenerator { get; }
        void SetConfiguration(IDictionary<string, object> a);
        bool ContinuousConnection { get; }
        int CommandTimeout { get; }
        string SchemaName { get; }

        List<T> BuildAggregateListDirect<T>(ConnectionInfo transaction, IDbCommand command, JoinDefinition join, int thisIndex) where T : IDataObject, new();

        List<T> GetObjectList<T>(IDbCommand command) where T : new();
    }
}
