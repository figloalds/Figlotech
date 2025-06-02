using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace Figlotech.BDados.DataAccessAbstractions
{
    public interface IRdbmsPluginAdapter {
        IDbConnection GetNewConnection();
        IDbConnection GetNewSchemalessConnection();
        IQueryGenerator QueryGenerator { get; }
        void SetConfiguration(IDictionary<string, object> a);
        bool ContinuousConnection { get; }
        int CommandTimeout { get; }
        int ConnectTimeout { get; }
        int PoolSize { get; }
        string SchemaName { get; }
        string ConnectionString { get; }
        Dictionary<string, string> InfoSchemaColumnsMap { get; }

        object ProcessParameterValue(object value);
    }
}
