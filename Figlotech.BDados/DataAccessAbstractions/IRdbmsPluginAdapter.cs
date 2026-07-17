using System;
using System.Collections.Generic;
using System.Data;

namespace Figlotech.BDados.DataAccessAbstractions {
    public interface IRdbmsPluginAdapter {
        IDbConnection GetNewConnection();
        IDbConnection GetNewSchemalessConnection();
        IQueryGenerator QueryGenerator { get; }
        void SetConfiguration(IDictionary<string, object> a);
        [Obsolete("ContinuousConnection is not consumed by RdbmsDataAccessor and will be removed in a future version.")]
        bool ContinuousConnection { get; }
        /// <summary>Timeout for individual commands issued against the server.</summary>
        TimeSpan CommandTimeout { get; }
        /// <summary>Timeout for acquiring a connection slot from the accessor pool / establishing a connection.</summary>
        TimeSpan ConnectTimeout { get; }
        int PoolSize { get; }
        string SchemaName { get; }
        string DatabaseHost { get; }
        string ConnectionString { get; }
        IReadOnlyDictionary<string, string> InfoSchemaColumnsMap { get; }

        object ProcessParameterValue(object value);
    }
}
