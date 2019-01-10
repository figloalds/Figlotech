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
        string SchemaName { get; }

    }
}
