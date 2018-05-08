using Figlotech.BDados.DataAccessAbstractions;
using Microsoft.Data.Sqlite;
using System;
using System.Data;
using System.Collections.Generic;
using Figlotech.Core.Helpers;
using System.Linq;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using System.Reflection;
using Figlotech.Core;

namespace Figlotech.BDados.SqliteDataAccessor {
    public class SqlitePlugin : IRdbmsPluginAdapter {
        public SqlitePlugin(SqlitePluginConfiguration cfg) {
            Config = cfg;
        }

        public SqlitePlugin() {
            
        }

        public IQueryGenerator QueryGenerator { get; } = new SqliteQueryGenerator();

        public SqlitePluginConfiguration Config { get; set; }

        public bool ContinuousConnection => true;

        public int CommandTimeout => throw new NotImplementedException();

        public string SchemaName => Config.Schema;

        public IDbConnection GetNewConnection() {
            return new SqliteConnection(Config.GetConnectionString());
        }


        static long idGen = 0;
        long myId = ++idGen;

        public void SetConfiguration(IDictionary<string, object> settings) {
            Config = new SqlitePluginConfiguration();
            ObjectReflector o = new ObjectReflector(Config);
            foreach (var a in settings) {
                o[a.Key] = a.Value;
            }
        }
    }
}
