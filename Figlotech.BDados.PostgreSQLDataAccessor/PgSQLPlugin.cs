using Figlotech.BDados.DataAccessAbstractions;
using System;
using System.Data;
using System.Collections.Generic;
using Figlotech.Core.Helpers;
using System.Linq;
using Figlotech.Core;
using System.Reflection;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using Figlotech.BDados;
using System.Text.RegularExpressions;
using Npgsql;

namespace Figlotech.BDados.PgSQLDataAccessor {
    public class PgSQLPlugin : IRdbmsPluginAdapter {
        public PgSQLPlugin(PgSQLPluginConfiguration cfg) {
            Config = cfg;
        }

        public PgSQLPlugin() {
        }

        public IQueryGenerator QueryGenerator { get; } = new PgSQLQueryGenerator();

        public PgSQLPluginConfiguration Config { get; set; }

        public bool ContinuousConnection => Config.ContinuousConnection;

        public int CommandTimeout => Config.Timeout;

        public string SchemaName => Config.Database;

        public IDbConnection GetNewConnection() {
            return new NpgsqlConnection(Config.GetConnectionString());
        }

        public IDbConnection GetNewSchemalessConnection() {
            var schemalessConfig = new PgSQLPluginConfiguration();
            schemalessConfig.CopyFrom(Config);
            schemalessConfig.Database = null;
            return new NpgsqlConnection(schemalessConfig.GetConnectionString());
        }

        static long idGen = 0;
        long myId = ++idGen;

        public void SetConfiguration(IDictionary<string, object> settings) {
            Config = new PgSQLPluginConfiguration();
            ObjectReflector o = new ObjectReflector(Config);
            foreach (var a in settings) {
                o[a.Key] = a.Value;
            }
        }

        public object ProcessParameterValue(object value) {
            if(value == null) {
                return DBNull.Value;
            } else
            if (value is UInt64) {
                return value.ToString();
            } else
            if (value.GetType().IsEnum) {
                return (int) value;
            } else
                return value;
        }
    }
}
