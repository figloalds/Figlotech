using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.Core.Helpers;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;

namespace Figlotech.BDados.PgSQLDataAccessor {
    public sealed class PgSQLPlugin : IRdbmsPluginAdapter {
        public PgSQLPlugin(PgSQLPluginConfiguration cfg) {
            Config = cfg;
        }

        public PgSQLPlugin() {
        }

        public IQueryGenerator QueryGenerator { get; } = new PgSQLQueryGenerator();

        public PgSQLPluginConfiguration Config { get; set; }

        public bool ContinuousConnection => Config.ContinuousConnection;

        public int CommandTimeout => Config.Timeout;
        public int ConnectTimeout => Config.Timeout;

        public int PoolSize => Config.PoolSize;
        public string SchemaName => Config.Database;
        public string DatabaseHost => Config.Host;

        public Dictionary<string, string> InfoSchemaColumnsMap => new Dictionary<string, string>() {
            { "TABLE_NAME"              , "TABLE_NAME"                  },
            { "COLUMN_NAME"             , "COLUMN_NAME"                 },
            { "COLUMN_DEFAULT"          , "COLUMN_DEFAULT"              },
            { "IS_NULLABLE"             , "IS_NULLABLE"                 },
            { "DATA_TYPE"               , "UDT_NAME"                    },
            { "CHARACTER_MAXIMUM_LENGTH", "CHARACTER_MAXIMUM_LENGTH"    },
            { "NUMERIC_PRECISION"       , "NUMERIC_PRECISION"           },
            { "CHARACTER_SET_NAME"      , "CHARACTER_SET_NAME"          },
            { "COLLATION_NAME"          , "COLLATION_NAME"              },
            { "COLUMN_COMMENT"          , "COLUMN_COMMENT"              },
            { "GENERATION_EXPRESSION"   , "GENERATION_EXPRESSION"       },
        };

        public IDbConnection GetNewConnection() {
            return new NpgsqlConnection(Config.GetConnectionString());
        }

        public string ConnectionString {
            get => Config.GetConnectionString();
        }

        public IDbConnection GetNewSchemalessConnection() {
            var schemalessConfig = new PgSQLPluginConfiguration();
            schemalessConfig.CopyFrom(Config);
            schemalessConfig.Database = null;
            return new NpgsqlConnection(schemalessConfig.GetConnectionString());
        }

        static long idGen = 0;
        readonly long myId = ++idGen;

        public void SetConfiguration(IDictionary<string, object> settings) {
            Config = new PgSQLPluginConfiguration();
            foreach (var a in settings) {
                ReflectionTool.SetValue(Config, a.Key, a.Value);
            }
        }

        public object ProcessParameterValue(object value) {
            if (value == null) {
                return DBNull.Value;
            } else
                if (value is UInt64) {
                    return value.ToString();
                } else
                    if (value.GetType().IsEnum) {
                        return (int)value;
                    } else
                        return value;
        }
    }
}
