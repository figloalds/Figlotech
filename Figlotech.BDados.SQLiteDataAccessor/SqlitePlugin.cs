using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.Core.Helpers;
using Microsoft.Data.Sqlite;
using System;
using System.Collections.Generic;
using System.Data;

namespace Figlotech.BDados.SqliteDataAccessor {
    public sealed class SqlitePlugin : IRdbmsPluginAdapter {
        private static readonly IReadOnlyDictionary<string, string> InfoSchemaColumns = new Dictionary<string, string>() {
            { "TABLE_NAME"              , "TABLE_NAME"                  },
            { "COLUMN_NAME"             , "COLUMN_NAME"                 },
            { "COLUMN_DEFAULT"          , "COLUMN_DEFAULT"              },
            { "IS_NULLABLE"             , "IS_NULLABLE"                 },
            { "DATA_TYPE"               , "DATA_TYPE"                 },
            { "CHARACTER_MAXIMUM_LENGTH", "CHARACTER_MAXIMUM_LENGTH"    },
            { "NUMERIC_PRECISION"       , "NUMERIC_PRECISION"           },
            { "CHARACTER_SET_NAME"      , "CHARACTER_SET_NAME"          },
            { "COLLATION_NAME"          , "COLLATION_NAME"              },
            { "COLUMN_COMMENT"          , "COLUMN_COMMENT"              },
            { "GENERATION_EXPRESSION"   , "GENERATION_EXPRESSION"       },
        };

        public SqlitePlugin(SqlitePluginConfiguration cfg) {
            Config = cfg;
        }

        public SqlitePlugin() {

        }

        public IQueryGenerator QueryGenerator { get; } = new SqliteQueryGenerator();

        public SqlitePluginConfiguration Config { get; set; }

        public bool ContinuousConnection => true;

        public int PoolSize => Config.PoolSize;
        public TimeSpan CommandTimeout => TimeSpan.FromSeconds(30);
        public TimeSpan ConnectTimeout => TimeSpan.FromSeconds(5);

        public string SchemaName => Config.Schema;
        public string DatabaseHost => Config.DataSource;

        public IReadOnlyDictionary<string, string> InfoSchemaColumnsMap => InfoSchemaColumns;

        public IDbConnection GetNewConnection() {
            return new SqliteConnection(Config.GetConnectionString());
        }

        public string ConnectionString {
            get => Config.GetConnectionString();
        }

        public IDbConnection GetNewSchemalessConnection() {
            return new SqliteConnection(Config.GetConnectionString());
        }

        static long idGen = 0;
        readonly long myId = ++idGen;

        public void SetConfiguration(IDictionary<string, object> settings) {
            Config = new SqlitePluginConfiguration();
            foreach (var a in settings) {
                ReflectionTool.SetValue(Config, a.Key, a.Value);
            }
        }

        public object ProcessParameterValue(object value) {
            return value;
        }
    }
}
