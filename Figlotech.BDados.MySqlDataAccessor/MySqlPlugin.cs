using Figlotech.BDados.DataAccessAbstractions;
using MySql.Data.MySqlClient;
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

namespace Figlotech.BDados.MySqlDataAccessor {
    public sealed class MySqlPlugin : IRdbmsPluginAdapter {
        public MySqlPlugin(MySqlPluginConfiguration cfg) {
            Config = cfg;
        }

        public MySqlPlugin() {

        }

        public IQueryGenerator QueryGenerator { get; } = new MySqlQueryGenerator();

        public MySqlPluginConfiguration Config { get; set; }

        public bool ContinuousConnection => Config.ContinuousConnection;

        public int CommandTimeout => Config.Timeout;

        public string SchemaName => Config.Database;

        public Dictionary<string, string> InfoSchemaColumnsMap => new Dictionary<string, string>() {
            { "TABLE_NAME"              , "TABLE_NAME"                  },
            { "COLUMN_NAME"             , "COLUMN_NAME"                 },
            { "COLUMN_DEFAULT"          , "COLUMN_DEFAULT"              },
            { "IS_NULLABLE"             , "IS_NULLABLE"                 },
            { "DATA_TYPE"               , "DATA_TYPE"                   },
            { "CHARACTER_MAXIMUM_LENGTH", "CHARACTER_MAXIMUM_LENGTH"    },
            { "NUMERIC_PRECISION"       , "NUMERIC_PRECISION"           },
            { "CHARACTER_SET_NAME"      , "CHARACTER_SET_NAME"          },
            { "COLLATION_NAME"          , "COLLATION_NAME"              },
            { "COLUMN_COMMENT"          , "COLUMN_COMMENT"              },
            { "GENERATION_EXPRESSION"   , "GENERATION_EXPRESSION"       },
        };

        public IDbConnection GetNewConnection() {
            return new MySqlConnection(Config.GetConnectionString());
        }

        public string ConnectionString {
            get => Config.GetConnectionString();
        }

        public IDbConnection GetNewSchemalessConnection() {
            var schemalessConfig = new MySqlPluginConfiguration();
            schemalessConfig.CopyFrom(Config);
            schemalessConfig.Database = null;
            MySqlDataReader m;
            return new MySqlConnection(schemalessConfig.GetConnectionString());
        }

        static long idGen = 0;
        long myId = ++idGen;
        
        public void SetConfiguration(IDictionary<string, object> settings) {
            Config = new MySqlPluginConfiguration();
            foreach (var a in settings) {
                ReflectionTool.SetValue(Config, a.Key, a.Value);
            }
        }

        public object ProcessParameterValue(object value) {
            return value;
        }
    }
}
