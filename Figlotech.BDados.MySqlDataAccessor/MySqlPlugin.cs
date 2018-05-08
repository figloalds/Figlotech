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
    public class MySqlPlugin : IRdbmsPluginAdapter {
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

        public IDbConnection GetNewConnection() {
            return new MySqlConnection(Config.GetConnectionString());
        }

        static long idGen = 0;
        long myId = ++idGen;
        
        public void SetConfiguration(IDictionary<string, object> settings) {
            Config = new MySqlPluginConfiguration();
            ObjectReflector o = new ObjectReflector(Config);
            foreach (var a in settings) {
                o[a.Key] = a.Value;
            }
        }
    }
}
