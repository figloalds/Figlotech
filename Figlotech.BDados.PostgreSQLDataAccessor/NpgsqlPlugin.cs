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

namespace Figlotech.BDados.NpgsqlDataAccessor {
    public class NpgsqlPlugin : IRdbmsPluginAdapter {
        public NpgsqlPlugin(NpgsqlPluginConfiguration cfg) {
            Config = cfg;
        }

        public NpgsqlPlugin() {
        }

        public IQueryGenerator QueryGenerator { get; } = new NpgsqlQueryGenerator();

        public NpgsqlPluginConfiguration Config { get; set; }

        public bool ContinuousConnection => Config.ContinuousConnection;

        public int CommandTimeout => Config.Timeout;

        public string SchemaName => Config.Database;

        public IDbConnection GetNewConnection() {
            return new NpgsqlConnection(Config.GetConnectionString());
        }

        static long idGen = 0;
        long myId = ++idGen;

        public void SetConfiguration(IDictionary<string, object> settings) {
            Config = new NpgsqlPluginConfiguration();
            ObjectReflector o = new ObjectReflector(Config);
            foreach (var a in settings) {
                o[a.Key] = a.Value;
            }
        }
    }
}
