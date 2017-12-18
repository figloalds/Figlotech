using Figlotech.BDados.DataAccessAbstractions;
using Microsoft.Data.Sqlite;
using System;
using System.Data;
using System.Collections.Generic;
using Figlotech.Core.Helpers;

namespace Figlotech.BDados.SqliteDataAccessor {
    public class SqlitePlugin : IRdbmsPluginAdapter {
        public SqlitePlugin(SqlitePluginConfiguration cfg) {
            Config = cfg;
        }

        public SqlitePlugin() {
            
        }

        IQueryGenerator queryGenerator = new SqliteQueryGenerator();
        public IQueryGenerator QueryGenerator => queryGenerator;

        public SqlitePluginConfiguration Config { get; set; }

        public bool ContinuousConnection => true;

        public int CommandTimeout => throw new NotImplementedException();

        public string SchemaName => Config.Schema;

        public IDbConnection GetNewConnection() {
            return new SqliteConnection(Config.GetConnectionString());
        }

        static long idGen = 0;
        long myId = ++idGen;

        public DataSet GetDataSet(IDbCommand command) {
            //lock(command) {
            using (var reader = (command as SqliteCommand).ExecuteReader()) {
                DataTable dt = new DataTable();
                for (int i = 0; i < reader.FieldCount; i++) {
                    dt.Columns.Add(new DataColumn(reader.GetName(i)));
                }

                while (reader.Read()) {
                    var dr = dt.NewRow();
                    for (int i = 0; i < reader.FieldCount; i++) {
                        var type = reader.GetFieldType(i);
                        if (reader.IsDBNull(i)) {
                            dr[i] = null;
                        } else {
                            var val = reader.GetValue(i);
                            dr[i] = Convert.ChangeType(val, type);
                        }
                    }
                    dt.Rows.Add(dr);
                }
                var ds = new DataSet();
                ds.Tables.Add(dt);
                return ds;
                //}
            }
        }

        public void SetConfiguration(IDictionary<string, object> settings) {
            Config = new SqlitePluginConfiguration();
            ObjectReflector o = new ObjectReflector(Config);
            foreach (var a in settings) {
                o[a.Key] = a.Value;
            }
        }
    }
}
