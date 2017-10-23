using Figlotech.BDados.DataAccessAbstractions;
using MySql.Data.MySqlClient;
using System;
using System.Data;

namespace Figlotech.BDados.MySqlDataAccessor {
    public class MySqlPlugin : IRdbmsPluginAdapter {
        public MySqlPlugin(DataAccessorConfiguration cfg) {
            Config = cfg;
        }

        public MySqlPlugin() {
        }

        IQueryGenerator queryGenerator = new MySqlQueryGenerator();
        public IQueryGenerator QueryGenerator => queryGenerator;

        public DataAccessorConfiguration Config { get; set; }

        public IDbConnection GetNewConnection() {
            return new MySqlConnection(Config.GetConnectionString());
        }

        public DataSet GetDataSet(IDbCommand command) {
            using (var reader = (command as MySqlCommand).ExecuteReader() as MySqlDataReader) {
                DataTable dt = new DataTable();
                for (int i = 0; i < reader.FieldCount; i++) {
                    dt.Columns.Add(new DataColumn(reader.GetName(i)));
                }

                while(reader.Read()) {
                    var dr = dt.NewRow();
                    for (int i = 0; i < reader.FieldCount; i++) {
                        var type = reader.GetFieldType(i);
                        if(reader.IsDBNull(i)) {
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
            }
        }
    }
}
