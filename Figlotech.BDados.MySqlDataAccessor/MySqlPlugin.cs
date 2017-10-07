using Figlotech.BDados.DataAccessAbstractions;
using MySql.Data.MySqlClient;
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

        public DataAccessorConfiguration Config {get; set; }

        public IDbConnection GetNewConnection() {
            return new MySqlConnection(Config.GetConnectionString());
        }

        public IDbDataAdapter GetNewDataAdapter(IDbCommand command) {

            return new MySqlDataAdapter(command as MySqlCommand);
        }
    }
}
