using Figlotech.BDados.DataAccessAbstractions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SoftLeader.Sistema.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Tests
{
    [TestClass]
    public class MySqlDaTests {
        [TestMethod]
        public void StructureCheckShouldWork() {
            var types = new Type[] { typeof(client) };
            var da = new MySqlDataAccessor(
                new DataAccessorConfiguration {
                    Host = "localhost",
                    Database = "fth_tests",
                    User = "root",
                    Password = "",
                }
            );
            da.CheckStructure(types);
        }

        [TestMethod]
        public void LoadAllShouldWork() {
            var da = new MySqlDataAccessor(
                new DataAccessorConfiguration {
                    Host = "localhost",
                    Database = "sistema",
                    User = "root",
                    Password = "asdafe1025",
                }
            );
            comanda comanda = new comanda() {
                cm_cartao = "asdf",
                cm_status = "A",
                cm_abertura = DateTime.Now
            };
            da.SaveItem(comanda);

        }
    }
}
