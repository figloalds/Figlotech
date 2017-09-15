using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.MySqlDataAccessor;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SoftLeader.Sistema.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Tests {
    [TestClass]
    public class MySqlDaTests {
        [TestMethod]
        public void StructureCheckShouldWork() {
            var types = new Type[] { typeof(client) };
            var da = new RdbmsDataAccessor(
                new MySqlPlugin(
                    new DataAccessorConfiguration {
                        Host = "localhost",
                        Database = "fth_tests",
                        User = "root",
                        Password = "asdafe1025",
                    })
            );
            da.CheckStructure(types);
        }

        [TestMethod]
        public void LoadAllShouldWork() {
            var da = new RdbmsDataAccessor(
                new MySqlPlugin(
                    new DataAccessorConfiguration {
                        Host = "localhost",
                        Database = "fth_tests",
                        User = "root",
                        Password = "asdafe1025",
                    }));
            comanda comanda = new comanda() {
                cm_cartao = "asdf",
                cm_status = "A",
                cm_abertura = DateTime.Now
            };
            da.SaveItem(comanda);

        }
    }
}
