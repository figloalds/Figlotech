using AppRostoJovem.Backend.Models;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using Figlotech.BDados.MySqlDataAccessor;
using Figlotech.Core;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Tests
{

    [TestClass]
    public class SelAlLTest {
        [TestMethod]
        public void TestShit() {
            var da = new RdbmsDataAccessor<MySqlPlugin>(
                new DataAccessorConfiguration {
                    Database = "erpsl",
                    Host = "localhost",
                    User = "root",
                    Password = "asdafe1025",
                });

            var retv = da.CheckStructure(typeof(Pessoas).Assembly);

            Pessoas pe = new Pessoas {
                Nome = "Figloald_"
            };
            da.SaveItem(pe);
            Assert.IsTrue(retv);

        }

        [TestMethod]
        public void RIDGeneratorMustWork() {
            for(int i = 0; i < 10000; i++) {
                Console.WriteLine(new RID().ToString());
            }

        }
    }
}
