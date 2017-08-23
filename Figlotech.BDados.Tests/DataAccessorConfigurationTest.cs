using Figlotech.BDados.DataAccessAbstractions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Tests
{
    [TestClass]
    public class DataAccessorConfigurationTest
    {
        [TestMethod]
        public void ConfigurationShouldSaveAndLoad() {
            var cfg = new DataAccessorConfiguration {
                Host = "localhost",
                Database = "fth_tests",
                User = "root",
                Password = "",
                PoolSize = 100,
            };
            var rid = RID.GenerateRID();
            cfg.SaveToFile("data.fth", rid);

            var cfg2 = DataAccessorConfiguration.LoadFromFile("data.fth", rid);
            Assert.Equals(cfg2.Database, cfg.Database);
        }
    }
}
