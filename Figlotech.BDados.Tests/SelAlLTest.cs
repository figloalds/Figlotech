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
            for (int i = 0; i < 10000; i++) {
                Console.WriteLine(new RID().ToString());
            }
        }

        [TestMethod]
        public void WorkQueuerShouldWork() {
            int a = 0;
            WorkQueuer wq = new WorkQueuer("wq", Environment.ProcessorCount);
            for (int i = 0; i < 10000; i++) {
                wq.Enqueue(() => a++);
            }
            wq.Start();
            wq.Stop();
            Assert.IsTrue(a > 9000);
        }


        [TestMethod]
        public void IntExReliability() {
            Random rng = new Random();
            ulong a = (ulong) rng.Next();
            var b36a = new IntEx((long) a).ToString(IntEx.Base36);
            ulong _a = (ulong)new IntEx(b36a, IntEx.Base36).ToLong();
            Assert.AreEqual(a, _a);

            ulong b = (ulong) rng.Next();
            var b36b = new IntEx(BitConverter.GetBytes(b)).ToString(IntEx.Base36);
            ulong _b = (ulong)new IntEx(b36b, IntEx.Base36).ToLong();
            Assert.AreEqual(b, _b);
        }


        [TestMethod]
        public void RIDGenerationShouldBeAwesome() {
            List<ulong> li = new List<ulong>();
            for(int i = 0; i < 5000; i++) {
                li.Add(new RID().AsULong);
            }

            for(int i = li.Count-1; i > 0; i--) {
                for(int j = i-1; j >= 0; j--) {
                    if (li[i] == li[j])
                        throw new Exception("RID GENERATOR IS DUPLICATING STUFF");
                }
            }
        }
    }
}
