using ErpSoftLeader;
using ErpSoftLeader.Models;
using Figlotech.BDados.Builders;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.MySqlDataAccessor;
using Figlotech.Core;
using Figlotech.Core.Autokryptex;
using Figlotech.Core.Extensions;
using Figlotech.Core.FileAcessAbstractions;
using Figlotech.Core.Helpers;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;

namespace Figlotech.BDados.Tests
{

    [TestClass]
    public class SelAlLTest
    {
        [TestMethod]
        public void TestShit()
        {
            var da = new RdbmsDataAccessor<MySqlPlugin>(
                new Settings {
                    { "Database", "erpsl" },
                    { "Host", "localhost" },
                    { "User", "root" },
                    { "Password", "asdafe1025" },
                });
            var u = da.LoadAll<Usuarios>(a => a.Login == "suporte").FirstOrDefault();
            var sess = new Sessoes();
            sess.Usuario = u.RID;
            sess.Token = new RID().AsBase36;
            sess.Ip = "asdfasdf";
            da.SaveItem(sess);
            da.LoadAll<Permissoes>(p => p.Usuario == u.RID);
        }

        [TestMethod]
        public async void fileAccessormustNotbeShit()
        {
            var fs = new FileAccessor(".");
            var context = "a";
            WorkQueuer wq = new WorkQueuer("Test", 4, true);
            for (int i = 0; i < 10; i++)
            {
                wq.Enqueue(() =>
                {
                    fs.Write(".test.txt", (stream) =>
                    {
                        using (var sw = new StreamWriter(stream))
                            sw.Write(context);
                    });
                });
                wq.Enqueue(() =>
                {
                    fs.Read(".test.txt", (stream) =>
                    {
                        using (var sw = new StreamReader(stream))
                            context = sw.ReadToEnd();
                    });
                });
            }
            await wq.Stop();
        }

        class sc : IMultiSerializableObject
        {
            public int i = new Random().Next();
        }

        [TestMethod]
        public void testmserializable()
        {
            var s = new sc();
            var sVal = s.i;
            s.ToJsonFile(new FileAccessor("."), "file.json", new FTHSerializableOptions
            {
                UseEncryption = new BinaryBlur(),
                UseGzip = true
            });
            s.i = 1234;
            s.FromJsonFile(new FileAccessor("."), "file.json", new FTHSerializableOptions
            {
                UseEncryption = new BinaryBlur(),
                UseGzip = true
            });

            var nVal = s.i;
            Assert.AreEqual(sVal, nVal);
        }

        [TestMethod]
        public void StrucheckShouldGatherInfo() {
            var da = new RdbmsDataAccessor<MySqlPlugin>(
                   new Settings {
                    { "Database", "erpsl" },
                    { "Host", "localhost" },
                    { "User", "root" },
                    { "Password", "asdafe1025" },
                   });
            var NEA = new StructureChecker(da, typeof(GlobalBusiness).Assembly.GetTypes()).EvaluateNecessaryActions().ToList();
            //new StructureChecker(da, typeof(GlobalBusiness).Assembly.GetTypes()).CheckStructure();
            Assert.IsTrue(NEA.Count > 0);
        }

        [TestMethod]
        public void daquerymustwork()
        {
            var da = new RdbmsDataAccessor<MySqlPlugin>(
                   new Settings {
                    { "Database", "erpsl" },
                    { "Host", "localhost" },
                    { "User", "root" },
                    { "Password", "asdafe1025" },
                   });
            var li = da.Query<Terminais>(new Qb("SELECT * FROM Terminais")).ToList();

            Assert.IsTrue(li.Count > 0);
        }

        [TestMethod]
        public void daquerymustwork2()
        {
            var da = new RdbmsDataAccessor<MySqlPlugin>(
                   new Settings {
                    { "Database", "erpsl" },
                    { "Host", "localhost" },
                    { "User", "root" },
                    { "Password", "asdafe1025" },
                   });
            var li = da.LoadFirstOrDefault<Pessoas>(
                new Conditions<Pessoas>(p=> true), 0,1, 
                p => p.Nome, OrderingType.Desc);
            Console.WriteLine(li);
        }

        [TestMethod]
        public void StreamProcessorsShouldWork()
        {
            var pro = new BatchStreamProcessor();
            var method = new BinaryScramble(451);
            pro.Add(new CypherStreamProcessor(method));
            pro.Add(new DecypherStreamProcessor(method));
            byte[] bytes = new byte[] { 0x10, 0x20, 0x30, 0x40 };
            MemoryStream ms = new MemoryStream(bytes);
            MemoryStream output = new MemoryStream(bytes);
            pro.Process(ms, input => input.CopyTo(output));

            var outbytes = output.ToArray();
            Assert.IsTrue(bytes.SequenceEqual(outbytes));
        }

        [TestMethod]
        public void RIDGeneratorMustWork()
        {
            for (int i = 0; i < 10000; i++)
            {
                Console.WriteLine(new RID().ToString());
            }
        }

        [TestMethod]
        public async void WorkQueuerShouldWork()
        {
            int a = 0;
            WorkQueuer wq = new WorkQueuer("wq", 4);
            wq.Start();
            for (int i = 0; i < 1000000; i++)
            {
                wq.Enqueue(() => {
                    Thread.Sleep(100);
                }, (x)=> {
                    throw x;
                });
            }
            await wq.Stop();
            Assert.IsTrue(a > 9000);
        }


        [TestMethod]
        public void IntExReliability()
        {
            Random rng = new Random();
            ulong a = (ulong)rng.Next();
            var b36a = new IntEx((long)a).ToString(IntEx.Base36);
            ulong _a = (ulong)new IntEx(b36a, IntEx.Base36).ToLong();
            Assert.AreEqual(a, _a);

            ulong b = (ulong)rng.Next();
            var b36b = new IntEx(BitConverter.GetBytes(b)).ToString(IntEx.Base36);
            ulong _b = (ulong)new IntEx(b36b, IntEx.Base36).ToLong();
            Assert.AreEqual(b, _b);
        }


        [TestMethod]
        public void RIDGenerationShouldBeAwesome()
        {
            List<ulong> li = new List<ulong>();
            for (int i = 0; i < 5000; i++)
            {
                li.Add(new RID().AsULong);
            }

            for (int i = li.Count - 1; i > 0; i--)
            {
                for (int j = i - 1; j >= 0; j--)
                {
                    if (li[i] == li[j])
                        throw new Exception("RID GENERATOR IS DUPLICATING STUFF");
                }
            }
        }
    }
}
