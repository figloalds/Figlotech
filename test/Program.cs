using ErpSoftLeader.Models;
using Figlotech.BDados;
using Figlotech.BDados.Builders;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.MySqlDataAccessor;
using Figlotech.Core;
using Figlotech.Core.FileAcessAbstractions;
using Figlotech.Core.Helpers;
using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;

namespace test {
    class Program {
        static void Main(string[] args) {

            var da = new RdbmsDataAccessor<MySqlPlugin>(
                   new Settings {
                    { "Database", "erpsl" },
                    { "Host", "localhost" },
                    { "User", "root" },
                    { "Password", "asdafe1025" },
                   });
            RecordSet<Produtos> li = null;
            for(int i = 0; i < 100; i++) {
                li = new RecordSet<Produtos>(da)
                    .OrderBy(p => p.Descricao, OrderingType.Desc)
                    .LoadAll(new Conditions<Produtos>(p => true), 0, 200);
                foreach (var a in li)
                    da.SaveItem(a);
                da.SaveRecordSet(li);
            }
            
            Console.WriteLine(li);
        }
    }
}
