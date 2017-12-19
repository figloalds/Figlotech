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
            FiTechCoreExtensions.EnableStdoutLogs = true;
            var da = new RdbmsDataAccessor<MySqlPlugin>(
                   new Settings {
                    { "Database", "erpsl" },
                    { "Host", "localhost" },
                    { "User", "root" },
                    { "Password", "asdafe1025" },
                   });
            RecordSet<ComercialContratosAluguel> li = null;
            for(int i = 0; i < 100; i++) {
                li = new RecordSet<ComercialContratosAluguel>(da)
                    //.OrderBy(p => p.IdExterno, OrderingType.Desc)
                    .LoadAll(new Conditions<ComercialContratosAluguel>(p => true), 0, 12);
                Console.WriteLine();
            }
            
            Console.WriteLine(li);
        }
    }
}
