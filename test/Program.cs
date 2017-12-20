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
            li = new RecordSet<ComercialContratosAluguel>(da)
                //.OrderBy(p => p.IdExterno, OrderingType.Desc)
                .LoadAll(new Conditions<ComercialContratosAluguel>(p => true), 0, 50);
            for (int i = 0; i < li.Count; i++) {
                var lii = li[i];
                var val = new RecordSet<ComercialContratosAluguel>(da).LoadAll(c=> c.RID == lii.RID);
                var val2 = new RecordSet<ComercialContratosAluguel>(da).LoadAll(c => c.Id == lii.Id);
            }
            
            Console.WriteLine(li);
        }
    }
}
