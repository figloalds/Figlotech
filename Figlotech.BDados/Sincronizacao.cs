using Figlotech.BDados.Entity;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados {
    public class Sincronizacao {
        DataAccessorConfiguration ConfigBDados;
        public Sincronizacao(DataAccessorConfiguration config) {
            ConfigBDados = config;
        }

        private void write(String s, MemoryStream ms) {
            byte[] bytes = Encoding.UTF8.GetBytes(s);
            ms.Write(bytes, 0, bytes.Length);
        }

        public byte[] GerarArqSincronizacao(Assembly a, DateTime DataSync) {
            byte[] retv = new byte[0];
            using (MemoryStream ms = new MemoryStream()) {
                write("__BDADOS_SYNC", ms);
                List<Type> inst = new List<Type>();
                foreach(Type t in a.GetTypes()) {
                    if (t.BaseType == typeof(DataObject)) {
                        inst.Add(t);
                    }
                }
                List<Object> lo = new List<Object>();
                for(int i = 0; i < inst.Count; i++) {
                    Type t = inst[i];
                    var metodoCarregar = typeof(FTH).GetMethod("CarregarTodos", new Type[] { typeof(DataAccessorConfiguration), typeof(String), typeof(Object[])});
                    var referencia = metodoCarregar.MakeGenericMethod(t);
                    var serial = referencia.Invoke(null, new object[] {
                        ConfigBDados, "DataUpdate>@1 OR DataUpdate IS NULL", new Object[] { DataSync }
                    });

                    lo.Add(serial);
                }

                write(JsonConvert.SerializeObject(lo), ms);

                retv = ms.ToArray();
            }
            return retv;
        }
    }
}
