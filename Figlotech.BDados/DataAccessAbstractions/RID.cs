using Figlotech.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.DataAccessAbstractions
{
    public class RID
    {
        String value;
        public RID() {
            value = GenerateRID();
        }
        public RID(String a) {
            value = a;
        }

        public override string ToString() {
            if (value == null) {
                value = GenerateRID();
            }
            return value;
        }

        public static implicit operator RID(String a) {
            return new RID(a);
        }

        public static implicit operator String(RID a) {
            if (a.value == null) {
                a.value = GenerateRID();
            }
            return a.value;
        }

        private static Random r = new Random();
        private static int sequentia = 0;
        public static String GenerateRID() {
            IntEx i = new IntEx((DateTime.Now.Ticks * 10000) + (sequentia++ % 10000));
            i *= 100000000;
            i *= r.Next(100000000);
            i *= new IntEx(FTH.CpuId, IntEx.Hexadecimal);
            //i += cpuhash;
            return FillBlanks((String)i.ToString(IntEx.Base36));
        }
        private static string FillBlanks(String rid) {
            var c = 64 - rid.Length;
            return FTH.GenerateIdString(rid, c) + rid;
        }
    }
}
