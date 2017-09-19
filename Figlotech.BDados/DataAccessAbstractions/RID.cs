using Figlotech.BDados;
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
        public static string IntToString(long value, char[] baseChars) {
            // 32 is the worst cast buffer size for base 2 and int.MaxValue
            int targetBase = baseChars.Length;
            StringBuilder sb = new StringBuilder(128);
            do {
                char c = baseChars[value % targetBase];
                sb.Append(c);
                value = value / targetBase;
            } while (value > 0);

            return sb.ToString();
        }

        static char[] IntEx36 = IntEx.Base36.ToCharArray();
        public static String GenerateRID() {
            try {
                //Console.WriteLine("RID Generation Called");
                long l = DateTime.Now.Ticks;
                //i += cpuhash;
                return FillBlanks(IntToString(l, IntEx36) + IntToString(sequentia++ % 1000, IntEx36) + IntToString(r.Next(1679616), IntEx36));
            } catch (Exception x) {
                return FillBlanks("");
            }
        }

        private static string FillBlanks(String rid) {
            var c = 64 - rid.Length;
            return Fi.Tech.GenerateIdString(rid, c) + rid;
        }
    }
}
