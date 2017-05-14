using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Entity
{
    public class RID
    {
        String value;
        public RID(String a) {
            value = a;
        }

        public static implicit operator RID(String a) {
            return new RID(a);
        }
        public static implicit operator String(RID a) {
            if(a.value == null) {
                a.value = FTH.GenerateRID();
            }
            return a.value;
        }
    }
}
