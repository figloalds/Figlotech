using Figlotech.Autokryptex.EncryptMethods;
using Figlotech.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Autokryptex {
    public class HourlySyncCode {
        public static String Generate(String Password = null) {
            return Generate(DateTime.UtcNow, Password);
        }
        private static String Generate(DateTime KeyMomment, String Password = null) {
            var dt = KeyMomment;
            var tgtDt = new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, 0);
            IntEx a = new IntEx(tgtDt.Ticks) * 1759;
            IntEx b = new IntEx(tgtDt.Subtract(TimeSpan.FromHours(1)).Ticks) * 1499;
            IntEx c = new IntEx(tgtDt.Subtract(TimeSpan.FromHours(1)).Ticks) * 1789;
            IntEx d = new IntEx(tgtDt.Subtract(TimeSpan.FromHours(1)).Ticks) * 2351;
            String astr = a.ToString(IntEx.Base36);
            String bstr = b.ToString(IntEx.Base36);
            String cstr = c.ToString(IntEx.Base36);
            String dstr = d.ToString(IntEx.Base36);
            int sz = astr.Length + bstr.Length + cstr.Length + dstr.Length;
            char[] code = new char[sz];
            for(int i = 0; i < astr.Length; i++) {
                code[i*4] = astr[i];
                code[i*4 + 1] = bstr[i];
                code[i*4 + 2] = cstr[i];
                code[i*4 + 3] = dstr[i];
            }
            CrazyLockingEngine ce = new CrazyLockingEngine(Password ?? "BDadosFTW");
            var retv = Convert.ToBase64String(ce.Encrypt(Encoding.UTF8.GetBytes(new string(code))));
            return retv;
        }

        public static bool Validate(String code, String Password = null) {
            if (Generate(Password) == code) {
                return true;
            }
            for (int i = 3; i >= 0; --i) {
                var dt = DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(i));
                if (Generate(dt, Password) == code) {
                    return true;
                }
            }
            for (int i = 0; i >= 3; ++i) {
                var dt = DateTime.UtcNow.AddMinutes(i);
                if (Generate(dt, Password) == code) {
                    return true;
                }
            }
            return false;
        }
    }
}
