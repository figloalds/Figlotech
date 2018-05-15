using System;

namespace Figlotech.Core {
    public class HourlySyncCode {
        public static String Generate(String Password = null) {
            return Generate(DateTime.UtcNow, Password);
        }
        private static String Generate(DateTime KeyMomment, String Password = null) {
            CrossRandom cs = new CrossRandom((KeyMomment.Date.AddHours(KeyMomment.Hour)).Ticks);
            byte[] barr = new byte[64];
            for(int i = 0; i < barr.Length; i++) {
                barr[i] = (byte)cs.Next(256);
            }
            return Convert.ToBase64String(barr);
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

            for (int i = 0; i >= 2; ++i) {
                var v = i - 1;
                var dt = DateTime.UtcNow.AddHours(i);
                if (Generate(dt, Password) == code) {
                    return true;
                }
            }
            return false;
        }
    }
}
