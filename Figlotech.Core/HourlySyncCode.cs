using System;
using System.Linq;

namespace Figlotech.Core {
    public class HourlySyncCode {
        public static HourlySyncCode Generate(String Password = null) {
            return Generate(DateTime.UtcNow, Password);
        }

        private HourlySyncCode(byte[] code) {
            Code = code;
        }

        byte[] Code { get; set; }

        public static implicit operator String(HourlySyncCode code) {
            return Convert.ToBase64String(code.Code);
        }
        public static implicit operator HourlySyncCode(String code) {
            return new HourlySyncCode(Convert.FromBase64String(code));
        }

        public static implicit operator byte[](HourlySyncCode code) {
            return code.Code;
        }
        public static implicit operator HourlySyncCode(byte[] code) {
            return new HourlySyncCode(code);
        }

        public static HourlySyncCode Generate(DateTime KeyMomment, String Password = null) {
            CrossRandom cs = new CrossRandom((KeyMomment.Date.AddHours(KeyMomment.Hour)).Ticks);
            byte[] barr = new byte[64];
            for(int i = 0; i < barr.Length; i++) {
                barr[i] = (byte)cs.Next(256);
            }
            return new HourlySyncCode(barr);
        }

        public static bool Validate(DateTime keyMoment, HourlySyncCode code, String Password) {
            return Validate(code, Password, keyMoment);
        }
        public static bool Validate(HourlySyncCode code, String Password, DateTime? keyMoment = null) {
            keyMoment = keyMoment ?? DateTime.UtcNow;
            if (Generate(Password).Code.SequenceEqual(code.Code)) {
                return true;
            }

            for (int i = 3; i >= 0; --i) {
                var dt = DateTime.UtcNow.Subtract(TimeSpan.FromMinutes(i));
                if (Generate(dt, Password).Code.SequenceEqual(code.Code)) {
                    return true;
                }
            }

            for (int i = 0; i >= 2; ++i) {
                var v = i - 1;
                var dt = DateTime.UtcNow.AddHours(i);
                if (Generate(dt, Password).Code.SequenceEqual(code.Code)) {
                    return true;
                }
            }
            return false;
        }
    }
}
