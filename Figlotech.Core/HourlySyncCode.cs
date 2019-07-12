using Figlotech.Core.Autokryptex;
using Figlotech.Core.Autokryptex.EncryptMethods;
using Figlotech.Core.Autokryptex.Legacy;
using System;
using System.Linq;

namespace Figlotech.Core {
    public class HourlySyncCode {
        public static HourlySyncCode Generate(String Password = null, DateTime? KeyMoment = null) {
            return Generate(KeyMoment??Fi.Tech.GetUtcTime(), Password);
        }

        private HourlySyncCode(byte[] code) {
            Code = code;
        }

        byte[] Code { get; set; }

        public static implicit operator String(HourlySyncCode code) {
            return code.ToString();
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

        public override string ToString() {
            return Convert.ToBase64String(this.Code);
        }

        public static HourlySyncCode Generate(DateTime KeyMomment, String Password = null) {
            Password = Password ?? "SuchDefaultVeryUnsafe";
            var mins = (long)(TimeSpan.FromTicks(KeyMomment.Ticks)).TotalMinutes;
            LegacyCrossRandom cs = new LegacyCrossRandom();
            byte[] barr = new byte[64];
            for(int i = 0; i < barr.Length; i++) {
                barr[i] = (byte)cs.Next(256);
            }

            int passCount = 32;
            while (passCount-- > 0) {
                for (int i = 0; i < Password.Length; i++) {
                    var place = cs.Next(0, barr.Length);
                    barr[place] ^= (byte)Password[i];
                }
            }
            var hs = (int)(TimeSpan.FromTicks(KeyMomment.Ticks).TotalHours);
            barr = new LegacyBinaryScramble(Password.GetHashCode()).Encrypt(barr);
            barr = new LegacyEnigmaEncryptor(hs).Encrypt(barr);
            return new HourlySyncCode(barr);
        }

        public static bool Validate(DateTime keyMoment, HourlySyncCode code, String Password) {
            return Validate(code, Password, keyMoment);
        }
        public static bool Validate(HourlySyncCode code, String Password, DateTime? keyMoment = null) {
            keyMoment = keyMoment ?? Fi.Tech.GetUtcTime();
            if (Generate(keyMoment.Value, Password).Code.SequenceEqual(code.Code)) {
                return true;
            }

            for (int i = -3; i <= 3; ++i) {
                var dt = Fi.Tech.GetUtcTime().Add(TimeSpan.FromMinutes(i));
                if (Generate(dt, Password).Code.SequenceEqual(code.Code)) {
                    return true;
                }
            }

            return false;
        }
    }
}
