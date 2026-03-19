/***
 * 
 * CrossCrypt.cs
 * This class provides a basic enigma encode algorithm.
 * 
*/

namespace Figlotech.Core.Autokryptex.Legacy {
    public sealed class LegacyBinaryScramble : IEncryptionMethod {
        LegacyCrossRandom cr;
        readonly int instancePin;
        public LegacyBinaryScramble(int pin) {
            instancePin = pin;
            cr = new LegacyCrossRandom();
        }

        public byte[] Encrypt(byte[] en) {
            int[] scr = new int[en.Length];
            for (int i = 0; i < scr.Length; i++)
                scr[i] = i;
            for (int a = 0; a < scr.Length; ++a) {
                int b = cr.Next(0, scr.Length);
                if (b <= a) continue;
                int ch = scr[a];
                scr[a] = scr[b];
                scr[b] = ch;
            }
            byte[] retv = new byte[en.Length];
            for (int a = 0; a < en.Length; ++a) {
                retv[a] = en[scr[a]];
            }
            cr = new LegacyCrossRandom();
            return retv;
        }

        public byte[] Decrypt(byte[] en) {
            int[] scr = new int[en.Length];
            for (int i = 0; i < scr.Length; i++)
                scr[i] = i;
            for (int a = 0; a < scr.Length; ++a) {
                int b = cr.Next(0, scr.Length);
                if (b <= a) continue;
                int ch = scr[a];
                scr[a] = scr[b];
                scr[b] = ch;
            }
            byte[] retv = new byte[en.Length];
            for (int a = 0; a < en.Length; ++a) {
                retv[scr[a]] = en[a];
            }
            cr = new LegacyCrossRandom();
            return retv;
        }

    }
}
