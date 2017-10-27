using Figlotech.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

/***
 * 
 * CrossCrypt.cs
 * This class provides a basic enigma encode algorithm.
 * 
*/

namespace Figlotech.Core.Autokryptex {
    public class BinaryScramble : IEncryptionMethod {
        CrossRandom cr;
        int instancePin;
        public BinaryScramble(int pin) {
            instancePin = pin;
            cr = new CrossRandom(Int32.MaxValue ^ instancePin);
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
            cr = new CrossRandom(Int32.MaxValue ^ instancePin);
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
            cr = new CrossRandom(Int32.MaxValue ^ instancePin);
            return retv;
        }

    }
}
