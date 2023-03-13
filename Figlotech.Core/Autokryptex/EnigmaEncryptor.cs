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
    public sealed class EnigmaEncryptor : IEncryptionMethod {
        private int enigmaPin;
        private static byte[] originalByteMap = new byte[byte.MaxValue + 1];
        FiRandom rng;
        int instancePin;
        public EnigmaEncryptor(int pin) {
            for (int i = 0; i < originalByteMap.Length; i++) {
                originalByteMap[i] = (byte)i;
            }
            instancePin = pin;
            rng = new FiRandom(instancePin);
        }
        private byte[] GenerateEnigmaStrip(byte[] Map) {
            byte[] enigma = new byte[Map.Length];
            Map.CopyTo(enigma, 0);

            // Runs pseudorandom swapping positions 
            // Its not as good as Turing's, but it's fine I guess.
            for (int a = 0; a < Map.Length - 1; ++a) {
                if (enigma[a] != a) {
                    continue;
                }
                int b = 0;
                do {
                    b = rng.Next(a + 1, Map.Length);
                } while (enigma[b] != b);
                enigma[a] = (byte)b;
                enigma[b] = (byte)a;
            }

            for (int i = 0; i < Map.Length; i++) {
                if (i != enigma[enigma[i]]) {
                    continue;
                }
            }

            return enigma;
        }

        public byte[] Encrypt(byte[] en) {
            // Brief: Generate enigma strip
            // set value of character a to value of character b at 
            // position a of the enigma strip.
            // regenerate the strip for each character.
            var retv = new byte[en.Length];
            var enigmaStrip = this.GenerateEnigmaStrip(originalByteMap);
            for (int a = 0; a < retv.Length; ++a) {
                retv[a] = enigmaStrip[en[a]];
                enigmaStrip = this.GenerateEnigmaStrip(enigmaStrip);
            }
            rng = new FiRandom(instancePin);
            return retv;
        }

        public byte[] Decrypt(byte[] en) {
            return Encrypt(en);
        }

    }
}
