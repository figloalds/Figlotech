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
    public class EnigmaEncryptor : IEncryptionMethod {
        private int enigmaPin;
        private static byte[] originalByteMap = new byte[byte.MaxValue + 1];
        private byte[] enigmaStrip;
        CrossRandom cr;
        int instancePin;
        public EnigmaEncryptor(int pin) {
            enigmaPin = pin;
            for(int i = 0; i < originalByteMap.Length; i++) {
                originalByteMap[i] = (byte) i;
            }
            instancePin = pin;
            cr = new CrossRandom(Int32.MaxValue ^ instancePin);
        }

        private byte[] GenerateEnigmaStrip(byte[] Map) {
            int num = cr.Next(7)+7;
            byte[] enigma = new byte[Map.Length];
            Map.CopyTo(enigma, 0);
            while (num-- > 0) {
                // Runs pseudorandom swapping positions for 7 to 14 times
                // Its not as good as Turing's, but it's fine I guess.
                for (int a = 0; a < Map.Length; ++a) {
                    int b = cr.Next(0, Map.Length);
                    if (b == a) continue;
                    byte ch = enigma[a];
                    enigma[a] = enigma[b];
                    enigma[b] = ch;
                }
            }
            return enigma;
        }

        public byte[] Encrypt(byte[] en) {
            // Brief: Generate enigma strip
            // set value of character a to value of character b at 
            // position a of the enigma strip.
            // regenerate the strip for each character.
            this.enigmaStrip = this.GenerateEnigmaStrip(originalByteMap);
            for (int a = 0; a < en.Length; ++a) {
                en[a] = this.enigmaStrip[en[a]];
                this.enigmaStrip = this.GenerateEnigmaStrip(this.enigmaStrip);
            }
            cr = new CrossRandom(Int32.MaxValue ^ instancePin);
            return en;
        }

        public byte[] Decrypt(byte[] en) {
            // Brief: Generate enigma strip
            // set value of character a to index of character b at 
            // position a of the original byte strip.
            // regenerate the strip for each character.
            this.enigmaStrip = this.GenerateEnigmaStrip(originalByteMap);
            for (int a = 0; a < en.Length; ++a) {
                var index = -1;
                for (int x = 0; x < enigmaStrip.Length; x++) {
                    if(enigmaStrip[x] == en[a]) {
                        index = x;
                        break;
                    }
                }
                en[a] = originalByteMap[index];
                this.enigmaStrip = this.GenerateEnigmaStrip(this.enigmaStrip);
            }
            cr = new CrossRandom(Int32.MaxValue ^ instancePin);
            return en;
        }

    }
}
