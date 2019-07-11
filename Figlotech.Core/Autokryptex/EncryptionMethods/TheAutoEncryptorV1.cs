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
    public class TheAutoEncryptorV1 : IEncryptionMethod {
        CrossRandom[] rands = new CrossRandom[16];
        byte[,] map = new byte[16, 256];

        public TheAutoEncryptorV1(string password, int pin) {
            CrossRandom cr0 = new CrossRandom(pin);
            for(int i = 0; i < 16; i++) {
                rands[i] = new CrossRandom(cr0.Next(Int32.MaxValue));
            }
        }

        public byte[] Encrypt(byte[] en) {
            return en;
        }

        public byte[] Decrypt(byte[] en) {
            return en;
        }

    }
}
