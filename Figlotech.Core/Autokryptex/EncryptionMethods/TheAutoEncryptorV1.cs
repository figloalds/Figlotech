using System;

/***
 * 
 * CrossCrypt.cs
 * This class provides a basic enigma encode algorithm.
 * 
*/

namespace Figlotech.Core.Autokryptex {
    public sealed class TheAutoEncryptorV1 : IEncryptionMethod {
        readonly FiRandom[] rands = new FiRandom[16];
        readonly byte[,] map = new byte[16, 256];

        public TheAutoEncryptorV1(string password, int pin) {
            FiRandom cr0 = new FiRandom(pin);
            for (int i = 0; i < 16; i++) {
                rands[i] = new FiRandom(cr0.Next(Int32.MaxValue));
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
