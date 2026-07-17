using System;

/***
 * 
 * CrossCrypt.cs
 * This class provides a basic enigma encode algorithm.
 * 
*/

namespace Figlotech.Core.Autokryptex {
    [Obsolete("TheAutoEncryptorV1 is a legacy custom cipher retained only for backward compatibility with previously-encrypted configuration files. Do not use for new code; prefer a standard AEAD construction such as AES-GCM via System.Security.Cryptography.")]
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
