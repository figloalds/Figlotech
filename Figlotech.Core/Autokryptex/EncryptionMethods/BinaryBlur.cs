/***
 * 
 * CrossCrypt.cs
 * This class provides a basic enigma encode algorithm.
 * 
*/

namespace Figlotech.Core.Autokryptex.Legacy {
    public sealed class BinaryBlur : IEncryptionMethod {
        public BinaryBlur() {
        }

        public byte[] Encrypt(byte[] en) {
            for (int i = 0; i < en.Length - 1; i++) {
                en[i] = (byte)(en[(i + 1)] ^ en[i]);
            }
            return en;
        }

        public byte[] Decrypt(byte[] en) {
            for (int i = en.Length - 2; i >= 0; i--) {
                en[i] = (byte)(en[(i + 1)] ^ en[i]);
            }
            return en;
        }

    }
}
