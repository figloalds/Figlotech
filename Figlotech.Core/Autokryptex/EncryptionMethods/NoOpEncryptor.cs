using System;
using System.Collections.Generic;
using System.Text;

namespace Figlotech.Core.Autokryptex.EncryptionMethods {
    public class NoOpEncryptor : IEncryptionMethod {
        public byte[] Decrypt(byte[] en) {
            return en;
        }

        public byte[] Encrypt(byte[] en) {
            return en;
        }
    }
}
