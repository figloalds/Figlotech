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

namespace Figlotech.Autokryptex {
    public class BinaryNegativation : IEncryptionMethod {
        public BinaryNegativation() {
        }

        public byte[] Encrypt(byte[] en) {
            for(int i = 0; i < en.Length; i++) {
                en[i] = (byte) (256 - en[i]);
            }
            return en;
        }

        public byte[] Decrypt(byte[] en) {
            for (int i = 0; i < en.Length; i++) {
                en[i] = (byte)(256 - en[i]);
            }
            return en;
        }

    }
}
