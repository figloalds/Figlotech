using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Figlotech.Core.Autokryptex.EncryptMethods
{
    public sealed class AggregateEncryptor : List<IEncryptionMethod>, IEncryptionMethod {

        public byte[] Encrypt(byte[] en) {
            for(int i = 0; i < this.Count; i++) {
                en = this[i].Encrypt(en);
            }

            return en;
        }

        public byte[] Decrypt(byte[] en) {
            for (int i = this.Count-1; i >= 0; i--) {
                en = this[i].Decrypt(en);
            }

            return en;
        }

    }
}