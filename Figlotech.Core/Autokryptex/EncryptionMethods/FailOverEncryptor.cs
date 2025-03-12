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
    public sealed class FailOverEncryptor : IEncryptionMethod {
        public List<IEncryptionMethod> Methods { get; private set; } = new List<IEncryptionMethod>();
        public FailOverEncryptor() {
        }
        /// <summary>
        /// The first encryptor is CURRENT and will be used to encrypt
        /// Add new methods to the start and remove old ones from the end
        /// </summary>
        /// <param name="methods"></param>
        public FailOverEncryptor(params IEncryptionMethod[] methods) {
            Methods.AddRange(methods);
        }

        public byte[] Encrypt(byte[] en) {
            return Methods[0].Encrypt(en);
        }

        public byte[] Decrypt(byte[] en) {
            var exces = new List<Exception>();
            foreach(var enc in Methods) {
                try {
                    return enc.Decrypt(en);
                } catch(Exception x) {
                    exces.Add(x);
                }
            }
            throw new AggregateException("Unable to decrypt data", exces);
        }
    }
}
