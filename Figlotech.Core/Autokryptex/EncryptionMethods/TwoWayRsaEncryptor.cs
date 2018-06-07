using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;

namespace Figlotech.Core.Autokryptex.EncryptionMethods
{
    public class TwoWayRsaPair {
        private TwoWayRsaPair() {

        }
        public string EncryptionKey { get; private set; }
        public string DecryptionKey { get; private set; }
        public static (TwoWayRsaPair, TwoWayRsaPair) Generate() {
            var RSA1 = new RSACryptoServiceProvider();
            var RSA2 = new RSACryptoServiceProvider();

            var Keypair1 = new TwoWayRsaPair {
                EncryptionKey = Convert.ToBase64String(RSA1.ExportCspBlob(false)),
                DecryptionKey = Convert.ToBase64String(RSA2.ExportCspBlob(true)),
            };

            var Keypair2 = new TwoWayRsaPair {
                EncryptionKey = Convert.ToBase64String(RSA2.ExportCspBlob(false)),
                DecryptionKey = Convert.ToBase64String(RSA1.ExportCspBlob(true)),
            };

            return (Keypair1, Keypair2);
        }

        public override string ToString() {
            return Convert.ToBase64String(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(this)));
        }

        public static TwoWayRsaPair FromString(string input) {
            return JsonConvert.DeserializeObject<TwoWayRsaPair>(Encoding.UTF8.GetString(Convert.FromBase64String(input)));
        }
    }

    public class TwoWayRsaEncryptor : IEncryptionMethod {
        private TwoWayRsaPair KeyPair { get;set; }
        public TwoWayRsaEncryptor(TwoWayRsaPair keypair) {
            KeyPair = keypair;
        }

        public byte[] Decrypt(byte[] en) {
            var rsa = new RSACryptoServiceProvider();
            rsa.ImportCspBlob(Convert.FromBase64String(KeyPair.DecryptionKey));
            return rsa.Decrypt(en, false);
        }

        public byte[] Encrypt(byte[] en) {
            var rsa = new RSACryptoServiceProvider();
            rsa.ImportCspBlob(Convert.FromBase64String(KeyPair.EncryptionKey));
            return rsa.Encrypt(en, false);
        }
    }
}
