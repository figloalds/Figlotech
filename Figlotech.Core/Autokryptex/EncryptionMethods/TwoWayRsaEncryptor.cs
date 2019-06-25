using Figlotech.Core.Autokryptex.EncryptMethods;
using Figlotech.Core.Extensions;
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
        public byte[] EncryptionKey { get; private set; }
        public byte[] DecryptionKey { get; private set; }
        public static (TwoWayRsaPair, TwoWayRsaPair) Generate() {
            var RSA1 = new RSACryptoServiceProvider();
            var RSA2 = new RSACryptoServiceProvider();

            var Keypair1 = new TwoWayRsaPair {
                EncryptionKey = RSA1.ExportCspBlob(false),
                DecryptionKey = RSA2.ExportCspBlob(true),
            };

            var Keypair2 = new TwoWayRsaPair {
                EncryptionKey = RSA2.ExportCspBlob(false),
                DecryptionKey = RSA1.ExportCspBlob(true),
            };

            return (Keypair1, Keypair2);
        }

        public override string ToString() {
            var pack = new byte[2 + 4 + EncryptionKey.Length + DecryptionKey.Length];
            pack[0] = 0x02;
            pack[pack.Length-1] = 0x03;
            Array.Copy(BitConverter.GetBytes((UInt16)EncryptionKey.Length), 0, pack, 1, 2);
            Array.Copy(BitConverter.GetBytes((UInt16)DecryptionKey.Length), 0, pack, 3 + EncryptionKey.Length, 2);

            Array.Copy(EncryptionKey, 0, pack, 3, EncryptionKey.Length);
            Array.Copy(DecryptionKey, 0, pack, 5 + EncryptionKey.Length, DecryptionKey.Length);

            return Convert.ToBase64String(pack);
        }

        public void SetFromString(string input) {
            var pack = Convert.FromBase64String(input);
            var encLen = BitConverter.ToUInt16(pack, 1);
            var decLen = BitConverter.ToUInt16(pack, 3 + encLen);

            this.EncryptionKey = new ArraySegment<byte>(pack, 3, encLen).ToSegmentArray();
            this.DecryptionKey = new ArraySegment<byte>(pack, 5 + encLen, decLen).ToSegmentArray();
        }

        public static TwoWayRsaPair FromString(string input) {
            var retv = new TwoWayRsaPair();
            retv.SetFromString(input);
            return retv;
        }
    }

    public class TwoWayRsaEncryptor : IEncryptionMethod {
        private TwoWayRsaPair KeyPair { get;set; }
        public TwoWayRsaEncryptor(TwoWayRsaPair keypair) {
            KeyPair = keypair;
        }

        public byte[] Decrypt(byte[] en) {
            var rsa = new RSACryptoServiceProvider();
            rsa.ImportCspBlob(KeyPair.DecryptionKey);
            return rsa.Decrypt(en, false);
        }

        public byte[] Encrypt(byte[] en) {
            var rsa = new RSACryptoServiceProvider();
            rsa.ImportCspBlob(KeyPair.EncryptionKey);
            return rsa.Encrypt(en, false);
        }

        private byte[] wrongEncrypt(byte[] en) {
            var rsa = new RSACryptoServiceProvider();
            rsa.ImportCspBlob(KeyPair.DecryptionKey);
            return rsa.Encrypt(en, false);
        }

        public TwoWayAesEncryptor GenerateTwoWayAesEncryptor(string password) {
            password = Fi.StandardEncoding.GetString(MathUtils.CramString(password, 64));
            return new TwoWayAesEncryptor(
                Fi.StandardEncoding.GetString(Encrypt(Fi.StandardEncoding.GetBytes(password))),
                Fi.StandardEncoding.GetString(wrongEncrypt(Fi.StandardEncoding.GetBytes(password)))
            );
        }
    }
}
