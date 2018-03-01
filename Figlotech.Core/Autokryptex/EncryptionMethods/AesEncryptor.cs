using Figlotech.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core.Autokryptex.EncryptMethods {
    public class AesEncryptor : IEncryptionMethod {
        String instanceSecret;
        int instancePin;
        CrossRandom cr;

        public AesEncryptor(String password, int pin = 179425879) {
            instancePin = pin;
            instanceSecret = password;
            cr = new CrossRandom(Int32.MaxValue ^ instancePin, instanceSecret);
        }
        byte[] _Key;
        byte[] _Iv;
        private byte[] Key {
            get {
                return _Key ?? (_Key = GenerateKey());
            }
        }
        private byte[] IV {
            get {
                return _Iv;
            }
        }

        private byte[] GenerateKey() {
            cr = new CrossRandom(Int32.MaxValue ^ instancePin, instanceSecret);
            byte[] key = new byte[16];
            byte[] iv = new byte[16];
            for (int i = 0; i < key.Length; i++)
                key[i] = (byte)cr.Next(byte.MaxValue);
            for (int i = 0; i < iv.Length; i++)
                iv[i] = (byte)cr.Next(byte.MaxValue);
            _Iv = iv;
            return key;
        }

        public byte[] Decrypt(byte[] en) {
            using (Aes aes = Aes.Create()) {
                aes.Key = Key;
                aes.IV = IV;
                using (var enc = aes.CreateDecryptor()) {
                    en = enc.TransformFinalBlock(en, 0, en.Length);
                }
                return en;
            }
        }

        public byte[] Encrypt(byte[] en) {
            using (Aes aes = Aes.Create()) {
                aes.Key = Key;
                aes.IV = IV;
                using (var enc = aes.CreateEncryptor()) {
                    en = enc.TransformFinalBlock(en, 0, en.Length);
                }
                return en;
            }
        }

    }
}
