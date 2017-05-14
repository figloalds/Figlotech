using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Autokryptex.EncryptMethods
{
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
            Aes aes = Aes.Create();
            aes.Key = Key;
            aes.IV = IV;
            en = aes.CreateDecryptor().TransformFinalBlock(en, 0, en.Length);
            cr = new CrossRandom(Int32.MaxValue ^ instancePin, instanceSecret);
            return en;
        }

        public byte[] Encrypt(byte[] en) {
            Aes aes = Aes.Create();
            aes.Key = Key;
            aes.IV = IV;
            en = aes.CreateEncryptor().TransformFinalBlock(en, 0, en.Length);
            cr = new CrossRandom(Int32.MaxValue ^ instancePin, instanceSecret);
            return en;
        }

    }
}
