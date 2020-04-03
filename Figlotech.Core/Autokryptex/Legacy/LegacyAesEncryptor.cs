using Figlotech.Core;
using Figlotech.Core.Autokryptex.Legacy;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core.Autokryptex.EncryptMethods.Legacy {

    public class LegacyAesEncryptor : IEncryptionMethod {
        String instanceSecret;
        int instancePin;
        LegacyCrossRandom cr;

        public LegacyAesEncryptor(String password, int pin = 179425879) {
            instancePin = pin;
            instanceSecret = password;
            //cr = new CrossRandom(Int32.MaxValue ^ instancePin, instanceSecret);
            GenerateKey();
        }
        byte[] _Key;
        byte[] _Iv;
        private byte[] Key {
            get {
                return _Key;
            }
        }
        private byte[] IV {
            get {
                return _Iv;
            }
        }

        private void GenerateKey() {
            cr = new LegacyCrossRandom(Int32.MaxValue ^ instancePin, instanceSecret);
            _Key = new byte[16];
            _Iv = new byte[16];
            for (int i = 0; i < _Key.Length; i++)
                _Key[i] = (byte)cr.Next(byte.MaxValue);
            for (int i = 0; i < _Iv.Length; i++)
                _Iv[i] = (byte)cr.Next(byte.MaxValue);
        }

        public byte[] Decrypt(byte[] en) {
            using (Aes aes = Aes.Create()) {
                aes.Padding = PaddingMode.ISO10126;
                aes.Key = Key;
                aes.IV = IV;
                using (var enc = aes.CreateDecryptor()) {
                    return enc.TransformFinalBlock(en, 0, en.Length);
                }
            }
        }

        public byte[] Encrypt(byte[] en) {
            using (Aes aes = Aes.Create()) {
                aes.Padding = PaddingMode.ISO10126;
                aes.Key = Key;
                aes.IV = IV;
                using (var enc = aes.CreateEncryptor()) {
                    return enc.TransformFinalBlock(en, 0, en.Length);
                }
            }
        }

    }
}
