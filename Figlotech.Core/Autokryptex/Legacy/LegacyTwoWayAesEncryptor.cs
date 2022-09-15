using Figlotech.Core;
using Figlotech.Core.Autokryptex.Legacy;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core.Autokryptex.EncryptMethods.Legacy {

    public sealed class TwoWayAesEncryptor : IEncryptionMethod {
        string instanceEncodeSecret;
        string instanceDecodeSecret;
        int instancePin;
        LegacyCrossRandom cr;

        public TwoWayAesEncryptor(string encodePassword, string decodePassword, int pin = 179425879) {
            instancePin = pin;
            instanceEncodeSecret = encodePassword;
            instanceDecodeSecret = decodePassword;
            //cr = new CrossRandom(Int32.MaxValue ^ instancePin, instanceSecret);
            GenerateKeys();
        }
        byte[] _encodeKey;
        byte[] _encodeIv;
        byte[] _decodeKey;
        byte[] _decodeIv;

        private void GenerateKeys() {
            cr = new LegacyCrossRandom(Int32.MaxValue ^ instancePin, instanceEncodeSecret);
            _encodeKey = new byte[16];
            _encodeIv = new byte[16];
            for (int i = 0; i < _encodeKey.Length; i++)
                _encodeKey[i] = (byte)cr.Next(byte.MaxValue);
            for (int i = 0; i < _encodeIv.Length; i++)
                _encodeIv[i] = (byte)cr.Next(byte.MaxValue);

            _decodeKey = new byte[16];
            _decodeIv = new byte[16];
            for (int i = 0; i < _decodeKey.Length; i++)
                _decodeKey[i] = (byte)cr.Next(byte.MaxValue);
            for (int i = 0; i < _decodeIv.Length; i++)
                _decodeIv[i] = (byte)cr.Next(byte.MaxValue);
        }

        public byte[] Decrypt(byte[] en) {
            using (Aes aes = Aes.Create()) {
                aes.Padding = PaddingMode.ISO10126;
                aes.Key = _decodeKey;
                aes.IV = _decodeIv;
                using (var dec = aes.CreateDecryptor()) {
                    return dec.TransformFinalBlock(en, 0, en.Length);
                }
            }
        }

        public byte[] Encrypt(byte[] en) {
            using (Aes aes = Aes.Create()) {
                aes.Padding = PaddingMode.ISO10126;
                aes.Key = _encodeKey;
                aes.IV = _encodeIv;
                using (var enc = aes.CreateEncryptor()) {
                    return enc.TransformFinalBlock(en, 0, en.Length);
                }
            }
        }

    }
}
