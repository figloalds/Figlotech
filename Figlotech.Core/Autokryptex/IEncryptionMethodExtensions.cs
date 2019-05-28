using System;
using System.Collections.Generic;
using System.Text;

namespace Figlotech.Core.Autokryptex.EncryptionMethods {
    public static class IEncryptionMethodExtensions {

        public static byte[] Encrypt(this IEncryptionMethod self, string inputString) {
            byte[] bytes;
            if(inputString.IsBase64()) {
                bytes = Convert.FromBase64String(inputString);
            } else {
                bytes = Fi.StandardEncoding.GetBytes(inputString);
            }
            return self.Encrypt(bytes);
        }

        public static String EncryptToBase64(this IEncryptionMethod self, byte[] bytes) {
            return Convert.ToBase64String(self.Encrypt(bytes));
        }

        public static String DecryptToBase64(this IEncryptionMethod self, byte[] bytes) {
            return Convert.ToBase64String(self.Decrypt(bytes));
        }

    }
}
