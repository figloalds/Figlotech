using System;
using System.Text;
using Figlotech.Autokryptex.EncryptMethods;

namespace Figlotech.BDados.Authentication {

    internal static class AuthenticationUtils {
        // Taking Single responsibility thingy too seriously.
        public static String HashPass(String pass, String encryptionKey) {
            return Convert.ToBase64String(
                new AutokryptexEncryptor(encryptionKey).Encrypt(
                    Encoding.UTF8.GetBytes(pass)
                ));
        }

    }
}
