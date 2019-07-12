using System;
using System.Text;
using Figlotech.Core;
using Figlotech.Core.Autokryptex;
using Figlotech.Core.Autokryptex.EncryptMethods;

namespace Figlotech.BDados.Authentication {

    internal static class AuthenticationUtils {
        // Taking Single responsibility thingy too seriously.
        public static String HashPass(String pass, String encryptionKey) {
            return Convert.ToBase64String(
                new TheAutoEncryptorV1(encryptionKey, pass.GetHashCode()).Encrypt(
                    Fi.StandardEncoding.GetBytes(pass)
                ));
        }

    }
}
