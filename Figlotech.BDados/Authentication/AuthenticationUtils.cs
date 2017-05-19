using Figlotech;
using Figlotech.BDados.Entity;
using Figlotech.Core;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Figlotech.BDados;
using Figlotech.Autokryptex;
using System.Data;
using System.Text.RegularExpressions;
using Figlotech.BDados.Interfaces;
using System.Globalization;
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
