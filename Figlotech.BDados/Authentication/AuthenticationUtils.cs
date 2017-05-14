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

namespace Figlotech.BDados.Authentication {

    internal static class AuthenticationUtils {
        // Taking Single responsibility thingy too seriously.
        public static String HashPass(String pass, String usr) {
            return Convert.ToBase64String(Enkryptador.Criptografar(pass, usr));
        }

    }
}
