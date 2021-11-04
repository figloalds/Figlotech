using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace System
{
    public static class StringExtensions {
        public static string Remove(this string me, string other) {
            return me.Replace(other, String.Empty);
        }

        public static string OrEmpty(this string self) {
            return self ?? "";
        }

        public static bool RegExp(this string me, string pattern) {
            if (me == null)
                return false;
            return Regex.Match(me, pattern).Success;
        }
        public static string RegExpExtract(this string me, string pattern, int groupNumber = 1) {
            if (me == null)
                return String.Empty;
            return Regex.Match(me, pattern).Groups[groupNumber].Value;
        }

        public static string RegExReplace(this string me, string pattern, string replace) {
            if (me == null)
                return null;
            return Regex.Replace(me, pattern, replace);
        }
        public static string RegExReplace(this string me, string pattern, Func<string> replace) {
            if (me == null)
                return null;
            var matches = Regex.Matches(me, pattern);
            StringBuilder retv = new StringBuilder();
            int cursor = 0;
            foreach(Match a in matches) {
                retv.Append(me.Substring(cursor, a.Index - cursor));
                retv.Append(replace());
                cursor = a.Index + a.Length;
            }
            return retv.ToString();
        }
        public static bool IsBase64(this string base64String) {
            // Credit: oybek https://stackoverflow.com/users/794764/oybek
            if (base64String == null || base64String.Length == 0 || base64String.Length % 4 != 0
               || base64String.Contains(" ") || base64String.Contains("\t") || base64String.Contains("\r") || base64String.Contains("\n"))
                return false;

            try {
                Convert.FromBase64String(base64String);
                return true;
            } catch (Exception exception) {
                // Handle the exception
            }
            return false;
        }

        public static bool IsNullOrEmpty(this string me) {
            return String.IsNullOrEmpty(me);
        }

        
    }

}
