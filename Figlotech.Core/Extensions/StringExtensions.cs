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

        public static bool RegExp(this string me, string pattern) {
            return Regex.Match(me, pattern).Success;
        }

        public static string RegExReplace(this string me, string pattern, string replace) {
            return Regex.Replace(me, pattern, replace);
        }

        public static T ValueTo<T>(this Object me) {
            try {
                return (T)Convert.ChangeType(me, typeof(T));
            } catch (Exception) {
                return default(T);
            }
        }
        
        public static string Remove(this string me, string other) {
            return me.Replace(other, String.Empty);
        }

    }

}
