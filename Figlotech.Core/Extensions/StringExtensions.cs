using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace System
{
    public static class StringExtensions
    {
        public static string Remove(this string me, string other) {
            return me.Replace(other, String.Empty);
        }
    }
}
