using Figlotech.Core;
using Figlotech.Core.Helpers;
using System.Collections.Generic;
using System.Data;
using System.Globalization;

namespace System {
    public static class DecimalExtensions {
        public static string ToMoney3(this decimal me) {
            
            return $"{me:C}";
        }
        public static string ToMoney(this decimal me) {

            return $"{me:C}";
        }
        public static decimal RoundTo(this decimal me, int decimalPlaces) {
            decimal mul = (decimal) Math.Pow( 10, decimalPlaces);
            return Math.Round(me * mul) / mul;
        }

    }
}
