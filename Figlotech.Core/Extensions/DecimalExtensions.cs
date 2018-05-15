using Figlotech.Core;
using Figlotech.Core.Helpers;
using System.Collections.Generic;
using System.Data;
using System.Globalization;

namespace System {
    public static class DecimalExtensions {
        public static string ToMoney3(this decimal me, CultureInfo culture = null) {
            if (culture == null) {
                culture = CultureInfo.CurrentCulture;
            }
            me = Math.Round(me * 1000) / 1000;
            var nf = (NumberFormatInfo) culture.NumberFormat.Clone();
            nf.CurrencyDecimalDigits = 3;
            return $"{culture.NumberFormat.CurrencySymbol} {me.ToString(nf)}";
        }
        public static string ToMoney(this decimal me, CultureInfo culture = null) {
            if (culture == null) {
                culture = CultureInfo.CurrentCulture;
            }
            me = Math.Round(me * 100) / 100;
            var nf = (NumberFormatInfo)culture.NumberFormat.Clone();
            nf.CurrencyDecimalDigits = 2;
            return $"{culture.NumberFormat.CurrencySymbol} {me.ToString(nf)}";
        }

    }
}
