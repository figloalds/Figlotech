using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace System
{
    public static class DateTimeExtensions {
        public static DateTime AtStartOfDay(this DateTime dt) {
            return new DateTime(dt.Year, dt.Month, dt.Day, 0, 0, 0, dt.Kind);
        }
        public static DateTime AtEndOfDay(this DateTime dt) {
            return new DateTime(dt.Year, dt.Month, dt.Day, 23, 59, 59, dt.Kind);
        }

    }
}
