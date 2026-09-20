using System;
using System.Globalization;
using System.IO;
using System.Numerics;

namespace Figlotech.BDados.Helpers {
    internal static class BDadosBackupValueConverter {
        private static bool IsNumeric(BDadosBackupDataType type) => type >= BDadosBackupDataType.Byte && type <= BDadosBackupDataType.Decimal;

        internal static bool CanConvert(BDadosBackupDataType source, BDadosBackupDataType target) {
            return source == target
                || IsNumeric(source) && IsNumeric(target)
                || source == BDadosBackupDataType.Boolean && IsNumeric(target)
                || IsNumeric(source) && target == BDadosBackupDataType.Boolean
                || source == BDadosBackupDataType.String && target != BDadosBackupDataType.Bytes
                || target == BDadosBackupDataType.String && source != BDadosBackupDataType.Bytes;
        }

        internal static object ConvertValue(object value, Type target) {
            if (target.IsEnum) {
                // The wire carries the numeric value, so enum type/namespace/name changes do not
                // affect existing data. Undefined values and flags are intentionally preserved.
                if (value is string enumText) return Enum.Parse(target, enumText, true);
                return Enum.ToObject(target, ConvertValue(value, Enum.GetUnderlyingType(target)));
            }
            if (value.GetType() == target) return value;
            if (target == typeof(string)) {
                return value switch {
                    DateTime date => date.ToString("O", CultureInfo.InvariantCulture),
                    DateTimeOffset date => date.ToString("O", CultureInfo.InvariantCulture),
                    DateOnly date => date.ToString("O", CultureInfo.InvariantCulture),
                    TimeOnly time => time.ToString("O", CultureInfo.InvariantCulture),
                    TimeSpan span => span.ToString("c", CultureInfo.InvariantCulture),
                    float number => number.ToString("R", CultureInfo.InvariantCulture),
                    double number => number.ToString("R", CultureInfo.InvariantCulture),
                    _ => Convert.ToString(value, CultureInfo.InvariantCulture)
                };
            }
            if (value is string text) {
                if (target == typeof(Guid)) return Guid.Parse(text);
                if (target == typeof(DateTime)) return DateTime.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
                if (target == typeof(DateTimeOffset)) return DateTimeOffset.ParseExact(text, "O", CultureInfo.InvariantCulture);
                if (target == typeof(DateOnly)) return DateOnly.Parse(text, CultureInfo.InvariantCulture);
                if (target == typeof(TimeOnly)) return TimeOnly.Parse(text, CultureInfo.InvariantCulture);
                if (target == typeof(TimeSpan)) return TimeSpan.Parse(text, CultureInfo.InvariantCulture);
                if (target == typeof(bool)) {
                    if (text == "0") return false;
                    if (text == "1") return true;
                }
                return Convert.ChangeType(text, target, CultureInfo.InvariantCulture);
            }
            var sourceType = BDadosBackupCodec.GetDataType(value.GetType());
            var targetType = BDadosBackupCodec.GetDataType(target);
            if (target == typeof(bool) && IsNumeric(sourceType)) {
                if (EqualsNumber(value, 0)) return false;
                if (EqualsNumber(value, 1)) return true;
                throw new InvalidDataException("Only numeric 0 or 1 can be restored as Boolean.");
            }
            object converted = Convert.ChangeType(value, target, CultureInfo.InvariantCulture);
            if (IsNumeric(sourceType) && IsNumeric(targetType) && !EqualsNumber(value, converted)) {
                throw new InvalidDataException("Numeric conversion would lose precision or truncate a value.");
            }
            return converted;
        }

        private static bool EqualsNumber(object left, object right) {
            double a = Convert.ToDouble(left, CultureInfo.InvariantCulture);
            double b = Convert.ToDouble(right, CultureInfo.InvariantCulture);
            if (!double.IsFinite(a) || !double.IsFinite(b)) return a.Equals(b);
            var x = Fraction(left);
            var y = Fraction(right);
            return x.Numerator * y.Denominator == y.Numerator * x.Denominator;
        }

        // Compare exact numeric values rather than converting back through rounded decimal/double
        // formatting (which can hide loss for large integers and decimal fractions).
        private static (BigInteger Numerator, BigInteger Denominator) Fraction(object value) {
            if (value is float || value is double) {
                long bits = BitConverter.DoubleToInt64Bits(Convert.ToDouble(value, CultureInfo.InvariantCulture));
                int exponent = (int)((bits >> 52) & 0x7ff);
                BigInteger numerator = bits & 0x000fffffffffffffL;
                if (exponent != 0) numerator += BigInteger.One << 52;
                int shift = (exponent == 0 ? -1022 : exponent - 1023) - 52;
                if (bits < 0) numerator = -numerator;
                return shift >= 0 ? (numerator << shift, BigInteger.One) : (numerator, BigInteger.One << -shift);
            }
            int[] parts = decimal.GetBits(Convert.ToDecimal(value, CultureInfo.InvariantCulture));
            BigInteger number = (uint)parts[0] + ((BigInteger)(uint)parts[1] << 32) + ((BigInteger)(uint)parts[2] << 64);
            if (parts[3] < 0) number = -number;
            return (number, BigInteger.Pow(10, (parts[3] >> 16) & 0xff));
        }
    }
}
