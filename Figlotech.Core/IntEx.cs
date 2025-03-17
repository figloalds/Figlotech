/** 
 * Iaetec::Core::IntEx  
 * Armazena inteiros infinitamente grandes e conversões de qualquer base pra qualquer base. 
 *@Author: Felype Rennan Alves dos Santos 
 *  
 * August/2014 
 *  
**/

using Figlotech.Core.Extensions;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Numerics;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core {
    public struct IntEx {
        private byte[] digits;
        private int digitCursor;
        private bool isNegative;
        public const int BaseSize = 256;
        public const string Binary = "01";
        public const string Octal = "01234567";
        public const string Decimal = "0123456789";
        public const string Hexadecimal = "0123456789ABCDEF";
        public const string Base26 = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        public const string Base36 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        public const string AlphanumericCS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        public const string Base64 = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        public static string DefaultInputBase = Decimal;

        public static int rtProgression;

        public void PushNewDigit(byte digit) {
            if (digitCursor >= digits.Length) {
                byte[] newDigits = new byte[digits.Length + 32];
                Array.Copy(digits, newDigits, digitCursor);
                digits = newDigits;
            }
            digits[digitCursor++] = digit;
        }

        public IntEx(byte[] number) {
            byte[] newDigits = new byte[number.Length];
            Array.Copy(number, newDigits, number.Length);
            digits = newDigits;
            digitCursor = newDigits.Length - 1;
            for (int i = newDigits.Length - 1; i >= 0; i--) {
                if (newDigits[digitCursor] != 0) {
                    digitCursor++;
                    break;
                } else {
                    digitCursor--;
                }
            }
            isNegative = false;
        }

        public IntEx(long number) {
            digits = new byte[32];
            digitCursor = 0;
            isNegative = number < 0;
            if (isNegative)
                number = -number;
            Set(number);
        }

        public byte[] ToByteArray() {
            var retv = digits.AsSpan(0, digitCursor);
            retv.Reverse();
            return retv.ToArray();
        }

        public IntEx(string number, string Base) {
            digits = new byte[32];
            digitCursor = 0;
            isNegative = number.StartsWith("-");
            if (isNegative)
                number = number.Substring(1);

            var v = StringToBigInteger(number, Base);
            this.digits = v.ToByteArray();
            this.digitCursor = v.ToByteArray().Length;
        }

        private static int sequentia = 0;
        private static int _runtimeHash = -1;
        private static int RuntimeHash {
            get {
                if (_runtimeHash > 0) {
                    return _runtimeHash;
                } else {
                    var ehash = new IntEx(new IntEx().GetHashCode()).ToString(Hexadecimal);
                    var cid = new Int64[]
                    {
                        new IntEx(ehash.Substring(0, 4), IntEx.Hexadecimal).ToLong(),
                        new IntEx(ehash.Substring(4, 4), IntEx.Hexadecimal).ToLong(),
                        new IntEx(ehash.Substring(8, 4), IntEx.Hexadecimal).ToLong(),
                        new IntEx(ehash.Substring(12, 4), IntEx.Hexadecimal).ToLong(),
                    };
                    var hash = cid[0] ^ cid[1] ^ cid[2] ^ cid[3];
                    return _runtimeHash = (int)hash;
                }
            }
        }
        private static Random r = new Random();
        public static String GenerateUniqueRID() {
            BigInteger i = new BigInteger();
            i = DateTime.Now.Ticks;
            sequentia++;
            i *= 100000;
            i += RuntimeHash;
            i *= 1000;
            i += (sequentia % 1000);
            i *= 1000000000;
            i += r.Next(1000000000);
            var retv = new IntEx(i.ToByteArray()).ToString(IntEx.Base36);
            StringBuilder preempt = new StringBuilder();
            preempt.Append(retv);
            while (preempt.Length < 64) {
                preempt.Append(IntEx.Base36[r.Next(IntEx.Base36.Length)]);
            }
            var final = preempt.ToString();
            return final.Length <= 64 ? final : final.Substring(0, 64);
        }
        static int _progRids = 0;
        private static int progRids {
            get => (_progRids > 0) ? _progRids : (_progRids = (r.Next(5000) * 7));
            set => _progRids = value > 0 ? value : (_progRids = (r.Next(5000) * 7));
        }
        public static String GenerateShortRid() {
            progRids = Math.Max(0, progRids) + 7 * (1 + r.Next(2));
            return progRids.ToString();
        }

        public static IntEx operator ++(IntEx a) {
            a.Add(1);
            return a;
        }
        public static bool operator <(IntEx a, IntEx b) {
            for (int i = Math.Max(a.digitCursor, b.digitCursor) - 1; i >= 0; i--) {
                if (i > a.digitCursor && b.digits[i] > 0) {
                    return false;
                }
                if (i > b.digitCursor && a.digits[i] > 0) {
                    return false;
                }
                if (i > a.digitCursor || i > b.digitCursor) {
                    continue;
                }
                return a.digits[i] < b.digits[i];
            }
            return false;
        }
        public static bool operator >(IntEx a, IntEx b) {
            for (int i = Math.Max(a.digitCursor, b.digitCursor) - 1; i >= 0; i--) {
                if (i > a.digitCursor && b.digits[i] > 0) {
                    return false;
                }
                if (i > b.digitCursor && a.digits[i] > 0) {
                    return false;
                }
                if (i > a.digitCursor || i > b.digitCursor) {
                    continue;
                }
                return a.digits[i] > b.digits[i];
            }
            return false;
        }
        public static IntEx operator +(IntEx a, IntEx b) {
            a.Add(b);
            return a;
        }
        public static IntEx operator +(IntEx a, long b) {
            if (b > 0)
                a.Add(b);
            return a;
        }
        public static IntEx operator *(IntEx a, IntEx b) {
            a.Mult(b);
            return a;
        }
        public static IntEx operator *(IntEx a, long b) {
            if (b == 0) return 0;
            a.Mult(b);
            if (a.isNegative ^ b < 0) {
                a.isNegative = true;
            }
            return a;
        }
        public static explicit operator long(IntEx Number) {
            return Number.ToLong();
        }
        public static implicit operator IntEx(long Number) {
            return new IntEx(Number);
        }
        public static implicit operator string(IntEx Number) {
            return Number.ToString();
        }
        public static implicit operator IntEx(string Number) {
            return new IntEx(Number, DefaultInputBase);
        }

        private void Mult(long number) {
            Mult(new IntEx(number));
        }

        private void Mult(IntEx numero) {
            IntEx[] MultiParts = new IntEx[digitCursor];
            for (int i = 0; i < digitCursor; i++) {
                IntEx ThisPart = new IntEx() {
                    digits = new byte[i + numero.digits.Length],
                    digitCursor = i
                };
                int leftovers = 0;
                for (int j = 0; j < numero.digitCursor; j++) {
                    int MultiFragment = (byte)digits[i] * (byte)numero.digits[j] + leftovers;
                    leftovers = MultiFragment / BaseSize;
                    ThisPart.digits[i + j] = (byte)(MultiFragment % BaseSize);
                    if (ThisPart.digitCursor < i + j + 1) {
                        ThisPart.digitCursor = i + j + 1;
                    }
                }
                while (leftovers > 0) {
                    ThisPart.PushNewDigit((byte)(leftovers % BaseSize));
                    leftovers = leftovers / BaseSize;
                }
                MultiParts[i] = ThisPart;
            }
            IntEx Novo = new IntEx(0);
            for (int i = 0; i < MultiParts.Length; i++) {
                Novo += MultiParts[i];
            }
            digits = Novo.digits;
            digitCursor = Novo.digitCursor;
            while (digits[digitCursor] > 0) {
                digitCursor++;
            }
        }

        private void Add(long Number) {
            Add(new IntEx(Number));
        }

        private void Add(IntEx Number) {
            IntEx greater = Number.digits.Length > digits.Length ? Number : this;
            IntEx lesser = Number.digits.Length < digits.Length ? Number : this;
            int leftover = 0;
            Span<byte> result = stackalloc byte[greater.digits.Length + 1];
            int step = 0;
            for (int i = 0; i < greater.digits.Length || leftover > 0; i++) {
                int sum;
                if (i >= greater.digits.Length)
                    sum = leftover;
                else if (i >= lesser.digits.Length)
                    sum = leftover + greater.digits[i];
                else
                    sum = digits[i] + Number.digits[i] + leftover;
                leftover = 0;
                if (sum > BaseSize - 1) {
                    while (sum > BaseSize - 1) {
                        sum -= BaseSize;
                        leftover += 1;
                    }
                    result[step++] = ((byte)sum);
                } else {
                    result[step++] = ((byte)sum);
                    leftover = 0;
                }
            }
            if (digits.Length < step) {
                digits = new byte[result.Length];
            }
            Array.Copy(result.ToArray(), digits, step);
            digitCursor = greater.digitCursor;
            while (digits[digitCursor] > 0) {
                digitCursor++;
            }
        }

        private int Dif(int a, int b) {
            int Retv = a - b;
            Retv = Retv < 0 ? -Retv : Retv;
            return Retv;
        }

        public void Set(long Number) {
            digits = new byte[32];
            long Backup = Number;
            do {
                int Indice = (int)(Backup % BaseSize);
                PushNewDigit((byte)Indice);
                Backup = Backup / BaseSize;
            } while ((Backup) > 0);
        }

        public long ToLong() {
            int position = 0;
            long Multi = 1;
            long retv = 0;
            if (digitCursor == 0) return 0;
            if (digitCursor > 16) throw new Exception("The number within IntEx class is too big to fit into an long");
            do {
                retv += digits.ElementAt(position) * Multi;
                Multi *= BaseSize;
                position++;
            } while (position < digitCursor);
            return retv;
        }

        public static long BaseConvert(string number, string Base) {
            bool negative = number.StartsWith("-");
            number = number.Replace("-", "");
            List<char> Digitos = new List<char>();
            Digitos.AddRange(number.ToCharArray());
            Digitos.Reverse();
            long Retorno = 0;
            long Multi = 1;
            char[] CharsBase = Base.ToCharArray();
            for (int i = 0; i < number.Length; i++) {
                int t = Base.IndexOf(Digitos[i]);
                Retorno += Base.IndexOf(Digitos[i]) * Multi;
                Multi *= Base.Length;
            }
            if (negative)
                Retorno = -Retorno;
            return Retorno;
        }

        // convert number to arbitrary base 
        public static string BaseConvert(long number, string digits) {
            if (number == 0) {
                return digits.Substring(0, 1);
            }
            bool negative = number < 0;
            number = Math.Abs(number);
            char[] result = new char[(int)Math.Log(number, digits.Length) + 1];
            int cursor = 0;
            while (number > 0) {
                result[result.Length - ++cursor] = digits[(int)(number % digits.Length)];
                number /= digits.Length;
            }
            if (negative)
                result[result.Length - ++cursor] = '-';
            return new String(result);
        }

        static ConcurrentDictionary<string, int[]> charToValCache = new ConcurrentDictionary<string, int[]>();
        public static string BaseMult(string a, string b, string baseStr) {
            if (a.Length == 1 && a[0] == baseStr[0] || b.Length == 1 && b.Length == baseStr[0]) {
                return baseStr[0].ToString();
            }

            int baseLen = baseStr.Length;

            // Retrieve or compute lookup: char -> int
            int[] charToVal = charToValCache.GetOrAddWithLocking(baseStr, key => {
                int[] lookup = new int[128];
                for (int i = 0; i < baseLen; i++) {
                    lookup[baseStr[i]] = i;
                }
                return lookup;
            });

            // Convert the input strings to digit arrays (in reverse order)
            int aLen = a.Length;
            int bLen = b.Length;
            int[] aDigits = new int[aLen];
            int[] bDigits = new int[bLen];
            for (int i = 0; i < aLen; i++) {
                aDigits[i] = charToVal[a[aLen - 1 - i]];
            }
            for (int i = 0; i < bLen; i++) {
                bDigits[i] = charToVal[b[bLen - 1 - i]];
            }

            // Allocate array for the product (max length = a.Length + b.Length)
            int[] product = new int[aLen + bLen];

            // Multiply digit-by-digit
            for (int i = 0; i < aLen; i++) {
                int carry = 0;
                for (int j = 0; j < bLen; j++) {
                    int tmp = product[i + j] + aDigits[i] * bDigits[j] + carry;
                    product[i + j] = tmp % baseLen;
                    carry = tmp / baseLen;
                }
                product[i + bLen] += carry;
            }

            // Find the actual length (skip any leading zero digits)
            int resultLen = product.Length;
            while (resultLen > 1 && product[resultLen - 1] == 0) {
                resultLen--;
            }

            // Build the result string (convert digits back to characters)
            char[] result = new char[resultLen];
            for (int i = 0; i < resultLen; i++) {
                // Note: product is in reverse order
                result[i] = baseStr[product[resultLen - 1 - i]];
            }

            return new string(result);
        }

        public static string BaseSum(string a, string b, string baseStr) {
            int baseLen = baseStr.Length;

            // Retrieve or compute lookup: char -> int
            int[] charToVal = charToValCache.GetOrAddWithLocking(baseStr, key => {
                int[] lookup = new int[128];
                for (int i = 0; i < baseLen; i++) {
                    lookup[baseStr[i]] = i;
                }
                return lookup;
            });

            int iA = a.Length - 1;
            int iB = b.Length - 1;
            int carry = 0;
            // The maximum possible length is max(a.Length, b.Length) + 1
            int maxLen = Math.Max(a.Length, b.Length) + 1;
            int[] sumDigits = new int[maxLen];
            int pos = maxLen - 1;

            while (iA >= 0 || iB >= 0 || carry > 0) {
                int sum = carry;
                if (iA >= 0) {
                    sum += charToVal[a[iA--]];
                }
                if (iB >= 0) {
                    sum += charToVal[b[iB--]];
                }
                sumDigits[pos--] = sum % baseLen;
                carry = sum / baseLen;
            }

            // Determine starting index (skip any leading zeros)
            int start = 0;
            while (start < maxLen - 1 && sumDigits[start] == 0) {
                start++;
            }

            // Convert the digits back to characters.
            char[] result = new char[maxLen - start];
            for (int i = start; i < maxLen; i++) {
                result[i - start] = baseStr[sumDigits[i]];
            }
            return new string(result);
        }

        public static string Int64ToString(long value, string Base = Decimal) {
            StringBuilder base36 = new StringBuilder();

            if (value == 0) {
                return Base[0].ToString();
            }

            while (value > 0) {
                int remainder = (int)(value % Base.Length);
                value /= Base.Length;
                base36.Insert(0, Base[remainder]);
            }

            return base36.ToString();
        }

        public static string BigIntegerToString(BigInteger value, string digits = "0123456789") {
            if (value == 0) {
                return digits[0].ToString();
            }

            // Calculate the number of digits needed:
            int length = (int)BigInteger.Log(value, digits.Length) + 1;
            char[] buffer = new char[length];
            int position = length;

            while (value > 0) {
                buffer[--position] = digits[(int)(value % digits.Length)];
                value /= digits.Length;
            }

            return new string(buffer, position, length - position);
        }

        public static BigInteger StringToBigInteger(string value, string Base = Decimal) {
            if (string.IsNullOrEmpty(value))
                throw new ArgumentException("Value cannot be null or empty", nameof(value));

            if (string.IsNullOrEmpty(Base))
                throw new ArgumentException("Base cannot be null or empty", nameof(Base));

            BigInteger result = BigInteger.Zero;
            BigInteger baseLength = new BigInteger(Base.Length);

            foreach (char c in value) {
                int digitValue = Base.IndexOf(c);
                if (digitValue < 0)
                    throw new ArgumentException($"Invalid character '{c}' for base {Base}");

                result = result * baseLength + digitValue;
            }

            return result;
        }

        public string ToString(string Base = Decimal) {
            BigInteger value = new BigInteger(this.digits, true);
            return BigIntegerToString(value, Base);
        }
        public static String NewRID() {
            ++rtProgression;
            return new IntEx((DateTime.UtcNow.Ticks * 100000) + (rtProgression % 100000)).ToString(Base36);
        }

    }
}
