/**
 * Iaetec::Core::IntEx 
 * Armazena inteiros infinitamente grandes e conversões de qualquer base pra qualquer base.
 *@Author: Felype Rennan Alves dos Santos
 * 
 * August/2014
 * 
**/

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core
{
    public struct IntEx
    {
        private List<byte> digits;
        private bool isNegative;
        public const int BaseSize = 256;
        public const string Binary = "01";
        public const string Octal = "01234567";
        public const string Decimal = "0123456789";
        public const string Hexadecimal = "0123456789ABCDEF";
        public const string Base26 = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        public const string Base36 = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
        public const string Alphanumeric = "01234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
        public const string AlphanumericPlus = "01234567890abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!@#$%&*()-_+=[].,:; ";
        public static string DefaultInputBase = Decimal;

        public static int rtProgression;

        public IntEx(long number)
        {
            digits = new List<byte> {
                (byte)0
            };
            isNegative = number < 0;
            if (isNegative)
                number = -number;
            Set(number);
        }
        public IntEx(string number, string Base)
        {
            digits = new List<byte> {
                (byte)0
            };
            isNegative = number.StartsWith("-");
            if (isNegative)
                number = new string(number.Skip(1).ToArray());
            List<char> inputDigits = new List<char>();
            inputDigits.AddRange(number.ToCharArray());
            inputDigits.Reverse();
            IntEx retv = 0;
            IntEx multi = 1;
            for (int i = 0; i < inputDigits.Count; i++)
            {
                if (!Base.Contains(inputDigits[i]))
                    continue;
                retv += Base.IndexOf(inputDigits[i]) * multi;
                multi *= Base.Length;
            }
            digits = retv.digits;
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
            IntEx i = new IntEx();
            i = DateTime.Now.Ticks;
            sequentia++;
            i *= 100000;
            i += RuntimeHash;
            i *= 1000;
            i += (sequentia % 1000);
            i *= 1000000000;
            i += r.Next(1000000000);
            var retv = i.ToString(IntEx.Base36);
            StringBuilder preempt = new StringBuilder();
            preempt.Append(retv);
            while (preempt.Length < 64) {
                preempt.Append(IntEx.Base36[r.Next(IntEx.Base36.Length)]);
            }
            var final = preempt.ToString();
            return final.Length <= 64? final : final.Substring(0, 64);
        }
        private static IntEx progRids = 42;
        public static String GenerateShortRid() {
            progRids += 7;
            return progRids.ToString(IntEx.Base36);
        }

        public static IntEx operator +(IntEx a, IntEx b)
        {
            a.Add(b);
            return a;
        }
        public static IntEx operator +(IntEx a, long b)
        {
            if (b > 0)
                a.Add(b);
            return a;
        }
        public static IntEx operator *(IntEx a, IntEx b)
        {
            a.Mult(b);
            return a;
        }
        public static IntEx operator *(IntEx a, long b)
        {
            if (b == 0) return 0;
            a.Mult(b);
            if (a.isNegative ^ b < 0) {
                a.isNegative = true;
            }
            return a;
        }
        public static explicit operator long(IntEx Number)
        {
            return Number.ToLong();
        }
        public static implicit operator IntEx(long Number)
        {
            return new IntEx(Number);
        }
        public static implicit operator string(IntEx Number)
        {
            return Number.ToString();
        }
        public static implicit operator IntEx(string Number)
        {
            return new IntEx(Number, DefaultInputBase);
        }
        
        private void Mult(long number)
        {
            Mult(new IntEx(number));
        }

        private void Mult(IntEx numero)
        {
            List<IntEx> MultiParts = new List<IntEx>();
            for (int i = 0; i < digits.Count; i++)
            {
                IntEx ThisPart = new IntEx() {
                    digits = new List<byte>()
                };
                for (int k = 0; k < i; k++)
                    ThisPart.digits.Add((byte)0);
                int leftovers = 0;
                for (int j = 0; j < numero.digits.Count; j++)
                {
                    int MultiFragment = (byte) digits[i] * (byte) numero.digits[j] + leftovers;
                    leftovers = MultiFragment / BaseSize;
                    ThisPart.digits.Add((byte)(MultiFragment % BaseSize));
                }
                while (leftovers > 0)
                {
                    ThisPart.digits.Add((byte)(leftovers % BaseSize));
                    leftovers = leftovers / BaseSize;
                }
                MultiParts.Add(ThisPart);
            }
            IntEx Novo = new IntEx(0);
            for (int i = 0; i < MultiParts.Count; i++)
            {
                Novo += MultiParts[i];
            }
            digits = Novo.digits;
        }

        private void Add(long Number)
        {
            Add(new IntEx(Number));
        }

        private void Add(IntEx Number)
        {
            IntEx greater = Number.digits.Count > digits.Count ? Number : this;
            IntEx lesser = Number.digits.Count < digits.Count ? Number : this;
            int leftover = 0;
            List<byte> result = new List<byte>();
            for (int i = 0; i < greater.digits.Count || leftover > 0; i++)
            {
                int sum;
                if (i >= greater.digits.Count)
                    sum = leftover;
                else if(i >= lesser.digits.Count)
                    sum = leftover + greater.digits[i];
                else
                    sum = digits[i] + Number.digits[i] + leftover;
                leftover = 0;
                if (sum > BaseSize-1)
                {
                    while (sum > BaseSize-1)
                    {
                        sum -= BaseSize;
                        leftover += 1;
                    }
                    result.Add((byte)sum);
                }
                else
                {
                    result.Add((byte)sum);
                    leftover = 0;
                }
            }
            digits = result;
        }

        private int Dif(int a, int b)
        {
            int Retv = a - b;
            Retv = Retv < 0 ? -Retv : Retv;
            return Retv;
        }

        public void Set(long Number)
        {
            digits = new List<byte>();
            long Backup = Number;
            do
            {
                int Indice = (int)(Backup % BaseSize);
                digits.Add((byte)Indice);
                Backup = Backup / BaseSize;
            } while ((Backup) > 0);
        }

        public long ToLong()
        {
            int position = 0;
            long Multi = 1;
            long retv = 0;
            if (digits.Count == 0) return 0;
            if (digits.Count > 16) throw new Exception("The number within IntEx class is too big to fit into an long");
            do
            {
                retv += digits.ElementAt(position) * Multi;
                Multi *= BaseSize;
                position++;
            } while (position < digits.Count);
            return retv;
        }
        
        public static long BaseConvert(string number, string Base)
        {
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

        public static string BaseConvert(long number, string baseStr)
        {
            List<char> retv = new List<char>();
            bool negativo = false;
            long Backup = number;
            if (negativo = (Backup < 0))
                Backup = -Backup;
            char[] CharsBase = baseStr.ToCharArray();
            do {
                int Indice = (int)(Backup % baseStr.Length);
                retv.Add(baseStr[Indice]);
                Backup = Backup / baseStr.Length;
            } while ((Backup) > 0);
            if (negativo)
                retv.Add('-');
            retv.Reverse();
            return new string(retv.ToArray());
        }
        public static string BaseMult(string a, string b, string baseStr)
        {
            List<string> multiParts = new List<string>();
            a = new string(a.Reverse().ToArray());
            b = new string(b.Reverse().ToArray());
            for (int i = 0; i < a.Length; i++) {
                List<char> thisPart = new List<char>();
                for (int k = 0; k < i; k++)
                    thisPart.Add(baseStr[0]);
                int leftovers = 0;
                for (int j = 0; j < b.Length; j++) {
                    int MultiFragment = baseStr.IndexOf(a[i]) * baseStr.IndexOf(b[j]) + leftovers;
                    leftovers = MultiFragment / baseStr.Length;
                    thisPart.Add(baseStr[MultiFragment % baseStr.Length]);
                }
                while (leftovers > 0) {
                    thisPart.Add(baseStr[leftovers % baseStr.Length]);
                    leftovers = leftovers / baseStr.Length;
                }
                thisPart.Reverse();
                multiParts.Add(new string(thisPart.ToArray()));
            }
            string Retorno = baseStr[0] + "";
            for (int i = 0; i < multiParts.Count; i++) {
                Retorno = BaseSum(Retorno, multiParts[i], baseStr);
            }
            return Retorno;
        }

        public static string BaseSum(string a, string b, string Base)
        {
            a = new string(a.Reverse().ToArray());
            b = new string(b.Reverse().ToArray());
            string Maior = a.Length > b.Length ? a : b;
            string Menor = a.Length < b.Length ? a : b;
            int leftovers = 0;
            List<char> Resultado = new List<char>();
            for (int i = 0; i < Maior.Length || leftovers > 0; i++) {
                int Soma;
                if (i >= Maior.Length)
                    Soma = leftovers;
                else if (i >= Menor.Length)
                    Soma = leftovers + Base.IndexOf(Maior[i]);
                else
                    Soma = Base.IndexOf(a[i]) + Base.IndexOf(b[i]) + leftovers;
                leftovers = 0;
                if (Soma > Base.Length - 1) {
                    while (Soma > Base.Length - 1) {
                        Soma -= Base.Length;
                        leftovers += 1;
                    }
                    Resultado.Add(Base[Soma]);
                }
                else {
                    Resultado.Add(Base[Soma]);
                    leftovers = 0;
                }
            }
            Resultado.Reverse();
            return new string(Resultado.ToArray());
        }

        public string ToString(string Base = Decimal)
        {
            if(digits == null) {
                return null;
            }
            List<char> Retorno = new List<char>();
            char[] CharsBase = Base.ToArray();
            int TamanhoBase = Base.Length;
            string Resultado = Base[0] + "";
            string Multi = Base[1] + "";
            string BaseLength = BaseConvert(BaseSize, Base);
            for (int i = 0; i < digits.Count; i++)
            {
                string ThisByte = BaseConvert((long)digits[i], Base);
                string Next = BaseMult(ThisByte, Multi, Base);
                Resultado = BaseSum(Resultado, Next, Base);
                Multi = BaseMult(Multi, BaseLength, Base);
            }
            return Resultado;
        }

        public static String NewRID() {
            ++rtProgression;
            return new IntEx((DateTime.UtcNow.Ticks * 100000) + (rtProgression % 100000)).ToString(Base36);
        }
    }
}
