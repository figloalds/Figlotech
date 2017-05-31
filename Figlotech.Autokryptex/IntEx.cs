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
        private List<byte> Digitos;
        private bool Negativo;
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

        public IntEx(long NumeroInicial)
        {
            Digitos = new List<byte> {
                (byte)0
            };
            Negativo = NumeroInicial < 0;
            if (Negativo)
                NumeroInicial = -NumeroInicial;
            Set(NumeroInicial);
        }
        public IntEx(string Numero, string Base)
        {
            Digitos = new List<byte> {
                (byte)0
            };
            Negativo = Numero.StartsWith("-");
            if (Negativo)
                Numero = new string(Numero.Skip(1).ToArray());
            List<char> DigNumero = new List<char>();
            DigNumero.AddRange(Numero.ToCharArray());
            DigNumero.Reverse();
            IntEx retv = 0;
            IntEx multi = 1;
            for (int i = 0; i < DigNumero.Count; i++)
            {
                if (!Base.Contains(DigNumero[i]))
                    continue;
                retv += Base.IndexOf(DigNumero[i]) * multi;
                multi *= Base.Length;
            }
            Digitos = retv.Digitos;
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
        public static String GerarUniqueRID() {
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
            if (a.Negativo ^ b < 0) {
                a.Negativo = true;
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
        
        private void Mult(long Numero)
        {
            Mult(new IntEx(Numero));
        }

        private void Mult(IntEx Numero)
        {
            List<IntEx> MultiParts = new List<IntEx>();
            for (int i = 0; i < Digitos.Count; i++)
            {
                IntEx ThisPart = new IntEx() {
                    Digitos = new List<byte>()
                };
                for (int k = 0; k < i; k++)
                    ThisPart.Digitos.Add((byte)0);
                int Leftovers = 0;
                for (int j = 0; j < Numero.Digitos.Count; j++)
                {
                    int MultiFragment = (byte) Digitos[i] * (byte) Numero.Digitos[j] + Leftovers;
                    Leftovers = MultiFragment / BaseSize;
                    ThisPart.Digitos.Add((byte)(MultiFragment % BaseSize));
                }
                while (Leftovers > 0)
                {
                    ThisPart.Digitos.Add((byte)(Leftovers % BaseSize));
                    Leftovers = Leftovers / BaseSize;
                }
                MultiParts.Add(ThisPart);
            }
            IntEx Novo = new IntEx(0);
            for (int i = 0; i < MultiParts.Count; i++)
            {
                Novo += MultiParts[i];
            }
            Digitos = Novo.Digitos;
        }

        private void Add(long Number)
        {
            Add(new IntEx(Number));
        }

        private void Add(IntEx Number)
        {
            IntEx Maior = Number.Digitos.Count > Digitos.Count ? Number : this;
            IntEx Menor = Number.Digitos.Count < Digitos.Count ? Number : this;
            int Leftovers = 0;
            List<byte> Resultado = new List<byte>();
            for (int i = 0; i < Maior.Digitos.Count || Leftovers > 0; i++)
            {
                int Soma;
                if (i >= Maior.Digitos.Count)
                    Soma = Leftovers;
                else if(i >= Menor.Digitos.Count)
                    Soma = Leftovers + Maior.Digitos[i];
                else
                    Soma = Digitos[i] + Number.Digitos[i] + Leftovers;
                Leftovers = 0;
                if (Soma > BaseSize-1)
                {
                    while (Soma > BaseSize-1)
                    {
                        Soma -= BaseSize;
                        Leftovers += 1;
                    }
                    Resultado.Add((byte)Soma);
                }
                else
                {
                    Resultado.Add((byte)Soma);
                    Leftovers = 0;
                }
            }
            Digitos = Resultado;
        }

        private int Dif(int a, int b)
        {
            int Retv = a - b;
            Retv = Retv < 0 ? -Retv : Retv;
            return Retv;
        }

        public void Set(long Number)
        {
            Digitos = new List<byte>();
            long Backup = Number;
            do
            {
                int Indice = (int)(Backup % BaseSize);
                Digitos.Add((byte)Indice);
                Backup = Backup / BaseSize;
            } while ((Backup) > 0);
        }

        public long ToLong()
        {
            int Posicao = 0;
            long Multi = 1;
            long Retorno = 0;
            if (Digitos.Count == 0) return 0;
            if (Digitos.Count > 16) throw new Exception("The number within IntEx class is too big to fit into an long");
            do
            {
                Retorno += Digitos.ElementAt(Posicao) * Multi;
                Multi *= BaseSize;
                Posicao++;
            } while (Posicao < Digitos.Count);
            return Retorno;
        }
        
        public static long BaseConvert(string Numero, string Base)
        {
            bool negativo = Numero.StartsWith("-");
            Numero = Numero.Replace("-", "");
            List<char> Digitos = new List<char>();
            Digitos.AddRange(Numero.ToCharArray());
            Digitos.Reverse();
            long Retorno = 0;
            long Multi = 1;
            char[] CharsBase = Base.ToCharArray();
            for (int i = 0; i < Numero.Length; i++) {
                int t = Base.IndexOf(Digitos[i]);
                Retorno += Base.IndexOf(Digitos[i]) * Multi;
                Multi *= Base.Length;
            }
            if (negativo)
                Retorno = -Retorno;
            return Retorno;
        }

        public static string BaseConvert(long Numero, string Base)
        {
            List<char> Retv = new List<char>();
            bool negativo = false;
            long Backup = Numero;
            if (negativo = (Backup < 0))
                Backup = -Backup;
            char[] CharsBase = Base.ToCharArray();
            do {
                int Indice = (int)(Backup % Base.Length);
                Retv.Add(Base[Indice]);
                Backup = Backup / Base.Length;
            } while ((Backup) > 0);
            if (negativo)
                Retv.Add('-');
            Retv.Reverse();
            return new string(Retv.ToArray());
        }
        public static string BaseMult(string a, string b, string Base)
        {
            List<string> MultiParts = new List<string>();
            a = new string(a.Reverse().ToArray());
            b = new string(b.Reverse().ToArray());
            for (int i = 0; i < a.Length; i++) {
                List<char> ThisPart = new List<char>();
                for (int k = 0; k < i; k++)
                    ThisPart.Add(Base[0]);
                int Leftovers = 0;
                for (int j = 0; j < b.Length; j++) {
                    int MultiFragment = Base.IndexOf(a[i]) * Base.IndexOf(b[j]) + Leftovers;
                    Leftovers = MultiFragment / Base.Length;
                    ThisPart.Add(Base[MultiFragment % Base.Length]);
                }
                while (Leftovers > 0) {
                    ThisPart.Add(Base[Leftovers % Base.Length]);
                    Leftovers = Leftovers / Base.Length;
                }
                ThisPart.Reverse();
                MultiParts.Add(new string(ThisPart.ToArray()));
            }
            string Retorno = Base[0] + "";
            for (int i = 0; i < MultiParts.Count; i++) {
                Retorno = BaseSum(Retorno, MultiParts[i], Base);
            }
            return Retorno;
        }

        public static string BaseSum(string a, string b, string Base)
        {
            a = new string(a.Reverse().ToArray());
            b = new string(b.Reverse().ToArray());
            string Maior = a.Length > b.Length ? a : b;
            string Menor = a.Length < b.Length ? a : b;
            int Leftovers = 0;
            List<char> Resultado = new List<char>();
            for (int i = 0; i < Maior.Length || Leftovers > 0; i++) {
                int Soma;
                if (i >= Maior.Length)
                    Soma = Leftovers;
                else if (i >= Menor.Length)
                    Soma = Leftovers + Base.IndexOf(Maior[i]);
                else
                    Soma = Base.IndexOf(a[i]) + Base.IndexOf(b[i]) + Leftovers;
                Leftovers = 0;
                if (Soma > Base.Length - 1) {
                    while (Soma > Base.Length - 1) {
                        Soma -= Base.Length;
                        Leftovers += 1;
                    }
                    Resultado.Add(Base[Soma]);
                }
                else {
                    Resultado.Add(Base[Soma]);
                    Leftovers = 0;
                }
            }
            Resultado.Reverse();
            return new string(Resultado.ToArray());
        }

        public string ToString(string Base = Decimal)
        {
            if(Digitos == null) {
                return null;
            }
            List<char> Retorno = new List<char>();
            char[] CharsBase = Base.ToArray();
            int TamanhoBase = Base.Length;
            string Resultado = Base[0] + "";
            string Multi = Base[1] + "";
            string BaseLength = BaseConvert(BaseSize, Base);
            for (int i = 0; i < Digitos.Count; i++)
            {
                string ThisByte = BaseConvert((long)Digitos[i], Base);
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
