/**
 * Iaetec::Database::Autokryptex::Enkryptador 
 * Classe auxiliar para o IntEx e talvez novos algs de criptografia usando Fibonacci
 * Mas, talvez não.
 * AVISO: Alterar esses algoritmos pode tornar coisas criptografadas irreversiveis.
 *@Author: Felype Rennan Alves dos Santos
 * Março/2014
 * 
**/

using Figlotech.Autokryptex;
using Figlotech;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Figlotech.Autokryptex {
    public static class MathUtils {

        public static string BaseConvert(long number, string baseString) {
            List<char> Retv = new List<char>();
            bool negativo = false;
            long Backup = number;
            if (negativo = (Backup < 0))
                Backup = -Backup;
            char[] CharsBase = baseString.ToCharArray();
            do {
                int Indice = (int)(Backup % baseString.Length);
                Retv.Add(baseString[Indice]);
                Backup = Backup / baseString.Length;
            } while ((Backup) > 0);
            if (negativo)
                Retv.Add('-');
            Retv.Reverse();
            return new string(Retv.ToArray());
        }
        
        public static byte[] CramString(String input, int digitCount) {
            // if I use CrossRandom here it might bug
            // when setting the app secret or instance secret mutiple times.
            // Now picture this, you could set several keys in a sequence
            // and those keys in that exact sequence would effectively create a
            // neat lock mechanism.
            // Except... When you have an object at static level you might accidentally
            // hit it twice with the same value, and we don't want that to screw 
            // everything up.
            // I'll be copying this function but with CrossRandom instead
            // to inside CrossRandom and I'll use that to "set instanceKey"
            // this will be neat 
            Random cr = new Random(Int32.MaxValue ^ 123456789);
            byte[] workset = Encoding.UTF8.GetBytes(input);
            while (workset.Count() > digitCount) {
                byte ch = workset[0];
                workset = workset.Skip(1).ToArray();
                workset[cr.Next(workset.Length)] = (byte)(workset[cr.Next(workset.Length)] ^ ch);
            }
            while (workset.Count() < digitCount) {
                var ws = new byte[workset.Count() + 1];
                workset.CopyTo(ws, 0);
                ws[ws.Count() - 1] = (byte)cr.Next(byte.MaxValue);
                workset = ws;
            }

            return workset;
        }

        public static long BaseConvert(string Numero, string Base) {
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

        public static string BaseMult(string a, string b, string Base) {
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

        public static string BaseSum(string a, string b, string Base) {
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
                } else {
                    Resultado.Add(Base[Soma]);
                    Leftovers = 0;
                }
            }
            Resultado.Reverse();
            return new string(Resultado.ToArray());
        }

        public static IEnumerable<long> FibbonacciNumbers() {
            long j = 0;
            long i = 1;
            for (long k = 0; k < long.MaxValue; k++) {
                if (j < 0)
                    yield break;
                yield return j;
                long oldi = i;
                i = i + j;
                j = oldi;
            }
        }

        static List<long> foundprimes = new List<long>();
        private static bool IsPrime(long Number) {
            for(int i = 0; i<foundprimes.Count; i++) {
                if(Number % foundprimes[i] == 0) {
                    return false;
                }
            }
            return true;
        }
        public static IEnumerable<long> PrimeNumbers() {
            for(int i = 0; i < foundprimes.Count; i++) {
                yield return foundprimes[i];
            }
            for (long k = 3; k < long.MaxValue; k += 2) {
                if (IsPrime(k)) {
                    foundprimes.Add(k);
                    yield return k;
                }
            }
        }
    }
}
