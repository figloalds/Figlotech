using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Autokryptex {
    /// <summary>
    /// This class provides stateful pseudo-random for 
    /// this lib's "lame" encryption methods.
    /// Calling this class with the same parameters will output
    /// the exact same numbers every time.
    /// </summary>
    public sealed class CrossRandom {
        private static long[] Primes = new long[] {
            32416187567, 32416188223, 32416188809, 32416189391,
            32416187627, 32416188227, 32416188839, 32416189459,
            32416187651, 32416188241, 32416188859, 32416189469,
            32416187659, 32416188257, 32416188877, 32416189493,
            32416187701, 32416188269, 32416188887, 32416189499,
            32416187719, 32416188271, 32416188899, 32416189511,
            32416187737, 32416188331, 32416188949, 32416189573,
            32416187747, 32416188349, 32416189019, 32416189633,
            32416187761, 32416188367, 32416189031, 32416189657,
            32416187773, 32416188397, 32416189049, 32416189669,
            32416187827, 32416188449, 32416189061, 32416189681,
            32416187863, 32416188451, 32416189063, 32416189717,
            32416187893, 32416188491, 32416189079, 32416189721,
            32416187899, 32416188499, 32416189081, 32416189733,
            32416187927, 32416188517, 32416189163, 32416189753,
            32416187929, 32416188527, 32416189181, 32416189777,
            32416187933, 32416188583, 32416189193, 32416189853,
            32416187953, 32416188589, 32416189231, 32416189859,
            32416187977, 32416188601, 32416189261, 32416189867,
            32416187987, 32416188647, 32416189277, 32416189877,
            32416188011, 32416188689, 32416189291, 32416189909,
            32416188037, 32416188691, 32416189321, 32416189919,
            32416188113, 32416188697, 32416189349, 32416189987,
            32416188127, 32416188767, 32416189361, 32416190039,
            32416188191, 32416188793, 32416189381, 32416190071,
        };

        private static long[] AppSecret = new long[16] {
            179424989, 179425529, 179425993, 179426447,
            179425003, 179425537, 179426003, 179426453,
            179425019, 179425559, 179426029, 179426491,
            179425027, 179425579, 179426081, 179426549,
        };
        private long[] InstanceSecret = new long[16] {
            104971,  105323,  105557,  105907,
            104987,  105331,  105563,  105913,
            104999,  105337,  105601,  105929,
            105019,  105341,  105607,  105943,
        };
        public static void UseAppSecret(String secret) {
            var secretBytes = MathUtils.CramString(secret, 16);
            for (int i = 0; i < secretBytes.Length; i++) {
                if (i <= AppSecret.Length)
                    AppSecret[i] = Primes[secretBytes[i] % Primes.Length];
            }
        }
        public void UseInstanceSecret(String secret) {
            var secretBytes = CramStringPlus(secret, 16);
            CrossRandom cr = new CrossRandom(Primes[77]);
            for (int i = 0; i < secretBytes.Length; i++) {
                if (i <= InstanceSecret.Length) {
                    InstanceSecret[i] = Primes[secretBytes[i] % Primes.Length] + cr.Next(Primes.Length ^ secretBytes[i]);
                }
            }
        }

        private static byte[] CramStringPlus(String input, int digitCount) {
            CrossRandom cr = new CrossRandom(Int32.MaxValue ^ 123456789);
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

        private long Seed;
        private long Subcount;
        private long CallCount;

        //    public CrossRandom()
        //    {
        //        DateTime now = DateTime.Now;
        //        this.Seed = (long)(now.Year ^ now.Month ^ now.Hour ^ now.Minute ^ now.Second);
        //        this.Subcount = 0L;
        //        this.CallCount = 0L;
        //    }

        public CrossRandom(long RandomSeed) {
            this.Seed = (AppSecret[0] ^ RandomSeed) + RandomSeed;
        }

        public CrossRandom(long RandomSeed, string password) {
            this.Seed = (AppSecret[0] ^ RandomSeed) + RandomSeed;
            this.UseInstanceSecret(password);
        }

        public int Next() {
            int num = (int)this.GenSalt();
            if (num < 0)
                num = -num;
            return num;
        }

        public int Next(int Maximum) {
            if(Maximum == 0) return 0;
            var retv = this.Next();
            return retv % Maximum;
        }

        public int Next(int Minimum, int Maximum) {
            return Minimum + this.Next() % Maximum;
        }

        long opc = 0;
        long[] historianVals = new long[16];
        private long Xor128() {
            long retv = Int64.MinValue;
            int asl = AppSecret.Length;
            int isl = InstanceSecret.Length;
            opc += CallCount;
            int r = 0;
            for (int i = 0; i < 1 + (opc % 2); i++) {
                ++opc;
                var val = opc * CallCount * i;
                retv += val;
                var a = AppSecret[val % asl];
                var b = Primes[val % Primes.Length];
                var c = InstanceSecret[val % isl];
                var d = historianVals[val % historianVals.Length];
                switch (opc % 6) {
                    case 0: retv += a*b; break;
                    case 1: retv += a*c; break;
                    case 2: retv += a*d; break;
                    case 3: retv += b*c; break;
                    case 4: retv += b*d; break;
                    case 5: retv += c*d; break;
                } 
            }
            //for (int i = 0; i < AppSecret.Length; i++) {
            //    switch (++opc % 3) {
            //        case 0:
            //            retv ^= AppSecret[i];
            //            break;
            //        case 1:
            //            retv ^= InstanceSecret[i];
            //            break;
            //        default:
            //            retv ^= AppSecret[i] * InstanceSecret[i];
            //            break;
            //    }
            //}
            historianVals[CallCount % historianVals.Length] = retv;
            return retv + CallCount;
        }

        public long GenSalt() {
            ++CallCount;
            var xor = Xor128();
            return xor;
        }
    }
}
