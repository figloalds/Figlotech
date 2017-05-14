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
            179424691,  179425033,   179425601,   179426083,
            179424697,  179425063,   179425619,   179426089,
            179424719,  179425069,   179425637,   179426111,
            179424731,  179425097,   179425657,   179426123,
            179424743,  179425133,   179425661,   179426129,
            179424779,  179425153,   179425693,   179426141,
            179424787,  179425171,   179425699,   179426167,
            179424793,  179425177,   179425709,   179426173,
            179424797,  179425237,   179425711,   179426183,
            179424799,  179425261,   179425777,   179426231,
            179424821,  179425319,   179425811,   179426239,
            179424871,  179425331,   179425817,   179426263,
            179424887,  179425357,   179425819,   179426321,
            179424893,  179425373,   179425823,   179426323,
            179424899,  179425399,   179425849,   179426333,
            179424907,  179425403,   179425859,   179426339,
            179424911,  179425423,   179425867,   179426341,
            179424929,  179425447,   179425879,   179426353,
            179424937,  179425453,   179425889,   179426363,
            179424941,  179425457,   179425907,   179426369,
            179424977,  179425517,   179425943,   179426407,
            179424989,  179425529,   179425993,   179426447,
            179425003,  179425537,   179426003,   179426453,
            179425019,  179425559,   179426029,   179426491,
            179425027,  179425579,   179426081,   179426549,
        };

        private long[] Secret = new long[] {
            104971,  105323,  105557,  105907,
            104987,  105331,  105563,  105913,
            104999,  105337,  105601,  105929,
            105019,  105341,  105607,  105943,
        };
        public void UseSecret(String secret) {
            var secretBytes = MathUtils.CramString(secret, 16);
            for (int i = 0; i < secretBytes.Length; i++) {
                if (i >= secretBytes.Length)
                Secret[i] = MathUtils.PrimeNumbers().ElementAt((int)secretBytes[i] + 1000 );
            }
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
            this.Seed = (1900L ^ RandomSeed) + RandomSeed;
        }

        public CrossRandom(long RandomSeed, string password) {
            this.Seed = (1900L ^ RandomSeed) + RandomSeed;
            this.UseSecret(password);
        }

        public int Next() {
            ++this.CallCount;
            int num = (int)this.GenSalt();
            if (num >= 0)
                return num;
            return -num;
        }

        public int Next(int Maximum) {
            if(Maximum == 0) return 0;
            return this.Next() % Maximum;
        }

        public int Next(int Minimum, int Maximum) {
            return Minimum + this.Next() % Maximum;
        }

        private long Xor128() {
            long retv = 0;
            for(int i = 0; i < Secret.Length; i++) {
                retv ^= Secret[i];
                retv += Seed;
                retv += ++CallCount;
            }
            return retv;
        }

        public long GenSalt() {
            ++this.Subcount;
            long num1 = (long)(71261974 + (int)this.Seed * (int)((long)(1 + (1 + (int)this.CallCount * (int)this.Subcount) * 7) + (long)((1 + (int)this.CallCount * (int)this.Subcount) * 13)));
            long num2 = 109L;
            long num3 = 5L;
            long num4 = num2 - this.Xor128() % num3 + this.Xor128() % (num3 * 2L);
            long num5 = num1 + (1L + num1 % num4);
            long num6 = num5 - (2L + num5 % num4);
            long num7 = num6 + (2L + num6 % num4);
            long num8 = num7 - (1L + num7 % num4);
            long num9 = this.Xor128() % 100L >= 50L ? num8 - this.Xor128() : num8 + this.Xor128();
            if (this.Xor128() % 100L < 50L)
                ++num9;
            long num10 = this.Xor128() % 100L >= 50L ? num4 + 1L : num4 - 1L;
            if (num10 < num2 - num3)
                num10 = num2 - num3;
            if (num10 > num2 + num3) {
                long num11 = num2 + num3;
            }
            return num9;
        }
    }
}
