using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

namespace Figlotech.Core.Autokryptex
{
    /// <summary>
    /// This class provides stateful pseudo-random for 
    /// this lib's "lame" encryption methods.
    /// Calling this class with the same parameters will output
    /// the exact same numbers every time.
    /// </summary>
    public sealed class FiRandom {
        
        private long Seed;

        public FiRandom() {
            InitSeed(DateTime.UtcNow.Ticks);
        }
        public FiRandom(long RandomSeed) {
            InitSeed(RandomSeed);
        }
        int g1 = 0;
        const int chunkSize = 256;
        byte[][] hashs = new byte[8][];
        byte[] chunk = new byte[chunkSize];
        const int maxHashCount = 1024;
        int cursor = 0;

        private void InitSeed(long seed) {
            Seed = seed;
            g1++;
            byte[] hash;
            var bytes = BitConverter.GetBytes(seed);
            for (int x = 0; x < chunk.Length / sizeof(Int64); x++) {
                bytes[2] ^= (byte)(bytes[3] * bytes[0]);
                bytes[3] ^= (byte)(bytes[0] * bytes[1]);
                bytes[0] ^= (byte)(bytes[bytes[1] % bytes.Length]);
                bytes[1] ^= (byte)(bytes[bytes[2] % bytes.Length]);
                bytes[0] ^= (byte)(bytes[1] * bytes[2]);
                bytes[1] ^= (byte)(bytes[2] * bytes[3]);
                bytes[2] ^= (byte)(bytes[bytes[3] % bytes.Length]);
                bytes[3] ^= (byte)(bytes[bytes[0] % bytes.Length]);
                Array.Copy(bytes, 0, chunk, x * sizeof(Int64), sizeof(Int64));
            }
            {
                for (int r = 0; r < chunk.Length; r++) {
                    chunk[r] ^= (byte)(1 << ((r + 1) * (r - 1) * g1 % 8));
                    chunk[r] ^= (byte)((r + 1) * (r - 1) * g1 % 255);
                }
                for (int r = 0; r < chunk.Length; r++) {
                    chunk[r] ^= chunk[chunk[r]];
                }
                if (g1 > 8)
                    for(int i = 0; i < 32; i++) {
                        for(int j = 0; j < 8; j++) {
                            chunk[i * j] ^= hashs[7 - j][i];
                        }
                    }
                hash = Fi.Tech.ComputeHash(chunk);
            }
            hashs[g1 % 8] = hash;
            cursor = 0;
        }
        
        public int Gen() {
            if(cursor > chunk.Length - sizeof(int)) {
                InitSeed(Seed++);
            }
            var gen = BitConverter.ToInt32(chunk, cursor);
            while(gen < 0) gen += Int32.MaxValue;
            cursor += sizeof(int);
            return gen;
        }

        public int Next(int Maximum) {
            if(Maximum == 0) return 0;
            var retv = this.Gen();
            return retv % Maximum;
        }

        public int Next(int Minimum, int Maximum) {
            return Minimum + this.Gen() % Maximum;
        }
    }
}
