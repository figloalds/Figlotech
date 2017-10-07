using Figlotech.Extensions;
using System;
using System.IO;
using System.Text;

namespace Figlotech.Core {
    public class RID
    {
        static Random rng = new Random();
        // DateTime.UtcNow.Ticks
        long Alpha;
        // Random number
        uint Beta;
        // Random initializer + sequential number
        uint Gamma;

        static uint gammaGen = (uint)rng.Next(0, Int32.MaxValue) / 11;

        public String Value64 {
            get {
                var val = this.ToString();
                return val;
            }
        }
        public String Value36 {
            get {
                var val = this.ToBase36();
                return val;
            }
        }
        public RID() {
            NewRid();
        }

        private void NewRid() {
            Alpha = DateTime.UtcNow.Ticks;
            Beta = (uint)rng.Next(0, Int32.MaxValue) + (uint)rng.Next(0, Int32.MaxValue);
            Gamma = gammaGen + 11;
        }

        public RID(String a) {
            try {
                byte[] barr = Convert.FromBase64String(a);
                using (var ms = new MemoryStream(barr)) {
                    Alpha = FiStreamExtensions.Read<Int64>(ms);
                    Beta = FiStreamExtensions.Read<UInt32>(ms);
                    Gamma = FiStreamExtensions.Read<UInt32>(ms);
                }
            } catch(Exception x) {
                NewRid(); 
            }
        }

        public RID(byte[] barr) {

            using (var ms = new MemoryStream(barr)) {
                Alpha   = BitConverter.ToInt64(barr, 0);
                Beta    = BitConverter.ToUInt32(barr, sizeof(Int64));
                Gamma   = BitConverter.ToUInt32(barr, sizeof(Int64) + sizeof(UInt32));
            }
        }

        public override string ToString() {
            return Convert.ToBase64String(this.ToByteArray());
        }

        public string ToBase36() {
            return new IntEx(this.ToByteArray()).ToString(IntEx.Base36);
        }

        public byte[] ToByteArray() {
            var barr = new byte[sizeof(Int64) + sizeof(UInt32) + sizeof(UInt32)];
            var a = BitConverter.GetBytes(Alpha);
            var b = BitConverter.GetBytes(Beta);
            var c = BitConverter.GetBytes(Gamma);
            a.CopyTo(barr, 0);
            b.CopyTo(barr, sizeof(Int64));
            c.CopyTo(barr, sizeof(Int64) + sizeof(UInt32));

            return barr;
        }

        public static implicit operator RID(String a) {
            return new RID(a);
        }

        public static implicit operator String(RID a) {
            return a.Value64;
        }

    }
}
