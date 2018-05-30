using Figlotech.Extensions;
using System;
using System.IO;
using System.Linq;
using System.Net.NetworkInformation;
using System.Text;

namespace Figlotech.Core {
    public class RID {
        static Random rng = new Random();
        static CrossRandom mRng = new CrossRandom(777);
        
        byte[] Signature = new byte[32];

        static uint segmentESequential = 0;
        static uint segmentEOrigin = (uint) (rng.Next(0, Int32.MaxValue)) + (uint) (rng.Next(0, Int32.MaxValue));

        private static RID _machineRID = null;
        public static RID MachineRID {
            get {
                if(_machineRID != null) {
                    return _machineRID;
                }

                foreach (var iface in NetworkInterface.GetAllNetworkInterfaces()) {
                    try {
                        var lbi = NetworkInterface.LoopbackInterfaceIndex;
                        var id = NetworkInterface.GetAllNetworkInterfaces()[lbi].Id.Replace("{","").Replace("}","").Replace("-", "");
                        var segmentA =  Enumerable.Range(0, id.Length)
                             .Where(x => x % 2 == 0)
                             .Select(x => Convert.ToByte(id.Substring(x, 2), 16))
                             .ToArray();
                        var segmentB = iface.GetPhysicalAddress().GetAddressBytes();
                        var segmentC = Fi.Range(0, 32 - (segmentA.Length + segmentB.Length)).Select(i => (byte)mRng.Next(256)).ToArray();
                        var finalRid = Fi.Tech.CombineArrays(segmentA, segmentB, segmentC);
                        
                        _machineRID = new RID(finalRid);
                        return _machineRID;
                    } catch (Exception x) {

                    }
                }
                if (!File.Exists("MACHINE.RID")) {
                    _machineRID = new RID(Fi.Range(0, 32).Select(i => (byte)rng.Next(256)).ToArray());
                    _machineRID = new RID();
                    File.WriteAllBytes("MACHINE.RID", _machineRID.AsByteArray);
                } else {
                    _machineRID = new RID(File.ReadAllBytes("MACHINE.RID"));
                }
                File.SetAttributes("MACHINE.RID", FileAttributes.Hidden);
                return _machineRID;
            }
        }

        byte[] _cSeg = null;
        byte[] CSeg {
            get {
                if(_cSeg == null) {
                    _cSeg = new byte[8];
                    Array.Copy(MachineRID.Signature, 16, _cSeg, 0, 8);
                }
                return _cSeg;
            }
        }

        public String AsBase64 {
            get {
                var val = this.ToString();
                return val;
            }
        }
        public String AsBase36 {
            get {
                var val = this.ToBase36();
                return val;
            }
        }

        public ulong AsULong {
            get {
                return new byte[][] {
                    Signature.Slice(0, 8).ToArray(),
                    Signature.Slice(7, 8).ToArray(),
                    Signature.Slice(15, 8).ToArray(),
                    Signature.Slice(23, 8).ToArray(),
                }
                .Select(a => BitConverter.ToUInt64(a, 0))
                .Aggregate((a, b) => a ^ b);
            }
        }

        public byte[] AsByteArray {
            get {
                var copy = new byte[Signature.Length];
                Signature.CopyTo(copy, 0);
                return copy;
            }
        }
    
        private RID(bool noAutoInit) {
            if(!noAutoInit) {
                NewRid();
            }    
        }

        public RID() {
            NewRid();
        }

        static Lazy<byte[]> RidSessionSegment = new Lazy<byte[]>(()=>BitConverter.GetBytes(DateTime.UtcNow.Ticks));
        private void NewRid() {
            lock ("GLOBAL_RID_GENERATION") {

                Buffer.BlockCopy(RidSessionSegment.Value, 0, Signature, 0, 8);
                Buffer.BlockCopy(BitConverter.GetBytes(DateTime.UtcNow.Ticks), 0, Signature, 8, 8);
                Buffer.BlockCopy(CSeg, 0, Signature, 16, 8);
                Buffer.BlockCopy(BitConverter.GetBytes(rng.Next()), 0, Signature, 24, 4);
                Buffer.BlockCopy(BitConverter.GetBytes(segmentEOrigin + ++segmentESequential), 0, Signature, 28, 4);
                
            }
        }

        public RID(byte[] barr) {
            if (barr.Length == 32) {
                Signature = barr;
            } else
                throw new InvalidOperationException("Trying to initialize RID to a byte[] that is not 32 bytes long");
        }

        public override string ToString() {
            return ToBase64();
        }

        public string ToBase64() {
            return Convert.ToBase64String(Signature);
        }

        public string ToBase36() {
            return new IntEx(Signature).ToString(IntEx.Base36);
        }

        public string ToHex() {
            return BitConverter.ToString(Signature).Replace("-", "");
        }

        public byte[] ToByteArray() {
            return this.AsByteArray;
        }
        
        public static implicit operator String(RID a) {
            return a.AsBase64;
        }

    }
}
