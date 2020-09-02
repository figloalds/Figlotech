using Figlotech.Core.Autokryptex;
using Figlotech.Core.Autokryptex.Legacy;
using Figlotech.Extensions;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Management;
using System.Net.NetworkInformation;
using System.Runtime.InteropServices;
using System.Text;

namespace Figlotech.Core {
    class PhysicalMediaInfo {
        public string SerialNumber { get; set; }
        public string UUID { get; set; }
    }

    public class RID {
        static FiRandom rng = new FiRandom();
        static LegacyCrossRandom mRng = new LegacyCrossRandom();

        byte[] Signature = new byte[32];

        static uint segmentESequential = 0;
        static uint segmentEOrigin = (uint)(rng.Next(0, Int32.MaxValue)) + (uint)(rng.Next(0, Int32.MaxValue));
        private static RID _machineRID = null;
        private static RID _machineRID2 = null;
        public static RID MachineRID {
            get {
                if (_machineRID != null) {
                    return _machineRID;
                }

                try {
                    DirectoryInfo rootDir;
                    if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows)) {
                        var di = new DriveInfo(Path.GetPathRoot(Environment.GetFolderPath(Environment.SpecialFolder.System)));
                        rootDir = di.RootDirectory;
                    } else {
                        rootDir = new DirectoryInfo("/");
                    }
                    var lbif = NetworkInterface.GetAllNetworkInterfaces().FirstOrDefault(i => i.NetworkInterfaceType == NetworkInterfaceType.Loopback);
                    var hwid = Fi.Tech.BinaryHashPad(Fi.Tech.ComputeHash((stream) => {

                        using (var writer = new StreamWriter(stream, new UTF8Encoding(false), 4096 * 1024, true)) {
                            if(Environment.OSVersion.Platform == PlatformID.Win32NT) {
                                var searcher = new ManagementObjectSearcher("SELECT * FROM Win32_PhysicalMedia");
                                foreach (ManagementObject wmi_HD in searcher.Get()) {
                                    if(wmi_HD.Path.Path.Contains("CDROM")) {
                                        continue;
                                    }
                                    var json = JsonConvert.SerializeObject(
                                        new PhysicalMediaInfo {
                                            SerialNumber = wmi_HD["SerialNumber"] as String,
                                            UUID = wmi_HD.Qualifiers["UUID"].Value as String
                                        }                                        
                                    , Formatting.Indented);
                                    writer.WriteLine(json);
                                    break;
                                }
                            } else {
                                writer.WriteLine(String.Join(",", NetworkInterface.GetAllNetworkInterfaces().Select(i => i.Id)));
                            }
                        }

                    }), 32);
                    var id = lbif.Id.RegExReplace("[^0-9A-F]", "");
                    var segmentA = BitConverter.GetBytes(rootDir.CreationTimeUtc.Ticks);
                    var segmentB = Fi.Tech.BinaryHashPad(Fi.Tech.ComputeHash(id), 8);
                    var segmentC = new byte[32 - (segmentA.Length + segmentB.Length)];
                    segmentC.IterateAssign((v, idx) => ((byte)((idx % 2 == 0 ? segmentA[idx / 2] : segmentB[idx / 2]) ^ mRng.Next(256))));
                    var finalRid = Fi.Tech.CombineArrays(segmentA, segmentB, segmentC);
                    finalRid.IterateAssign((v, idx) => (byte)(v ^ hwid[idx]));

                    _machineRID = new RID(finalRid);
                    return _machineRID;

                } catch (Exception x) {
                    Console.WriteLine(x.Message);
                }
                if(Environment.UserName == "IISAPPPOOL") {
                    var envRid = Environment.GetEnvironmentVariable("MACHINE.RID");
                    if (!string.IsNullOrEmpty(envRid)) {
                        var itx = new IntEx(envRid, IntEx.Base36).ToByteArray();
                        if (itx.Length == 32) {
                            _machineRID = new RID(itx);
                            return _machineRID;
                        }
                    }
                }

                var fileName = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), "FTH", "MACHINE.RID");
                if(!Directory.Exists(Path.GetDirectoryName(fileName))) {
                    Directory.CreateDirectory(Path.GetDirectoryName(fileName));
                }
                if (!File.Exists(fileName)) {
                    _machineRID = new RID(Fi.Range(0, 32).Select(i => (byte)rng.Next(256)).ToArray());
                    _machineRID = new RID();
                    File.WriteAllBytes(fileName, _machineRID.AsByteArray);
                    try {
                        File.SetAttributes(fileName, FileAttributes.System | FileAttributes.ReadOnly | FileAttributes.Hidden);

                    } catch(Exception x) {


                    }
                } else {
                    try {
                        _machineRID = new RID(File.ReadAllBytes(fileName));

                    } catch (Exception x) {

                        File.Delete(fileName);
                        return MachineRID;
                    }
                }
                File.SetAttributes(fileName, FileAttributes.Hidden);
                return _machineRID;
            }
        }

        public static RID MachineRID2 {
            get {
                if (_machineRID2 != null) {
                    return _machineRID2;
                }

                try {
                    DirectoryInfo rootDir;
                    if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows)) {
                        var di = new DriveInfo(Path.GetPathRoot(Environment.GetFolderPath(Environment.SpecialFolder.System)));
                        rootDir = di.RootDirectory;
                    } else {
                        rootDir = new DirectoryInfo("/");
                    }
                    var lbif = NetworkInterface.GetAllNetworkInterfaces().FirstOrDefault(i => i.NetworkInterfaceType == NetworkInterfaceType.Loopback);
                    var hwid = Fi.Tech.BinaryHashPad(Fi.Tech.ComputeHash((stream) => {

                        using (var writer = new StreamWriter(stream, new UTF8Encoding(false), 4096 * 1024, true)) {

                            if (Environment.OSVersion.Platform == PlatformID.Win32NT) {
                                var searcher = new ManagementObjectSearcher("SELECT * FROM Win32_Processor");
                                var objs = new List<ManagementObject>();
                                foreach (ManagementObject obj in searcher.Get()) {
                                    objs.Add(obj);
                                }
                                foreach (ManagementObject wmi_HD in objs.OrderBy(x => x.Path.Path)) {
                                    var json = JsonConvert.SerializeObject(
                                        new {
                                            Name = wmi_HD["Name"] as String,
                                            ProcessorId = wmi_HD["ProcessorId"] as String,
                                            UUID = wmi_HD.Qualifiers["UUID"].Value as String
                                        }
                                    , Formatting.Indented);
                                    writer.WriteLine(json);
                                }
                            } else {
                                writer.WriteLine(String.Join(",", NetworkInterface.GetAllNetworkInterfaces().Select(i => i.Id)));
                            }

                            if (Environment.OSVersion.Platform == PlatformID.Win32NT) {
                                var searcher = new ManagementObjectSearcher("SELECT * FROM Win32_BaseBoard");
                                var objs = new List<ManagementObject>();
                                foreach (ManagementObject obj in searcher.Get()) {
                                    objs.Add(obj);
                                }
                                foreach (ManagementObject wmi_HD in objs.OrderBy(x => x.Path.Path)) {
                                    var json = JsonConvert.SerializeObject(
                                        new {
                                            Product = wmi_HD["Product"] as String,
                                            SerialNumber = wmi_HD["SerialNumber"] as String,
                                            UUID = wmi_HD.Qualifiers["UUID"].Value as String
                                        }
                                    , Formatting.Indented);
                                    writer.WriteLine(json);
                                }
                            } else {
                                writer.WriteLine(String.Join(",", NetworkInterface.GetAllNetworkInterfaces().Select(i => i.Id)));
                            }
                        }

                    }), 32);
                    var id = lbif.Id.RegExReplace("[^0-9A-F]", "");
                    var segmentA = BitConverter.GetBytes(rootDir.CreationTimeUtc.Ticks);
                    var segmentB = Fi.Tech.BinaryHashPad(Fi.Tech.ComputeHash(id), 8);
                    var segmentC = new byte[32 - (segmentA.Length + segmentB.Length)];
                    segmentC.IterateAssign((v, idx) => ((byte)((idx % 2 == 0 ? segmentA[idx / 2] : segmentB[idx / 2]) ^ mRng.Next(256))));
                    var finalRid = Fi.Tech.CombineArrays(segmentA, segmentB, segmentC);
                    finalRid.IterateAssign((v, idx) => (byte)(v ^ hwid[idx]));

                    _machineRID2 = new RID(finalRid);
                    return _machineRID2;

                } catch (Exception x) {
                    Console.WriteLine(x.Message);
                }

                if(Environment.UserName == "IISAPPPOOL") {
                    var envRid = Environment.GetEnvironmentVariable("MACHINE.RID");
                    if (!string.IsNullOrEmpty(envRid)) {
                        var itx = new IntEx(envRid, IntEx.Base36).ToByteArray();
                        if (itx.Length == 32) {
                            _machineRID2 = new RID(itx);
                            return _machineRID2;
                        }
                    }
                }

                var fileName = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), "FTH", "MACHINE.RID2");
                if(!Directory.Exists(Path.GetDirectoryName(fileName))) {
                    Directory.CreateDirectory(Path.GetDirectoryName(fileName));
                }

                if (!File.Exists(fileName)) {
                    _machineRID2 = new RID(Fi.Range(0, 32).Select(i => (byte)rng.Next(256)).ToArray());
                    _machineRID2 = new RID();
                    File.WriteAllBytes(fileName, _machineRID2.AsByteArray);
                    try {
                        File.SetAttributes(fileName, FileAttributes.System | FileAttributes.ReadOnly | FileAttributes.Hidden);

                    } catch(Exception x) {


                    }
                } else {
                    try {
                        _machineRID2 = new RID(File.ReadAllBytes(fileName));

                    } catch (Exception x) {

                        File.Delete(fileName);
                        return MachineRID;
                    }
                }
                File.SetAttributes(fileName, FileAttributes.Hidden);
                return _machineRID2;
            }
        }

        byte[] _cSeg = null;
        byte[] CSeg {
            get {
                if (_cSeg == null) {
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
            if (!noAutoInit) {
                NewRid();
            }
        }

        public RID() {
            NewRid();
        }

        static Lazy<byte[]> RidSessionSegment = new Lazy<byte[]>(() => BitConverter.GetBytes(DateTime.UtcNow.Ticks));
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
