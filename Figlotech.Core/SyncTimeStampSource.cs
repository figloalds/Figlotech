﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core {

    public class SyncTimeStampSource {
        DateTime BaseTimeStamp { get; set; } = DateTime.UtcNow;
        Stopwatch Watch { get; set; }

        private SyncTimeStampSource() { }

        public DateTime GetUtc() {
            var retv = BaseTimeStamp + Watch.Elapsed;
            return retv;
        }

        // GetUtcNetworkTime is based on stackoverflow.com/questions/1193955
        public static async Task<SyncTimeStampSource> FromNtpServer(string ntpServer) {
            SyncTimeStampSource retv = new SyncTimeStampSource();
            Stopwatch requestTimer = new Stopwatch();
            requestTimer.Start();
            retv.BaseTimeStamp = await Fi.Tech.GetUtcNetworkTime(ntpServer);
            requestTimer.Stop();
            retv.BaseTimeStamp.Subtract(TimeSpan.FromMilliseconds(requestTimer.ElapsedMilliseconds / 2));
            retv.Watch = new Stopwatch();
            retv.Watch.Start();
            return retv;
        }

        public static async Task<SyncTimeStampSource> FromNtpServerCached(string ntpServer) {
            var cacheDir = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData), "FTH");
            Directory.CreateDirectory(cacheDir);
            var cacheFile = Path.Combine(cacheDir, "system.time");
            if(File.Exists(cacheFile)) {
                var bytes = File.ReadAllBytes(cacheFile);
                if(bytes.Length < sizeof(Int64) * 2) {
                    File.Delete(cacheFile);
                    return await FromNtpServerCached(ntpServer);
                }
                var localTimeSaved = new DateTime(BitConverter.ToInt64(bytes, 0));
                var remoteTimeSaved = new DateTime(BitConverter.ToInt64(bytes, sizeof(Int64)));
                if (
                    localTimeSaved.Ticks != new FileInfo(cacheFile).CreationTimeUtc.Ticks ||
                    localTimeSaved > DateTime.UtcNow ||
                    remoteTimeSaved > DateTime.UtcNow ||
                    DateTime.UtcNow - localTimeSaved > TimeSpan.FromHours(2)
                ) {
                    File.Delete(cacheFile);
                    return await FromNtpServerCached(ntpServer);
                }
                var offsetLocal = DateTime.UtcNow - localTimeSaved;
                var offsetRemote = remoteTimeSaved + offsetLocal;
                var retv = FromTicks(offsetRemote.Ticks);
                return retv;
            } else {
                var bytes = new byte[sizeof(Int64) * 2];
                var retv = await FromNtpServer(ntpServer);
                var remoteTime = (retv).BaseTimeStamp;
                var localTime = DateTime.UtcNow;
                Array.Copy(BitConverter.GetBytes(localTime.Ticks), 0, bytes, 0, sizeof(Int64));
                Array.Copy(BitConverter.GetBytes(remoteTime.Ticks), 0, bytes, sizeof(Int64), sizeof(Int64));
                File.WriteAllBytes(cacheFile, bytes);
                File.SetCreationTime(cacheFile, localTime);
                return retv;
            }
        }

        public static SyncTimeStampSource FromTicks(long ticks) {
            SyncTimeStampSource retv = new SyncTimeStampSource();
            retv.BaseTimeStamp = new DateTime(ticks, DateTimeKind.Utc);
            retv.Watch = new Stopwatch();
            retv.Watch.Start();
            return retv;
        }

        public static SyncTimeStampSource FromLocalTime() {
            SyncTimeStampSource retv = new SyncTimeStampSource();
            retv.BaseTimeStamp = DateTime.UtcNow;
            retv.Watch = new Stopwatch();
            retv.Watch.Start();
            return retv;
        }
    }
}
