using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;

namespace Figlotech.Core {

    public class SyncTimeStampSource {
        DateTime BaseTimeStamp { get; set; }
        Stopwatch Watch { get; set; }

        private SyncTimeStampSource() { }

        public DateTime GetUtc() {
            var retv = BaseTimeStamp + Watch.Elapsed;
            return retv;
        }

        // GetUtcNetworkTime is based on stackoverflow.com/questions/1193955
        public static SyncTimeStampSource FromNtpServer(string ntpServer) {
            SyncTimeStampSource retv = new SyncTimeStampSource();
            retv.BaseTimeStamp = Fi.Tech.GetUtcNetworkTime(ntpServer);
            retv.Watch = new Stopwatch();
            retv.Watch.Start();
            return retv;
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
