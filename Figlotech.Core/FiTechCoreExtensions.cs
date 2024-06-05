using System;
using System.Collections.Generic;
using System.Data;
using System.Reflection;
using System.Linq;
using System.Linq.Expressions;
using Newtonsoft.Json;
using System.Threading.Tasks;
using System.Diagnostics;
using Figlotech.Core.Helpers;
using Figlotech.Core.I18n;
using Figlotech.Core.Interfaces;
using System.Security.Cryptography;
using System.IO;
using System.Threading;
using Figlotech.Core.Extensions;
using Figlotech.Core.FileAcessAbstractions;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Drawing;
using System.Text.RegularExpressions;
using Figlotech.Core.Autokryptex;
using System.Runtime.CompilerServices;
using System.IO.Compression;
using System.Data.Common;

namespace Figlotech.Core {
    public struct RGB {
        private byte _r;
        private byte _g;
        private byte _b;

        public RGB(byte r, byte g, byte b) {
            this._r = r;
            this._g = g;
            this._b = b;
        }

        public byte R {
            get { return this._r; }
            set { this._r = value; }
        }

        public byte G {
            get { return this._g; }
            set { this._g = value; }
        }

        public byte B {
            get { return this._b; }
            set { this._b = value; }
        }

        public bool Equals(RGB rgb) {
            return (this.R == rgb.R) && (this.G == rgb.G) && (this.B == rgb.B);
        }
    }

    public struct HSL {
        private float _h;
        private float _s;
        private float _l;

        public HSL(float h, float s, float l) {
            this._h = h;
            this._s = s;
            this._l = l;
        }

        public float H {
            get { return this._h; }
            set { this._h = value; }
        }

        public float S {
            get { return this._s; }
            set { this._s = value; }
        }

        public float L {
            get { return this._l; }
            set { this._l = value; }
        }

        public bool Equals(HSL hsl) {
            return (this.H == hsl.H) && (this.S == hsl.S) && (this.L == hsl.L);
        }
    }

    public sealed class ScheduledWorkJob
    {
        public WorkQueuer Queuer { get; set; }
        public WorkJob WorkJob { get; set; }
        public DateTime ScheduledTime { get; set; }
        public TimeSpan? RecurrenceInterval { get; set; }
        public string Identifier { get; set; }
        public Timer Timer { get; set; }
        public bool IsActive { get; internal set; } = true;
        public string SchedulingStackTrace { get; internal set; }
        public CancellationTokenSource Cancellation { get; internal set; }

        ~ScheduledWorkJob() {
            try {
                Cancellation.Dispose();
            } catch (Exception) { }
        }
    }

    public delegate dynamic ComputeField(dynamic o);
    public static class BDadosActivator<T> {
        public static readonly Func<T> Activate =
            Expression.Lambda<Func<T>>(Expression.New(typeof(T))).Compile();
    }

    public static class FiTechCoreExtensions {
        private static Object _readLock = new Object();

        private static int _generalId = 0;

        public static ILogger ApiLogger;
        public static bool EnableDebug { get; set; } = false;

        public static object Null(this Fi _selfie) {
            return null;
        }

        public static void AssertNotNull(this Fi _selfie, object o) {
            if (o == null) {
                try {
                    if (Debugger.IsAttached) {
                        Debugger.Break();
                    }
                    Debugger.Launch();
                    var data = new List<string>();
                    int maxFrames = 20;
                    var stack = new StackTrace(true);
                    foreach (var f in stack.GetFrames()) {
                        data.Add($" at {f.ToString()}");
                        if (maxFrames-- <= 0) {
                            break;
                        }
                    }
                    throw new Exception(String.Join("\r\n", data));
                } catch (Exception) {
                    throw new Exception("Error asserting not null");
                }
            }
        }

        public static T CopyOf<T>(this Fi _selfie, T other) where T : new() {
            T retv = new T();
            retv.CopyFrom(other);
            return retv;
        }
        public static T CloneDataObject<T>(this Fi _selfie, T other) where T : IDataObject, new() {
            T retv = new T();
            Fi.Tech.CloneDataObject(other, retv);
            return retv;
        }

        private static bool CheckParams(IEnumerable<Type> types, IEnumerable<object> vals) {
            var numitems = 0;
            if ((numitems = types.Count()) != vals.Count())
                return false;
            IEnumerator<Type> et = types.GetEnumerator();
            IEnumerator<object> ev = vals.GetEnumerator();
            while (et.MoveNext()) {
                ev.MoveNext();
                if (ev.Current == null && et.Current.IsValueType)
                    return false;
                if (!et.Current.IsAssignableFrom(ev.Current.GetType()))
                    return false;
            }
            return true;
        }

        public static T Invoke<T>(this Fi _selfie, Type staticType, string GenericMethodName, Type genericType, params object[] args) {
            return (T)staticType
                .GetMethods()
                .Where(m => m.Name == (nameof(FiTechCoreExtensions.MapMeta)))
                .FirstOrDefault(m => CheckParams(m.GetParameters().Select(p => p.GetType()), args))
                .MakeGenericMethod(genericType)
                .Invoke(null, args);
        }

        public static string WildcardToRegex(this Fi selfie, String input) {
            input = input.Replace("*", "____WCASTER____");
            input = input.Replace("%", "____WCPCT____");
            input = input.Replace("?", "____WCQUEST____");
            var escaped = Regex.Escape(input);
            escaped = escaped.Replace("____WCASTER____", "[.]{0,}");
            escaped = escaped.Replace("____WCPCT____", "[.]{0,1}");
            escaped = escaped.Replace("____WCPCT____", "[.]{1}");
            return escaped;
        }

        public static bool EnableStdoutLogs { get; set; } = false;

        public static Color ColorFromHSL(this Fi __selfie, float h, float s, float l) {
            return ColorFromHSL(__selfie, new HSL(h, s, l));
        }

        public static Color ColorFromHSL(this Fi __selfie, HSL hsl) {
            byte r = 0;
            byte g = 0;
            byte b = 0;

            if (hsl.S == 0) {
                r = g = b = (byte)(hsl.L * 255);
            } else {
                float v1, v2;
                float hue = hsl.H / 360;

                v2 = (hsl.L < 0.5) ? (hsl.L * (1 + hsl.S)) : ((hsl.L + hsl.S) - (hsl.L * hsl.S));
                v1 = 2 * hsl.L - v2;

                r = (byte)(255 * HueToRGB(__selfie, v1, v2, hue + (1.0f / 3)));
                g = (byte)(255 * HueToRGB(__selfie, v1, v2, hue));
                b = (byte)(255 * HueToRGB(__selfie, v1, v2, hue - (1.0f / 3)));
            }

            return Color.FromArgb(r, g, b);
        }

        private static float HueToRGB(this Fi __selfie, float v1, float v2, float vH) {
            if (vH < 0)
                vH += 1;

            if (vH > 1)
                vH -= 1;

            if ((6 * vH) < 1)
                return (v1 + (v2 - v1) * 6 * vH);

            if ((2 * vH) < 1)
                return v2;

            if ((3 * vH) < 2)
                return (v1 + (v2 - v1) * ((2.0f / 3) - vH) * 6);

            return v1;
        }

        public static void Write(this Fi _selfie, string s = "") {

            if (!EnableStdoutLogs)
                return;

            if (Debugger.IsAttached)
                Debug.Write(s);
            Console.Write(s);
        }

        public static bool StdoutEventHubLogs {
            get => EnabledSystemLogs["FTH:EventHub"]; set => EnabledSystemLogs["FTH:EventHub"] = value;
        }
        public static bool StdoutRdbmsDataAccessorLogs {
            get => EnabledSystemLogs["FTH:RDB"];
            set => EnabledSystemLogs["FTH:RDB"] = value;
        }
        public static bool StdoutWorkQueuerLogs {
            get => EnabledSystemLogs["FTH:WorkQueuer"]; set => EnabledSystemLogs["FTH:WorkQueuer"] = value;
        }

        static StreamWriter fthLogStream;
        static WorkQueuer FthLogStreamWriter = new WorkQueuer("FTHLogStreamWriter", 1, false);
        public static StreamWriter FTHLogStream {
            get => fthLogStream ?? (fthLogStream = new StreamWriter(Console.OpenStandardOutput()));
            set => fthLogStream = value;
        }

        static bool isNtpTimeRequestedAlready = false;
        static SyncTimeStampSource _globalTimeStampSource = SyncTimeStampSource.FromLocalTime();
        public static SyncTimeStampSource GlobalTimeStampSource {
            get {
                return _globalTimeStampSource;
            }
        }

        private static bool NtpTimeHasBeenInitialized = false;
        private static bool _allowNTPTimeRequest;
        public static bool AllowNTPTimeRequest {
            get {
                return _allowNTPTimeRequest;
            }
            set {
                if (value && !NtpTimeHasBeenInitialized) {
                    _initGlobalNtpRequest();
                }
                _allowNTPTimeRequest = value;
            }
        }

        private static void _initGlobalNtpRequest() {
            var taskid = string.Intern("GLOBAL_TIMESTAMP_NTP_INIT");
            lock (taskid) {
                if (!NtpTimeHasBeenInitialized) {
                    NtpTimeHasBeenInitialized = true;
                    Fi.Tech.ScheduleTask(taskid, Fi.Tech.GetUtcTime(), new WorkJob(async () => {
                        await Task.Yield();
                        _globalTimeStampSource = await SyncTimeStampSource.FromNtpServerCached("0.pool.ntp.org");
                        Fi.Tech.Unschedule(taskid);
                        Fi.Tech.WriteLineInternal("SyncTime", () => "NTP time initialized");
                    }, async x => {
                        Fi.Tech.WriteLineInternal("SyncTime", () => "Error updating time with NTP server");
                    }), TimeSpan.FromSeconds(5));
                }
            }
        }

        public static DateTime GetUtcTime(this Fi _selfie) {
            return GlobalTimeStampSource.GetUtc();
        }

        // stackoverflow.com/questions/1193955
        public static async Task<DateTime> GetUtcNetworkTime(this Fi _selfie, string ntpServer) {

            // NTP message size - 16 bytes of the digest (RFC 2030)
            var ntpData = new byte[48];

            //Setting the Leap Indicator, Version Number and Mode values
            ntpData[0] = 0x1B; //LI = 0 (no warning), VN = 3 (IPv4 only), Mode = 3 (Client Mode)

            var addresses = Dns.GetHostEntry(ntpServer).AddressList;

            //The UDP port number assigned to NTP is 123
            var ipEndPoint = new IPEndPoint(addresses[0], 123);
            //NTP uses UDP

            using (var socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp)) {
                await socket.ConnectAsync(ipEndPoint);

                //Stops code hang if NTP is blocked
                socket.ReceiveTimeout = 3000;

                await Task.Run(() => socket.Send(ntpData));
                await Task.Run(() => socket.Receive(ntpData));
                socket.Close();
            }

            //Offset to get to the "Transmit Timestamp" field (time at which the reply 
            //departed the server for the client, in 64-bit timestamp format."
            const byte serverReplyTime = 40;

            //Get the seconds part
            ulong intPart = BitConverter.ToUInt32(ntpData, serverReplyTime);

            //Get the seconds fraction
            ulong fractPart = BitConverter.ToUInt32(ntpData, serverReplyTime + 4);

            //Convert From big-endian to little-endian
            intPart = SwapEndianness(intPart);
            fractPart = SwapEndianness(fractPart);

            var milliseconds = (intPart * 1000) + ((fractPart * 1000) / 0x100000000L);

            //**UTC** time
            var networkDateTime = (new DateTime(1900, 1, 1, 0, 0, 0, DateTimeKind.Utc)).AddMilliseconds((long)milliseconds);
            if (milliseconds < 1420070400) {
                return Fi.Tech.GetUtcTime();
            }
            return networkDateTime;
        }

        // stackoverflow.com/a/3294698/162671
        static uint SwapEndianness(ulong x) {
            return (uint)(((x & 0x000000ff) << 24) +
                ((x & 0x0000ff00) << 8) +
                ((x & 0x00ff0000) >> 8) +
                ((x & 0xff000000) >> 24));
        }

        public static void WriteLine(this Fi _selfie, string s = "") {
            WriteLineInternal(_selfie, "FTH:Generic", () => s);
        }
        public static void WriteLine(this Fi _selfie, string origin, string s) {
            WriteLineInternal(_selfie, origin, () => s);
        }

        public static SelfInitializerDictionary<string, bool> EnabledSystemLogs { get; private set; } = new SelfInitializerDictionary<string, bool>((str) => false);
        internal static void WriteLineInternal(this Fi _selfie, string origin, Func<string> s) {
            if (EnableStdoutLogs && EnabledSystemLogs[origin]) {
                FTHLogStream.WriteLine($"[{origin}] {s()}");
                FTHLogStream.Flush();
            }
        }

        public static T[,] decimalGroupingToMatrix<T>(List<IGrouping<string, T>> groupingA, List<IGrouping<string, T>> groupingB, Func<T, T, bool> JoiningClause, Func<IEnumerable<T>, T> aggregator) {

            var tbl = new T[groupingA.Count, groupingB.Count];
            for (int i = 0; i < groupingA.Count; i++) {
                for (int v = 0; v < groupingB.Count; v++) {
                    tbl[i, v] = aggregator(groupingA[i].Where(id => JoiningClause.Invoke(id, groupingB[v].First())));
                }
            }

            return tbl;
        }

        public static string BytesToString(this Fi __selfie, Fi.DataUnitGeneralFormat format, long value) {
            var val = (decimal)value;
            int mult = 0;
            var unitNames = Fi.DataUnitNames[format];
            var duf = Fi.DataUnitFactors[format];
            while (val > duf && mult < unitNames.Length - 1) {
                val /= duf;
                mult++;
            }
            var plu = val > 1 ? "" : "";
            return $"{val.ToString(mult > 1 ? "0.00" : mult > 0 ? "0.0" : "0")} {unitNames[mult]}{plu}";
        }

        //public static T Field<T>(this DataRow dr, int index) {
        //    object o = dr.ItemArray[index];
        //    return (T)o;
        //}

        //public static T Field<T>(this DataRow dr, string name) {
        //    var dt = dr.Table;
        //    List<DataColumn> properColumns = new List<DataColumn>();
        //    foreach (DataColumn column in dt.Columns)
        //        properColumns.Add(column);

        //    var properCol = properColumns.FirstOrDefault((c) => c.ColumnName == name);
        //    if (properCol == null) return default(T);
        //    var properColIndex = properColumns.IndexOf(properCol);
        //    object o = dr.ItemArray[properColIndex];
        //    return (T)o;
        //}

        public static T SyncLazyInit<T>(this Fi __selfie, ref T value, Func<T> init) {
            if (value == null) {
                var lockStr = $"{init.Target?.GetHashCode()}_{init.Method.Module.MDStreamVersion}_{init.Method.MetadataToken}".ToString();
                lock (String.Intern(lockStr)) {
                    if (value == null) {
                        value = init.Invoke();
                    }
                }
            }
            return value;
        }

        static List<ScheduledWorkJob> GlobalScheduledJobs { get; set; } = new List<ScheduledWorkJob>();

        private static void Reschedule(ScheduledWorkJob sched) {
            lock (sched) {
                if (sched.IsActive && sched.RecurrenceInterval.HasValue) {
                    var nextRun = sched.ScheduledTime;
                    while (nextRun < Fi.Tech.GetUtcTime()) {
                        nextRun += sched.RecurrenceInterval.Value;
                    }
                    try {
                        sched.Timer?.Change(System.Threading.Timeout.Infinite, System.Threading.Timeout.Infinite);
                    } catch (Exception x) {

                    }
                    try {
                        sched.Timer?.Dispose();
                    } catch (Exception x) {

                    }
                    sched.Timer = new Timer(_timerFn, sched, (int)Math.Max(0.0, (nextRun - Fi.Tech.GetUtcTime()).TotalMilliseconds), System.Threading.Timeout.Infinite);
                }
            }
        }

        private static void _timerFn(object a) {
            var sched = a as ScheduledWorkJob;
            if (sched.Cancellation.IsCancellationRequested) {
                return;
            }
            if(!sched.Queuer.Active) {
                return;
            }
            var millisDiff = (sched.ScheduledTime - Fi.Tech.GetUtcTime()).TotalMilliseconds;
            WorkJobExecutionRequest request = null;
            if (millisDiff > 5000) {
                if (Debugger.IsAttached && DebugSchedules) {
                    Debugger.Break();
                }
            } else {
                request = sched.Queuer.Enqueue(
                    new WorkJob(sched.WorkJob.action, sched.WorkJob.handling, sched.WorkJob.finished) {
                        Name = sched.WorkJob.Name,
                        SchedulingStackTrace = sched.SchedulingStackTrace
                    }, sched.Cancellation.Token
                );
            }
            if (request == null) {
                Fi.Tech.ScheduleTask(sched);
            } else {
                if (sched.RecurrenceInterval.HasValue) {
                    request.GetAwaiter().OnCompleted(() => {
                        Reschedule(sched);
                    });
                }
            }
        }

        public static void ScheduleTask(this Fi _selfie, string identifier, DateTime when, WorkJob job, TimeSpan? RecurrenceInterval = null) {
            ScheduleTask(_selfie, identifier, FiTechFireTaskWorker, when, job, RecurrenceInterval);
        }
        public static bool DebugSchedules { get; set; } = false;

        public static void ScheduleTask(this Fi _selfie, ScheduledWorkJob sched) {
            lock (GlobalScheduledJobs) {
                if (sched.Cancellation.IsCancellationRequested) {
                    return;
                }
                if(!sched.Queuer.Active) {
                    return;
                }
                if(sched.SchedulingStackTrace == null) {
                    sched.SchedulingStackTrace = new StackTrace().ToString();
                }
                var longRunningCheckEvery = DebugSchedules ? 5000 : 60000;
                var ms = (long)(sched.ScheduledTime - Fi.Tech.GetUtcTime()).TotalMilliseconds;
                var timeToFire = Math.Max(0, ms > longRunningCheckEvery ? longRunningCheckEvery : ms);
                sched.Timer = new Timer(_timerFn, sched, timeToFire, System.Threading.Timeout.Infinite);
                GlobalScheduledJobs.Add(sched);
                if (Debugger.IsAttached && DebugSchedules) {
                    Debugger.Break();
                }
            }
        }
        public static void ScheduleTask(this Fi _selfie, string identifier, WorkQueuer queuer, DateTime when, WorkJob job, TimeSpan? RecurrenceInterval = null, CancellationToken? cancellation = null) {
            var sched = new ScheduledWorkJob {
                Queuer = queuer,
                Identifier = identifier,
                WorkJob = job,
                ScheduledTime = when,
                RecurrenceInterval = RecurrenceInterval,
                Cancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellation ?? CancellationToken.None)
            };
            ScheduleTask(_selfie, sched);
        }
        public static void ClearSchedulesByWorker(this Fi _selfie, WorkQueuer instance, bool cancelRunning) {
            lock (GlobalScheduledJobs) {
                GlobalScheduledJobs.RemoveAll(s => {
                    if (s.Queuer != instance) {
                        return false;
                    }
                    s.Timer.Dispose();
                    if (cancelRunning) {
                        s.Cancellation.Cancel();
                    }
                    s.IsActive = false;
                    return true;
                });
            }
        }
        public static void ClearSchedules(this Fi _selfie, bool cancelRunning = false) {
            lock (GlobalScheduledJobs) {
                GlobalScheduledJobs.RemoveAll(s => {
                    s.Timer.Dispose();
                    if (cancelRunning) {
                        s.Cancellation.Cancel();
                    }
                    s.IsActive = false;
                    return true;
                });
            }
        }

        public static void Unschedule(this Fi _selfie, string identifier) {
            lock (GlobalScheduledJobs) {
                GlobalScheduledJobs.RemoveAll(s => {
                    if (s.Identifier == identifier) {
                        s.Timer.Dispose();
                        s.IsActive = false;
                        return true;
                    } else {
                        return false;
                    }
                });
            }
        }

        public static byte[] ComputeHash(this Fi _selfie, string str) {
            using (MemoryStream ms = new MemoryStream(new UTF8Encoding(false).GetBytes(str))) {
                ms.Seek(0, SeekOrigin.Begin);
                using (var sha = SHA256.Create()) {
                    return sha.ComputeHash(ms);
                }
            }
        }

        public static byte[] ComputeHash(this Fi _selfie, byte[] data) {
            using (var sha = SHA256.Create()) {
                return sha.ComputeHash(data);
            }
        }

        public static byte[] ComputeHash(this Fi _selfie, Stream stream) {
            using (var sha = SHA256.Create()) {
                return sha.ComputeHash(stream);
            }
        }

        public static byte[] ComputeHash(this Fi _selfie, Action<Stream> streamFn) {
            using (var sha = SHA256.Create()) {
                using (MemoryStream s = new MemoryStream()) {
                    streamFn(s);
                    s.Seek(0, SeekOrigin.Begin);
                    return sha.ComputeHash(s);
                }
            }
        }

        public static string LegacyGetHashFromStream(this Fi _selfie, Stream stream) {
            using (var md5 = MD5.Create()) {
                return Convert.ToBase64String(md5.ComputeHash(stream));
            }
        }

        public static async Task<List<T>> ReaderToObjectListUsingMapAsync<T>(this Fi _selfie, DbDataReader reader, Dictionary<string, string> map) where T : new() {
            var retv = new List<T>();
            var readerNames = Fi.Range(0, reader.FieldCount).Select(x => reader.GetName(x).ToUpper()).ToList();
            while (await reader.ReadAsync()) {
                var field = new T();
                foreach (var kvp in map) {
                    if (readerNames.Contains(kvp.Value.ToUpper())) {
                        ReflectionTool.SetValue(field, kvp.Key, reader[kvp.Value]);
                    }
                }
                retv.Add(field);
            }
            return retv;
        }
        public static List<T> ReaderToObjectListUsingMap<T>(this Fi _selfie, IDataReader reader, Dictionary<string, string> map) where T : new() {
            var retv = new List<T>();
            var readerNames = Fi.Range(0, reader.FieldCount).Select(x => reader.GetName(x).ToUpper()).ToList();
            while (reader.Read()) {
                var field = new T();
                foreach (var kvp in map) {
                    if (readerNames.Contains(kvp.Value.ToUpper())) {
                        ReflectionTool.SetValue(field, kvp.Key, reader[kvp.Value]);
                    }
                }
                retv.Add(field);
            }
            return retv;
        }

        public static Tuple<List<MemberInfo>, List<DataColumn>> MapMeta(this Fi _selfie, Type t, DataTable dt) {
            var fields = ReflectionTool.FieldsAndPropertiesOf(t);
            DataColumnCollection columns = dt.Columns;
            List<DataColumn> properColumns = new List<DataColumn>();
            foreach (DataColumn column in columns)
                properColumns.Add(column);

            return new Tuple<List<MemberInfo>, List<DataColumn>>(fields.ToList(), properColumns);
        }
        public static Tuple<List<MemberInfo>, List<DataColumn>> MapMeta<T>(this Fi _selfie, DataTable dt) {
            return MapMeta(_selfie, typeof(T), dt);
        }

        public static T ResultOf<T>(this Fi _selfie, Func<T> fn) {
            return fn.Invoke();
        }
        public static B ValueOrInitInDictionary<A, B>(this Fi _selfie, IDictionary<A, B> dict, A key, Func<B> customInit) {
            if (!dict.ContainsKey(key)) {
                dict.Add(key, customInit.Invoke());
            }
            return dict[key];
        }

        public static void Map(this Fi _selfie, object retv, DataRow dr, Tuple<List<MemberInfo>, List<DataColumn>> preMeta, Dictionary<String, string> mapReplacements = null) {
            if (preMeta == null) {
                preMeta = Fi.Tech.MapMeta(retv.GetType(), dr.Table);
            }
            var fields = preMeta.Item1;
            var propercolumns = preMeta.Item2;
            foreach (var col in propercolumns) {
                object o = dr[col.ColumnName];
                string objField;
                if (mapReplacements != null && mapReplacements.ContainsKey(col.ColumnName)) {
                    objField = mapReplacements[col.ColumnName];
                } else {
                    objField = col.ColumnName;
                }
                ReflectionTool.SetValue(retv, objField, Fi.Tech.ProperMapValue(o));
            }
        }

        public static object ProperMapValue(this Fi __selfie, object o) {
            if (o is DateTime dt && dt.Kind != DateTimeKind.Utc) {
                // I reluctantly admit that I'm using this horrible gimmick
                // It pains my soul to do this, because the MySQL Connector doesn't support
                // Timezone field in the connection string
                // And besides, this is code is supposed to be abstract for all ADO Plugins
                // But I'll go with "If it isn't UTC, then you're saving dates wrong"
                o = new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, dt.Second, dt.Millisecond, DateTimeKind.Utc);
            }
            return o;
        }

        public static T[] CombineArrays<T>(this Fi __selfie, params T[][] arrays) {
            var finalLength = 0;
            for (int i = 0; i < arrays.Length; i++) {
                finalLength += arrays[i].Length;
            }
            var final = new T[finalLength];
            int offset = 0;
            foreach (var a in arrays) {
                Buffer.BlockCopy(a, 0, final, offset, a.Length);
                offset += a.Length;
            }
            return final;
            //return arrays.Flatten().ToArray();
        }

        public static T Map<T>(this Fi _selfie, DataRow dr, Tuple<List<MemberInfo>, List<DataColumn>> preMeta = null, Dictionary<String, string> mapReplacements = null) where T : new() {
            var retv = new T();
            Map(_selfie, retv, dr, preMeta, mapReplacements);
            return retv;
        }

        private static DateTime _startupstamp = Fi.Tech.GetUtcTime();
        public static DateTime ProgramStartupTimestamp => _startupstamp;
        public static bool DidTimeElapseFromProgramStart(this Fi _selfie, TimeSpan ts) {
            return Fi.Tech.GetUtcTime().Subtract(_startupstamp) > ts;
        }

        public static List<T> Map<T>(this Fi _selfie, DataTable dt, Dictionary<String, string> mapReplacements = null) where T : new() {
            if (dt == null) {
                throw new NullReferenceException("Input DataTable to Map<T> cannot be null");
            }
            if (dt.Rows.Count < 1) return new List<T>();
            var init = Fi.Tech.GetUtcTime();
            var fields = ReflectionTool.FieldsAndPropertiesOf(typeof(T));
            var mapMeta = Fi.Tech.MapMeta<T>(dt);
            List<T> retv = new List<T>();
            Parallel.For(0, dt.Rows.Count, i => {
                var val = Fi.Tech.Map<T>(dt.Rows[i], mapMeta, mapReplacements);
                //yield return val;
                lock (retv)
                    retv.Add(val);
            });
            Fi.Tech.WriteLine($"MAP<T> took {Fi.Tech.GetUtcTime().Subtract(init).TotalMilliseconds}ms");
            return retv;
        }

        public static Lazy<IBDadosStringsProvider> _Strings = new Lazy<IBDadosStringsProvider>(() => new BDadosEnglishStringsProvider());
        public static IBDadosStringsProvider strings { get => _Strings.Value; set { _Strings = new Lazy<IBDadosStringsProvider>(() => value); } }

        private static Lazy<WorkQueuer> _globalQueuer = new Lazy<WorkQueuer>(() => new WorkQueuer("FIGLOTECH_GLOBAL_QUEUER", 2, true) {

        });
        public static WorkQueuer GlobalQueuer { get => _globalQueuer.Value; }

        public static int currentBDadosConnections = 0;

        public static List<String> RanChecks = new List<String>();

        public static string DefaultLogRepository = "Logs\\Fi.TechLogs";

        public static string DefaultBackupStore { get; set; } = "../Backups/";

        public static string Version {
            get {
                return Assembly.GetExecutingAssembly().GetName().Version.ToString();
            }
        }

        public static bool EnableBenchMarkers { get; set; } = false;
        public static bool EnableLiveBenchmarkerStdOut { get; set; } = false;

        public static string GetVersion(this Fi _selfie) {
            return Version;
        }

        public static T As<T>(this Fi _selfie, object input) {
            return (T)ReflectionTool.TryCast(input, typeof(T));
        }

        public static IBDadosStringsProvider GetStrings(this Fi _selfie) {
            return strings;
        }

        public static void SetStrings(this Fi _selfie, IBDadosStringsProvider provider) {
            strings = provider;
        }

        private static IDictionary<String, string> _mappings = new Dictionary<String, string>(StringComparer.InvariantCultureIgnoreCase) {

        #region Big freaking list of mime types
        // combination of values from Windows 7 Registry and 
        // from C:\Windows\System32\inetsrv\config\applicationHost.config
        // some added, including .7z and .dat
        {".323", "text/h323"},
        {".3g2", "video/3gpp2"},
        {".3gp", "video/3gpp"},
        {".3gp2", "video/3gpp2"},
        {".3gpp", "video/3gpp"},
        {".7z", "application/x-7z-compressed"},
        {".aa", "audio/audible"},
        {".AAC", "audio/aac"},
        {".aaf", "application/octet-stream"},
        {".aax", "audio/vnd.audible.aax"},
        {".ac3", "audio/ac3"},
        {".aca", "application/octet-stream"},
        {".accda", "application/msaccess.addin"},
        {".accdb", "application/msaccess"},
        {".accdc", "application/msaccess.cab"},
        {".accde", "application/msaccess"},
        {".accdr", "application/msaccess.runtime"},
        {".accdt", "application/msaccess"},
        {".accdw", "application/msaccess.webapplication"},
        {".accft", "application/msaccess.ftemplate"},
        {".acx", "application/internet-property-stream"},
        {".AddIn", "text/xml"},
        {".ade", "application/msaccess"},
        {".adobebridge", "application/x-bridge-url"},
        {".adp", "application/msaccess"},
        {".ADT", "audio/vnd.dlna.adts"},
        {".ADTS", "audio/aac"},
        {".afm", "application/octet-stream"},
        {".ai", "application/postscript"},
        {".aif", "audio/x-aiff"},
        {".aifc", "audio/aiff"},
        {".aiff", "audio/aiff"},
        {".air", "application/vnd.adobe.air-application-installer-package+zip"},
        {".amc", "application/x-mpeg"},
        {".application", "application/x-ms-application"},
        {".art", "image/x-jg"},
        {".asa", "application/xml"},
        {".asax", "application/xml"},
        {".ascx", "application/xml"},
        {".asd", "application/octet-stream"},
        {".asf", "video/x-ms-asf"},
        {".ashx", "application/xml"},
        {".asi", "application/octet-stream"},
        {".asm", "text/plain"},
        {".asmx", "application/xml"},
        {".aspx", "application/xml"},
        {".asr", "video/x-ms-asf"},
        {".asx", "video/x-ms-asf"},
        {".atom", "application/atom+xml"},
        {".au", "audio/basic"},
        {".avi", "video/x-msvideo"},
        {".axs", "application/olescript"},
        {".bas", "text/plain"},
        {".bcpio", "application/x-bcpio"},
        {".bin", "application/octet-stream"},
        {".bmp", "image/bmp"},
        {".c", "text/plain"},
        {".cab", "application/octet-stream"},
        {".caf", "audio/x-caf"},
        {".calx", "application/vnd.ms-office.calx"},
        {".cat", "application/vnd.ms-pki.seccat"},
        {".cc", "text/plain"},
        {".cd", "text/plain"},
        {".cdda", "audio/aiff"},
        {".cdf", "application/x-cdf"},
        {".cer", "application/x-x509-ca-cert"},
        {".chm", "application/octet-stream"},
        {".class", "application/x-java-applet"},
        {".clp", "application/x-msclip"},
        {".cmx", "image/x-cmx"},
        {".cnf", "text/plain"},
        {".cod", "image/cis-cod"},
        {".config", "application/xml"},
        {".contact", "text/x-ms-contact"},
        {".coverage", "application/xml"},
        {".cpio", "application/x-cpio"},
        {".cpp", "text/plain"},
        {".crd", "application/x-mscardfile"},
        {".crl", "application/pkix-crl"},
        {".crt", "application/x-x509-ca-cert"},
        {".cs", "text/plain"},
        {".csdproj", "text/plain"},
        {".csh", "application/x-csh"},
        {".csproj", "text/plain"},
        {".css", "text/css"},
        {".csv", "text/csv"},
        {".cur", "application/octet-stream"},
        {".cxx", "text/plain"},
        {".dat", "application/octet-stream"},
        {".datasource", "application/xml"},
        {".dbproj", "text/plain"},
        {".dcr", "application/x-director"},
        {".def", "text/plain"},
        {".deploy", "application/octet-stream"},
        {".der", "application/x-x509-ca-cert"},
        {".dgml", "application/xml"},
        {".dib", "image/bmp"},
        {".dif", "video/x-dv"},
        {".dir", "application/x-director"},
        {".disco", "text/xml"},
        {".dll", "application/x-msdownload"},
        {".dll.config", "text/xml"},
        {".dlm", "text/dlm"},
        {".doc", "application/msword"},
        {".docm", "application/vnd.ms-word.document.macroEnabled.12"},
        {".docx", "application/vnd.openxmlformats-officedocument.wordprocessingml.document"},
        {".dot", "application/msword"},
        {".dotm", "application/vnd.ms-word.template.macroEnabled.12"},
        {".dotx", "application/vnd.openxmlformats-officedocument.wordprocessingml.template"},
        {".dsp", "application/octet-stream"},
        {".dsw", "text/plain"},
        {".dtd", "text/xml"},
        {".dtsConfig", "text/xml"},
        {".dv", "video/x-dv"},
        {".dvi", "application/x-dvi"},
        {".dwf", "drawing/x-dwf"},
        {".dwp", "application/octet-stream"},
        {".dxr", "application/x-director"},
        {".eml", "message/rfc822"},
        {".emz", "application/octet-stream"},
        {".eot", "application/octet-stream"},
        {".eps", "application/postscript"},
        {".etl", "application/etl"},
        {".etx", "text/x-setext"},
        {".evy", "application/envoy"},
        {".exe", "application/octet-stream"},
        {".exe.config", "text/xml"},
        {".fdf", "application/vnd.fdf"},
        {".fif", "application/fractals"},
        {".filters", "Application/xml"},
        {".fla", "application/octet-stream"},
        {".flr", "x-world/x-vrml"},
        {".flv", "video/x-flv"},
        {".fsscript", "application/fsharp-script"},
        {".fsx", "application/fsharp-script"},
        {".generictest", "application/xml"},
        {".gif", "image/gif"},
        {".group", "text/x-ms-group"},
        {".gsm", "audio/x-gsm"},
        {".gtar", "application/x-gtar"},
        {".gz", "application/x-gzip"},
        {".h", "text/plain"},
        {".hdf", "application/x-hdf"},
        {".hdml", "text/x-hdml"},
        {".hhc", "application/x-oleobject"},
        {".hhk", "application/octet-stream"},
        {".hhp", "application/octet-stream"},
        {".hlp", "application/winhlp"},
        {".hpp", "text/plain"},
        {".hqx", "application/mac-binhex40"},
        {".hta", "application/hta"},
        {".htc", "text/x-component"},
        {".htm", "text/html"},
        {".html", "text/html"},
        {".htt", "text/webviewhtml"},
        {".hxa", "application/xml"},
        {".hxc", "application/xml"},
        {".hxd", "application/octet-stream"},
        {".hxe", "application/xml"},
        {".hxf", "application/xml"},
        {".hxh", "application/octet-stream"},
        {".hxi", "application/octet-stream"},
        {".hxk", "application/xml"},
        {".hxq", "application/octet-stream"},
        {".hxr", "application/octet-stream"},
        {".hxs", "application/octet-stream"},
        {".hxt", "text/html"},
        {".hxv", "application/xml"},
        {".hxw", "application/octet-stream"},
        {".hxx", "text/plain"},
        {".i", "text/plain"},
        {".ico", "image/x-icon"},
        {".ics", "application/octet-stream"},
        {".idl", "text/plain"},
        {".ief", "image/ief"},
        {".iii", "application/x-iphone"},
        {".inc", "text/plain"},
        {".inf", "application/octet-stream"},
        {".inl", "text/plain"},
        {".ins", "application/x-internet-signup"},
        {".ipa", "application/x-itunes-ipa"},
        {".ipg", "application/x-itunes-ipg"},
        {".ipproj", "text/plain"},
        {".ipsw", "application/x-itunes-ipsw"},
        {".iqy", "text/x-ms-iqy"},
        {".isp", "application/x-internet-signup"},
        {".ite", "application/x-itunes-ite"},
        {".itlp", "application/x-itunes-itlp"},
        {".itms", "application/x-itunes-itms"},
        {".itpc", "application/x-itunes-itpc"},
        {".IVF", "video/x-ivf"},
        {".jar", "application/java-archive"},
        {".java", "application/octet-stream"},
        {".jck", "application/liquidmotion"},
        {".jcz", "application/liquidmotion"},
        {".jfif", "image/pjpeg"},
        {".jnlp", "application/x-java-jnlp-file"},
        {".jpb", "application/octet-stream"},
        {".jpe", "image/jpeg"},
        {".jpeg", "image/jpeg"},
        {".jpg", "image/jpeg"},
        {".js", "application/javascript"},
        {".json", "application/json"},
        {".jsx", "text/jscript"},
        {".jsxbin", "text/plain"},
        {".latex", "application/x-latex"},
        {".library-ms", "application/windows-library+xml"},
        {".lit", "application/x-ms-reader"},
        {".loadtest", "application/xml"},
        {".lpk", "application/octet-stream"},
        {".lsf", "video/x-la-asf"},
        {".lst", "text/plain"},
        {".lsx", "video/x-la-asf"},
        {".lzh", "application/octet-stream"},
        {".m13", "application/x-msmediaview"},
        {".m14", "application/x-msmediaview"},
        {".m1v", "video/mpeg"},
        {".m2t", "video/vnd.dlna.mpeg-tts"},
        {".m2ts", "video/vnd.dlna.mpeg-tts"},
        {".m2v", "video/mpeg"},
        {".m3u", "audio/x-mpegurl"},
        {".m3u8", "audio/x-mpegurl"},
        {".m4a", "audio/m4a"},
        {".m4b", "audio/m4b"},
        {".m4p", "audio/m4p"},
        {".m4r", "audio/x-m4r"},
        {".m4v", "video/x-m4v"},
        {".mac", "image/x-macpaint"},
        {".mak", "text/plain"},
        {".man", "application/x-troff-man"},
        {".manifest", "application/x-ms-manifest"},
        {".map", "text/plain"},
        {".master", "application/xml"},
        {".mda", "application/msaccess"},
        {".mdb", "application/x-msaccess"},
        {".mde", "application/msaccess"},
        {".mdp", "application/octet-stream"},
        {".me", "application/x-troff-me"},
        {".mfp", "application/x-shockwave-flash"},
        {".mht", "message/rfc822"},
        {".mhtml", "message/rfc822"},
        {".mid", "audio/mid"},
        {".midi", "audio/mid"},
        {".mix", "application/octet-stream"},
        {".mk", "text/plain"},
        {".mmf", "application/x-smaf"},
        {".mno", "text/xml"},
        {".mny", "application/x-msmoney"},
        {".mod", "video/mpeg"},
        {".mov", "video/quicktime"},
        {".movie", "video/x-sgi-movie"},
        {".mp2", "video/mpeg"},
        {".mp2v", "video/mpeg"},
        {".mp3", "audio/mpeg"},
        {".mp4", "video/mp4"},
        {".mp4v", "video/mp4"},
        {".mpa", "video/mpeg"},
        {".mpe", "video/mpeg"},
        {".mpeg", "video/mpeg"},
        {".mpf", "application/vnd.ms-mediapackage"},
        {".mpg", "video/mpeg"},
        {".mpp", "application/vnd.ms-project"},
        {".mpv2", "video/mpeg"},
        {".mqv", "video/quicktime"},
        {".ms", "application/x-troff-ms"},
        {".msi", "application/octet-stream"},
        {".mso", "application/octet-stream"},
        {".mts", "video/vnd.dlna.mpeg-tts"},
        {".mtx", "application/xml"},
        {".mvb", "application/x-msmediaview"},
        {".mvc", "application/x-miva-compiled"},
        {".mxp", "application/x-mmxp"},
        {".nc", "application/x-netcdf"},
        {".nsc", "video/x-ms-asf"},
        {".nws", "message/rfc822"},
        {".ocx", "application/octet-stream"},
        {".oda", "application/oda"},
        {".odc", "text/x-ms-odc"},
        {".odh", "text/plain"},
        {".odl", "text/plain"},
        {".odp", "application/vnd.oasis.opendocument.presentation"},
        {".ods", "application/oleobject"},
        {".odt", "application/vnd.oasis.opendocument.text"},
        {".one", "application/onenote"},
        {".onea", "application/onenote"},
        {".onepkg", "application/onenote"},
        {".onetmp", "application/onenote"},
        {".onetoc", "application/onenote"},
        {".onetoc2", "application/onenote"},
        {".orderedtest", "application/xml"},
        {".osdx", "application/opensearchdescription+xml"},
        {".p10", "application/pkcs10"},
        {".p12", "application/x-pkcs12"},
        {".p7b", "application/x-pkcs7-certificates"},
        {".p7c", "application/pkcs7-mime"},
        {".p7m", "application/pkcs7-mime"},
        {".p7r", "application/x-pkcs7-certreqresp"},
        {".p7s", "application/pkcs7-signature"},
        {".pbm", "image/x-portable-bitmap"},
        {".pcast", "application/x-podcast"},
        {".pct", "image/pict"},
        {".pcx", "application/octet-stream"},
        {".pcz", "application/octet-stream"},
        {".pdf", "application/pdf"},
        {".pfb", "application/octet-stream"},
        {".pfm", "application/octet-stream"},
        {".pfx", "application/x-pkcs12"},
        {".pgm", "image/x-portable-graymap"},
        {".pic", "image/pict"},
        {".pict", "image/pict"},
        {".pkgdef", "text/plain"},
        {".pkgundef", "text/plain"},
        {".pko", "application/vnd.ms-pki.pko"},
        {".pls", "audio/scpls"},
        {".pma", "application/x-perfmon"},
        {".pmc", "application/x-perfmon"},
        {".pml", "application/x-perfmon"},
        {".pmr", "application/x-perfmon"},
        {".pmw", "application/x-perfmon"},
        {".png", "image/png"},
        {".pnm", "image/x-portable-anymap"},
        {".pnt", "image/x-macpaint"},
        {".pntg", "image/x-macpaint"},
        {".pnz", "image/png"},
        {".pot", "application/vnd.ms-powerpoint"},
        {".potm", "application/vnd.ms-powerpoint.template.macroEnabled.12"},
        {".potx", "application/vnd.openxmlformats-officedocument.presentationml.template"},
        {".ppa", "application/vnd.ms-powerpoint"},
        {".ppam", "application/vnd.ms-powerpoint.addin.macroEnabled.12"},
        {".ppm", "image/x-portable-pixmap"},
        {".pps", "application/vnd.ms-powerpoint"},
        {".ppsm", "application/vnd.ms-powerpoint.slideshow.macroEnabled.12"},
        {".ppsx", "application/vnd.openxmlformats-officedocument.presentationml.slideshow"},
        {".ppt", "application/vnd.ms-powerpoint"},
        {".pptm", "application/vnd.ms-powerpoint.presentation.macroEnabled.12"},
        {".pptx", "application/vnd.openxmlformats-officedocument.presentationml.presentation"},
        {".prf", "application/pics-rules"},
        {".prm", "application/octet-stream"},
        {".prx", "application/octet-stream"},
        {".ps", "application/postscript"},
        {".psc1", "application/PowerShell"},
        {".psd", "application/octet-stream"},
        {".psess", "application/xml"},
        {".psm", "application/octet-stream"},
        {".psp", "application/octet-stream"},
        {".pub", "application/x-mspublisher"},
        {".pwz", "application/vnd.ms-powerpoint"},
        {".qht", "text/x-html-insertion"},
        {".qhtm", "text/x-html-insertion"},
        {".qt", "video/quicktime"},
        {".qti", "image/x-quicktime"},
        {".qtif", "image/x-quicktime"},
        {".qtl", "application/x-quicktimeplayer"},
        {".qxd", "application/octet-stream"},
        {".ra", "audio/x-pn-realaudio"},
        {".ram", "audio/x-pn-realaudio"},
        {".rar", "application/octet-stream"},
        {".ras", "image/x-cmu-raster"},
        {".rat", "application/rat-file"},
        {".rc", "text/plain"},
        {".rc2", "text/plain"},
        {".rct", "text/plain"},
        {".rdlc", "application/xml"},
        {".resx", "application/xml"},
        {".rf", "image/vnd.rn-realflash"},
        {".rgb", "image/x-rgb"},
        {".rgs", "text/plain"},
        {".rm", "application/vnd.rn-realmedia"},
        {".rmi", "audio/mid"},
        {".rmp", "application/vnd.rn-rn_music_package"},
        {".roff", "application/x-troff"},
        {".rpm", "audio/x-pn-realaudio-plugin"},
        {".rqy", "text/x-ms-rqy"},
        {".rtf", "application/rtf"},
        {".rtx", "text/richtext"},
        {".ruleset", "application/xml"},
        {".s", "text/plain"},
        {".safariextz", "application/x-safari-safariextz"},
        {".scd", "application/x-msschedule"},
        {".sct", "text/scriptlet"},
        {".sd2", "audio/x-sd2"},
        {".sdp", "application/sdp"},
        {".sea", "application/octet-stream"},
        {".searchConnector-ms", "application/windows-search-connector+xml"},
        {".setpay", "application/set-payment-initiation"},
        {".setreg", "application/set-registration-initiation"},
        {".settings", "application/xml"},
        {".sgimb", "application/x-sgimb"},
        {".sgml", "text/sgml"},
        {".sh", "application/x-sh"},
        {".shar", "application/x-shar"},
        {".shtml", "text/html"},
        {".sit", "application/x-stuffit"},
        {".sitemap", "application/xml"},
        {".skin", "application/xml"},
        {".sldm", "application/vnd.ms-powerpoint.slide.macroEnabled.12"},
        {".sldx", "application/vnd.openxmlformats-officedocument.presentationml.slide"},
        {".slk", "application/vnd.ms-excel"},
        {".sln", "text/plain"},
        {".slupkg-ms", "application/x-ms-license"},
        {".smd", "audio/x-smd"},
        {".smi", "application/octet-stream"},
        {".smx", "audio/x-smd"},
        {".smz", "audio/x-smd"},
        {".snd", "audio/basic"},
        {".snippet", "application/xml"},
        {".snp", "application/octet-stream"},
        {".sol", "text/plain"},
        {".sor", "text/plain"},
        {".spc", "application/x-pkcs7-certificates"},
        {".spl", "application/futuresplash"},
        {".src", "application/x-wais-source"},
        {".srf", "text/plain"},
        {".SSISDeploymentManifest", "text/xml"},
        {".ssm", "application/streamingmedia"},
        {".sst", "application/vnd.ms-pki.certstore"},
        {".stl", "application/vnd.ms-pki.stl"},
        {".sv4cpio", "application/x-sv4cpio"},
        {".sv4crc", "application/x-sv4crc"},
        {".svc", "application/xml"},
        {".swf", "application/x-shockwave-flash"},
        {".t", "application/x-troff"},
        {".tar", "application/x-tar"},
        {".tcl", "application/x-tcl"},
        {".testrunconfig", "application/xml"},
        {".testsettings", "application/xml"},
        {".tex", "application/x-tex"},
        {".texi", "application/x-texinfo"},
        {".texinfo", "application/x-texinfo"},
        {".tgz", "application/x-compressed"},
        {".thmx", "application/vnd.ms-officetheme"},
        {".thn", "application/octet-stream"},
        {".tif", "image/tiff"},
        {".tiff", "image/tiff"},
        {".tlh", "text/plain"},
        {".tli", "text/plain"},
        {".toc", "application/octet-stream"},
        {".tr", "application/x-troff"},
        {".trm", "application/x-msterminal"},
        {".trx", "application/xml"},
        {".ts", "video/vnd.dlna.mpeg-tts"},
        {".tsv", "text/tab-separated-values"},
        {".ttf", "application/octet-stream"},
        {".tts", "video/vnd.dlna.mpeg-tts"},
        {".txt", "text/plain"},
        {".u32", "application/octet-stream"},
        {".uls", "text/iuls"},
        {".user", "text/plain"},
        {".ustar", "application/x-ustar"},
        {".vb", "text/plain"},
        {".vbdproj", "text/plain"},
        {".vbk", "video/mpeg"},
        {".vbproj", "text/plain"},
        {".vbs", "text/vbscript"},
        {".vcf", "text/x-vcard"},
        {".vcproj", "Application/xml"},
        {".vcs", "text/plain"},
        {".vcxproj", "Application/xml"},
        {".vddproj", "text/plain"},
        {".vdp", "text/plain"},
        {".vdproj", "text/plain"},
        {".vdx", "application/vnd.ms-visio.viewer"},
        {".vml", "text/xml"},
        {".vscontent", "application/xml"},
        {".vsct", "text/xml"},
        {".vsd", "application/vnd.visio"},
        {".vsi", "application/ms-vsi"},
        {".vsix", "application/vsix"},
        {".vsixlangpack", "text/xml"},
        {".vsixmanifest", "text/xml"},
        {".vsmdi", "application/xml"},
        {".vspscc", "text/plain"},
        {".vss", "application/vnd.visio"},
        {".vsscc", "text/plain"},
        {".vssettings", "text/xml"},
        {".vssscc", "text/plain"},
        {".vst", "application/vnd.visio"},
        {".vstemplate", "text/xml"},
        {".vsto", "application/x-ms-vsto"},
        {".vsw", "application/vnd.visio"},
        {".vsx", "application/vnd.visio"},
        {".vtx", "application/vnd.visio"},
        {".wav", "audio/wav"},
        {".wave", "audio/wav"},
        {".wax", "audio/x-ms-wax"},
        {".wbk", "application/msword"},
        {".wbmp", "image/vnd.wap.wbmp"},
        {".wcm", "application/vnd.ms-works"},
        {".wdb", "application/vnd.ms-works"},
        {".wdp", "image/vnd.ms-photo"},
        {".webarchive", "application/x-safari-webarchive"},
        {".webtest", "application/xml"},
        {".wiq", "application/xml"},
        {".wiz", "application/msword"},
        {".wks", "application/vnd.ms-works"},
        {".WLMP", "application/wlmoviemaker"},
        {".wlpginstall", "application/x-wlpg-detect"},
        {".wlpginstall3", "application/x-wlpg3-detect"},
        {".wm", "video/x-ms-wm"},
        {".wma", "audio/x-ms-wma"},
        {".wmd", "application/x-ms-wmd"},
        {".wmf", "application/x-msmetafile"},
        {".wml", "text/vnd.wap.wml"},
        {".wmlc", "application/vnd.wap.wmlc"},
        {".wmls", "text/vnd.wap.wmlscript"},
        {".wmlsc", "application/vnd.wap.wmlscriptc"},
        {".wmp", "video/x-ms-wmp"},
        {".wmv", "video/x-ms-wmv"},
        {".wmx", "video/x-ms-wmx"},
        {".wmz", "application/x-ms-wmz"},
        {".wpl", "application/vnd.ms-wpl"},
        {".wps", "application/vnd.ms-works"},
        {".wri", "application/x-mswrite"},
        {".wrl", "x-world/x-vrml"},
        {".wrz", "x-world/x-vrml"},
        {".wsc", "text/scriptlet"},
        {".wsdl", "text/xml"},
        {".wvx", "video/x-ms-wvx"},
        {".x", "application/directx"},
        {".xaf", "x-world/x-vrml"},
        {".xaml", "application/xaml+xml"},
        {".xap", "application/x-silverlight-app"},
        {".xbap", "application/x-ms-xbap"},
        {".xbm", "image/x-xbitmap"},
        {".xdr", "text/plain"},
        {".xht", "application/xhtml+xml"},
        {".xhtml", "application/xhtml+xml"},
        {".xla", "application/vnd.ms-excel"},
        {".xlam", "application/vnd.ms-excel.addin.macroEnabled.12"},
        {".xlc", "application/vnd.ms-excel"},
        {".xld", "application/vnd.ms-excel"},
        {".xlk", "application/vnd.ms-excel"},
        {".xll", "application/vnd.ms-excel"},
        {".xlm", "application/vnd.ms-excel"},
        {".xls", "application/vnd.ms-excel"},
        {".xlsb", "application/vnd.ms-excel.sheet.binary.macroEnabled.12"},
        {".xlsm", "application/vnd.ms-excel.sheet.macroEnabled.12"},
        {".xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"},
        {".xlt", "application/vnd.ms-excel"},
        {".xltm", "application/vnd.ms-excel.template.macroEnabled.12"},
        {".xltx", "application/vnd.openxmlformats-officedocument.spreadsheetml.template"},
        {".xlw", "application/vnd.ms-excel"},
        {".xml", "text/xml"},
        {".xmta", "application/xml"},
        {".xof", "x-world/x-vrml"},
        {".XOML", "text/plain"},
        {".xpm", "image/x-xpixmap"},
        {".xps", "application/vnd.ms-xpsdocument"},
        {".xrm-ms", "text/xml"},
        {".xsc", "application/xml"},
        {".xsd", "text/xml"},
        {".xsf", "text/xml"},
        {".xsl", "text/xml"},
        {".xslt", "text/xml"},
        {".xsn", "application/octet-stream"},
        {".xss", "application/xml"},
        {".xtp", "application/octet-stream"},
        {".xwd", "image/x-xwindowdump"},
        {".z", "application/x-compress"},
        {".zip", "application/x-zip-compressed"},
        #endregion

        };

        public static string GetMimeType(this Fi _selfie, string filename) {
            string extension = filename;
            while (extension.Contains(".")) {
                extension = extension.Substring(extension.IndexOf('.') + 1);
            }
            if (extension == null) {
                throw new ArgumentNullException($"Extension not present in {filename}");
            }

            if (!extension.StartsWith(".")) {
                extension = "." + extension;
            }

            return _mappings.TryGetValue(extension, out string mime) ? mime : "application/octet-stream";
        }

        const int keySize = 16;

        public static int IntSeedFromString(this Fi _selfie, string SeedStr) {
            int Seed = 0;
            for (int i = 0; i < SeedStr.Length; i++) {
                Seed ^= SeedStr[i] * (int)MathUtils.PrimeNumbers().ElementAt(1477);
            }
            return Seed;
        }

        internal static Thread MainThreadHandler;
        public static void SetMainThread(this Fi _selfie) {
            MainThreadHandler = Thread.CurrentThread;
        }

        static List<Task> FiredTasksPreventGC = new List<Task>();


        public static void Throw(this Fi _selfie, Exception x) {
            if (Debugger.IsAttached) {
                Debugger.Break();
            }
            try {
                OnUltimatelyUnhandledException?.Invoke(x);
            } catch (Exception y) {
                Fi.Tech.Error(y);
            }
        }

        public static Logger logger = new Logger(new FileAccessor(Environment.CurrentDirectory)) {
            Filename = "fi_uncaughterrors.txt"
        };

        public static void Error(this Fi _selfie, Exception x) {
            //logger.WriteLog(x);
        }

        public static IEnumerable<Exception> ToExceptionArray(this Exception ex) {
            return Fi.Tech.ExceptionTreeToArray(ex);
        }

        public static async Task<byte[]> GzipDeflateAsync(this Fi _selfie, byte[] bytes) {
            using var ms = new MemoryStream(bytes);
            using var msout = new MemoryStream();
            using (var gzs = new GZipStream(msout, CompressionLevel.Optimal, true)) {
                await ms.CopyToAsync(gzs);
            }
            msout.Seek(0, SeekOrigin.Begin);
            return msout.ToArray();
        }

        public static async Task<byte[]> GzipInflateAsync(this Fi _selfie, byte[] bytes) {
            using var ms = new MemoryStream(bytes);
            using var msout = new MemoryStream();
            using (var gzs = new GZipStream(ms, CompressionMode.Decompress, true)) {
                await gzs.CopyToAsync(msout);
            }
            return msout.ToArray();
        }

        public static byte[] GzipDeflate(this Fi _selfie, byte[] bytes) {
            using var ms = new MemoryStream(bytes);
            using var msout = new MemoryStream();
            using (var gzs = new GZipStream(msout, CompressionLevel.Optimal, true)) {
                ms.CopyTo(gzs);
            }
            msout.Seek(0, SeekOrigin.Begin);
            return msout.ToArray();
        }

        public static byte[] GzipInflate(this Fi _selfie, byte[] bytes) {
            using var ms = new MemoryStream(bytes);
            using var msout = new MemoryStream();
            using (var gzs = new GZipStream(ms, CompressionMode.Decompress, true)) {
                gzs.CopyTo(msout);
            }
            return msout.ToArray();
        }

        public static IEnumerable<Exception> ExceptionTreeToArray(this Fi _selfie, Exception x, int maxRecursionDepth = 12) {
            var ex = x;
            if (maxRecursionDepth <= 0) {
                yield break;
            }
            while (ex != null) {
                yield return ex;
                if (ex is AggregateException agex) {
                    foreach (var inex in agex.InnerExceptions) {
                        foreach (var inex2 in ExceptionTreeToArray(_selfie, inex, ++maxRecursionDepth)) {
                            yield return inex2;
                        }
                    }
                }
                ex = ex.InnerException;
            }
        }

        public static void SafeReadKeyOrIgnore(this Fi __selfie) {
            TryIf(__selfie, () => !Console.IsInputRedirected, () => { Console.ReadKey(); });
        }

        public static Action StackActions(this Fi __selfie, params Action[] args) {
            return () => {
                args.ForEach(a => a?.Invoke());
            };
        }
        public static byte[] BinaryHashPad(this Fi __selfie, byte[] input, int sz) {
            var retv = new byte[sz];
            Array.Copy(input, 0, retv, 0, Math.Min(retv.Length, input.Length));
            if (retv.Length < input.Length) {
                for (int i = retv.Length; i < input.Length; i++) {
                    retv[i % retv.Length] ^= input[i];
                }
            }
            return retv;
        }
        public static byte[] BinaryPad(this Fi __selfie, byte[] input, int sz) {
            var retv = new byte[sz];
            Array.Copy(input, 0, retv, 0, Math.Min(retv.Length, input.Length));

            return retv;
        }

        public static Thread SafeCreateThread(this Fi __selfie, Action a, Action<Exception> handler = null) {
            return new Thread(() => {
                try {
                    a?.Invoke();
                } catch (Exception x) {
                    handler?.Invoke(x);
                }
            });
        }

        public static void RunOnlyUntil(this Fi __selfie, DateTime max, Action a, Action<Exception> h = null, Action t = null) {
            if (Fi.Tech.GetUtcTime() > max)
                return;
            try {
                try {
                    a?.Invoke();
                } catch (Exception x) {
                    h?.Invoke(x);
                } finally {
                    t?.Invoke();
                }
            } catch (Exception x) {
                Fi.Tech.Error(x);
            }
        }
        public static void RunOnlyUntil(this Fi __selfie, DateTime max, Func<ValueTask> a, Func<Exception, ValueTask> h = null, Func<bool, ValueTask> t = null) {
            if (Fi.Tech.GetUtcTime() > max)
                return;
            FireTask(__selfie, a, h, t);
        }

        public static event Action<Exception> OnUltimatelyUnhandledException;

        public static int FreeTcpPort(this Fi __selfie) {
            TcpListener l = new TcpListener(IPAddress.Loopback, 0);
            l.Start();
            int port = ((IPEndPoint)l.LocalEndpoint).Port;
            l.Stop();
            return port;
        }


        public static void Try(this Fi __selfie, Action Try, Action<Exception> Catch, Action<bool> Finally) {
            bool success = false;
            try {
                Try?.Invoke();
                success = true;
            } catch (Exception x) {
                try {
                    Catch?.Invoke(x);

                } catch (Exception y) {


                }
            } finally {
                try {
                    Finally?.Invoke(success);

                } catch (Exception y) {


                }
            }
        }

        public static void TryIf(this Fi __selfie, Func<bool> condition, Action Try, Action<Exception> Catch = null, Action<bool> Finally = null) {
            FiTechCoreExtensions.Try(__selfie, () => {
                if (Try != null && condition != null && condition.Invoke()) {
                    Try?.Invoke();
                }
            }, Catch, Finally);
        }

        public static List<TDest> CastList<TSrc, TDest>(this Fi __selfie, List<TSrc> input) {
            return new List<TDest>(input.Select(i => (TDest)(object)i));
        }

        public static List<T> CastUnknownList<T>(this Fi __selfie, Type src, object o) {
            return (List<T>)typeof(FiTechCoreExtensions)
                .GetMethod("CastList")
                .MakeGenericMethod(src, typeof(T))
                .Invoke(null, new object[] { null, o });
        }

        public static void FireTaskTasks(this Fi _selfie, Action<WorkQueuer> action, string name = "Annonymous_multi_tasks") {
            FireTask(_selfie, async () => {
                await Task.Yield();
                var wq = new WorkQueuer(name, Environment.ProcessorCount);
                action?.Invoke(wq);
                await wq.Stop(true);
            }, async x => {
                await Task.Yield();
                Throw(_selfie, x);
            });
        }
        public static bool InlineFireTask { get; set; }
        public static bool DebugConnectionLifecycle { get; set; }

        public static async Task<bool> WaitForCondition(this Fi __selfie, Func<bool> condition, TimeSpan checkInterval, Func<TimeSpan> timeout) {
            Stopwatch sw = Stopwatch.StartNew();
            while (!condition()) {
                await Task.Delay(checkInterval);
                if (sw.Elapsed > timeout()) {
                    return false;
                }
            }
            return true;
        }

        public static async Task<bool> Timesout(this Fi __selfie, WorkJobExecutionRequest job, TimeSpan to) {
            return await Timesout(__selfie, job.TaskCompletionSource.Task, to);
        }
        public static async Task ThrowIfTimesout(this Fi __selfie, WorkJobExecutionRequest job, TimeSpan to, string message) {
            await ThrowIfTimesout(__selfie, job.TaskCompletionSource.Task, to, message);
        }
        public static async Task<bool> Timesout(this Fi __selfie, Task task, TimeSpan to) {
            Task timeoutTask = Task.Delay(to);
            Task completedTask = await Task.WhenAny(task, timeoutTask);

            // Check if the completed task is the original task or the timeout task
            if (completedTask == task) {
                // Original task completed before timeout
                await task; // Ensure the original task completes before returning
                return false;
            } else {
                // Timeout occurred
                return true;
            }
        }

        public static async Task ThrowIfTimesout(this Fi __selfie, Task task, TimeSpan to, string message) {
            if (await Timesout(
                __selfie,
                task,
                to
            )) {
                throw new InternalProgramException(message);
            }
        }

        static WorkQueuer FiTechFireTaskWorker = new WorkQueuer("FireTaskHost", Int32.MaxValue, true) { };

        public static WorkJobExecutionRequest FireTask(this Fi _selfie, string name, Func<CancellationToken, ValueTask> job, Func<Exception, ValueTask> handler = null, Func<bool, ValueTask> then = null) {
            var wj = FiTechFireTaskWorker.EnqueueTask(job, handler, then);
            wj.WorkJob.Name = name;
            FiTechFireTaskWorker.Start();
            return wj;
        }

        public static WorkJobExecutionRequest FireTask(this Fi _selfie, string name, Func<ValueTask> job, Func<Exception, ValueTask> handler = null, Func<bool, ValueTask> then = null) {
            var wj = FiTechFireTaskWorker.EnqueueTask(job, handler, then);
            wj.WorkJob.Name = name;
            FiTechFireTaskWorker.Start();
            return wj;
        }

        static FiAsyncMultiLock _globalMultiLock = new FiAsyncMultiLock();
        public static async Task<FiAsyncDisposableLock> Lock(this Fi _selfie, string key) {
            return await _globalMultiLock.Lock(key);
        }

        public static WorkJobExecutionRequest FireTask(this Fi _selfie, Func<CancellationToken, ValueTask> job, Func<Exception, ValueTask> handler = null, Func<bool, ValueTask> then = null) {
            return FireTask(_selfie, "Anonymous_FireTask", job, handler, then);
        }
        public static WorkJobExecutionRequest FireTask(this Fi _selfie, Func<ValueTask> job, Func<Exception, ValueTask> handler = null, Func<bool, ValueTask> then = null) {
            return FireTask(_selfie, "Anonymous_FireTask", job, handler, then);
        }

        public static void FireAndForget(this Fi _selfie, string name, Func<CancellationToken, ValueTask> job, Func<Exception, ValueTask> handler = null, Func<bool, ValueTask> then = null) {
            _ = FireTask(_selfie, name, job, handler, then);
        }
        public static void FireAndForget(this Fi _selfie, string name, Func<ValueTask> job, Func<Exception, ValueTask> handler = null, Func<bool, ValueTask> then = null) {
            _ = FireTask(_selfie, name, job, handler, then);
        }

        public static void FireAndForget(this Fi _selfie, Func<CancellationToken, ValueTask> job, Func<Exception, ValueTask> handler = null, Func<bool, ValueTask> then = null) {
            _ = FireTask(_selfie, "Anonymous_FireTask", job, handler, then);
        }
        public static void FireAndForget(this Fi _selfie, Func<ValueTask> job, Func<Exception, ValueTask> handler = null, Func<bool, ValueTask> then = null) {
            _ = FireTask(_selfie, "Anonymous_FireTask", job, handler, then);
        }

        public static CoroutineChannel<T> FireCoroutine<T>(this Fi _selfie, Func<CoroutineChannel<T>, ValueTask> job, Func<Exception, ValueTask> handler = null, Func<bool, ValueTask> then = null) {
            var retv = new CoroutineChannel<T>();
            _ = FireTask(_selfie, "Anonymous_FireTask", async () => {
                var t = job(retv);
                await t;
                if (!retv.IsClosed) {
                    retv.Close();
                }
            }, handler, then);
            return retv;
        }

        public static async Task<T> Promisify<T>(this Fi _selfie, Func<T> fn) {
            T retv = default(T);
            await Task.Run(async () => {
                try {
                    await Task.Yield();
                    retv = fn();
                } catch (Exception x) {
                    throw x;
                }
            });
            return retv;
        }
        public static async Task<T> Promisify<T>(this Fi _selfie, Func<Task<T>> fn) {
            T retv = default(T);
            await Fi.Tech.FireTask(async () => {
                await Task.Yield();
                retv = await fn();
            });
            return retv;
        }

        public static byte[] GenerateKey(this Fi _selfie, string Str) {
            Random random = new Random(Fi.Tech.IntSeedFromString(Str));

            byte[] numArray = new byte[16];

            for (int index = 0; index < 16; ++index)
                numArray[index] = (byte)random.Next(256);
            return numArray;
        }

        public static byte[] CramString(this Fi _selfie, String input, int digitCount) {
            byte[] workset = Fi.StandardEncoding.GetBytes(input);
            FiRandom cr = new FiRandom(BitConverter.ToInt64(Fi.Tech.ComputeHash(workset), 0));
            byte[] result = new byte[digitCount];

            for (int i = 0; i < result.Length; i++) {
                result[i] = (byte)cr.Next(255);
            }

            var loops = Math.Max(16, (digitCount / workset.Length) * 2);
            for (int i = 0; i < loops; i++) {
                var idx = cr.Next(Int32.MaxValue) % result.Length;
                foreach (byte c in workset) {
                    result[idx] ^= c;
                }
            }

            return result;
        }

        public static string GenerateIdString(this Fi _selfie, string uniqueId, int numDigits = 128) {
            char[] retval = new char[numDigits];
            Random r = new Random(8547 + (numDigits * 11));
            Random r2 = new Random(Fi.Tech.IntSeedFromString(uniqueId));
            for (int i = 0; i < retval.Length; i++) {
                int pos = r.Next(IntEx.Base36.Length / 2) + r2.Next(IntEx.Base36.Length / 2);
                pos = pos % IntEx.Base36.Length;
                retval[i] = IntEx.Base36[pos];
            }
            return new string(retval);
        }

        private static SelfInitializerDictionary<(Type, Type), List<(MemberInfo, MemberInfo)>> mwc_MemberRelationCache =
            new SelfInitializerDictionary<(Type, Type), List<(MemberInfo, MemberInfo)>>
        (
            ((Type, Type) tup) => {
                var (t1, t2) = tup;
                var retv = new List<(MemberInfo, MemberInfo)>();
                foreach (var m1 in ReflectionTool.FieldsAndPropertiesOf(t1)) {
                    foreach (var m2 in ReflectionTool.FieldsAndPropertiesOf(t2)) {
                        if (m1.Name == m2.Name) {
                            retv.Add((m1, m2));
                        }
                    }
                }
                return retv;
            }
        );

        public static void MemberwiseCopy(this Fi _selfie, object origin, object destination) {
            if (origin == null)
                return;
            if (destination == null)
                return;
            var sametype = origin.GetType() == destination.GetType();
            foreach (var rel in mwc_MemberRelationCache[(origin.GetType(), destination.GetType())]) {
                ReflectionTool.SetMemberValue(rel.Item2, destination, ReflectionTool.GetMemberValue(rel.Item1, origin));
            }
        }

        private static void RecursiveEnqueue(List<object> retv, object input, IEnumerator<WorkQueuer> queuers, IEnumerator<(Func<object, Task<object>> act, Func<Exception, Task> except)> step) {
            var hasNext = queuers.MoveNext();
            if (hasNext) {
                step.MoveNext();
                var thisStep = step.Current;
                queuers.Current.Enqueue(new WorkJob(async () => {
                    var next = await thisStep.act(input).ConfigureAwait(false);
                    RecursiveEnqueue(retv, next, queuers, step);
                }, async (ex) => {
                    Console.Write(ex.Message);
                    await thisStep.except(ex);
                }, async (b) => { }));
            } else {
                lock (retv) {
                    retv.Add(input);
                }
            }
        }

        public sealed class QueueFlowStepIn<T> : IParallelFlowStepIn<T> {
            Queue<T> Host { get; set; }
            public QueueFlowStepIn(Queue<T> host) {
                this.Host = host;
            }

            public void Put(T input) {
                this.Host.Enqueue(input);
            }

            public Task NotifyDoneQueueing() {
                return Task.FromResult(0);
            }
        }

        public sealed class FlowYield<T> {
            IParallelFlowStepIn<T> root { get; set; }
            ParallelFlowOutEnumerator<T> Enumerator { get; set; }
            public FlowYield(IParallelFlowStepIn<T> root, ParallelFlowOutEnumerator<T> enumerator) {
                this.root = root;
                this.Enumerator = enumerator;
            }
            public void Return(T o) {
                root.Put(o);
                Enumerator.Publish(o);
            }
            public void ReturnRange(IEnumerable<T> list) {
                list.ForEach(x => Return(x));
            }
        }

        public interface IParallelFlowStep {
        }
        public interface IParallelFlowStepOut<TOut> : IParallelFlowStep, IAsyncEnumerable<TOut> {
            TaskAwaiter<List<TOut>> GetAwaiter();
            IAsyncEnumerator<TOut> GetAsyncEnumerator(CancellationToken cancellation);
            Task<List<TOut>> TaskObj { get; }
        }
        public interface IParallelFlowStepIn<TIn> : IParallelFlowStep {
            void Put(TIn input);
            Task NotifyDoneQueueing();
        }

        public class ParallelFlowOutEnumerator<T> : IAsyncEnumerator<T> {
            public T Current { get; set; }

            public ValueTask DisposeAsync() {
                throw new NotImplementedException();
            }

            public async ValueTask<bool> MoveNextAsync() {
                lock(cache) {
                    if(cache.Count > 0) {
                        Current = cache.Dequeue();
                        return true;
                    }
                }
                var retv = await MoveNext.Task;
                if(!retv) {
                    return false;
                }
                Current = cache.Dequeue();
                return retv;
            }

            public void Publish(T o) {
                lock(cache) {
                    cache.Enqueue(o);
                }
                var mn = MoveNext;
                MoveNext = new TaskCompletionSource<bool>();
                mn.SetResult(true);
            }
            public void Finish() {
                MoveNext.SetResult(false);
            }

            Queue<T> cache = new Queue<T>();

            TaskCompletionSource<bool> MoveNext = new TaskCompletionSource<bool>();

        }

        public sealed class ParallelFlowStepInOut<TIn, TOut> : IParallelFlowStepIn<TIn>, IParallelFlowStepOut<TOut> {
            WorkQueuer queuer { get; set; }
            Queue<TOut> ValueQueue { get; set; } = new Queue<TOut>();
            Func<TIn, ValueTask<TOut>> SimpleAct { get; set; }
            Func<TIn, FlowYield<TOut>, ValueTask> YieldAct { get; set; }
            Func<Exception, Task> ExceptionHandler { get; set; }
            IParallelFlowStepIn<TOut> ConnectTo { get; set; }
            bool IgnoreOutput { get; set; } = false;
            IParallelFlowStepOut<TIn> Parent { get; set; }

            public TaskCompletionSource<List<TOut>> TaskCompletionSource { get; set; } = new TaskCompletionSource<List<TOut>>();
            public Task<List<TOut>> TaskObj => TaskCompletionSource.Task;
            public ParallelFlowStepInOut(Func<TIn, ValueTask<TOut>> Act, IParallelFlowStepOut<TIn> parent, int maxParallelism) {
                this.SimpleAct = Act;
                this.Parent = parent;
                this.queuer = new WorkQueuer("flow_step_enqueuer", Math.Max(1, maxParallelism));
            }
            public ParallelFlowStepInOut(Func<TIn, FlowYield<TOut>, ValueTask> Act, IParallelFlowStepOut<TIn> parent, int maxParallelism) {
                this.YieldAct = Act;
                this.Parent = parent;
                this.queuer = new WorkQueuer("flow_step_enqueuer", Math.Max(1, maxParallelism));
            }

            public void Put(TIn input) {
                queuer.Enqueue(async () => {
                    if (SimpleAct != null) {
                        var output = await SimpleAct(input).ConfigureAwait(false);
                        if (this.ConnectTo != null) {
                            this.ConnectTo.Put(output);
                        } else {
                            if (!IgnoreOutput) {
                                lock (ValueQueue)
                                    ValueQueue.Enqueue(output);
                            }
                        }
                        enumerator.Publish(output);
                    } else if (YieldAct != null) {
                        if (this.ConnectTo != null) {
                            await YieldAct(input, new FlowYield<TOut>(this.ConnectTo, enumerator)).ConfigureAwait(false);
                        } else {
                            await YieldAct(input, new FlowYield<TOut>(new QueueFlowStepIn<TOut>(this.ValueQueue), enumerator)).ConfigureAwait(false);
                        }
                    }
                }, async x => {
                    if (ExceptionHandler != null) {
                        await ExceptionHandler(x);
                    }
                });
            }
            Queue<TaskCompletionSource<int>> AlsoQueue { get; set; } = new Queue<TaskCompletionSource<int>>();
            public ParallelFlowStepInOut<TIn, TOut> Also(Func<FlowYield<TOut>, Task> yieldFn) {
                var src = new TaskCompletionSource<int>();
                AlsoQueue.Enqueue(src);
                Fi.Tech.FireAndForget(async () => {
                    if (this.ConnectTo != null) {
                        await yieldFn(new FlowYield<TOut>(this.ConnectTo, enumerator)).ConfigureAwait(false);
                    } else {
                        await yieldFn(new FlowYield<TOut>(new QueueFlowStepIn<TOut>(this.ValueQueue), enumerator)).ConfigureAwait(false);
                    }
                    src.SetResult(0);
                });
                return this;
            }

            public ParallelFlowStepInOut<TIn, TOut> Except(Func<Exception, Task> except) {
                this.ExceptionHandler = except;
                return this;
            }

            public ParallelFlowStepInOut<TOut, TNext> Then<TNext>(Func<TOut, TNext> act)
                => Then(Environment.ProcessorCount, (i)=> Fi.Result(act(i)));
            public ParallelFlowStepInOut<TOut, TNext> Then<TNext>(Func<TOut, ValueTask<TNext>> act)
                => Then(Environment.ProcessorCount, act);
            public ParallelFlowStepInOut<TOut, TNext> Then<TNext>(int maxParallelism, Func<TOut, ValueTask<TNext>> act) {
                if (maxParallelism < 0) {
                    maxParallelism = Environment.ProcessorCount;
                }
                var retv = new ParallelFlowStepInOut<TOut, TNext>(act, this, maxParallelism);
                this.ConnectTo = retv;
                FlushToConnected();
                return retv;
            }
            public ParallelFlowStepInOut<TOut, TNext> Then<TNext>(Action<TOut, FlowYield<TNext>> act)
                => Then<TNext>(Environment.ProcessorCount, (o, yield) => {
                    act(o, yield);
                    return Fi.Result();
                });
            public ParallelFlowStepInOut<TOut, TNext> Then<TNext>(Func<TOut, FlowYield<TNext>, ValueTask> act)
                => Then(Environment.ProcessorCount, act);
            public ParallelFlowStepInOut<TOut, TNext> Then<TNext>(int maxParallelism, Func<TOut, FlowYield<TNext>, ValueTask> act) {
                if (maxParallelism < 0) {
                    maxParallelism = Environment.ProcessorCount;
                }
                var retv = new ParallelFlowStepInOut<TOut, TNext>(act, this, maxParallelism);
                this.ConnectTo = retv;
                FlushToConnected();
                return retv;
            }

            public ParallelFlowStepInOut<TOut, TOut> Then(Func<TOut, Task> act)
                => Then(Environment.ProcessorCount, act);
            public ParallelFlowStepInOut<TOut, TOut> Then(int maxParallelism, Func<TOut, Task> act) {
                if (maxParallelism < 0) {
                    maxParallelism = Environment.ProcessorCount;
                }
                var retv = new ParallelFlowStepInOut<TOut, TOut>(async (x) => {
                    await act(x).ConfigureAwait(false);
                    return x;
                }, this, maxParallelism);
                this.ConnectTo = retv;
                FlushToConnected();
                return retv;
            }
            public void FlushToConnected() {
                if(this.ConnectTo != null) {
                    lock (ValueQueue)
                        while(ValueQueue.Count > 0)
                            this.ConnectTo.Put(ValueQueue.Dequeue());
                }
            }
            public async Task NotifyDoneQueueing() {
                while(AlsoQueue.Count > 0) {
                    await AlsoQueue.Dequeue().Task.ConfigureAwait(false);
                }
                await queuer.Stop(true).ConfigureAwait(false);
                if(this.ConnectTo != null) {
                    FlushToConnected();
                    await this.ConnectTo.NotifyDoneQueueing().ConfigureAwait(false);
                }
                TaskCompletionSource.SetResult(ValueQueue.ToList());
            }
            public TaskAwaiter<List<TOut>> GetAwaiter() {
                return TaskCompletionSource.Task.GetAwaiter();
            }
            private ParallelFlowOutEnumerator<TOut> enumerator = new ParallelFlowOutEnumerator<TOut>();
            public IAsyncEnumerator<TOut> GetAsyncEnumerator(CancellationToken cancellation) {
                return enumerator;
            }

        }

        public static ParallelFlowStepInOut<TIn, TIn> ParallelFlow<TIn>(this Fi __selfie, Action<FlowYield<TIn>> input, int maxParallelism = -1) {
            return ParallelFlow<TIn>(__selfie, (yield) => {
                input(yield);
                return Fi.Result();
            }, maxParallelism);
        }
        public static ParallelFlowStepInOut<TIn, TIn> ParallelFlow<TIn>(this Fi __selfie, Func<FlowYield<TIn>, ValueTask> input, int maxParallelism = -1) {
            if (maxParallelism < 0) {
                maxParallelism = Environment.ProcessorCount;
            }

            var retv = new ParallelFlowStepInOut<TIn, TIn>(async (x, yield) => {
                await input(yield);
            }, null, maxParallelism);

            Fi.Tech.FireAndForget(async () => {
                await Task.Yield();
                retv.Put(default(TIn));
            });

            return retv;
        }

        public static async Task<List<TResult>> ParallelFlow<TResult, T>(this Fi __selfie, IEnumerable<T> input, params (Func<object, Task<object>> act, Func<Exception, Task> except)[] steps) {
            WorkQueuer[] queuers = steps.Select(x => new WorkQueuer("ParallelFlow_Step", Environment.ProcessorCount, true)).ToArray();
            List<object> result = new List<object>();
            if (typeof(T) == typeof(Task<>)) {
                Console.WriteLine("ay");
            }
            foreach (var item in input) {
                FiTechCoreExtensions.RecursiveEnqueue(result, item, queuers.AsEnumerable().GetEnumerator(), steps.AsEnumerable().GetEnumerator());
            }
            foreach (var q in queuers) {
                await q.Stop(true);
            }

            return result.Select(x=> (TResult) x).ToList();
        }

        public static void CloneDataObject(this Fi _selfie, object origin, object destination) {
            if (origin == null)
                return;
            if (destination == null)
                return;

            var sameType = origin.GetType() == destination.GetType();
            var aMembers = ReflectionTool.FieldsAndPropertiesOf(origin.GetType()).ToDictionary(
                (x)=> x.Name, x=> x   
            );

            foreach(var field in ReflectionTool.FieldsAndPropertiesOf(destination.GetType())) {
                if(sameType || aMembers.ContainsKey(field.Name)) {
                    var value = ReflectionTool.GetMemberValue(sameType ? field : aMembers[field.Name], origin);
                    ReflectionTool.SetMemberValue(field, destination, value);
                }
            }

            ReflectionTool.SetValue(destination, "Id", 0);
            ReflectionTool.SetValue(destination, "RID", null);
        }

        static Random rng = new Random();
        public static string GenerateCode(this Fi _selfie, int numDigits, bool useLetters) {

            char[] vector = new char[numDigits];
            List<char> map = new List<char>();
            string numbers = "0123456789";
            string digits = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
            map.AddRange(numbers.ToCharArray());
            if (useLetters) {
                map.AddRange(digits.ToUpper().ToCharArray());
                map.AddRange(digits.ToLower().ToCharArray());
            }
            char[] secondMap = map.ToArray();
            for (int i = 0; i < secondMap.Length; i++) {
                int next = rng.Next(0, secondMap.Length);
                char old = secondMap[i];
                secondMap[i] = secondMap[next];
                secondMap[next] = old;
            }
            for (int i = 0; i < numDigits; i++) {
                int randomDigit = rng.Next(0, secondMap.Length);
                vector[i] = secondMap[randomDigit];
            }
            return new string(vector);
        }

        public static string GenerateCode(this Fi _selfie, int numDigits, string charMap) {
            var list = charMap.ToList();
            list.Sort((a, b) => {
                return -1 + rng.Next(3);
            });
            char[] vector = new char[numDigits];
            char[] secondMap = list.ToArray();
            for (int i = 0; i < secondMap.Length; i++) {
                int next = rng.Next(0, secondMap.Length);
                char old = secondMap[i];
                secondMap[i] = secondMap[next];
                secondMap[next] = old;
            }
            for (int i = 0; i < numDigits; i++) {
                int randomDigit = rng.Next(0, secondMap.Length);
                vector[i] = secondMap[randomDigit];
            }
            return new string(vector);
        }
    }
}