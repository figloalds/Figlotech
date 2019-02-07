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
using System.Threading.Tasks.Dataflow;

namespace Figlotech.Core {
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

        public static T CopyOf<T>(this Fi _selfie, T other) where T : new() {
            T retv = new T();
            retv.CopyFrom(other);
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

        public static bool EnableStdoutLogs { get; set; } = false;

        public static void StdoutLogs(this Fi _selfie, bool status) {
            EnableStdoutLogs = status;
            if (status) {
                FthLogStreamWriter.Start();
            } else {
                FthLogStreamWriter.Stop(false);
            }
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

        static SyncTimeStampSource _globalTimeStampSource = null;
        public static SyncTimeStampSource GlobalTimeStampSource {
            get {
                if(_globalTimeStampSource != null) {
                    return _globalTimeStampSource;
                }
                _globalTimeStampSource = SyncTimeStampSource.FromLocalTime();
                Fi.Tech.RunAndForget(() => {
                    _globalTimeStampSource = SyncTimeStampSource.FromNtpServer("pool.ntp.org");
                });
                return _globalTimeStampSource;
            }
        }

        public static DateTime GetUtcTime(this Fi _selfie) {
            return GlobalTimeStampSource.GetUtc();
        }

        // stackoverflow.com/questions/1193955
        public static DateTime GetUtcNetworkTime(this Fi _selfie, string ntpServer) {

            // NTP message size - 16 bytes of the digest (RFC 2030)
            var ntpData = new byte[48];

            //Setting the Leap Indicator, Version Number and Mode values
            ntpData[0] = 0x1B; //LI = 0 (no warning), VN = 3 (IPv4 only), Mode = 3 (Client Mode)

            var addresses = Dns.GetHostEntry(ntpServer).AddressList;

            //The UDP port number assigned to NTP is 123
            var ipEndPoint = new IPEndPoint(addresses[0], 123);
            //NTP uses UDP

            using (var socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp)) {
                socket.Connect(ipEndPoint);

                //Stops code hang if NTP is blocked
                socket.ReceiveTimeout = 3000;

                socket.Send(ntpData);
                socket.Receive(ntpData);
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
            lock (FTHLogStream) {
                if (EnableStdoutLogs && EnabledSystemLogs[origin]) {
                    FTHLogStream.WriteLine($"[{origin}] {s()}");
                    FTHLogStream.Flush();
                }
            }
        }

        public static T[,] DoubleGroupingToMatrix<T>(List<IGrouping<string, T>> groupingA, List<IGrouping<string, T>> groupingB, Func<T, T, bool> JoiningClause, Func<IEnumerable<T>, T> aggregator) {

            var tbl = new T[groupingA.Count, groupingB.Count];
            for (int i = 0; i < groupingA.Count; i++) {
                for (int v = 0; v < groupingB.Count; v++) {
                    tbl[i, v] = aggregator(groupingA[i].Where(id => JoiningClause.Invoke(id, groupingB[v].First())));
                }
            }

            return tbl;
        }

        public static string BytesToString(this Fi __selfie, Fi.DataUnitGeneralFormat format, long value) {
            int mult = 0;
            var unitNames = Fi.DataUnitNames[format.ToString()];
            var duf = Fi.DataUnitFactors[format.ToString()];
            while (value > duf && mult < unitNames.Length - 1) {
                value /= duf;
                mult++;
            }
            var plu = value > 1 ? "s" : "";
            return $"{value} {unitNames[mult]}{plu}";
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

        public static byte[] ComputeHash(this Fi _selfie, string str) {
            using(MemoryStream ms = new MemoryStream(new UTF8Encoding(false).GetBytes(str))) {
                ms.Seek(0, SeekOrigin.Begin);
                using (var sha = SHA256.Create()) {
                    return sha.ComputeHash(ms);
                }
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
            var objBuilder = new ObjectReflector();
            objBuilder.Slot(retv);
            foreach (var col in propercolumns) {
                object o = dr[col.ColumnName];
                string objField;
                if (mapReplacements != null && mapReplacements.ContainsKey(col.ColumnName)) {
                    objField = mapReplacements[col.ColumnName];
                } else {
                    objField = col.ColumnName;
                }
                objBuilder[objField] = o;
            }
        }

        public static T[] CombineArrays<T>(this Fi __selfie, params T[][] arrays) {
            var final = new T[arrays.Sum(a => a.Length)];
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

        private static DateTime _startupstamp = DateTime.UtcNow;
        public static DateTime ProgramStartupTimestamp => _startupstamp;
        public static bool DidTimeElapseFromProgramStart(this Fi _selfie, TimeSpan ts) {
            return DateTime.UtcNow.Subtract(_startupstamp) > ts;
        }

        public static List<T> Map<T>(this Fi _selfie, DataTable dt, Dictionary<String, string> mapReplacements = null) where T : new() {
            if(dt == null) {
                throw new NullReferenceException("Input DataTable to Map<T> cannot be null");
            }
            if (dt.Rows.Count < 1) return new List<T>();
            var init = DateTime.UtcNow;
            var fields = ReflectionTool.FieldsAndPropertiesOf(typeof(T));
            var objBuilder = new ObjectReflector();
            var mapMeta = Fi.Tech.MapMeta<T>(dt);
            List<T> retv = new List<T>();
            Parallel.For(0, dt.Rows.Count, i => {
                var val = Fi.Tech.Map<T>(dt.Rows[i], mapMeta, mapReplacements);
                //yield return val;
                lock (retv)
                    retv.Add(val);
            });
            Fi.Tech.WriteLine($"MAP<T> took {DateTime.UtcNow.Subtract(init).TotalMilliseconds}ms");
            return retv;
        }

        public static Lazy<IBDadosStringsProvider> _Strings = new Lazy<IBDadosStringsProvider>(() => new BDadosEnglishStringsProvider());
        public static IBDadosStringsProvider strings { get => _Strings.Value; set { _Strings = new Lazy<IBDadosStringsProvider>(() => value); } }

        private static Lazy<WorkQueuer> _globalQueuer = new Lazy<WorkQueuer>(() => new WorkQueuer("FIGLOTECH_GLOBAL_QUEUER", Environment.ProcessorCount, true));
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
            return (T) ReflectionTool.TryCast(input, typeof(T));
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
                throw new ArgumentNullException("extension");
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

        public static void FireTaskAndForget<T>(this Fi _selfie, Func<T> task, Action<Exception> handling = null, Action<bool> executeAnywaysWhenFinished = null) {
            var t = FireTask<T>(_selfie, task, handling, executeAnywaysWhenFinished);
        }

        public static async Task FireTask(this Fi _selfie, Action task, Action<Exception> handling = null, Action<bool> executeAnywaysWhenFinished = null) {
            await FireTask<int>(_selfie, () => { task?.Invoke(); return 0; }, handling, executeAnywaysWhenFinished);
        }
        public static async Task<T> FireTask<T>(this Fi _selfie, Func<T> task, Action<Exception> handling = null, Action<bool> executeAnywaysWhenFinished = null) {
            // Could just call the other function here
            // Decided to CTRL+C in favor of runtime performance.
            bool success = false;
            Task<T> retv = Task.Run<T>(() => {
                if (task != null) {
                    try {
                        return task.Invoke();
                    } catch (Exception x) {
                        try {
                            handling?.Invoke(x);
                        } catch (Exception z) {
                            Fi.Tech.Throw(z);
                        }
                    } finally {
                        try {
                            executeAnywaysWhenFinished?.Invoke(success);
                            lock (FiredTasksPreventGC) {
                                FiredTasksPreventGC
                                    .Where(t => t.IsCanceled || t.IsCompleted || t.IsFaulted)
                                    .ForEach(t => {
                                        if (t.Exception != null) {
                                            Fi.Tech.WriteLine($"Task {t.Id} threw {t.Exception.GetType().Name}:{t.Exception.Message}");
                                            // "Handling" need not apply maybe.
                                            // It probably didn't even throw an exception.
                                            // I'm checking this just to tell the .Net API to not crash the application
                                            // When this task is GC'ed
                                            // Because .Net loves to fuck my life by causing exceptions in tasks to completely break 
                                            // the entire program. I don't like that at all.
                                            Fi.Tech.Throw(t.Exception);
                                        }
                                    });
                                FiredTasksPreventGC.RemoveAll(t => t.IsCanceled || t.IsCompleted || t.IsFaulted);
                            }
                        } catch (Exception x) {
                            Fi.Tech.Throw(x);
                        }
                    }
                }
                return default(T);
            });
            lock (FiredTasksPreventGC)
                FiredTasksPreventGC.Add(retv);
            return await retv;
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
            if(retv.Length < input.Length) {
                for(int i = retv.Length; i < input.Length; i++) {
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
            if (DateTime.UtcNow > max)
                return;
            RunAndForget(__selfie, a, h, t);
        }

        public static event Action<Exception> OnUltimatelyUnhandledException;

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
            return new List<TDest>(input.Select(i => (TDest) (object) i));
        }

        public static List<T> CastUnknownList<T>(this Fi __selfie, Type src, object o) {
            return (List<T>) typeof(FiTechCoreExtensions)
                .GetMethod("CastList")
                .MakeGenericMethod(src, typeof(T))
                .Invoke(null, new object[] { null, o });
        }

        public static void BackgroundProcessList<T>(this Fi _selfie, IEnumerable<T> list, Action<T> work, Action<Exception> perWorkExceptionHandler = null, Action preWork = null, Action postWork = null, Action<Exception> preWorkExceptionHandling = null, Action<Exception> postWorkExceptionHandling = null) {
            RunAndForgetTasks(_selfie, (wq) => {
                try {
                    preWork?.Invoke();
                } catch (Exception x) {
                    preWorkExceptionHandling?.Invoke(x);
                    return;
                }
                list.ForEach(i => wq.Enqueue(() => work?.Invoke(i), perWorkExceptionHandler));
                try {
                    postWork?.Invoke();
                } catch (Exception x) {
                    postWorkExceptionHandling?.Invoke(x);
                    return;
                }
            });
        }

        public static void RunAndForgetTasks(this Fi _selfie, Action<WorkQueuer> action, string name = "Annonymous_multi_tasks") {
            RunAndForget(_selfie, () => {
                var wq = new WorkQueuer(name, Environment.ProcessorCount);
                action?.Invoke(wq);
                wq.Stop(true);
            }, x => {
                Throw(_selfie, x);
            });
        }
        public static bool InlineRunAndForget { get; set; }
        static WorkQueuer FiTechRAF = new WorkQueuer("RunAndForgetHost", Environment.ProcessorCount, true) { MinWorkers = Environment.ProcessorCount, MainWorkerTimeout = 60000, ExtraWorkerTimeout = 45000, ExtraWorkers = Environment.ProcessorCount * 4 };
        public static WorkJob RunAndForget(this Fi _selfie, string name, Action job, Action<Exception> handler = null, Action then = null) {
            if (InlineRunAndForget) {
                try {
                    job?.Invoke();
                } catch (Exception x) {
                    try {
                        handler?.Invoke(x);
                    } catch (Exception y) {
                        Throw(_selfie, y);
                    }
                } finally {
                    try {
                        then?.Invoke();
                    } catch (Exception y) {
                        Throw(_selfie, y);
                    }
                }
                return null;
            }

            var wj = FiTechRAF.Enqueue(job, handler, then);
            wj.Name = name;
            FiTechRAF.Start();
            return wj;
        }

        public static WorkJob RunAndForget(this Fi _selfie, Action job, Action<Exception> handler = null, Action then = null) {
            return RunAndForget(_selfie, "Anonymous_RunAndForget", job, handler, then);
        }

        public static byte[] GenerateKey(this Fi _selfie, string Str) {
            Random random = new Random(Fi.Tech.IntSeedFromString(Str));

            byte[] numArray = new byte[16];

            for (int index = 0; index < 16; ++index)
                numArray[index] = (byte)random.Next(256);
            return numArray;
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

        public static void MemberwiseCopy(this Fi _selfie, object origin, object destination) {
            if (origin == null)
                return;
            if (destination == null)
                return;
            ObjectReflector.Open(origin, (objA) => {
                ObjectReflector.Open(destination, (objB) => {
                    foreach (var field in objB) {
                        if (objA.ContainsKey(field.Key.Name)) {
                            objB[field.Key] = objA[field.Key.Name];
                        }
                    }
                });
            });
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
    }
}