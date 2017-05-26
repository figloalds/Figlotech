using System;
using System.Collections.Generic;
using System.Data;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.Core;
using Figlotech.BDados.Builders;
using System.Reflection;
using System.Linq;
using System.Linq.Expressions;
using System.Management;
using Figlotech.BDados.Helpers;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using Newtonsoft.Json;
using Figlotech.Autokryptex;
using Figlotech.BDados.Authentication;
using Figlotech.BDados.I18n;
using System.Threading.Tasks;

namespace Figlotech.BDados {
    public delegate dynamic ComputeField(dynamic o);
    public static class BDadosActivator<T> {
        public static readonly Func<T> Activate =
            Expression.Lambda<Func<T>>(Expression.New(typeof(T))).Compile();
    }
    /// <summary>
    /// This is an utilitarian class, it provides quick static logic 
    /// for the rest of BDados to operate
    /// </summary>
    public static class FTH {
        private static Object _readLock = new Object();
        private static int _generalId = 0;

        public static T Map<T>(DataRow dr, DataColumnCollection columns) where T : new() {
            var fields = ReflectionTool.FieldsAndPropertiesOf(typeof(T));
            var objBuilder = new ObjectReflector();
            var retv = new T();
            objBuilder.Slot(retv);
            foreach (var col in fields) {
                if (!columns.Contains(col.Name)) continue;
                var typeofCol = ReflectionTool.GetTypeOf(col);

                object o = dr.Field<object>(col.Name);
                var tocUlType = Nullable.GetUnderlyingType(typeofCol);
                if (typeofCol.IsValueType && o == null) {
                    continue;
                }
                if (tocUlType != null) {
                    typeofCol = Nullable.GetUnderlyingType(typeofCol);
                }
                else {
                }

                if (typeofCol.IsEnum) {
                    objBuilder[col] = Enum.ToObject(typeofCol, o);
                }
                else
                if (o != null && o.GetType() != typeofCol) {
                    objBuilder[col] = Convert.ChangeType(o, typeofCol);
                }
                else {
                    objBuilder[col] = o;
                }
            }
            return retv;
        }

        public static void Map<T>(IList<T> input, DataTable dt) where T : new() {
            var init = DateTime.UtcNow;
            var fields = ReflectionTool.FieldsAndPropertiesOf(typeof(T));
            var objBuilder = new ObjectReflector();
            Parallel.For(0, dt.Rows.Count, (i) =>
            {
                var val = Map<T>(dt.Rows[i], dt.Columns);
                lock (input) {
                    input.Add(val);
                }
            });
            Console.WriteLine($"MAP<T> took {DateTime.UtcNow.Subtract(init).TotalMilliseconds}ms");
        }

        public static Lazy<IBDadosStringsProvider> _strings = new Lazy<IBDadosStringsProvider>(()=> new BDadosEnglishStringsProvider());
        public static IBDadosStringsProvider Strings { get => _strings.Value; set { _strings = new Lazy<IBDadosStringsProvider>(()=>value); } }

        private static Lazy<WorkQueuer> _globalQueuer = new Lazy<WorkQueuer>(()=> new WorkQueuer("FIGLOTECH_GLOBAL_QUEUER", Environment.ProcessorCount, true));
        public static WorkQueuer GlobalQueuer { get => _globalQueuer.Value; }

        public static int currentBDadosConnections = 0;

        public static List<string> RanChecks = new List<string>();

        public static string DefaultLogRepository = "Logs\\FTHLogs";

        public static String DefaultBackupStore { get; set; } = "../Backups/";

        public static String Version {
            get {
                return Assembly.GetExecutingAssembly().GetName().Version.ToString();
            }
        }

        public static void As<T>(object input, Action<T> act) {
            if (
                (typeof(T).IsInterface && input.GetType().GetInterfaces().Contains(typeof(T))) ||
                input.GetType().IsAssignableFrom(typeof(T))
            ) {
                act((T)input);
            }

        }
        public static void RecursiveGiveRids(IDataObject obj) {
            if(obj.RID == null) {
                obj.RID = FTH.GenerateIdString(obj.GetType().Name, 64);
                var props = ReflectionTool.FieldsAndPropertiesOf(obj.GetType());

                for(int i = 0; i < props.Count; i++) {
                    var t = ReflectionTool.GetTypeOf(props[i]);
                    if (t.GetInterfaces().Contains(typeof(IDataObject))) {
                        RecursiveGiveRids((IDataObject) ReflectionTool.GetValue(obj, props[i].Name));
                    }
                }
            }
        }

        public static T Deserialize<T>(String txt) where T: IDataObject, new() {
            var v = JsonConvert.DeserializeObject<T>(txt);
            RecursiveGiveRids(v);
            return v;
        }

        /// <summary>
        /// deprecated
        /// this gimmick should barely be used by the data accessors
        /// provided by the rdbms language providers
        /// But it's a contravention, must be avoided whenever possible.
        /// </summary>
        /// <param name="valor"></param>
        /// <returns></returns>

        public static String CheapSanitize(Object valor) {
            String valOutput;
            if (valor == null)
                return "NULL";
            if (valor.GetType().IsEnum) {
                return $"{(int)Convert.ChangeType(valor, Enum.GetUnderlyingType(valor.GetType()))}";
            }
            switch (valor.GetType().Name.ToLower()) {
                case "string":
                    if (valor.ToString() == "CURRENT_TIMESTAMP")
                        return "CURRENT_TIMESTAMP";
                    valOutput = ((String)valor);
                    valOutput = valOutput.Replace("\\", "\\\\");
                    valOutput = valOutput.Replace("\'", "\\\'");
                    valOutput = valOutput.Replace("\"", "\\\"");
                    return $"'{valOutput}'";
                case "float":
                case "double":
                case "decimal":
                    valOutput = Convert.ToString(valor).Replace(",", ".");
                    return $"{valOutput}";
                case "short":
                case "int":
                case "long":
                case "int16":
                case "int32":
                case "int64":
                    return Convert.ToString(valor);
                case "datetime":
                    return $"'{((DateTime)valor).ToString("yyyy-MM-dd HH:mm:ss")}'";
                default:
                    return Convert.ToString(valor);
            }

        }

        private static IDictionary<string, string> _mappings = new Dictionary<string, string>(StringComparer.InvariantCultureIgnoreCase) {

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

        public static Object GetRecordSetOf(Type t, IDataAccessor da) {
            try {
                var retv = Activator.CreateInstance(typeof(RecordSet<>).MakeGenericType(t), da);
                return retv;
            } catch (Exception x) {
                //Console.WriteLine(x.Message);
            }
            return null;
        }
        public static void SetPageSizeOfRecordSet(Object o, int? val) {
            try {
                o.GetType().GetMethod("Limit").Invoke(o, new object[] { val });
            } catch (Exception x) {
                Console.WriteLine(x.Message);
            }
        }

        public static int GetCountOfRecordSet(Object o) {
            try {
                return (int)o.GetType().GetProperties().Where((p) => p.Name == "Count").FirstOrDefault().GetValue(o);
            } catch (Exception x) {
                Console.WriteLine(x.Message);
            }
            return -1;
        }
        public static void SetAccessorOfRecordSet(Object o, IDataAccessor da) {
            try {
                o.GetType().GetProperties().Where((p) => p.Name == "DataAccessor").FirstOrDefault().SetValue(o, da);
            } catch (Exception x) {
                Console.WriteLine(x.Message);
            }
        }

        public static Object LoadAllOfType(IDataAccessor da, Type o, int p, int? limit) {
            try {
                var type = da
                        .GetType();
                var methods = type
                        .GetMethods();
                var method = methods
                        .Where(
                            (m) => m.Name == "LoadAll" && m.GetParameters().Count() == 3
                            && m.GetParameters()[1].ParameterType == typeof(int?) && m.GetParameters()[2].ParameterType == typeof(int?))
                        .FirstOrDefault();
                var finalMethod =
                    method
                        .MakeGenericMethod(o);
                return finalMethod?.Invoke(da, new object[] { null, p, limit });
                //o.GetType().GetMethod("LoadAll").Invoke(o, new object[] { null, p });
            } catch (Exception x) {
                Console.WriteLine(x.Message);
            }
            return null;
        }

        public static RecordSet<T> LoadAllOfByUpdateTime<T>(IDataAccessor da, long lastUpdate, int p, int? limit) where T : IDataObject, new() {
            return new RecordSet<T>(da);
        }

        public static Object LoadAllOfByUpdateTime(Type type, IDataAccessor da, long lastUpdate, int p, int? limit) {
            // Holy mother of Reflection!
            var updateTime = new DateTime(lastUpdate);
            var paramExpr = Expression.Parameter(type, "x");
            var member = Expression.MakeMemberAccess(paramExpr, type.GetMembers().FirstOrDefault(m => m.Name == "DataUpdate"));
            var conditionBody = Expression.GreaterThan(member, Expression.Convert(Expression.Constant(updateTime), member.Type));
            var funcTBool = typeof(Func<,>).MakeGenericType(
                        type, typeof(bool));
            var exprMethod = typeof(Expression)
                .GetMethods().FirstOrDefault(
                    m => m.Name == "Lambda")
                .MakeGenericMethod(
                    funcTBool);
            var expr = exprMethod
                .Invoke(null, new object[] { conditionBody, new ParameterExpression[] { paramExpr } });
            var condition = Activator.CreateInstance(
                typeof(Conditions<>).MakeGenericType(type)
                , new object[] { expr });

            var loadAllMethod = da.GetType().GetMethods().FirstOrDefault(
                m =>
                    m.Name == "LoadAll" &&
                    m.GetParameters().Length == 3)
                .MakeGenericMethod(type);
            var retv = loadAllMethod
                .Invoke(da, new object[] { expr, 1, null });
            // IT FUCKING WORKS BIATCH YAY!
            return retv;
        }

        public static void SaveRecordSet(Object o) {
            try {
                o.GetType().GetMethod("Save").Invoke(o, new object[1]);
            } catch (Exception x) {
                Console.WriteLine(x.Message);
            }
        }
        public class CopyProgressReport {
            int min = 0;
            int max = 1;
            int current = 0;
        }

        public static QueryBuilder ListRids<T>(RecordSet<T> set) where T: IDataObject, new() {
            QueryBuilder retv = new QueryBuilder();
            for (int i = 0; i < set.Count; i++) {
                retv.Append(
                    new QueryBuilder(
                        $"@{IntEx.GerarShortRID()}",
                        set[i].RID
                    )
                );
                if (i < set.Count - 1)
                    retv.Append(",");
            }
            return retv;
        }

        static int numPerms = Enum.GetValues(typeof(Acl)).Length;
        static string notEnoughBytesMsg =
@"
Your implementation of IBDadosUserPermissions doesn't have enough bytes.
You need at least 5 * 'bpmi' bits, being bpmi the 'biggest permission index' you intend to use
This math is in bits, do divide by 8 and round up to get the exact count of bytes you need for
your program.
EG: If I need a permission BDADOS_CREATE_SKYNET and that permission is runtime int value of
77, I'll need ((77 * 5)+BDPIndex) / 8 = 49 bytes on my permissions buffer because this permisison will be
saught for using this logic:
byteIndex=((77 * 5)+BDPIndex) / 8;
bitIndex=((77 * 5)+BDPIndex) % 8;
permissionCheck = (Permissions[byteIndex] & (1 << bitOffset)) > 0
Refer to the source code for more info
";
        internal static bool CheckForPermission(byte[] buffer, Acl p, int permission) {
            if (buffer == null)
                return false;
            var permIndex = (permission * numPerms) + (int) p;
            var byteIndex = permIndex / 8;
            if (buffer.Length < byteIndex) {
                throw new IndexOutOfRangeException(notEnoughBytesMsg);
            }
            var bitOffset = permIndex % 8;
            return (buffer[byteIndex] & (1 << bitOffset)) > 0;
        }
        internal static void SetPermission(byte[] buffer, Acl p, int permission, bool value) {
            if (buffer == null)
                return;
            var permIndex = (permission * numPerms) + (int)p;
            var byteIndex = permIndex / 8;
            if (buffer.Length < byteIndex) {
                throw new IndexOutOfRangeException(notEnoughBytesMsg);
            }
            var bitOffset = permIndex % 8;
            byte targetBit = (byte) (1 << bitOffset);
            if (value) {
                buffer[byteIndex] |= targetBit;
            } else {
                buffer[byteIndex] ^= targetBit;
            }
        }

        public static QueryBuilder ListIds<T>(RecordSet<T> set) where T : IDataObject, new()  {
            QueryBuilder retv = new QueryBuilder();
            for (int i = 0; i < set.Count; i++) {
                retv.Append(
                    new QueryBuilder(
                        $"@{IntEx.GerarShortRID()}",
                        set[i].Id
                    )
                );
                if (i < set.Count - 1)
                    retv.Append(",");
            }
            return retv;
        }

        public static string GetMimeType(string filename) {
            String extension = filename;
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

        public static String[] GetFieldNames(Type t) {
            var members = ReflectionTool.FieldsAndPropertiesOf(t)
                .Where(m=> m.GetCustomAttribute<FieldAttribute>() != null);
            List<String> retv = new List<String>();
            foreach(var a in members) {
                retv.Add(a.Name);
            }
            return retv.ToArray();
        }

        internal static bool FindColumn(string ChaveA, Type type) {
            FieldInfo[] fields = type.GetFields();
            return fields.Where((f) => f.GetCustomAttribute<FieldAttribute>() != null && f.Name == ChaveA).Any();
        }

        const int keySize = 16;

        public static int IntSeedFromString(String SeedStr) {
            int Seed = 0;
            for (int i = 0; i < SeedStr.Length; i++) {
                Seed ^= SeedStr[i] * (int) MathUtils.PrimeNumbers().ElementAt(1477);
            }
            return Seed;
        }

        /// <summary>
        /// deprecated
        /// this is responsibility of the rdbms query generator
        /// </summary>
        /// <param name="field"></param>
        /// <param name="info"></param>
        /// <returns></returns>
        private static String GetColumnDefinition(FieldInfo field, FieldAttribute info = null) {
            if (info == null)
                info = field.GetCustomAttribute<FieldAttribute>();
            if (info == null)
                return "VARCHAR(128)";
            var nome = field.Name;
            String tipo = GetDatabaseType(field, info);
            if (info.Type != null && info.Type.Length > 0)
                tipo = info.Type;
            var options = "";
            if (info.Options != null && info.Options.Length > 0) {
                options = info.Options;
            } else {
                if (!info.AllowNull) {
                    options += " NOT NULL";
                } else if (Nullable.GetUnderlyingType(field.GetType()) == null && field.FieldType.IsValueType && !info.AllowNull) {
                    options += " NOT NULL";
                }
                if (info.Unique)
                    options += " UNIQUE";
                if ((info.AllowNull && info.DefaultValue == null) || info.DefaultValue != null)
                    options += $" DEFAULT {CheapSanitize(info.DefaultValue)}";
                foreach (var att in field.GetCustomAttributes())
                    if (att is PrimaryKeyAttribute)
                        options += " AUTO_INCREMENT PRIMARY KEY";
            }

            return $"{nome} {tipo} {options}";
        }

        /// <summary>
        /// deprecated
        /// Must implement this on each rdbms query generator
        /// </summary>
        /// <param name="field"></param>
        /// <param name="info"></param>
        /// <returns></returns>
        private static String GetDatabaseType(FieldInfo field, FieldAttribute info = null) {
            if (info == null)
                foreach (var att in field.GetCustomAttributes())
                    if (att is FieldAttribute) {
                        info = (FieldAttribute)att; break;
                    }
            if (info == null)
                return "VARCHAR(100)";

            string dataType;
            if (Nullable.GetUnderlyingType(field.FieldType) != null)
                dataType = Nullable.GetUnderlyingType(field.FieldType).Name;
            else
                dataType = field.FieldType.Name;
            if (field.FieldType.IsEnum) {
                return "INT";
            }
            String type = "VARCHAR(20)";
            if (info.Type != null && info.Type.Length > 0) {
                type = info.Type;
            } else {
                switch (dataType.ToLower()) {
                    case "string":
                        type = $"VARCHAR({info.Size})";
                        break;
                    case "RID":
                        type = $"VARCHAR(64)";
                        break;
                    case "int":
                    case "int32":
                        type = $"INT";
                        break;
                    case "short":
                    case "int16":
                        type = $"SMALLINT";
                        break;
                    case "long":
                    case "int64":
                        type = $"BIGINT";
                        break;
                    case "bool":
                    case "boolean":
                        type = $"TINYINT(1)";
                        break;
                    case "float":
                    case "double":
                    case "single":
                        type = $"FLOAT(16,3)";
                        break;
                    case "datetime":
                        type = $"DATETIME";
                        break;
                }
            }
            return type;
        }

        public static String _cpuid = null;
        public static String CpuId {
            get {
                if (_cpuid != null)
                    return _cpuid;
                else {
                    try {
                        ManagementClass managClass = new ManagementClass("win32_processor");
                        ManagementObjectCollection managCollec = managClass.GetInstances();

                        foreach (ManagementObject managObj in managCollec) {
                            return managObj.Properties["processorID"].Value.ToString();
                        }
                    } catch (Exception) { }
                    return _cpuid = "0F0F0F0F0F0F0F0F";
                }
            }
        }


        public static byte[] GenerateKey(string Str) {
            Random random = new Random(FTH.IntSeedFromString(Str));
            byte[] numArray = new byte[16];
            for (int index = 0; index < 16; ++index)
                numArray[index] = (byte)random.Next(256);
            return numArray;
        }

        public static String GenerateIdString(String uniqueId, int numDigits = 128) {
            char[] retval = new char[numDigits];
            Random r = new Random();
            Random r2 = new Random(IntSeedFromString(uniqueId));
            for (int i = 0; i < retval.Length; i++) {
                int pos = r.Next(IntEx.Base36.Length / 2) + r2.Next(IntEx.Base36.Length / 2);
                if (pos > IntEx.Base36.Length - 1)
                    pos = IntEx.Base36.Length - 1;
                retval[i] = IntEx.Base36[pos];
            }
            return new String(retval);
        }

        public static void MemberwiseCopy(object origin, object destination) {
            ObjectReflector.Open(origin, (objA) => {
                var members = ReflectionTool.FieldsAndPropertiesOf(origin.GetType());
                ObjectReflector.Open(destination, (objB) => {
                    foreach(var field in members) {
                        objB[field.Name] = objA[field];
                    }
                });
            });
        }

        private static String GenerateCode(int numDigits, bool useLetters) {
            char[] vector = new char[numDigits];
            List<char> map = new List<char>();
            String numbers = "0123456789";
            String digits = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
            map.AddRange(numbers.ToCharArray());
            Random rng = new Random();
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
            return new String(vector);
        }
    }
}