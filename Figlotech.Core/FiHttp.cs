using Figlotech.Core.Extensions;
using Figlotech.Extensions;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core {

    public class FiHttpResult {
        public HttpStatusCode StatusCode { get; set; }
        public String StatusDescription { get; set; }
        public String ContentType { get; set; }
        public long ContentLength { get; set; }
        public Dictionary<String, String> Headers { get; set; } = new Dictionary<string, string>();
        private MemoryStream ResultStream { get; set; }
        private bool resultIsRead = false;
        
        internal FiHttpResult() {
            
        }
        public bool IsSuccess() {
            return (int)StatusCode >= 200 && (int)StatusCode < 300;
        }

        public static async Task<FiHttpResult> InitFromGet(HttpWebRequest req) {
            var retv = new FiHttpResult();
            try {
                using (var resp = await req.GetResponseAsync()) {
                    retv.Init(resp);
                }
            } catch (WebException wex) {
                using (var resp = wex.Response) {
                    retv.Init(resp);
                }
            }
            return retv;
        }
        public static async Task<FiHttpResult> InitFromPost(HttpWebRequest req, Func<Stream, Task> UploadRequestStream) {
            var retv = new FiHttpResult();
            try {
                using (var reqStream = req.GetRequestStream()) {
                    var t = UploadRequestStream?.Invoke(reqStream);
                    if (t != null)
                        await t;
                }
                using (var resp = await req.GetResponseAsync()) {
                    retv.Init(resp);
                }
            } catch (WebException wex) {
                using (var resp = wex.Response) {
                    retv.Init(resp);
                }
            }
            return retv;
        }

        void Init(WebResponse resp) {
            if(resp == null) {
                StatusCode = 0;
                ResultStream = new MemoryStream();
                return;
            }
            if(resp is HttpWebResponse htresp) {
                StatusCode = htresp.StatusCode;
                StatusDescription = htresp.StatusDescription;
            }
            ContentType = resp.ContentType;
            ContentLength = resp.ContentLength;
            using (var ms = new MemoryStream()) {
                using (var respStream = resp.GetResponseStream()) {
                    respStream.CopyTo(ms);
                    this.ResultStream = ms;
                    this.ResultStream.Seek(0, SeekOrigin.Begin);
                }
            }
            for (int i = 0; i < resp.Headers.Count; ++i) {
                string key = resp.Headers.GetKey(i);
                foreach (string value in resp.Headers.GetValues(i)) {
                    Headers[key] = value;
                    break;
                }
            }
            if (this.Headers.ContainsKey("ContentType")) {
                ContentType = this.Headers["ContentType"];
            }
            if (this.Headers.ContainsKey("ContentLength") && Int64.TryParse(this.Headers["ContentLength"], out long cl)) {
                this.ContentLength = cl;
            }
        }

        public string ResultAsString() {
            if (resultIsRead) {
                return null;
            }
            using (ResultStream) {
                var bytes = ResultStream.ToArray();
                var retv = Encoding.UTF8.GetString(bytes);
                this.resultIsRead = true;
                return retv;
            }
        }
        public T ResultAs<T>() {
            if(resultIsRead) {
                return default(T);
            }
            T retv = default(T);
            var json = this.ResultAsString();
            try {
                T obj = JsonConvert.DeserializeObject<T>(json);
                retv = obj;
                resultIsRead = true;
            } catch (Exception x) {
            }
            return retv;
        }
    }

    public class FiHttp {
        IDictionary<string, string> headers = new Dictionary<string, string>();
        public FiHttp(string urlPrefix = null) {
            this.UrlPrefix = urlPrefix;
            if(this.UrlPrefix != null) {

                if (this.UrlPrefix.EndsWith("/")) {
                    this.UrlPrefix = this.UrlPrefix.Substring(0, this.UrlPrefix.Length - 1);
                }
            }
        }

        private string UrlPrefix { get; set; }

        private string MapUrl(string Url) {
            if(string.IsNullOrEmpty(UrlPrefix)) {
                return Url;
            }
            if(Url.ToLower().RegExp("$\\w+\\:")) {
                return Url;
            }
            if (Url.StartsWith("/"))
                Url = Url.Substring(1);
            return $"{UrlPrefix}/{Url}";
        }

        public async Task<bool> Check(string Url) {
            var st = (int) (await Get(Url)).StatusCode;
            return st >= 200 && st < 300;
        }
        
        public async Task<T> Get<T>(string Url) {
            T retv = default(T);
            await Get(Url, async (status, rstream) => {
                using (StreamReader sr = new StreamReader(rstream)) {
                    var json = sr.ReadToEnd();
                    try {
                        T obj = JsonConvert.DeserializeObject<T>(json);
                        retv = obj;
                    } catch(Exception x) {
                    }
                }
            });
            return retv;
        }

        public async Task<FiHttpResult> Get(string Url) {
            var req = (HttpWebRequest)WebRequest.Create(MapUrl(Url));
            req.Method = "GET";
            req.UserAgent = UserAgent;
            return await FiHttpResult.InitFromGet(req);
        }

        public async Task<HttpStatusCode> Get(string Url, Func<HttpStatusCode, Stream, Task> ActOnResponse = null) {
            var req = (HttpWebRequest)WebRequest.Create(MapUrl(Url));
            req.Method = "GET";
            req.UserAgent = UserAgent;
            try {
                using (var resp = req.GetResponse() as HttpWebResponse) {
                    using (var stream = resp.GetResponseStream()) {
                        var t = ActOnResponse?.Invoke(resp.StatusCode, stream);
                        if (t != null)
                            await t;
                    }
                    return resp.StatusCode;
                }
            } catch (WebException x) {
                using (var resp = x.Response as HttpWebResponse) {
                    using (var stream = resp.GetResponseStream()) {
                        var t = ActOnResponse?.Invoke(resp.StatusCode, stream);
                        if (t != null)
                            await t;
                    }
                    return resp.StatusCode;
                }
            }
        }

        public async Task<FiHttpResult> Post(String Url, Func<Stream, Task> UploadRequestStream = null) {
            var req = (HttpWebRequest)WebRequest.Create(MapUrl(Url));
            req.Method = "POST";
            req.UserAgent = UserAgent;
            headers.ForEach((h) => {
                switch(h.Key) {
                    case "Content-Type":
                        req.ContentType = h.Value;
                        break;
                    case "Content-Length":
                        if(Int64.TryParse(h.Value, out long len)) {
                            req.ContentLength = len;
                        }
                        break;
                    default:
                        req.Headers.Add(h.Key, h.Value);
                        break;
                }
            });

            return await FiHttpResult.InitFromPost(req, UploadRequestStream);
        }

        public async Task<FiHttpResult> Post<T>(String Url, T postData) {
            var buff = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(postData));
            ContentType = "application/json";
            ContentLength = buff.Length;
            return await Post(Url, async (upstream) => {
                await upstream.WriteAsync(buff, 0, buff.Length);
            });
        }

        public string UserAgent { get; set; } = "Figlotech Http Abstraction on NetStandard2.0";
        public int ContentLength {
            get => Int32.Parse(headers["Content-Length"]);
            set => headers["Content-Length"] = value.ToString();
        }
        public String ContentType {
            get => headers["Content-Type"];
            set => headers["Content-Type"] = value;
        }
        public string this[string k] {
            get {
                if (headers.ContainsKey(k)) {
                    return headers[k];
                }
                return null;
            }
            set {
                headers[k] = value;
            }
        }
    }
}
