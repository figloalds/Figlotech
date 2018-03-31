using Figlotech.Core.Extensions;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core {

    public class FiHttp {
        IDictionary<string, string> headers = new Dictionary<string, string>();
        public FiHttp(string urlPrefix = null) {
            this.UrlPrefix = UrlPrefix;
        }

        private string UrlPrefix { get; set; }

        private string MapUrl(string Url) {
            if(string.IsNullOrEmpty(UrlPrefix)) {
                return Url;
            }
            if (Url.StartsWith("/"))
                Url = Url.Substring(1);
            return $"{UrlPrefix}/{Url}";
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

        public async Task<HttpStatusCode> Post(String Url, Func<Stream, Task> UploadRequestStream = null, Func<HttpStatusCode, Stream, Task> ActOnResponse = null) {
            var req = (HttpWebRequest)WebRequest.Create(MapUrl(Url));
            req.Method = "POST";
            req.UserAgent = UserAgent;
            headers.Iterate((h) => {
                req.Headers.Add(h.Key, h.Value);
            });

            using (var reqStream = req.GetRequestStream()) {
                var t = UploadRequestStream?.Invoke(reqStream);
                if(t != null)
                    await t;
            }

            try {
                using (var resp = req.GetResponse() as HttpWebResponse) {
                    using (var respStream = resp.GetResponseStream()) {
                        var t = ActOnResponse?.Invoke(resp.StatusCode, respStream);
                        if (t != null)
                            await t;
                    }
                    return resp.StatusCode;
                }
            } catch (WebException x) {
                using (var resp = x.Response as HttpWebResponse) {
                    using (var respStream = resp.GetResponseStream()) {
                        var t = ActOnResponse?.Invoke(resp.StatusCode, respStream);
                        if (t != null)
                            await t;
                    }
                    return resp.StatusCode;
                }
            }

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
