﻿using Figlotech.Core.Autokryptex;
using Figlotech.Core.Extensions;
using Figlotech.Core.FileAcessAbstractions;
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

    public class FiHttpResult : IDisposable {
        public HttpStatusCode StatusCode { get; set; }
        public String StatusDescription { get; set; }
        public String ContentType { get; set; }
        public long ContentLength { get; set; }
        public Dictionary<String, String> Headers { get; set; } = new Dictionary<string, string>();
        private MemoryStream ResultStream { get; set; }

        internal FiHttpResult() {

        }

        public bool IsSuccess => (int)StatusCode >= 200 && (int)StatusCode < 300;

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
            if (resp == null) {
                StatusCode = 0;
                ResultStream = new MemoryStream();
                return;
            }
            if (resp is HttpWebResponse htresp) {
                StatusCode = htresp.StatusCode;
                StatusDescription = htresp.StatusDescription;
            }
            ContentType = resp.ContentType;
            ContentLength = resp.ContentLength;
            using (var respStream = resp.GetResponseStream()) {
                this.ResultStream = new MemoryStream();
                respStream.CopyTo(this.ResultStream);
                this.ResultStream.Seek(0, SeekOrigin.Begin);
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

        public string AsString() {
            ResultStream.Seek(0, SeekOrigin.Begin);
            var bytes = ResultStream.ToArray();
            var retv = Encoding.UTF8.GetString(bytes);
            return retv;
        }

        public string AsDecodedString(IEncryptionMethod method) {
            ResultStream.Seek(0, SeekOrigin.Begin);
            var bytes = ResultStream.ToArray();
            var retv = Encoding.UTF8.GetString(method.Decrypt(bytes));
            return retv;
        }

        public Stream AsStream() {
            ResultStream.Seek(0, SeekOrigin.Begin);
            return ResultStream;
        }

        public byte[] AsBuffer() {
            ResultStream.Seek(0, SeekOrigin.Begin);
            return ResultStream.ToArray();
        }

        public byte[] AsDecodedBuffer(IEncryptionMethod method) {
            ResultStream.Seek(0, SeekOrigin.Begin);
            return method.Decrypt(ResultStream.ToArray());
        }

        public T As<T>() {
            ResultStream.Seek(0, SeekOrigin.Begin);
            T retv = default(T);
            var json = this.AsString();
            try {
                T obj = JsonConvert.DeserializeObject<T>(json);
                retv = obj;
            } catch (Exception x) {

            }
            return retv;
        }

        public T Use<T>(Func<FiHttpResult, T> usefn) {
            using (this) {
                return usefn(this);
            }
        }

        public string UseAsString() {
            return Use<string>(i => i.AsString());
        }

        public T UseAs<T>() {
            return Use<T>(i => i.As<T>());
        }

        public void SaveToFile(IFileSystem fs, string fileName) {
            ResultStream.Seek(0, SeekOrigin.Begin);
            fs.Write(fileName, stream => ResultStream.CopyTo(stream));
        }

        public void Dispose() {
            ResultStream?.Dispose();
        }
    }

    public class FiHttp {
        public string SyncKeyCodePassword { get; set; } = null;
        IDictionary<string, string> headers = new Dictionary<string, string>();
        public FiHttp(string urlPrefix = null) {
            this.UrlPrefix = urlPrefix;
            if (this.UrlPrefix != null) {

                if (this.UrlPrefix.EndsWith("/")) {
                    this.UrlPrefix = this.UrlPrefix.Substring(0, this.UrlPrefix.Length - 1);
                }
            }
        }

        private string UrlPrefix { get; set; }

        private string MapUrl(string Url) {
            if (string.IsNullOrEmpty(UrlPrefix)) {
                return Url;
            }
            if (Url.ToLower().RegExp("$\\w+\\:")) {
                return Url;
            }
            if (Url.StartsWith("/"))
                Url = Url.Substring(1);
            return $"{UrlPrefix}/{Url}";
        }

        public async Task<bool> Check(string Url) {
            var st = (int)(await Get(Url)).StatusCode;
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

                    } catch (Exception x) {

                    }
                }
            });
            return retv;
        }

        private void UpdateSyncCode() {
            if (!SyncKeyCodePassword.IsNullOrEmpty()) {
                var hsc = HourlySyncCode.Generate(SyncKeyCodePassword).ToString();
                this["SyncKeyCode"] = hsc;
            }
        }

        public async Task<FiHttpResult> Get(string Url) {
            var req = (HttpWebRequest)WebRequest.Create(MapUrl(Url));
            req.Method = "GET";
            req.UserAgent = UserAgent;
            UpdateSyncCode();
            headers.ForEach((h) => {
                switch (h.Key) {
                    case "Content-Type":
                        break;
                    case "Content-Length":
                        break;
                    default:
                        req.Headers.Add(h.Key, h.Value);
                        break;
                }
            });
            return await FiHttpResult.InitFromGet(req);
        }

        public async Task<HttpStatusCode> Get(string Url, Func<HttpStatusCode, Stream, Task> ActOnResponse = null) {
            var req = (HttpWebRequest)WebRequest.Create(MapUrl(Url));
            req.Method = "GET";
            req.UserAgent = UserAgent;
            UpdateSyncCode();
            headers.ForEach((h) => {
                switch (h.Key) {
                    case "Content-Type":
                        break;
                    case "Content-Length":
                        break;
                    default:
                        req.Headers.Add(h.Key, h.Value);
                        break;
                }
            });

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
            UpdateSyncCode();
            headers.ForEach((h) => {
                switch (h.Key) {
                    case "Content-Type":
                        req.ContentType = h.Value;
                        break;
                    case "Content-Length":
                        if (Int64.TryParse(h.Value, out long len)) {
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
