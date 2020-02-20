using Figlotech.Core.Autokryptex;
using Figlotech.Core.Extensions;
using Figlotech.Core.FileAcessAbstractions;
using Figlotech.Core.Helpers;
using Figlotech.Extensions;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.Core {

    public sealed class FiHttpResult : IDisposable {
        public HttpStatusCode StatusCode { get; set; }
        public String StatusDescription { get; set; }
        public String ContentType { get; set; }
        public long ContentLength { get; set; }
        public WebResponse Response { get; set; }
        public Dictionary<String, String> Headers { get; set; } = new Dictionary<string, string>();
        private Stream ResultStream { get; set; }
        bool UseResponseBuffer { get; set; }

        internal FiHttpResult(bool useBuffer) {
            UseResponseBuffer = useBuffer;
        }
        ~FiHttpResult() {
            Dispose();
        }

        public bool IsSuccess => (int)StatusCode >= 200 && (int)StatusCode < 300;

        public static async Task<FiHttpResult> InitFromGet(HttpWebRequest req, bool useBuffer) {
            var retv = new FiHttpResult(useBuffer);
            try {
                retv.Response = await req.GetResponseAsync();
                retv.Init(retv.Response);
                
            } catch (WebException wex) {
                retv.Response = wex.Response;
                retv.Init(retv.Response);
            }
            return retv;
        }
        public static async Task<FiHttpResult> InitFromPost(HttpWebRequest req, Func<Stream, Task> UploadRequestStream, bool useBuffer) {
            var retv = new FiHttpResult(useBuffer);
            try {
                using (var reqStream = await req.GetRequestStreamAsync()) {
                    await UploadRequestStream?.Invoke(reqStream);
                }
                retv.Response = req.GetResponse();
                retv.Init(retv.Response);
            } catch (WebException wex) {
                using (var resp = wex.Response) {
                    retv.Init(resp);
                }
            }
            return retv;
        }

        public static async Task<FiHttpResult> Init(string verb, HttpWebRequest req, byte[] data, bool useBuffer) {
            var retv = new FiHttpResult(useBuffer);
            try {
                if(verb != "GET" && verb != "OPTIONS") {
                    try {
                        using (var reqStream = await req.GetRequestStreamAsync()) {
                            await reqStream.WriteAsync(data, 0, data.Length);
                            await reqStream.FlushAsync();
                            reqStream.Close();
                        }
                    } catch(Exception x) {
                        Debugger.Break();
                        throw x;
                    }
                }

                var resp = await req.GetResponseAsync();
                retv.Init(resp);
                
            } catch (WebException wex) {
                using (var resp = wex.Response) {
                    retv.Init(resp);
                }
            } catch (Exception x) {
                if (Debugger.IsAttached) {
                    Debugger.Break();
                }
                throw x;
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
            if (UseResponseBuffer) {
                using (var respStream = resp.GetResponseStream()) {
                    respStream.ReadTimeout = Timeout.Infinite;
                    respStream.WriteTimeout = Timeout.Infinite;
                    this.ResultStream = new MemoryStream();

                    respStream.CopyTo(ResultStream);

                    this.ResultStream.Seek(0, SeekOrigin.Begin);
                }
            } else {
                this.ResultStream = resp.GetResponseStream();
            }
        }

        public string AsString() {
            if(ResultStream.CanSeek) {
                ResultStream.Seek(0, SeekOrigin.Begin);
            }
            var retv = Fi.StandardEncoding.GetString(AsBuffer());
            return retv;
        }

        public string AsDecodedString(IEncryptionMethod method) {
            var retv = Fi.StandardEncoding.GetString(method.Decrypt(AsBuffer()));
            return retv;
        }

        public Stream AsStream() {
            if (ResultStream.CanSeek) {
                ResultStream.Seek(0, SeekOrigin.Begin);
            }
            return ResultStream;
        }

        public MemoryStream AsSeekableStream() {
            if (ResultStream.CanSeek) {
                ResultStream.Seek(0, SeekOrigin.Begin);
            }
            using (ResultStream) {
                var ms = new MemoryStream();
                ResultStream.CopyTo(ms);
                ResultStream.Flush();
                return ms;
            }
        }

        //public Stream AsRawStream() {
        //    var rawStream = new MemoryStream();
        //    using (var writer = new StreamWriter(rawStream, new UTF8Encoding(), 8192, true)) {
        //        writer.WriteLine($"HTTP/1.1 {StatusCode} {StatusDescription}");
        //        lock (this.Headers) {
        //            this.Headers.ForEach(header => {
        //                writer.WriteLine();
        //            });
        //        }
        //        writer.WriteLine();
        //    }
        //    ResultStream.CopyTo(rawStream);
        //    rawStream.Seek(0, SeekOrigin.Begin);
        //    return rawStream;
        //}

        public byte[] AsBuffer() {
            if (ResultStream.CanSeek) {
                ResultStream.Seek(0, SeekOrigin.Begin);
            }
            byte[] bytes;
            if (ResultStream is MemoryStream rsms) {
                bytes = rsms.ToArray();
            } else {
                using (var ms = new MemoryStream()) {
                    ResultStream.CopyTo(ms);
                    ms.Seek(0, SeekOrigin.Begin);
                    bytes = ms.ToArray();
                }
            }
            return bytes;
        }

        public byte[] AsDecodedBuffer(IEncryptionMethod method) {
            ResultStream.Seek(0, SeekOrigin.Begin);
            return method.Decrypt(AsBuffer());
        }

        public T As<T>() {
            T retv = default(T);
            var json = this.AsString();
            try {
                T obj = JsonConvert.DeserializeObject<T>(json);
                retv = obj;
            } catch (Exception x) {
                return default(T);
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

        public async Task SaveToFile(IFileSystem fs, string fileName) {
            ResultStream.Seek(0, SeekOrigin.Begin);
            await fs.Write(fileName, async stream => await ResultStream.CopyToAsync(stream));
        }

        public void Dispose() {
            Response?.Dispose();
            ResultStream?.Dispose();
        }
    }

    public class FiHttp
    {
        public bool UseResponseBuffer { get; set; }
        public bool UseRequestBuffer { get; set; } = true;
        public string SyncKeyCodePassword { get; set; } = null;
        IDictionary<string, string> headers = new Dictionary<string, string>();
        public IWebProxy Proxy { get; set; } = null;
        public FiHttp(string urlPrefix = null) {
            this.UrlPrefix = urlPrefix;
            if (this.UrlPrefix != null) {

                if (this.UrlPrefix.EndsWith("/")) {
                    this.UrlPrefix = this.UrlPrefix.Substring(0, this.UrlPrefix.Length - 1);
                }
            }
        }

        string[] reservedHeaders = new string[] {
            "Host",
            "Origin",
            "Connection",
            "User-Agent",
            "Accept",
            "Referer",
            "Content-Length",
            "Content-Type",
        };

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
            var st = (int) (await (Get(Url))).StatusCode;
            return st >= 200 && st < 300;
        }

        public async Task<T> Get<T>(string Url) {
            T retv = default(T);

            await Get(Url, async (status, rstream) => {
                await Task.Yield();
                using (StreamReader sr = new StreamReader(rstream)) {
                    var json = await sr.ReadToEndAsync();
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

        public HttpWebRequest GetRequest(string Url) {
            var request = HttpWebRequest.CreateHttp(MapUrl(Url));
            request.ServicePoint.Expect100Continue = false;
            request.Timeout = Timeout.Infinite;
            return request;
        }

        public async Task<FiHttpResult> Get(string Url) {
            var req = GetRequest(Url);
            req.Proxy = this.Proxy;
            req.Method = "GET";
            req.UserAgent = UserAgent;
            UpdateSyncCode();
            AddHeaders(req);
            return await FiHttpResult.InitFromGet(req, UseResponseBuffer);
        }

        private void AddHeaders(HttpWebRequest req) {
            lock (headers) {
                headers.ForEach((h) => {
                    if(h.Key == "Content-Length") {
                        return;
                    }
                    if (reservedHeaders.Contains(h.Key)) {
                        ReflectionTool.SetValue(req, h.Key.Replace("-", ""), h.Value);
                    } else {
                        req.Headers.Add(h.Key, h.Value);
                    }
                });
            }
        }

        public async Task<HttpStatusCode> Get(string Url, Func<HttpStatusCode, Stream, Task> ActOnResponse = null) {
            using (var result = await Custom("GET", Url)) {
                var code = result.StatusCode;
                await ActOnResponse?.Invoke(code, result.AsStream());
                return code;
            }
        }

        public async Task<FiHttpResult> Custom(String verb, String Url, Func<Stream, Task> UploadRequestStream = null) {
            if(UploadRequestStream == null || UseRequestBuffer) {
                byte[] bytes = new byte[0];
                if (UploadRequestStream != null) {
                    using (var ms = new MemoryStream()) {
                        await UploadRequestStream.Invoke(ms);
                        ms.Seek(0, SeekOrigin.Begin);
                        bytes = ms.ToArray();
                    }
                }
                var req = GetRequest(Url);
                req.Proxy = this.Proxy;
                req.Method = verb;
                req.UserAgent = UserAgent;
                UpdateSyncCode();
                AddHeaders(req);

                return await FiHttpResult.Init(verb, req, bytes, UseResponseBuffer);
            } else {
                var req = GetRequest(Url);
                req.Proxy = this.Proxy;
                req.Method = verb;
                req.UserAgent = UserAgent;
                UpdateSyncCode();
                AddHeaders(req);
                return await FiHttpResult.InitFromPost(req, UploadRequestStream, UseResponseBuffer);
            }
        }
        public async Task<FiHttpResult> Custom(String verb, String Url, byte[] data) {
            var req = GetRequest(Url);
            req.Proxy = this.Proxy;
            req.ContentLength = data.Length;
            req.Method = verb;
            req.UserAgent = UserAgent;
            UpdateSyncCode();
            AddHeaders(req);

            return await FiHttpResult.Init(verb, req, data, UseResponseBuffer);
        }
        public async Task<FiHttpResult> Post(String Url, Func<Stream, Task> UploadRequestStream = null) {
            return await Custom("POST", Url, UploadRequestStream);
        }

        public async Task<FiHttpResult> Post<T>(String Url, T postData) {
            var buff = Fi.StandardEncoding.GetBytes(JsonConvert.SerializeObject(postData));
            ContentType = "application/json";
            ContentLength = buff.Length;
            return await Post(Url, async (upstream) => {
                await upstream.WriteAsync(buff, 0, buff.Length);
            });
        }

        public string UserAgent { get; set; } = "Figlotech Http Abstraction on NetStandard2.0";
        public int ContentLength {
            get => Int32.Parse(this["Content-Length"]);
            set => this["Content-Length"] = value.ToString();
        }
        public String ContentType {
            get => this["Content-Type"];
            set => this["Content-Type"] = value;
        }
        public string this[string k] {
            get {
                lock(headers) {
                    if (headers.ContainsKey(k)) {
                        return headers[k];
                    }
                }
                return null;
            }
            set {
                lock(headers) {
                    var rectifiedHeaderArray = new char[k.Length];
                    for (int i = 0; i < rectifiedHeaderArray.Length; i++) {
                        if (i == 0 || k[i - 1] == '-') {
                            rectifiedHeaderArray[i] = Char.ToUpper(k[i]);
                        } else {
                            rectifiedHeaderArray[i] = k[i];
                        }
                    }
                    var rectifiedHeader = new String(rectifiedHeaderArray);
                    headers[rectifiedHeader] = value;
                }
            }
        }
    }
}
