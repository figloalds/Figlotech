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
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.Core {

    public sealed class  FiHttpResult : IDisposable {
        public HttpStatusCode StatusCode { get; set; }
        public String StatusDescription { get; set; }
        public String ContentType { get; set; }
        public long ContentLength { get; set; }
        public HttpResponseMessage Response { get; set; }
        private MemoryStream CachedResponseBody { get; set; }
        public Dictionary<String, String> Headers { get; set; } = new Dictionary<string, string>();

        internal FiHttpResult() {
        }
        ~FiHttpResult() {
            Dispose();
        }

        public bool IsSuccess => (int)StatusCode >= 200 && (int)StatusCode < 300;

        public static async Task<FiHttpResult> InitFromRequest(HttpRequestMessage httpRequestMessage) {
            var retv = new FiHttpResult();
            try {
                var response = await FiHttp.httpClient.SendAsync(httpRequestMessage);
                retv.Init(response);
            } catch (WebException ex) {
                retv.Init(ex.Response as HttpWebResponse);
            } catch (Exception ex) {
                retv.Init(null as HttpResponseMessage);
            }
            return retv;
        }

        void Init(HttpWebResponse resp) {
            if (resp == null) {
                StatusCode = 0;
                return;
            }
            var response = new HttpResponseMessage(resp.StatusCode) {
                ReasonPhrase = resp.StatusDescription,
            };
            foreach (var header in resp.Headers.AllKeys) {
                response.Headers.Add(header, resp.Headers.GetValues(header));
            }
            response.Content = new System.Net.Http.StreamContent(resp.GetResponseStream());
            Init(response);
        }

        void Init(HttpResponseMessage resp) {
            if (resp == null) {
                StatusCode = 0;
                return;
            }
            this.Response = resp;
            StatusCode = resp.StatusCode;
            StatusDescription = resp.ReasonPhrase;
            ContentType = resp.Content.Headers.ContentType?.MediaType;
            ContentLength = resp.Content.Headers.ContentLength ?? 0;

            foreach (var header in resp.Headers) {
                string key = header.Key;
                Headers[key] = header.Value.FirstOrDefault();
            }
        }

        private async Task CacheResponseBody() {
            if(CachedResponseBody == null && Response.Content != null) {
                CachedResponseBody = new MemoryStream();
                await Response.Content.CopyToAsync(CachedResponseBody);
            }
            CachedResponseBody.Seek(0, SeekOrigin.Begin);
        }

        public async Task<string> AsString() {
            await CacheResponseBody();
            if (CachedResponseBody == null) {
                return null;
            }
            return Encoding.UTF8.GetString(CachedResponseBody.ToArray());
        }

        public async Task<string> AsDecodedString(IEncryptionMethod method) {
            var retv = Fi.StandardEncoding.GetString(method.Decrypt(await AsBuffer()));
            return retv;
        }

        public async Task<Stream> AsStream() {
            await CacheResponseBody();
            return CachedResponseBody;
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

        public async Task<byte[]> AsBuffer() {
            await CacheResponseBody();
            return CachedResponseBody.ToArray();
        }

        public async Task<byte[]> AsDecodedBuffer(IEncryptionMethod method) {
            return method.Decrypt(await AsBuffer());
        }

        public async Task<T> As<T>() {
            T retv = default(T);
            var json = await this.AsString();
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

        public async Task SaveToFile(IFileSystem fs, string fileName) {
            await fs.Write(fileName, async stream => await (await AsStream()).CopyToAsync(stream));
        }

        bool isDisposed = false;
        public void Dispose() {
            if (isDisposed) {
                return;
            }
            Response?.Dispose();
            CachedResponseBody?.Dispose();
        }
    }

    public sealed class NonSuccessResponseException : Exception
    {
        public HttpStatusCode StatusCode { get; set; }
        public HttpResponseMessage Response { get; set; }
        public NonSuccessResponseException(string Message) : base(Message) { }
        public NonSuccessResponseException(HttpStatusCode statusCode, string Message) : base(Message) { 
            this.StatusCode = statusCode;
        }
        public NonSuccessResponseException(HttpResponseMessage resp) : base(resp.ReasonPhrase) {
            this.StatusCode = resp.StatusCode;
            this.Response = resp;
        }
    }

    public sealed class FiHttp
    {
        internal static HttpClient httpClient = new HttpClient() {
            Timeout = TimeSpan.FromMinutes(120),
        };
        
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
            var result = await Get(Url);
            if(result.IsSuccess) {
                return await result.As<T>();
            } else {
                throw new NonSuccessResponseException(result.Response);
            }
        }
        public async Task<FiHttpResult> Post<T>(String url, T postData) {
            var json = JsonConvert.SerializeObject(postData);
            var bytes = Fi.StandardEncoding.GetBytes(json);
            var req = CreateRequest(url);
            req.Method = HttpMethod.Post;
            req.Content = new ByteArrayContent(bytes);
            req.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
            req.Content.Headers.ContentLength = bytes.Length;
            return await FiHttpResult.InitFromRequest(req);
        }
        public async Task<FiHttpResult> Put<T>(String url, T postData) {
            var json = JsonConvert.SerializeObject(postData);
            var bytes = Fi.StandardEncoding.GetBytes(json);
            var req = CreateRequest(url);
            req.Method = HttpMethod.Put;
            req.Content = new ByteArrayContent(bytes);
            req.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
            req.Content.Headers.ContentLength = bytes.Length;
            return await FiHttpResult.InitFromRequest(req);
        }

        public async Task<FiHttpResult> Post(string url, Func<Stream, Task> streamFunction) {
            var req = CreateRequest(url);
            req.Method = HttpMethod.Post;
            using (var ms = new MemoryStream()) {
                await streamFunction(ms);
                ms.Seek(0, SeekOrigin.Begin);
                req.Content = new StreamContent(ms);
                return await FiHttpResult.InitFromRequest(req);
            }
        }
        public async Task<FiHttpResult> Post(string url, Stream stream, string contentType) {
            var req = CreateRequest(url);
            req.Method = HttpMethod.Post;
            req.Content = new StreamContent(stream) {
                Headers = {
                    ContentType = new MediaTypeHeaderValue(contentType)
                }
            };
            return await FiHttpResult.InitFromRequest(req);
        }
        public async Task<FiHttpResult> Post(string url, string body) {
            var req = CreateRequest(url);
            req.Method = HttpMethod.Post;
            req.Content = new StringContent(body);
            return await FiHttpResult.InitFromRequest(req);
        }
        public async Task<FiHttpResult> Post(string url) {
            var req = CreateRequest(url);
            req.Method = HttpMethod.Post;
            return await FiHttpResult.InitFromRequest(req);
        }

        public async Task<FiHttpResult> Put(string url, Stream stream, string contentType) {
            var req = CreateRequest(url);
            req.Method = HttpMethod.Put;
            req.Content = new StreamContent(stream) {
                Headers = {
                    ContentType = new MediaTypeHeaderValue(contentType)
                }
            };
            return await FiHttpResult.InitFromRequest(req);
        }
        public async Task<FiHttpResult> Put(string url, string body) {
            var req = CreateRequest(url);
            req.Method = HttpMethod.Post;
            req.Content = new StringContent(body);
            return await FiHttpResult.InitFromRequest(req);
        }
        public async Task<FiHttpResult> Put(string url) {
            var req = CreateRequest(url);
            req.Method = HttpMethod.Put;
            return await FiHttpResult.InitFromRequest(req);
        }

        private void UpdateSyncCode() {
            if (!SyncKeyCodePassword.IsNullOrEmpty()) {
                var hsc = HourlySyncCode.Generate(SyncKeyCodePassword).ToString();
                this["SyncKeyCode"] = hsc;
            }
        }

        public HttpRequestMessage CreateRequest(string Url) {
            var request = new HttpRequestMessage();
            request.RequestUri = new Uri(MapUrl(Url));
            UpdateSyncCode();
            AddHeaders(request);
            return request;
        }

        public async Task<FiHttpResult> Get(string Url) {
            var req = CreateRequest(Url);
            req.Method = HttpMethod.Get;
            return await FiHttpResult.InitFromRequest(req);
        }

        private void AddHeaders(HttpRequestMessage req) {
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
            req.Headers.Add("User-Agent", UserAgent);
            if (!SyncKeyCodePassword.IsNullOrEmpty()) {
                var hsc = HourlySyncCode.Generate(SyncKeyCodePassword).ToString();
                req.Headers.Add("sync-key", hsc);
            }
        }

        public async Task<HttpStatusCode> Get(string Url, Func<HttpStatusCode, Stream, Task> ActOnResponse = null) {
            using (var result = await Get(Url)) {
                var code = result.StatusCode;
                await ActOnResponse?.Invoke(code, await result.AsStream());
                return code;
            }
        }

        public string UserAgent { get; set; } = "Figlotech Http Abstraction on netstandard2.1";
        
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
                    headers[k] = value;
                }
            }
        }
    }
}
