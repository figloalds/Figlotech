﻿using Figlotech.Core.Autokryptex;
using Figlotech.Core.Extensions;
using Figlotech.Core.FileAcessAbstractions;
using Figlotech.Core.Helpers;
using Figlotech.Extensions;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Pipelines;
using System.IO.Pipes;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.Sockets;
using System.Numerics;
using System.Reflection;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.Core {

    public sealed class FiHttpResult : IDisposable {
        public HttpStatusCode StatusCode { get; set; }
        public String StatusDescription { get; set; }
        public String ContentType { get; set; }
        public long ContentLength { get; set; }
        public HttpResponseMessage Response { get; set; }
        private MemoryStream CachedResponseBody { get; set; }
        public Dictionary<String, String> Headers { get; set; } = new Dictionary<string, string>();

        FiHttp Caller;

        internal FiHttpResult(FiHttp caller) {
            this.Caller = caller;
        }
        ~FiHttpResult() {
            Dispose();
        }

        public bool IsSuccess => (int)StatusCode >= 200 && (int)StatusCode < 300;

        public static async Task<FiHttpResult> InitFromRequest(FiHttp caller, HttpRequestMessage httpRequestMessage) {
            var retv = new FiHttpResult(caller);
            try {
                var client = caller.HttpClient;
                var response = await client.SendAsync(httpRequestMessage, HttpCompletionOption.ResponseHeadersRead, CancellationToken.None);
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
            response.Content = new StreamContent(resp.GetResponseStream());
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
            if (CachedResponseBody == null) {
                CachedResponseBody = new MemoryStream();
                if (Response?.Content != null) {
                    await Response.Content.CopyToAsync(CachedResponseBody);
                }
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
            if(CachedResponseBody != null) {
                CachedResponseBody.Seek(0, SeekOrigin.Begin);
                return CachedResponseBody;
            }
            return await Response.Content.ReadAsStreamAsync();
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
            if (typeof(T) == typeof(String)) {
                return (T)(object)await AsString();
            }
            T retv = default(T);
            var json = await this.AsString();
            try {
                T obj = JsonConvert.DeserializeObject<T>(json, Caller.JsonSettings);
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

    public sealed class NonSuccessResponseException : Exception {
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

    public sealed class FiHttp {
        internal static HttpClient DefaultClient = new HttpClient() {
            Timeout = TimeSpan.FromMinutes(120),
            MaxResponseContentBufferSize = 1,
        };
        internal static HttpClient DefaultClientIgnoreBadCerts = new HttpClient(
            new HttpClientHandler {
                ServerCertificateCustomValidationCallback = (message, cert, chain, errors) => true,
            }    
        ) {
            Timeout = TimeSpan.FromMinutes(120),
            MaxResponseContentBufferSize = 1,
        };

        public bool IgnoreBadCertificates { get; set; } = false;
        internal static SelfInitializedCache<string, HttpClient> clientCache = new SelfInitializedCache<string, HttpClient>(
            s => {
                return null;
            }, TimeSpan.FromMinutes(120)
        );

        public JsonSerializerSettings JsonSettings { get; set; } = new JsonSerializerSettings();

        HttpClient client;
        internal HttpClient HttpClient {
            get {
                return GetHttpClientForInstance(this);
            }
        }

        internal static HttpClient GetHttpClientForInstance(FiHttp instance) {
            var certs = instance.Certificates;
            if (certs.Count == 0) {
                return instance.IgnoreBadCertificates ? DefaultClientIgnoreBadCerts : DefaultClient;
            }

            var id = string.Join("|", certs.OrderBy(x => x.Thumbprint).Select(
                x => Convert.ToBase64String(
                    BigInteger.Parse(
                        x.Thumbprint,
                        System.Globalization.NumberStyles.HexNumber)
                        .ToByteArray()
                        .Reverse()
                        .ToArray()
                    )
                ));
            id += $";IgnoreBadCerts={instance.IgnoreBadCertificates}";

            if (!clientCache.ContainsKey(id)) {
                var handler = new HttpClientHandler();
                foreach (var item in certs) {
                    handler.ClientCertificates.Add(item);
                }
                if (instance.IgnoreBadCertificates) {
                    handler.ServerCertificateCustomValidationCallback = (message, cert, chain, errors) => true;
                }
                var client = new HttpClient(handler) {
                    Timeout = TimeSpan.FromMinutes(120),
                    MaxResponseContentBufferSize = 1,
                };
                clientCache[id] = client;
            }
            return clientCache[id];
        }

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

        private List<X509Certificate2> Certificates { get; set; } = new List<X509Certificate2>();

        public void AddCertificate(X509Certificate2 certificate) {
            lock (Certificates) {
                if (!Certificates.Contains(certificate)) {
                    Certificates.Add(certificate);
                    client = null;
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
            var st = (int)(await (Get(Url))).StatusCode;
            return st >= 200 && st < 300;
        }

        private byte[] GetObjectBytes<T>(T postData, string contentType) {
            switch (contentType) {
                case ContentTypeFormUrlEncoded:
                    StringBuilder retv = new StringBuilder();
                    bool isFirst = true;
                    foreach (var item in ReflectionTool.FieldsAndPropertiesOf(typeof(T))) {
                        if (item.GetCustomAttribute<JsonIgnoreAttribute>() != null) {
                            continue;
                        }
                        if (isFirst) {
                            isFirst = false;
                        } else {
                            retv.Append("&");
                        }
                        var value = ReflectionTool.GetMemberValue(item, postData);
                        if (value != null) {
                            retv.Append($"{item.Name}={System.Net.WebUtility.UrlEncode(value.ToString())}");
                        }
                    }
                    var retvStr = retv.ToString();
                    return Fi.StandardEncoding.GetBytes(retvStr);
                case ContentTypeJson:
                default:
                    return Fi.StandardEncoding.GetBytes(JsonConvert.SerializeObject(postData, JsonSettings));
            }
        }

        public const string ContentTypeJson = "application/json";
        public const string ContentTypeFormUrlEncoded = "application/x-www-form-urlencoded";
        // GET
        public async Task<FiHttpResult> Get(string Url) {
            return await SendRequest(HttpMethod.Get, Url);
        }
        public async Task<T> Get<T>(string Url) {
            return await SendRequest<T>(HttpMethod.Get, Url);
        }


        // DELETEdp1
        public async Task<FiHttpResult> Delete(String url, MultipartFormDataContent form) {
            return await SendRequest(HttpMethod.Delete, url, form);
        }

        public async Task<FiHttpResult> Delete(string url, Func<Stream, Task> streamFunction) {
            return await SendRequest(HttpMethod.Delete, url, streamFunction);
        }
        public async Task<FiHttpResult> Delete(string url, Stream stream, string contentType) {
            return await SendRequest(HttpMethod.Delete, url, stream, contentType);
        }
        public async Task<FiHttpResult> Delete(string url, string body) {
            return await SendRequest(HttpMethod.Delete, url, body);
        }
        public async Task<FiHttpResult> Delete(string url) {
            return await SendRequest(HttpMethod.Delete, url);
        }
        public async Task<T> Delete<T>(string url) {
            return await SendRequest<T>(HttpMethod.Delete, url);
        }
        public async Task<FiHttpResult> Delete<T>(String url, T bodyData, string contentType = null) {
            return await SendRequest<T>(HttpMethod.Delete, url, bodyData, contentType);
        }

        // POST
        public async Task<FiHttpResult> Post(String url, MultipartFormDataContent form) {
            return await SendRequest(HttpMethod.Post, url, form);
        }

        public async Task<FiHttpResult> Post(string url, Func<Stream, Task> streamFunction) {
            return await SendRequest(HttpMethod.Post, url, streamFunction);
        }
        public async Task<FiHttpResult> Post(string url, Stream stream, string contentType) {
            return await SendRequest(HttpMethod.Post, url, stream, contentType);
        }
        public async Task<FiHttpResult> Post(string url, string body) {
            return await SendRequest(HttpMethod.Post, url, body);
        }

        public async Task<FiHttpResult> Post<T>(string url, IAsyncEnumerable<T> body) {
            return await SendRequest(HttpMethod.Post, url, async stream => {
                await using var writer = new StreamWriter(stream, Fi.StandardEncoding, 8192, true);
                var isFirst = true;
                await writer.WriteLineAsync("[");
                await foreach (var item in body) {
                    if (isFirst) {
                        isFirst = false;
                    } else {
                        await writer.WriteLineAsync(",");
                    }
                    await writer.WriteLineAsync(JsonConvert.SerializeObject(item, JsonSettings));
                }
                await writer.WriteLineAsync("]");
            }, "application/json");
        }

        public async Task<FiHttpResult> Post(string url) {
            return await SendRequest(HttpMethod.Post, url);
        }

        public async Task<T> Post<T>(string url) {
            return await SendRequest<T>(HttpMethod.Post, url);
        }

        public async Task<FiHttpResult> Post<T>(String url, T bodyData, string contentType = null) {
            return await SendRequest<T>(HttpMethod.Post, url, bodyData, contentType);
        }

        // Put
        public async Task<FiHttpResult> Put(String url, MultipartFormDataContent form) {
            return await SendRequest(HttpMethod.Put, url, form);
        }

        public async Task<FiHttpResult> Put(string url, Func<Stream, Task> streamFunction) {
            return await SendRequest(HttpMethod.Put, url, streamFunction);
        }
        public async Task<FiHttpResult> Put(string url, Stream stream, string contentType) {
            return await SendRequest(HttpMethod.Put, url, stream, contentType);
        }
        public async Task<FiHttpResult> Put(string url, string body) {
            return await SendRequest(HttpMethod.Put, url, body);
        }
        public async Task<FiHttpResult> Put(string url) {
            return await SendRequest(HttpMethod.Put, url);
        }
        public async Task<T> Put<T>(string url) {
            return await SendRequest<T>(HttpMethod.Put, url);
        }
        public async Task<FiHttpResult> Put<T>(String url, T bodyData, string contentType = null) {
            return await SendRequest<T>(HttpMethod.Put, url, bodyData, contentType);
        }

        // Patch
        public async Task<FiHttpResult> Patch(String url, MultipartFormDataContent form) {
            return await SendRequest(HttpMethod.Patch, url, form);
        }

        public async Task<FiHttpResult> Patch(string url, Func<Stream, Task> streamFunction) {
            return await SendRequest(HttpMethod.Patch, url, streamFunction);
        }
        public async Task<FiHttpResult> Patch(string url, Stream stream, string contentType) {
            return await SendRequest(HttpMethod.Patch, url, stream, contentType);
        }
        public async Task<FiHttpResult> Patch(string url, string body) {
            return await SendRequest(HttpMethod.Patch, url, body);
        }
        public async Task<FiHttpResult> Patch(string url) {
            return await SendRequest(HttpMethod.Patch, url);
        }
        public async Task<T> Patch<T>(string url) {
            return await SendRequest<T>(HttpMethod.Patch, url);
        }
        public async Task<FiHttpResult> Patch<T>(String url, T bodyData, string contentType = null) {
            return await SendRequest<T>(HttpMethod.Patch, url, bodyData, contentType);
        }

        //**

        public async Task<FiHttpResult> SendRequest(HttpMethod method, string url, MultipartFormDataContent form) {
            var req = CreateRequest(url);
            req.Method = method;
            req.Content = form;
            return await FiHttpResult.InitFromRequest(this, req);
        }

        public async Task<FiHttpResult> SendRequest(HttpMethod method, string url, Func<Stream, Task> streamFunction, string contentType = null) {
            var req = CreateRequest(url);
            req.Method = method;
            using (var ms = new MemoryStream()) {
                await streamFunction(ms);
                ms.Seek(0, SeekOrigin.Begin);
                req.Content = new StreamContent(ms);
                if (!string.IsNullOrEmpty(contentType)) {
                    req.Content.Headers.ContentType = new MediaTypeHeaderValue(contentType);
                }
                return await FiHttpResult.InitFromRequest(this, req);
            }
        }

        public async Task<FiHttpResult> SendRequest(HttpMethod method, string url, Stream stream, string contentType) {
            var req = CreateRequest(url);
            req.Method = method;
            req.Content = new StreamContent(stream) {
                Headers = {
                    ContentType = new MediaTypeHeaderValue(contentType)
                }
            };
            return await FiHttpResult.InitFromRequest(this, req);
        }

        public async Task<FiHttpResult> SendRequest(HttpMethod method, string url, string body) {
            var req = CreateRequest(url);
            req.Method = method;
            req.Content = new StringContent(body);
            return await FiHttpResult.InitFromRequest(this, req);
        }

        public async Task<FiHttpResult> SendRequest(HttpMethod method, string url) {
            var req = CreateRequest(url);
            req.Method = method;
            return await FiHttpResult.InitFromRequest(this, req);
        }

        public async Task<T> SendRequest<T>(HttpMethod method, string url) {

            var result = await SendRequest(method, url);

            if (result.IsSuccess) {
                return await result.As<T>();
            } else {
                throw new NonSuccessResponseException(result.Response);
            }
        }

        public async Task<FiHttpResult> SendRequest<T>(HttpMethod method, string url, T postData, string contentType = null) {
            if (postData is Stream s) {
                return await SendRequest(method, url, s, contentType);
            }
            contentType = contentType ?? ContentTypeJson;
            var bytes = GetObjectBytes(postData, contentType);
            var req = CreateRequest(url);
            req.Method = method;
            req.Content = new ByteArrayContent(bytes);
            req.Content.Headers.ContentType = new MediaTypeHeaderValue(contentType);
            req.Content.Headers.ContentLength = bytes.Length;
            return await FiHttpResult.InitFromRequest(this, req);
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

        private void AddHeaders(HttpRequestMessage req) {
            lock (headers) {
                headers.ForEach((h) => {
                    if (h.Key == "Content-Length") {
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
                lock (headers) {
                    if (headers.ContainsKey(k)) {
                        return headers[k];
                    }
                }
                return null;
            }
            set {
                lock (headers) {
                    headers[k] = value;
                }
            }
        }
    }
}
