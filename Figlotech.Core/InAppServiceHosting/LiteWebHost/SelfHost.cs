using Figlotech.Core;
using Figlotech.Core.DomainEvents;
using Figlotech.Core.FileAcessAbstractions;
using Figlotech.Core.Helpers;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.Core.InAppServiceHosting.LiteWebHost {

    public interface IApiHandler {
        bool CanHandleApi(string method, string uri);

        Task HandleRequest(string reqUri, IDictionary<string, string> query, Dictionary<string, string> reqHeaders, byte[] bodyBytes, Stream stream);
    }

    public static class SelfHostCacheService {

    }

    public sealed class SelfHost {

        public static async Task SendHeaders(StreamWriter writer, Dictionary<string, string> headers) {
            foreach(var header in headers) {
                await writer.WriteLineAsync($"{header.Key}: {header.Value}");
            }
        }

        public static async Task SendCorsHeadersAllowAll(StreamWriter writer, Dictionary<string, string> headers) {
            await writer.WriteLineAsync($"Access-Control-Allow-Origin: *");
            await writer.WriteLineAsync($"Access-Control-Allow-Headers: content-type");
            await writer.WriteLineAsync($"Access-Control-Allow-Methods: GET, POST");
            await writer.WriteLineAsync();
        }

        public static async Task SendStatusCode(StreamWriter writer, int status, string text) {
            await writer.WriteLineAsync($"HTTP/1.1 {status} {text}");
        }

        public static async Task SendResponse(int status, string statusText, Stream stream, string mime, byte[] bytes) {
            using (var writer = new StreamWriter(stream, Fi.StandardEncoding, 8196, true)) {
                await SendStatusCode(writer, status, statusText);
                await SendHeaders(writer, new Dictionary<string, string>() {
                    { "Content-Type", $"{mime}" },
                    { "Content-Length", $"{bytes.Length}" },
                    { "x-powered-by", $"SelfHost do Emissor SL" },
                    { "Access-Control-Allow-Origin", "*" }
                });
                await writer.FlushAsync();
            }
            await stream.WriteAsync(bytes, 0, bytes.Length);
        }

        public static async Task SendJson<T>(int status, string statusText, Stream stream, T obj) {
            var bytes = Fi.StandardEncoding.GetBytes(JsonConvert.SerializeObject(obj));
            await SendResponse(status, statusText, stream, "application/json", bytes);
        }
        public static async Task SendJsonSuccess<T>(Stream stream, T obj) {
            await SendJson(200, "OK", stream, obj);
        }
        public static async Task<T> ReadJson<T>(byte[] bytes) {
            await Task.Yield();
            var txt = Fi.StandardEncoding.GetString(bytes);
            return JsonConvert.DeserializeObject<T>(txt);
        }

        WorkQueuer work = new WorkQueuer("EmbededProxyRequestHandler", Int32.MaxValue, true) {};
                
        TcpListener listener;
        bool started;
        bool running;
        Thread serverThread;
        public IPAddress Host { get; private set; }
        public int Port { get; private set; }

        private static Type[] _evtTypes = null;
        public static Type[] EvtTypes {
            get {
                lock("INIT_EVT_TYPES") {
                    if(_evtTypes == null) {
                        _evtTypes = new Type[0];
                    }

                    return _evtTypes;
                }
            }
        }

        public SelfHost(IPAddress host, int port) {
            Host = host;
            Port = port;
        }

        SelfInitializerDictionary<string, byte[]> AppFiles = new SelfInitializerDictionary<string, byte[]>(
            key => {
                return null;
            }
        );

        public Thread EventServiceThread { get; set; }

        public bool SendFromCache(string name, Stream s) {
            var cache = AppFiles[name];
            if (cache != null) {
                lock (cache) {
                    lock (s) {
                        using (var writer = new StreamWriter(s, Fi.StandardEncoding, 8196, true)) {
                            writer.WriteLine($"HTTP/1.1 200 OK");
                            writer.WriteLine($"Content-Type: {Fi.Tech.GetMimeType(name)}");
                            writer.WriteLine($"Content-Length: {cache.Length}");
                            writer.WriteLine($"x-src: webui.zip/{name}");
                            writer.WriteLine();
                            writer.Flush();
                        }

                        s.Write(cache, 0, cache.Length);
                        s.Flush();
                    }
                }
                return true;
            }

            return false;
        }

        public void Join()
        {
            serverThread.Join();
        }

        public void Start() {
            if (started || running) {
                return;
            }
            ServicePointManager.DefaultConnectionLimit = Int32.MaxValue;
            ServicePointManager.MaxServicePoints = Int32.MaxValue;
            ServicePointManager.ReusePort = true;
            ServicePointManager.UseNagleAlgorithm = false;

            List<IApiHandler> Handlers = new List<IApiHandler>();
            Handlers.AddRange(
                typeof(SelfHost).Assembly
                    .GetTypes()
                    .Where(t=> !t.IsAbstract && t.Implements(typeof(IApiHandler)))
                    .Select(t=> (IApiHandler) Activator.CreateInstance(t))
            );

            serverThread = Fi.Tech.SafeCreateThread((Action)(() => {
                running = true;
                var listener = new TcpListener(Host, Port);
                listener.Start();
                Console.WriteLine($"LiteWebHost running on http://{Host.ToString()}:{Port}");

                while (running) {
                    try {
                        var client = listener.AcceptTcpClient();
#if DEBUG
                        Console.WriteLine($"Request {client.Client.RemoteEndPoint.ToString()}");
#endif
                        client.NoDelay = false;
                        work.Enqueue(async () => {
                            await Task.Yield();
                            using (client) {
                                using (var stream = client.GetStream()) {
                                    var row = 0;
                                    string uri = "";
                                    string method = "";
                                    List<string> body = new List<string>();
                                    Dictionary<string, string> headers = new Dictionary<string, string>();
                                    using (var reqCache = new MemoryStream()) {
                                        var buffer = new byte[8196];
                                        while (stream.DataAvailable || reqCache.Length == 0) {
                                            int bytesRead = stream.Read(buffer, 0, buffer.Length);
                                            reqCache.Write(buffer, 0, bytesRead);
                                            if (bytesRead < buffer.Length) {
                                                break;
                                            }
                                        }
                                        reqCache.Seek(0, SeekOrigin.Begin);
                                        using (var reader = new StreamReader(reqCache, Fi.StandardEncoding, false, 8196, true)) {
                                            var bodyBytes = new byte[0];
                                            while (!reader.EndOfStream) {
                                                var line = reader.ReadLine();
                                                if (string.IsNullOrEmpty(line.Trim())) {
                                                    if (method != "GET" && method != "OPTIONS") {
                                                        bodyBytes = new UTF8Encoding(true).GetBytes(reader.ReadToEnd());
                                                    }
                                                    break;
                                                }
                                                if (row == 0) {
                                                    var reqArray = line.Split(' ');
                                                    if (reqArray.Length < 2) {
                                                        return;
                                                    }
                                                    method = reqArray[0];
                                                    uri = reqArray[1];
                                                } else {
                                                    var array = line.Split(':');
                                                    var key = array[0];
                                                    var value = string.Join(":", array.Skip(1).ToArray());
                                                    headers[key] = value;
                                                }
                                                row++;
                                            }
                                            if (string.IsNullOrEmpty(method)) {
                                                return;
                                            }

                                            Console.WriteLine($"REQUEST: {method} {uri}");
                                            if(method == "OPTIONS") {
                                                using (var writer = new StreamWriter(stream, Fi.StandardEncoding, 8196, true)) {
                                                    writer.WriteLine($"HTTP/1.1 200 OK");
                                                    writer.WriteLine($"Access-Control-Allow-Origin: *");
                                                    writer.WriteLine($"Access-Control-Allow-Headers: content-type");
                                                    writer.WriteLine($"Access-Control-Allow-Methods: GET, POST");
                                                    writer.WriteLine();
                                                    writer.Flush();
                                                    return;
                                                }
                                            }

                                            var realPath = uri;
                                            LenientDictionary<string, string> query = new LenientDictionary<string, string>();
                                            if (realPath.Contains('?')) {
                                                var idx = uri.IndexOf("?") + 1;
                                                var queryString = uri.Substring(idx, uri.Length - idx);
                                                realPath = uri.Substring(0, uri.IndexOf('?'));

                                                query = queryString.Split('&')
                                                    .Where(kvps => !string.IsNullOrEmpty(kvps) && kvps.Contains("="))
                                                    .Select(
                                                    (kvps)=> {
                                                        var split = kvps.Split('=');
                                                        return new KeyValuePair<string, string>(split[0], split[1]);
                                                    }
                                                ).ToDictionary(x=>x.Key, x=> x.Value);
                                            }



                                            if (!realPath.StartsWith("/api/")) {
                                                if (!realPath.Contains('.')) {
                                                    realPath = "/index.html";
                                                }
                                                var cacheFile = realPath.Substring(1);
                                                if (AppFiles.ContainsKey(cacheFile)) {
                                                    try {
                                                        if (SendFromCache(cacheFile, stream))
                                                            return;
                                                    } catch (Exception x) {
                                                        Console.WriteLine(x.Message);
                                                        return;
                                                    }
                                                }
                                            } else {
                                                var request = new LiteWebRequest(
                                                    uri, query, headers, bodyBytes, stream
                                                );
                                                try {
                                                    foreach (var handler in Handlers) {
                                                        var rp = realPath.Replace("/api/", "");
                                                        if (handler.CanHandleApi(method, rp)) {
                                                            await handler.HandleRequest(uri, query, headers, bodyBytes, stream);
                                                            return;
                                                        }
                                                    }
                                                } catch(MainLogicGeneratedException mlex) {
                                                    await SelfHost.SendJson(mlex.StatusCode, "Error", stream, mlex);
                                                    return;
                                                } catch(Exception x) {
                                                    await SelfHost.SendJson(500, "Error", stream, x);
                                                    return;
                                                }
                                            }

                                            using (var writer = new StreamWriter(stream, Fi.StandardEncoding, 8196, true)) {
                                                writer.WriteLine($"HTTP/1.1 404 NOT FOUND");
                                                writer.WriteLine();
                                                writer.Flush();
                                            }
                                        }
                                    }
                                }
                            }
                        }, async x => {
                            await Task.Yield();
                            Console.WriteLine(x.Message);
                        });
                    } catch (Exception ex) {
                        Console.WriteLine($"Error accepting Client: {ex.Message}");
                    }
                }
            }), x => {
                Console.WriteLine(x.Message);
            });
            serverThread.Start();
        }

        IFileSystem RequestLog;

    }
}
