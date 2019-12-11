using Figlotech.Core;
using Figlotech.Core.FileAcessAbstractions;
using Figlotech.Core.Helpers;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.Core.InAppServiceHosting
{

    public interface IApiHandler
    {
        bool CanHandleApi(string method, string uri);

        void HandleRequest(string reqUri, Dictionary<string, string> query, Dictionary<string, string> reqHeaders, byte[] bodyBytes, Stream stream);
    }

    public abstract class ApiHandler : IApiHandler
    {
        public abstract bool CanHandleApi(string method, string uri);
        public abstract void HandleRequest(string reqUri, Dictionary<string, string> query, Dictionary<string, string> reqHeaders, byte[] bodyBytes, Stream stream);

    }

    public class GenericInlineHandler : ApiHandler
    {
        Func<string, string, bool> CanHandle { get; set; }
        Action<string, Dictionary<string, string> , Dictionary<string, string>, byte[], Stream> DoHandle { get; set; }

        public GenericInlineHandler(Func<string, string, bool> canHandle, Action<string, Dictionary<string, string>, Dictionary<string, string>, byte[], Stream> doHandle) {
            this.CanHandle = canHandle;
            this.DoHandle = doHandle;
        }

        public override bool CanHandleApi(string method, string uri) {
            return this.CanHandle(method, uri);
        }
        public override void HandleRequest(string reqUri, Dictionary<string, string> query, Dictionary<string, string> reqHeaders, byte[] bodyBytes, Stream stream) {
            DoHandle(reqUri, query, reqHeaders, bodyBytes, stream);
        }

    }


    public static class SelfHostCacheService
    {
        public static LenientDictionary<string, object> DataCache = new LenientDictionary<string, object>();

        public static void PutCache<T>(string label, T obj) {
            DataCache[label] = obj;
        }
        public static T GetCache<T>(string label) {
            if (DataCache[label] is T retv) {
                return retv;
            }
            return default(T);
        }
        public static T GetOrInitCache<T>(string label, Func<T> InitCache) {
            if (DataCache[label] is T retv) {
                return retv;
            }
            var ret = InitCache();
            DataCache[label] = ret;
            return ret;
        }

    }

    public class SelfHost
    {

        public static void SendHeaders(StreamWriter writer, Dictionary<string, string> headers) {
            writer.WriteLine($"Access-Control-Allow-Origin: *");
            writer.WriteLine($"Access-Control-Allow-Headers: content-type");
            writer.WriteLine($"Access-Control-Allow-Methods: GET, POST");
            writer.WriteLine();
        }

        public static void SendStatusCode(StreamWriter writer, int status, string text) {
            writer.WriteLine($"HTTP/1.1 {status} {text}");
        }

        public static void SendResponse(int status, string statusText, Stream stream, string mime, byte[] bytes) {
            using (var writer = new StreamWriter(stream, Fi.StandardEncoding, 8196, true)) {
                SendStatusCode(writer, status, statusText);
                SendHeaders(writer, new Dictionary<string, string>() {
                    { "Content-Type", $"{mime}" },
                    { "Content-Length", $"{bytes.Length}" },
                });
                writer.Flush();
            }
            stream.Write(bytes, 0, bytes.Length);
        }

        public static void SendJson<T>(int status, string statusText, Stream stream, T obj) {
            var bytes = Fi.StandardEncoding.GetBytes(JsonConvert.SerializeObject(obj));
            SendResponse(status, statusText, stream, "application/json", bytes);
        }
        public static void SendJsonSuccess<T>(Stream stream, T obj) {
            SendJson(200, "OK", stream, obj);
        }

        WorkQueuer work = new WorkQueuer("EmbededRequestHandler", 32, true) {
            MaxParallelTasks = 16
        };

        TcpListener listener;
        bool started;
        bool running;
        Thread serverThread;
        int Port;
        IPAddress Ip;

        public SelfHost(IPAddress Ip, int Port) {
            this.Ip = Ip;
            this.Port = Port;
        }

        SelfInitializerDictionary<string, byte[]> AppFiles = new SelfInitializerDictionary<string, byte[]>(
            key => {
                return null;
            }
        );

        public bool SendFromCache(string name, Stream s) {
            var cache = AppFiles[name];
            if (cache != null) {
                lock (cache) {
                    lock (s) {
                        using (var writer = new StreamWriter(s, Fi.StandardEncoding, 8196, true)) {
                            writer.WriteLine($"HTTP/1.1 200 OK");
                            writer.WriteLine($"Content-Type: {Fi.Tech.GetMimeType(name)}");
                            writer.WriteLine($"Content-Length: {cache.Length}");
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

        public void Join() {
            serverThread.Join();
        }

        List<IApiHandler> Handlers { get; set; } = new List<IApiHandler>();

        public void AddHandler(Func<string, string, bool> canHandle, Action<string, Dictionary<string, string>, Dictionary<string, string>, byte[], Stream> doHandle) {
            this.Handlers.Add(new GenericInlineHandler(canHandle, doHandle));
        }

        public void Start() {
            if (started || running) {
                return;
            }
            ServicePointManager.DefaultConnectionLimit = Int32.MaxValue;
            ServicePointManager.MaxServicePoints = Int32.MaxValue;
            ServicePointManager.ReusePort = true;
            ServicePointManager.UseNagleAlgorithm = false;
            
            serverThread = Fi.Tech.SafeCreateThread((Action)(() => {
                running = true;
                var listener = new TcpListener(Ip, Port);
                listener.Start();

                while (running) {
                    try {
                        var client = listener.AcceptTcpClient();
                        Console.WriteLine($"Request {client.Client.RemoteEndPoint.ToString()}");
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

#if DEBUG
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
#endif

                                            var realPath = uri;
                                            Dictionary<string, string> query = new Dictionary<string, string>();
                                            if (realPath.Contains('?')) {
                                                var idx = uri.IndexOf("?") + 1;
                                                var queryString = uri.Substring(idx, uri.Length - idx);
                                                realPath = uri.Substring(0, uri.IndexOf('?'));

                                                query = queryString.Split('&')
                                                    .Where(kvps => !string.IsNullOrEmpty(kvps) && kvps.Contains("="))
                                                    .Select(
                                                    (kvps) => {
                                                        var split = kvps.Split('=');
                                                        return new KeyValuePair<string, string>(split[0], split[1]);
                                                    }
                                                ).ToDictionary(x => x.Key, x => x.Value);
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
                                                try {
                                                    foreach (var handler in Handlers) {
                                                        var rp = realPath.Replace("/api/", "");
                                                        if (handler.CanHandleApi(method, rp)) {

                                                            handler.HandleRequest(uri, query, headers, bodyBytes, stream);
                                                            return;
                                                        }
                                                    }
                                                } catch (MainLogicGeneratedException mlex) {
                                                    SelfHost.SendJson(mlex.StatusCode, "Error", stream, mlex);
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
