using Figlotech.Core.Autokryptex.EncryptMethods;
using Figlotech.Core.FileAcessAbstractions;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core.Autokryptex {
    public class SafeDataPayload {
        public string Type { get; set; }
        public byte[] Hash { get; set; }
        public byte[] Data { get; set; }
        public long CreatedTime { get; set; }
        [JsonIgnore]
        internal string RID {get;set;}
    }
    public class SafeObjectStorage {
        IFileSystem fileSystem { get; set; }
        IEncryptionMethod dataEncryptor { get; set; }
        IEncryptionMethod fileEncryptor { get; set; }

        List<SafeDataPayload> Cache { get; set; } = new List<SafeDataPayload>();
        Queue<string> DeletionQueue { get; set; } = new Queue<string>();

        public int CacheLength {
            get {
                lock (Cache)
                    return Cache.Count;
            }
        }


        List<Type> KnownTypes { get; set; } = new List<Type>();
        int _fetchLock = 0;

        public SafeObjectStorage(IFileSystem fs, string password) {
            fileSystem = fs;
            dataEncryptor = new AutokryptexEncryptor(password, 2);
            fileEncryptor = new AesEncryptor(RID.MachineRID);
        }

        public SafeObjectStorage(IFileSystem fs, IEncryptionMethod fileEncryptionMethod, IEncryptionMethod dataEncryptionMethod) {
            fileSystem = fs;
            dataEncryptor = dataEncryptionMethod;
            fileEncryptor = fileEncryptionMethod;
        }

        public void AddWorkingTypes(params Type[] types) {
            KnownTypes.AddRange(types
                .Where(t => !KnownTypes.Contains(t))
            );
        }

        public void PreloadAll() {
            var cachedRids = Cache.Select(c => c.RID);
            var newList = new List<SafeDataPayload>();
            fileSystem.ForFilesIn("", rid => {
                if (cachedRids.Contains(rid)) {
                    return;
                }
                _cachePayload(_getPayloadFromFile(rid));
            });
            lock(Cache) {
                Cache.Sort((a, b) =>
                    a.CreatedTime > b.CreatedTime ? 1 :
                    a.CreatedTime < b.CreatedTime ? -1 :
                    0
                );
            }
        }

        public IEnumerable<(string RID, Type Type, object Value)> CacheFetch() {
            lock(Cache)
                foreach(var payload in Cache) {
                    var type = KnownTypes.FirstOrDefault(t => t.Name == payload.Type);
                    if(type == null) {
                        continue;
                    }
                    var value = _decryptObject(type, payload.Data);
                    yield return (payload.RID, type, value);
                }
        }

        private byte[] _gzip(byte[] data) {
            using (MemoryStream ms = new MemoryStream(data)) {
                using (var retv = new MemoryStream()) {
                    using (var zs = new GZipStream(retv, CompressionLevel.Optimal, true)) {
                        ms.CopyTo(zs);
                    }
                    return retv.ToArray();
                }
            }
        }

        private byte[] _gunzip(byte[] data) {
            using (MemoryStream ms = new MemoryStream(data)) {
                using (var zs = new GZipStream(ms, CompressionMode.Decompress)) {
                    using (var retv = new MemoryStream()) {
                        zs.CopyTo(retv);
                        return retv.ToArray();
                    }
                }
            }
        }

        private byte[] _encryptObject(object o) {
            return dataEncryptor.Encrypt(
                _gzip(
                    Fi.StandardEncoding.GetBytes(
                    JsonConvert.SerializeObject(o)
                    )
                )
            );
        }

        private T _decryptObject<T>(byte[] data) {
            return JsonConvert.DeserializeObject<T>(
                Fi.StandardEncoding.GetString(
                    _gunzip(
                        dataEncryptor.Decrypt(data)
                    )
                )
            );
        }

        private object _decryptObject(Type type, byte[] data) {
            return JsonConvert.DeserializeObject(Fi.StandardEncoding.GetString(
                dataEncryptor.Decrypt(data)
            ), type);
        }

        public IEnumerable<(string RID, T Data)> Fetch<T>() {
            _fetchLock++;
            try {
                var cachedRids = Cache.Select(c => c.RID);
                var newList = new List<SafeDataPayload>();
                var t = Task.Run(() => {
                    fileSystem.ForFilesIn("", rid => {
                        if (cachedRids.Contains(rid)) {
                            return;
                        }
                        newList.Add(_getPayloadFromFile(rid));
                    });
                });

                foreach (var payload in Cache) {
                    if (payload.Type == typeof(T).Name)
                        yield return (payload.RID, _getObjectFromPayload<T>(payload));
                }
                t.Wait();
                foreach (var payload in newList) {
                    _cachePayload(payload);
                    if (payload.Type == typeof(T).Name)
                        yield return (payload.RID, _getObjectFromPayload<T>(payload));
                }
            } finally {
                _fetchLock--;
            }
            lock (DeletionQueue)
                while (DeletionQueue.Count > 0) {
                    Delete(DeletionQueue.Dequeue());
                }
        }

        public void Delete(string rid) {
            if (_fetchLock > 0) {
                QueueDelete(rid);
                return;
            }
            lock(Cache)
                Cache.RemoveAll(c => c.RID == rid);
            fileSystem.Delete(rid);
        }

        public void QueueDelete(string rid) {
            lock(DeletionQueue)
                DeletionQueue.Enqueue(rid);
        }

        public string Put(object item) {
            var t = item.GetType();
            if(!KnownTypes.Contains(t)) {
                AddWorkingTypes(t);
            }
            var data = _encryptObject(item);
            var hash = Fi.Tech.ComputeHash(data);
            var objectRID = new RID().AsBase36;
            var payload = new SafeDataPayload {
                Type = item.GetType().Name,
                Hash = hash,
                Data = data,
                CreatedTime = DateTime.UtcNow.Ticks,
                RID = objectRID
            };
            _cachePayload(payload);
            var bytes = _gzip(
                Fi.StandardEncoding.GetBytes(JsonConvert.SerializeObject(payload))
            );
            var encBytes = fileEncryptor.Encrypt(bytes);

            fileSystem.WriteAllBytes(objectRID, encBytes);
            
            return objectRID;
        }

        public T GetObject<T>(string rid) {
            if(!fileSystem.Exists(rid)) {
                return default(T);
            }
            SafeDataPayload needle;
            lock(Cache) needle = Cache.FirstOrDefault(c => c.RID == rid);
            if(needle != null) {
                return _decryptObject<T>(needle.Data);
            }
            return _getObjectFromFile<T>(rid);
        }


        public void _cachePayload(SafeDataPayload payload) {
            lock (Cache) Cache.Add(payload);
        }

        public T _getObjectFromPayload<T>(SafeDataPayload payload) {
            return _decryptObject<T>(payload.Data);
        }

        public SafeDataPayload _getPayloadFromFile(string rid) {
            var buffer = _gunzip(
                fileEncryptor.Decrypt(
                    fileSystem.ReadAllBytes(rid)
                )
            );
            var retv = JsonConvert.DeserializeObject<SafeDataPayload>(
                Fi.StandardEncoding.GetString(buffer)
            );
            retv.RID = rid;
            return retv;
        }

        public T _getObjectFromFile<T>(string rid) {
            var payload = _getPayloadFromFile(rid);
            _cachePayload(payload);
            if (typeof(T).Name != payload.Type) {
                Fi.Tech.WriteLine($"Warning: Type mismatch opening {payload.Type} as {typeof(T).Name} from SafeObjectStorage");
            }
            return _decryptObject<T>(payload.Data);
        }
    }
}
