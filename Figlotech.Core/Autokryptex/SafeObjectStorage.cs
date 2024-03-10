﻿using Figlotech.Core.Autokryptex.EncryptionMethods;
using Figlotech.Core.Autokryptex.EncryptMethods;
using Figlotech.Core.Autokryptex.EncryptMethods.Legacy;
using Figlotech.Core.Autokryptex.Legacy;
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

namespace Figlotech.Core.Autokryptex
{
    public sealed class SafeDataPayload
    {
        public string Type { get; set; }
        public byte[] Hash { get; set; }
        public byte[] Data { get; set; }
        public long CreatedTime { get; set; }
        [JsonIgnore]
        internal string RID { get; set; }
    }
    public sealed class SafeObjectStorage
    {
        IFileSystem fileSystem { get; set; }
        IEncryptionMethod dataEncryptor { get; set; }
        IEncryptionMethod fileEncryptor { get; set; }

        List<SafeDataPayload> Cache { get; set; } = new List<SafeDataPayload>();
        Queue<string> DeletionQueue { get; set; } = new Queue<string>();

        public int CacheLength {
            get {
                lock (Cache) {
                    return Cache.Count;
                }
            }
        }

        List<Type> KnownTypes { get; set; } = new List<Type>();

        public SafeObjectStorage(IFileSystem fs) {
            fileSystem = fs;
            dataEncryptor = new NoOpEncryptor();
            fileEncryptor = new NoOpEncryptor();
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
            List<string> cachedRids;
            lock (Cache)
                cachedRids = Cache.Select(c => c.RID).ToList();
            var newList = new List<SafeDataPayload>();
            fileSystem.ForFilesIn("", rid => {
                if (cachedRids.Contains(rid)) {
                    return;
                }
                _cachePayload(_getPayloadFromFile(rid));
            });
            lock (Cache) {
                Cache.Sort((a, b) =>
                    a.CreatedTime > b.CreatedTime ? 1 :
                    a.CreatedTime < b.CreatedTime ? -1 :
                    0
                );
            }
        }

        public int CacheCount<T>() {
            lock (Cache)
                return Cache.Count(payload => payload.Type == typeof(T).Name);
        }

        public IEnumerable<(string RID, Type Type, object Value)> CacheFetch() {
            List<SafeDataPayload> localCopyCache;
            lock (Cache)
                localCopyCache = new List<SafeDataPayload>(Cache);
            foreach (var payload in localCopyCache) {
                var type = KnownTypes.FirstOrDefault(t => t.Name == payload.Type);
                if (type == null) {
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

        public List<(string RID, T Data)> Fetch<T>() {
            List<(string RID, T Data)> retv = new List<(string RID, T Data)>();
            try {
                List<string> cachedRids;
                lock (Cache)
                    lock(DeletionQueue)
                        cachedRids = Cache.Select(c => c.RID).Where(x=> !DeletionQueue.Contains(x)).ToList();
                var newList = new List<SafeDataPayload>();
                fileSystem.ForFilesIn("", rid => {
                    if (cachedRids.Contains(rid)) {
                        return;
                    }
                    lock (DeletionQueue) 
                        if(!DeletionQueue.Contains(rid)) {
                            newList.Add(_getPayloadFromFile(rid));
                        }
                });

                List<SafeDataPayload> localCopyCache;
                lock (Cache)
                    localCopyCache = new List<SafeDataPayload>(Cache);
                foreach (var payload in localCopyCache) {
                    if (payload.Type == typeof(T).Name)
                        retv.Add((payload.RID, _getObjectFromPayload<T>(payload)));
                }
                foreach (var payload in newList) {
                    _cachePayload(payload);
                    if (payload.Type == typeof(T).Name)
                        retv.Add((payload.RID, _getObjectFromPayload<T>(payload)));
                }
            } finally {

            }

            ExecuteDeleteQueue();

            return retv;
        }

        public void ExecuteDeleteQueue() {
            lock (DeletionQueue)
                while (DeletionQueue.Count > 0) {
                    Delete(DeletionQueue.Dequeue());
                }
        }

        public void Delete(string rid) {
            lock (Cache)
                Cache.RemoveAll(c => c.RID == rid);
            fileSystem.DeleteAsync(rid);
        }

        public void QueueDelete(string rid) {
            lock (DeletionQueue)
                DeletionQueue.Enqueue(rid);
        }

        public string Put(object item) {
            var t = item.GetType();
            if (!KnownTypes.Contains(t)) {
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

        public async Task<T> GetObject<T>(string rid) {
            if (!await fileSystem.ExistsAsync(rid).ConfigureAwait(false)) {
                return default(T);
            }
            SafeDataPayload needle;
            lock (Cache) needle = Cache.FirstOrDefault(c => c.RID == rid);
            if (needle != null) {
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
