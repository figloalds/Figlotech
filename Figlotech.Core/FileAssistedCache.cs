using Figlotech.Core.FileAcessAbstractions;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;

namespace Figlotech.Core {
    public sealed class FileAssistedCache<T> where T : class, new() {
        public IFileSystem FileSystem { get; private set; }
        public string? Filename { get; private set; }

        public Task Initialization { get; private set; }

        public bool UseFileReplacementTechnique { get; set; } = false;

        public FileAssistedCache(IFileSystem fs, string? filename) {
            this.FileSystem = fs;
            this.Filename = filename;
            this.Cache = new LenientDictionary<string, T>();
            Initialization = this.LoadAllFromFile();
        }

        public string[] Keys => this.Cache.Keys.ToArray();
        public T[] Values => this.Cache.Values.ToArray();
        public KeyValuePair<string, T>[] KeyValuePairs => this.Cache.ToArray();

        public async Task LoadAllFromFile() {
            if (UseFileReplacementTechnique) {
                if (!await this.FileSystem.ExistsAsync(this.Filename)) {
                    foreach(var f in this.FileSystem.GetFilesIn("")) {
                        var fn = Path.GetFileName(f);
                        if (fn.StartsWith(Path.GetFileName(this.Filename + ".new."))) {
                            await this.FileSystem.RenameAsync(fn, this.Filename);
                        }
                        if (fn.StartsWith(Path.GetFileName(this.Filename + ".old."))) {
                            await this.FileSystem.RenameAsync(fn, this.Filename);
                        }
                    }
                }
            }

            if (await this.FileSystem.ExistsAsync(this.Filename)) {
                try {
                    using (var stream = await this.FileSystem.OpenAsync(this.Filename, FileMode.Open, FileAccess.Read)) {
                        using (var zipstream = new ZipArchive(stream)) {
                            foreach (var entry in zipstream.Entries) {
                                if (entry != null) {
                                    using (var entryStream = entry.Open()) {
                                        using (var reader = new StreamReader(entryStream)) {
                                            var obj = JsonConvert.DeserializeObject<T>(await reader.ReadToEndAsync());
                                            this.Cache[entry.Name] = obj;
                                        }
                                    }
                                }
                            }
                        }
                    }
                } catch (Exception x) {
                    await this.FileSystem.DeleteAsync(this.Filename);
                }
            }
        }

        public async Task<T> TryGetTFromfile(string? rid) {
            try {
                if (await this.FileSystem.ExistsAsync(this.Filename)) {
                    using (var stream = await this.FileSystem.OpenAsync(this.Filename, FileMode.Open, FileAccess.Read)) {
                        using (var zipstream = new ZipArchive(stream)) {
                            var entry = zipstream.GetEntry(rid);
                            if (entry != null) {
                                using (var entryStream = entry.Open()) {
                                    using (var reader = new StreamReader(entryStream)) {
                                        return JsonConvert.DeserializeObject<T>(await reader.ReadToEndAsync());
                                    }
                                }
                            }
                        }
                    }
                }
            } catch (Exception x) {

            }
            return default(T);
        }
        FiAsyncMultiLock FileLocks = new FiAsyncMultiLock();
        public async Task TryPutTTofile(string? rid, T value) {
            try {
                var json = JsonConvert.SerializeObject(value);
                using (var handle = await FileLocks.Lock(Filename)) {
                    var srid = IntEx.GenerateShortRid();
                    var fileNameNew = Filename + $".new.{srid}";
                    var fileNameOld = Filename + $".old.{srid}";

                    if(UseFileReplacementTechnique) {
                        await this.FileSystem.CopyFile(Filename, FileSystem, fileNameNew);
                    } else {
                        fileNameNew = Filename;
                    }
                    
                    using (var stream = await this.FileSystem.OpenAsync(fileNameNew, FileMode.OpenOrCreate, FileAccess.ReadWrite)) {
                        using (var zipstream = new ZipArchive(stream, ZipArchiveMode.Update)) {
                            var entry = zipstream.GetEntry(rid);
                            if (entry != null) {
                                entry.Delete();
                            }
                            if (value != null) {
                                entry = zipstream.CreateEntry(rid);
                                using (var entryStream = entry.Open()) {
                                    using (var writer = new StreamWriter(entryStream)) {
                                        await writer.WriteAsync(json);
                                    }
                                }
                            } else if (rid != null && this.Cache.ContainsKey(rid!)) {
                                this.Cache.Remove(rid);
                            }
                        }
                    }

                    if(UseFileReplacementTechnique) {
                        await this.FileSystem.DeleteAsync(Filename);
                        await this.FileSystem.RenameAsync(Filename, fileNameOld);
                        await this.FileSystem.RenameAsync(fileNameNew, Filename);
                        await this.FileSystem.DeleteAsync(fileNameOld);
                    }
                }
            } catch (Exception x) {
                await this.FileSystem.DeleteAsync(this.Filename);
                foreach (var item in this.Cache) {
                    await this.TryPutTTofile(item.Key, item.Value);
                }
            }
        }

        public T this[string? rid] {
            get {
                return this.Cache[rid];
            }
            set {
                Fi.Tech.FireAndForget(async () => {
                    await this.TryPutTTofile(rid, value);
                });
                this.Cache[rid] = value;
            }
        }

        public void Remove(string? rid) {
            Fi.Tech.FireAndForget(async () => {
                await this.TryPutTTofile(rid, null);
            });
        }

        LenientDictionary<string, T> Cache { get; set; }

    }
}
