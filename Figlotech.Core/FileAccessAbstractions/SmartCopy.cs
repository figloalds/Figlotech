using Figlotech.Core.Helpers;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Figlotech.Core.FileAcessAbstractions {
    public class FileData {
        public string RelativePath { get; set; }
        public long Date { get; set; }
        public long Length { get; set; }
        public string Hash { get; set; }
    }
    public class CopyMirrorFileData
    {
        public string RelativePath { get; set; }
        public bool Changed { get; set; }
    }

    // Note: The cyclomatic complexity here is overwhelming, I may need to refactor this soon.

    /// <summary>
    /// This is meant to provide easy in-program "robocopy" utility.
    /// </summary>
    public class SmartCopy {

        IFileSystem local;
        IFileSystem remote;
        SmartCopyOptions options;

        /// <summary>
        /// Provides easy in-program robust copy utility
        /// </summary>
        /// <param name="localAccessor">"Local" or "Origin" accessor</param>
        public SmartCopy(IFileSystem localAccessor, SmartCopyOptions copyOptions) {
            local = localAccessor;
            options = copyOptions;
        }

        public List<String> Excludes = new List<String>();
        
        private bool CheckMatch(string file, string criteria) {
            var regex = Fi.Tech.WildcardToRegex(criteria);
            return Regex.Match(file, regex, RegexOptions.IgnoreCase).Success;
        }

        /// <summary>
        /// Sets the "other" file acessor to work with;
        /// </summary>
        /// <param name="localAccessor">"Remote" or "Destination" accessor</param>
        public void SetRemote(IFileSystem remoteAccessor) {
            remote = remoteAccessor;
        }

        public static async Task<string> GetHash(Stream stream) {
            using (var md5 = MD5.Create()) {
                return Convert.ToBase64String(await Task.Run((()=> md5.ComputeHash(stream))));
            }
        }

        public static async Task<string> GetHash(IFileSystem fa, String path) {
            string hash = "";
            await fa.Read(path, async (stream) => {
                await Task.Yield();
                hash = await GetHash(stream);
            });

            return hash;
        }

        public async Task<List<FileData>> EnumerateFilesToDownload(string path = "") {
            if (remote == null) {
                throw new NullReferenceException("The Remote server was not specified, do call SetRemote(IFileAccessor) to specify it.");
            }
            if (options.UseHashList) {
                return await EnumerateDownloadableFilesFromHashList(path);
            } else {
                return await EnumerateDownloadableFiles(remote, local, path);
            }
        }

        private async Task<List<FileData>> EnumerateDownloadableFiles(IFileSystem origin, IFileSystem destination, string path = "") {
            List<FileData> retv = new List<FileData>();
            if (options.UseHashList) {
                HashList = await SmartCopy.GetHashList(this.remote, true);
            }

            workedFiles = 0;
            foreach(var f in origin.GetFilesIn(path)) {
                if (f == HASHLIST_FILENAME) {
                    continue;
                }
                if (Excludes.Any(excl => CheckMatch(f, excl))) {
                    continue;
                }

                var changed = CopyDecisionCriteria != null ?
                    await CopyDecisionCriteria(f) :
                    await Changed(origin, destination, f);

                if (changed) {
                    retv.Add(new FileData {
                        RelativePath = f,
                        Date = origin.GetLastModified(f)?.Ticks ?? 0,
                        Hash = "", // GetHash(origin, f),
                        Length = origin.GetSize(f)
                    });
                }
            }

            if (options.Recursive) {
                foreach(var dir in origin.GetDirectoriesIn(path)) {
                    destination.MkDirs(dir);
                    var newAdds = await EnumerateDownloadableFiles(origin, destination, dir);
                    foreach(var a in newAdds) {
                        retv.Add(a);
                    }
                }
            }

            return retv;
        }

        private async Task<List<FileData>> EnumerateDownloadableFilesFromHashList(string path) {
            HashList = await SmartCopy.GetHashList(remote, true);
            List<FileData> workingList = HashList.Where(f => f.RelativePath.StartsWith(path)).ToList();
            OnReportTotalFilesCount?.Invoke(workingList.Count);
            var retv = new List<FileData>();
            foreach (var a in HashList) {
                string hash = "";
                if (local.Exists(a.RelativePath)) {
                    local.Read(a.RelativePath, async (stream) => {
                        await Task.Yield();
                        hash = await GetHash(stream);
                    }).Wait();
                }

                var gzSuffix = "";
                if (options.UseGZip)
                    gzSuffix = GZIP_FILE_SUFFIX;
                while ((a.Hash != hash || !local.Exists(a.RelativePath))) {
                    if (remote.Exists(a.RelativePath + gzSuffix)) {
                        retv.Add(a);
                    }
                }
            }
            return retv;
        }

        /// <summary>
        /// Copies files from local/origin accessor to remote/destination accessor
        /// Uploads data from the local to the remote accessor
        /// </summary>
        public async Task MirrorUp(string path = "") {
            if (remote == null) {
                throw new NullReferenceException("The Remote server was not specified, do call SetRemote(IFileAccessor) to specify it.");
            }
            var totalFilesCount = CountFiles(local, path);
            OnReportTotalFilesCount?.Invoke(totalFilesCount);
            await Mirror(local, remote, path, MirrorWay.Up);
        }

        /// <summary>
        /// Copies files remote/destination from accessor to local/origin accessor
        /// Downloads data from the remote to the local accessor
        /// </summary>
        public async Task MirrorDown(string path = "") {
            if (remote == null) {
                throw new NullReferenceException("The Remote server was not specified, do call SetRemote(IFileAccessor) to specify it.");
            }
            if (options.UseHashList) {
                await MirrorFromList(path);
            } else {
                var totalFilesCount = CountFiles(remote, path);
                OnReportTotalFilesCount?.Invoke(totalFilesCount);
                await Mirror(remote, local, path, MirrorWay.Down);
            }
        }

        private int CountFiles(IFileSystem origin, string path) {
            int count = 0;
            origin.ForFilesIn(path, (f) => {
                count++;
            });

            if (options.Recursive) {
                origin.ForDirectoriesIn(path, (dir) => {
                    count += CountFiles(origin, dir);
                });
            }

            return count;
        }

        /// <summary>
        /// The copier triggers this event to notify when it finishes counting files.
        /// </summary>
        public event Action<int> OnReportTotalFilesCount;

        /// <summary>
        /// The copier triggers this event to notify that it finished processing a file.
        /// The arguments are Boolean saying rather the file changed or not and String with the 
        /// relative file path.
        /// </summary>
        public event Action<bool, String> OnReportProcessedFile;
        /// <summary>
        /// The copier triggers this event to notify that it finished processing a file.
        /// The arguments are Boolean saying rather the file changed or not and String with the 
        /// relative file path.
        /// </summary>
        public event Action<string, Exception> OnFileCopyException;

        /// <summary>
        /// <para>
        /// Set to null to use default copy criteria
        /// Default criteria uses SmartCopyOptions to decide rather the file should be copied or not.
        /// </para>
        /// <para>
        /// The copier will run your function to decide if it should or not copy the file, return true and the copier will copy
        /// return false and it will not, the parameter is the relative file path (you can use it with your file accessor
        /// </para>
        /// <para>
        /// With a new revolutionary technology you can define your won copy criteria through a Lambda expression.
        /// </para>
        /// </summary>
        public Func<String, Task<bool>> CopyDecisionCriteria { get; set; }

        public Action<String> OnFileStaged { get; set; }

        private List<FileData> HashList = new List<FileData>();

        private async Task<bool> Changed(IFileSystem o, IFileSystem d, String f) {
            if (options.UseHashList) {
                FileData match;
                lock (HashList)
                    match = HashList.FirstOrDefault(fd => fd.RelativePath == f);
                var hash = await GetHash(o, f);
                if (match != null) {
                    if (hash != match.Hash) {
                        Fi.Tech.WriteLine($"SmartCopy: Hash Changed: {f} ({hash}) ({match.Hash})");
                        var idx = HashList.IndexOf(match);
                        HashList[idx].Date = o.GetLastModified(f)?.Ticks ?? 0;
                        HashList[idx].Length = o.GetSize(f);
                        HashList[idx].Hash = hash;
                        return true;
                    } else {
                        Fi.Tech.WriteLine($"SmartCopy: Hash unchanged: {f} ({hash})");
                        return false;
                    }
                } else {
                    Fi.Tech.WriteLine($"SmartCopy: New File: {f} ({hash})");
                    lock (HashList) {
                        HashList.Add(
                            new FileData {
                                RelativePath = f,
                                Hash = hash,
                                Date = o.GetLastModified(f)?.Ticks ?? 0,
                                Length = o.GetSize(f)
                            });
                    }
                    return true;
                }
            }

            bool changed = false;
            var destLen = d.GetSize(f);
            var oriLen = o.GetSize(f);

            if (oriLen != destLen) {
                return true;
            } else
            if (options.UseHash) {
                var originHash = GetHash(o, f);
                var destinationHash = GetHash(d, f);
                if (originHash == destinationHash) {
                    return true;
                } else {
                    return false;
                }
            } else {
                var originDate = o.GetLastModified(f);
                var destinationDate = d.GetLastModified(f);
                changed = (
                    (originDate > destinationDate) ||
                    (
                        (originDate < destinationDate && !options.IgnoreOlder)
                    )
                );
                return changed;
            }
        }

        private async Task MirrorFromList(string path) {
            HashList = await SmartCopy.GetHashList(remote, true);
            int numWorkers = options.Multithreaded ? options.NumWorkers : 1;
            var wq = new WorkQueuer("SmartCopy_Operation", numWorkers, false);
            var bufferSize = (int)options.BufferSize / options.NumWorkers;
            if (bufferSize < 0) bufferSize = 256 * 1024;
            List<FileData> workingList = HashList.Where(f => f.RelativePath.StartsWith(path)).ToList();
            workingList = workingList.GroupBy(f => f.RelativePath).Select(g => g.First()).ToList();
            workingList.RemoveAll(f => Excludes.Any(excl=> CheckMatch(f.RelativePath, excl)));
            OnReportTotalFilesCount?.Invoke(workingList.Count);
            foreach (var a in HashList) {
                wq.Enqueue(async () => {
                    await Task.Yield();
                    if (a.RelativePath == HASHLIST_FILENAME || Excludes.Any(excl => CheckMatch(a.RelativePath, excl))) {
                        return;
                    }
                    string hash = "";
                    if (local.Exists(a.RelativePath)) {
                        await local.Read(a.RelativePath, async (stream) => {
                            await Task.Yield();
                            hash = await GetHash(stream);
                        });
                    }

                    var processed = false;

                    var gzSuffix = "";
                    if (options.UseGZip)
                        gzSuffix = GZIP_FILE_SUFFIX;
                    int maxTries = 10;
                    while ((a.Hash != hash || !local.Exists(a.RelativePath)) && maxTries-- > 0) {
                        processed = true;
                        if (remote.Exists(a.RelativePath + gzSuffix)) {
                            await remote.Read(a.RelativePath + gzSuffix, async (downStream) => {
                                await Task.Yield();
                                //local.Delete(a.RelativePath);
                                await local.Write(a.RelativePath + "_$ft_new", async (fileStream) => {
                                    await Task.Yield();
                                    if (options.UseGZip)
                                        downStream = new GZipStream(downStream, CompressionMode.Decompress);

                                    await downStream.CopyToAsync(fileStream, bufferSize);
                                });
                                var oldTempName = a.RelativePath + "_$ft_old-"+IntEx.GenerateShortRid();
                                if (local.Exists(a.RelativePath)) {
                                    local.Rename(a.RelativePath, oldTempName);
                                }
                                local.Rename(a.RelativePath + "_$ft_new", a.RelativePath);
                                if (local.Exists(a.RelativePath)) {
                                    await local.Read(a.RelativePath, async (stream) => {
                                        await Task.Yield();
                                        hash = await GetHash(stream);
                                    });
                                }
                                try {
                                    if(local.Exists(oldTempName))
                                        local.Delete(oldTempName);
                                } catch(Exception x) {
                                    local.Hide(oldTempName);
                                }
                                if (a.RelativePath.EndsWith(".dll")) {

                                }
                            });
                        }

                        await local.Read(a.RelativePath, async (stream) => {
                            await Task.Yield();
                            hash = await GetHash(stream);
                        });
                        if (a.Hash != hash) {
                            Console.Error.Write($"Hash Mismatch: {a.RelativePath}{a.Hash}/{hash}");
                        }
                    }
                    OnReportProcessedFile?.Invoke(processed, a.RelativePath);

                }, async (ex) => {
                    await Task.Yield();
                    OnFileCopyException?.Invoke(a.RelativePath, ex);
                });
            }
            
            wq.Start();
            await wq.Stop(true);

            DeleteExtras(local, path, workingList, true);
        }

        public void DeleteExtras(IFileSystem fs, string path, List<FileData> workinglist, bool recursive) {
            if(!options.AllowDelete) {
                return;
            }
            Func<FileData, string, bool> cmpFn = (wl,file) => wl.RelativePath == file;
            if (!fs.IsCaseSensitive) {
                cmpFn = (wl, file) => wl.RelativePath?.ToLower() == file?.ToLower();
            }
            fs.ForFilesIn(path, file => {
                if (Excludes.Any(x=> CheckMatch(file, x))) {
                    return;
                }
                if (!workinglist.Any(wl=> cmpFn(wl, file))) {
                    try {
                        fs.Delete(file);
                    }
                    catch (Exception x) {

                    }
                }
            });

            if(recursive) {
                fs.ForDirectoriesIn(path, dir => DeleteExtras(fs, dir, workinglist, recursive));
            }
        }
        
        int workedFiles = 0;

        private const string GZIP_FILE_SUFFIX = ".gz";

        private enum MirrorWay {
            Up,
            Down
        }

        const string HASHLIST_FILENAME = ".hashlist.json";

        public static async Task<List<FileData>> GetHashList(IFileSystem destination, bool ThrowOnError) {
            try {
                string txt = await Task.Run(()=> destination.ReadAllText(HASHLIST_FILENAME));
                var hashList = JsonConvert.DeserializeObject<List<FileData>>(txt);
                hashList = hashList.GroupBy(a => a.RelativePath).Select(a => a.First()).ToList();
                return hashList;
            } catch (Exception x) {
                if(ThrowOnError) {
                    throw x;
                }
                return new List<FileData>();
            }
        }

        private async Task<List<CopyMirrorFileData>> EnumerateFilesForMirroring(IFileSystem origin, IFileSystem destination, string path, MirrorWay way) {
            List<CopyMirrorFileData> retv = new List<CopyMirrorFileData>();
            foreach(var f in origin.GetFilesIn(path)) {
                if (f == HASHLIST_FILENAME) {
                    continue;
                }
                if (Excludes.Any(excl => CheckMatch(f, excl))) {
                    continue;
                }

                try {
                    OnFileStaged?.Invoke(f);
                } catch (Exception x) {
                    continue;
                }

                var changed = CopyDecisionCriteria != null ?
                    await CopyDecisionCriteria(f) : 
                    await Changed(origin, destination, f);

                retv.Add(new CopyMirrorFileData { RelativePath = f, Changed = changed });
            }
            if (options.Recursive) {
                foreach (var dir in origin.GetDirectoriesIn(path)) {
                    destination.MkDirs(dir);
                    retv.AddRange(await EnumerateFilesForMirroring(origin, destination, dir, way));
                }
            }

            return retv;
        }

        private async Task Mirror(IFileSystem origin, IFileSystem destination, string path, MirrorWay way) {
            int numWorkers = options.Multithreaded ? options.NumWorkers : 1;
            var wq = new WorkQueuer("SmartCopy_Operation", numWorkers, false);

            if (options.UseHashList) {
                HashList = await SmartCopy.GetHashList(destination, way == MirrorWay.Down);
            }

            workedFiles = 0;
            var files = await EnumerateFilesForMirroring(origin, destination, path, way);
            foreach(var f in files) {
                wq.Enqueue(async () => {
                    
                    await Task.Yield();
                    OnFileStaged?.Invoke(f.RelativePath);
                    if(f.Changed) {
                        switch(way) {
                            case MirrorWay.Up:
                                await processFileUp(origin, destination, f.RelativePath);
                                break;
                            case MirrorWay.Down:
                                await processFileDown(origin, destination, f.RelativePath);
                                break;
                        }
                    }
                    OnReportProcessedFile?.Invoke(f.Changed, f.RelativePath);
                }, async x => {
                    await Task.Yield();
                    OnFileCopyException?.Invoke(f.RelativePath, x);
                });
            }

            if (options.AllowDelete) {
                int DeleteLimit = 15;
                destination.ForFilesIn(path, (f) => {
                    if (DeleteLimit < 1) return;
                    if (Excludes.Any(x => CheckMatch(f, x))) {
                        return;
                    }
                    if (!origin.Exists(f)) {
                        if (DeleteLimit-- > 0) {
                            destination.Delete(f);
                            return;
                        }
                    }
                });
            }

            wq.Start();
            await wq.Stop(true);
            if(Debugger.IsAttached) {
                Debugger.Break();
            }
            await SaveHashList(origin, destination);

        }

        private async Task SaveHashList(IFileSystem origin, IFileSystem destination) {
            if (options.UseHashList) {
                HashList.RemoveAll((f) =>
                    !origin.Exists(f?.RelativePath)
                );
                Console.WriteLine("Saving HashList...");
                if (HashList.Count > 0) {
                    destination.Delete(HASHLIST_FILENAME);
                    await destination.Write(HASHLIST_FILENAME, async (stream) => {
                        await Task.Yield();
                        string text = JsonConvert.SerializeObject(HashList);
                        byte[] writev = Fi.StandardEncoding.GetBytes(text);
                        await stream.WriteAsync(writev, 0, writev.Length);
                    });

                    origin.Delete(HASHLIST_FILENAME);
                    await origin.Write(HASHLIST_FILENAME, async (stream) => {
                        await Task.Yield();
                        string text = JsonConvert.SerializeObject(HashList);
                        byte[] writev = Fi.StandardEncoding.GetBytes(text);
                        await stream.WriteAsync(writev, 0, writev.Length);
                    });
                }
            }
        }

        private String ProcessPath(string path) {
            var l1 = path.Split('\\');
            var mid = Path.Combine(l1);
            var l2 = path.Split('/');
            return Path.Combine(l2);
        }

        private async Task processFileUp(IFileSystem origin, IFileSystem destination, string workingFile) {
            workingFile = ProcessPath(workingFile);

            var outPostFix = "";
            if (options.UseGZip)
                outPostFix = GZIP_FILE_SUFFIX;
            int tries = 0;
            while(tries < options.RetriesPerFile) {
                try {
                    await origin.Read(workingFile, async (input) => {
                        await Task.Yield();
                        var bufferSize = (int)options.BufferSize / options.NumWorkers;
                        if (bufferSize <= 0) bufferSize = Int32.MaxValue;


                        await destination.Write(workingFile + outPostFix, async (output) => {
                            await Task.Yield();
                            if (options.UseGZip) {
                                using (var realOut = new GZipStream(output, CompressionLevel.Optimal)) {
                                    await input.CopyToAsync(realOut);
                                }
                            } else {
                                await input.CopyToAsync(output);
                            }

                        });
                    });
                    break;
                } catch(Exception) {
                    await Task.Delay(options.RetryTimeout);
                    continue;
                }
            }

            if (!this.options.UseHashList) {
                var originHash = await GetHash(origin, workingFile);
                var destHash = await GetHash(destination, workingFile);
                if (originHash != destHash) {
                    Console.Error.Write($"Hash Mismatch: {workingFile}{originHash}/{destHash}");
                    await processFileUp(origin, destination, workingFile);
                }
            }
        }

        private async Task processFileDown(IFileSystem origin, IFileSystem destination, string workingFile) {
            workingFile = ProcessPath(workingFile);

            var outPostFix = "";
            if (options.UseGZip)
                outPostFix = GZIP_FILE_SUFFIX;

            int tries = 0;
            while(tries < options.RetriesPerFile) {
                try {
                    await origin.Read(workingFile + outPostFix, async (input) => {
                        await Task.Yield();
                        var bufferSize = (int)options.BufferSize / options.NumWorkers;
                        if (bufferSize < 0) bufferSize = Int32.MaxValue;

                        await destination.Write(workingFile, async (output) => {
                            await Task.Yield();
                            if (options.UseGZip) {
                                using (var gzipOut = new GZipStream(input, CompressionMode.Decompress)) {
                                    await gzipOut.CopyToAsync(output);
                                }
                            } else {
                                await input.CopyToAsync(output, bufferSize);
                            }
                        });

                    });
                    break;
                } catch (Exception) {
                    await Task.Delay(options.RetryTimeout);
                    continue;
                }
            }

        }

    }

}

