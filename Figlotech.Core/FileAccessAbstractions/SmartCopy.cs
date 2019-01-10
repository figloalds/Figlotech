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

namespace Figlotech.Core.FileAcessAbstractions {
    public class FileData {
        public string RelativePath;
        public long Date;
        public long Length;
        public string Hash;
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

        private string WildcardToRegex(String input) {
            input = input.Replace("*", "____WCASTER____");
            input = input.Replace("%", "____WCPCT____");
            input = input.Replace("?", "____WCQUEST____");
            var escaped = Regex.Escape(input);
            escaped = escaped.Replace("____WCASTER____", "[.]{0,}");
            escaped = escaped.Replace("____WCPCT____", "[.]{0,1}");
            escaped = escaped.Replace("____WCPCT____", "[.]{1}");
            return escaped;
        }

        private bool CheckMatch(string file, string criteria) {
            var regex = WildcardToRegex(criteria);
            return Regex.Match(file, regex, RegexOptions.IgnoreCase).Success;
        }

        /// <summary>
        /// Sets the "other" file acessor to work with;
        /// </summary>
        /// <param name="localAccessor">"Remote" or "Destination" accessor</param>
        public void SetRemote(IFileSystem remoteAccessor) {
            remote = remoteAccessor;
        }

        public static string GetHash(Stream stream) {
            using (var md5 = MD5.Create()) {
                return Convert.ToBase64String(md5.ComputeHash(stream));
            }
        }

        public static String GetHash(IFileSystem fa, String path) {
            string hash = "";
            fa.Read(path, (stream) => {
                hash = GetHash(stream);
            });

            return hash;
        }

        public IEnumerable<FileData> EnumerateFilesToDownload(string path = "") {
            if (remote == null) {
                throw new NullReferenceException("The Remote server was not specified, do call SetRemote(IFileAccessor) to specify it.");
            }
            if (options.UseHashList) {
                return EnumerateDownloadableFilesFromHashList(path);
            } else {
                return EnumerateDownloadableFiles(remote, local, path);
            }
        }

        private IEnumerable<FileData> EnumerateDownloadableFiles(IFileSystem origin, IFileSystem destination, string path = "", bool isRecursing = false) {

            if (!isRecursing && options.UseHashList) {
                HashList = SmartCopy.GetHashList(this.remote, true);
            }

            workedFiles = 0;
            var adds = new Queue<FileData>();
            origin.ForFilesIn(path, (f) => {
                if (f == HASHLIST_FILENAME) {
                    return;
                }
                if (Excludes.Any(excl => CheckMatch(f, excl))) {
                    return;
                }

                var changed = CopyDecisionCriteria != null ?
                    CopyDecisionCriteria(f) :
                    Changed(origin, destination, f);

                if (changed) {
                    adds.Enqueue(new FileData {
                        RelativePath = f,
                        Date = origin.GetLastModified(f)?.Ticks ?? 0,
                        Hash = "", // GetHash(origin, f),
                        Length = origin.GetSize(f)
                    });
                }
            });

            if (options.Recursive) {
                origin.ForDirectoriesIn(path, (dir) => {
                    destination.MkDirs(dir);
                    var newAdds = EnumerateDownloadableFiles(origin, destination, dir);
                    foreach(var a in newAdds) {
                        adds.Enqueue(a);
                    }
                });
            }

            while (adds.Count > 0) {
                yield return adds.Dequeue();
            }

            if (!isRecursing) {

            }
        }

        private IEnumerable<FileData> EnumerateDownloadableFilesFromHashList(string path) {
            HashList = SmartCopy.GetHashList(remote, true);
            List<FileData> workingList = HashList.Where(f => f.RelativePath.StartsWith(path)).ToList();
            OnReportTotalFilesCount?.Invoke(workingList.Count);
            var retv = new List<FileData>();
            foreach (var a in HashList) {
                string hash = "";
                if (local.Exists(a.RelativePath)) {
                    local.Read(a.RelativePath, (stream) => {
                        hash = GetHash(stream);
                    });
                }

                var gzSuffix = "";
                if (options.UseGZip)
                    gzSuffix = GZIP_FILE_SUFFIX;
                while ((a.Hash != hash || !local.Exists(a.RelativePath))) {
                    if (remote.Exists(a.RelativePath + gzSuffix)) {
                        yield return a;
                    }
                }
            }
        }

        /// <summary>
        /// Copies files from local/origin accessor to remote/destination accessor
        /// Uploads data from the local to the remote accessor
        /// </summary>
        public void MirrorUp(string path = "") {
            if (remote == null) {
                throw new NullReferenceException("The Remote server was not specified, do call SetRemote(IFileAccessor) to specify it.");
            }
            var totalFilesCount = CountFiles(local, path);
            OnReportTotalFilesCount?.Invoke(totalFilesCount);
            Mirror(local, remote, path, MirrorWay.Up);
        }

        /// <summary>
        /// Copies files remote/destination from accessor to local/origin accessor
        /// Downloads data from the remote to the local accessor
        /// </summary>
        public void MirrorDown(string path = "") {
            if (remote == null) {
                throw new NullReferenceException("The Remote server was not specified, do call SetRemote(IFileAccessor) to specify it.");
            }
            if (options.UseHashList) {
                MirrorFromList(path);
            } else {
                var totalFilesCount = CountFiles(remote, path);
                OnReportTotalFilesCount?.Invoke(totalFilesCount);
                Mirror(remote, local, path, MirrorWay.Down);
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
        public Func<String, bool> CopyDecisionCriteria { get; set; }

        public Action<String> OnFileStaged { get; set; }

        private List<FileData> HashList = new List<FileData>();

        private bool Changed(IFileSystem o, IFileSystem d, String f) {
            if (options.UseHashList) {
                FileData match;
                lock (HashList)
                    match = HashList.FirstOrDefault(fd => fd.RelativePath == f);
                var hash = GetHash(o, f);
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

        private void MirrorFromList(string path) {
            HashList = SmartCopy.GetHashList(remote, true);
            int numWorkers = options.Multithreaded ? options.NumWorkers : 1;
            var wq = new WorkQueuer("SmartCopy_Operation", numWorkers, false);
            var bufferSize = (int)options.BufferSize / options.NumWorkers;
            if (bufferSize < 0) bufferSize = 256 * 1024;
            List<FileData> workingList = HashList.Where(f => f.RelativePath.StartsWith(path)).ToList();
            workingList = workingList.GroupBy(f => f.RelativePath).Select(g => g.First()).ToList();
            workingList.RemoveAll(f => Excludes.Any(excl=> CheckMatch(f.RelativePath, excl)));
            OnReportTotalFilesCount?.Invoke(workingList.Count);
            foreach (var a in HashList) {
                wq.Enqueue(() => {
                    if (a.RelativePath == HASHLIST_FILENAME || Excludes.Any(excl => CheckMatch(a.RelativePath, excl))) {
                        return;
                    }
                    string hash = "";
                    if (local.Exists(a.RelativePath)) {
                        local.Read(a.RelativePath, (stream) => {
                            hash = GetHash(stream);
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
                            remote.Read(a.RelativePath + gzSuffix, (downStream) => {
                                //local.Delete(a.RelativePath);
                                local.Write(a.RelativePath + "_$ft_new", (fileStream) => {
                                    if (options.UseGZip)
                                        downStream = new GZipStream(downStream, CompressionMode.Decompress);

                                    downStream.CopyTo(fileStream, bufferSize);
                                });
                                var oldTempName = a.RelativePath + "_$ft_old-"+IntEx.GenerateShortRid();
                                if (local.Exists(a.RelativePath)) {
                                    local.Rename(a.RelativePath, oldTempName);
                                }
                                local.Rename(a.RelativePath + "_$ft_new", a.RelativePath);
                                if (local.Exists(a.RelativePath)) {
                                    local.Read(a.RelativePath, (stream) => {
                                        hash = GetHash(stream);
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

                        local.Read(a.RelativePath, (stream) => {
                            hash = GetHash(stream);
                        });
                        if (a.Hash != hash) {
                            Console.Error.Write($"Hash Mismatch: {a.RelativePath}{a.Hash}/{hash}");
                        }
                    }
                    OnReportProcessedFile?.Invoke(processed, a.RelativePath);

                }, (ex) => {
                    OnFileCopyException?.Invoke(a.RelativePath, ex);
                }, () => {

                });
            }
            
            wq.Start();
            wq.Stop();

            DeleteExtras(local, path, workingList, true);
        }

        public void DeleteExtras(IFileSystem fs, string path, List<FileData> workinglist, bool recursive) {
            Func<FileData, string, bool> cmpFn = (wl,file) => wl.RelativePath == file;
            if (!fs.IsCaseSensitive) {
                cmpFn = (wl, file) => wl.RelativePath?.ToLower() == file?.ToLower();
            }
            fs.ForFilesIn(path, file => {
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

        public static List<FileData> GetHashList(IFileSystem destination, bool ThrowOnError) {
            try {
                var txt = destination.ReadAllText(HASHLIST_FILENAME);
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

        private void Mirror(IFileSystem origin, IFileSystem destination, string path, MirrorWay way, WorkQueuer wq = null) {
            bool isRecursing = wq != null;
            if (wq == null) {
                int numWorkers = options.Multithreaded ? options.NumWorkers : 1;
                wq = new WorkQueuer("SmartCopy_Operation", numWorkers, false);
            }

            if (!isRecursing && options.UseHashList) {
                HashList = SmartCopy.GetHashList(destination, way == MirrorWay.Down);
            }

            workedFiles = 0;
            origin.ForFilesIn(path, (f) => {
                if (f == HASHLIST_FILENAME) {
                    return;
                }
                if (Excludes.Any(excl => CheckMatch(f, excl))) {
                    return;
                }

                wq.Enqueue(() => {
                    try {
                        OnFileStaged?.Invoke(f);
                    } catch (Exception x) {
                        return;
                    }

                    var changed = CopyDecisionCriteria != null ?
                        CopyDecisionCriteria(f) :
                        Changed(origin, destination, f);

                    if (changed) {
                        lock (f) {
                            if (way == MirrorWay.Up) {
                                processFileUp(origin, destination, f);
                            }
                            if (way == MirrorWay.Down) {
                                processFileDown(origin, destination, f);
                            }
                        }
                    }
                    OnReportProcessedFile?.Invoke(changed, f);
                }, (x) => {
                    OnFileCopyException?.Invoke(f, x);
                    //Console.WriteLine(x.Message);
                }, () => {
                });
            });

            int DeleteLimit = 15;
            if (options.AllowDelete) {
                destination.ForFilesIn(path, (f) => {
                    if (DeleteLimit < 1) return;
                    if (!origin.Exists(f)) {
                        if (DeleteLimit-- > 0) {
                            destination.Delete(f);
                            return;
                        }
                    }
                });
            }

            if (options.Recursive) {
                origin.ForDirectoriesIn(path, (dir) => {
                    destination.MkDirs(dir);
                    Mirror(origin, destination, dir, way, wq);
                });
            }

            if (!isRecursing) {

                wq.Start();
                wq.Stop();

                if (options.UseHashList) {
                    HashList.RemoveAll((f) =>
                        !origin.Exists(f?.RelativePath)
                        );

                    if (HashList.Count > 0) {
                        destination.Delete(HASHLIST_FILENAME);
                        destination.Write(HASHLIST_FILENAME, (stream) => {
                            string text = JsonConvert.SerializeObject(HashList);
                            byte[] writev = Encoding.UTF8.GetBytes(text);
                            stream.Write(writev, 0, writev.Length);
                        });

                        origin.Delete(HASHLIST_FILENAME);
                        origin.Write(HASHLIST_FILENAME, (stream) => {
                            string text = JsonConvert.SerializeObject(HashList);
                            byte[] writev = Encoding.UTF8.GetBytes(text);
                            stream.Write(writev, 0, writev.Length);
                        });
                    }
                }
            }

        }

        private String ProcessPath(string path) {
            var l1 = path.Split('\\');
            var mid = Path.Combine(l1);
            var l2 = path.Split('/');
            return Path.Combine(l2);
        }

        private void processFileUp(IFileSystem origin, IFileSystem destination, string workingFile) {
            workingFile = ProcessPath(workingFile);

            var outPostFix = "";
            if (options.UseGZip)
                outPostFix = GZIP_FILE_SUFFIX;

            origin.Read(workingFile, (input) => {
                var bufferSize = (int)options.BufferSize / options.NumWorkers;
                if (bufferSize <= 0) bufferSize = Int32.MaxValue;

                destination.Write(workingFile + outPostFix, (output) => {
                    //BatchStreamProcessor processor = new BatchStreamProcessor();
                    //if (options.UseGZip) {
                    //    processor.Add(new GzipCompressStreamProcessor(true));
                    //}

                    //processor.Process(input, (stream) => stream.CopyTo(output));

                    if (options.UseGZip) {
                        using (var realOut = new GZipStream(output, CompressionLevel.Optimal)) {
                            input.CopyTo(realOut);
                        }
                    } else {
                        input.CopyTo(output);
                    }

                });

            });

            var originHash = GetHash(origin, workingFile);
            var destHash = GetHash(origin, workingFile);
            if (originHash != destHash) {
                Console.Error.Write($"Hash Mismatch: {workingFile}{originHash}/{destHash}");
                processFileUp(origin, destination, workingFile);
            }
        }

        private void processFileDown(IFileSystem origin, IFileSystem destination, string workingFile) {
            workingFile = ProcessPath(workingFile);

            var outPostFix = "";
            if (options.UseGZip)
                outPostFix = GZIP_FILE_SUFFIX;

            origin.Read(workingFile + outPostFix, (input) => {
                var bufferSize = (int)options.BufferSize / options.NumWorkers;
                if (bufferSize < 0) bufferSize = Int32.MaxValue;

                destination.Write(workingFile, (output) => {
                    if (options.UseGZip) {
                        using (var gzipOut = new GZipStream(input, CompressionMode.Decompress)) {
                            gzipOut.CopyTo(output);
                        }
                    } else {
                        input.CopyTo(output, bufferSize);
                    }
                });

            });

        }

    }

}

