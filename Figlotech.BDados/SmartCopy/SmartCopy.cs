using Figlotech.BDados.Entity;
using Figlotech.BDados.Interfaces;
using System;
using System.IO;
using System.Security.Cryptography;

namespace Figlotech.BDados.SmartCopy {
    /// <summary>
    /// This is meant to provide easy in-program "robocopy" utility.
    /// </summary>
    public class SmartCopy
    {
        IFileAccessor local;
        IFileAccessor remote;
        ISmartCopyOptions options;

        /// <summary>
        /// Provides easy in-program robust copy utility
        /// </summary>
        /// <param name="localAccessor">"Local" or "Origin" accessor</param>
        public SmartCopy(IFileAccessor localAccessor, ISmartCopyOptions copyOptions)
        {
            local = localAccessor;
            options = copyOptions;
        }

        /// <summary>
        /// Sets the "other" file acessor to work with;
        /// </summary>
        /// <param name="localAccessor">"Remote" or "Destination" accessor</param>
        public void SetRemote(IFileAccessor remoteAccessor) {
            remote = remoteAccessor;
        }

        private static string GetHash(Stream stream) {
            using (var md5 = MD5.Create()) {
                return Convert.ToBase64String(md5.ComputeHash(stream));
            }
        }

        private String GetHash(IFileAccessor fa, String path) {
            string hash = "";
            fa.Read(path, (stream) => {
                hash = GetHash(stream);
            });
            
            return hash;
        }

        /// <summary>
        /// Copies files from local/origin accessor to remote/destination accessor
        /// Uploads data from the local to the remote accessor
        /// </summary>
        public void mirrorUp(string path) {
            mirror(local, remote, path);
        }

        /// <summary>
        /// Copies files remote/destination from accessor to local/origin accessor
        /// Downloads data from the remote to the local accessor
        /// </summary>
        public void mirrorDown(string path) {
            mirror(remote, local, path);
        }

        private void mirror(IFileAccessor origin, IFileAccessor destination, string path)
        {
            int numWorkers = options.Multithreaded ? options.NumWorkers : 1;
            WorkQueuer wq = new WorkQueuer("SmartCopy_Operation", numWorkers, true);
            origin.ForFilesIn(path, (f) => {
                wq.Enqueue(() => {
                    bool changed = false;
                    var destLen = destination.GetSize(f);
                    var oriLen = origin.GetSize(f);
                    if (oriLen != destLen) {
                        changed = true;
                    } else
                    if (options.Usehash) {
                        var originHash = GetHash(origin, path);
                        var destinationHash = GetHash(destination, path);
                        changed = originHash == destinationHash;
                    } else {
                        var originDate = origin.GetLastFileWrite(f);
                        var destinationDate = destination.GetLastFileWrite(f);
                        changed =
                            (
                                (originDate > destinationDate) ||
                                (
                                    (originDate < destinationDate && !options.IgnoreOlder)
                                )
                            );
                    }

                    if (changed)
                        processFile(origin, destination, f);
                });
            });
            
            if(options.AllowDelete) {
                destination.ForFilesIn(path, (f) => {
                    if (!origin.Exists(f)) {
                        destination.Delete(f);
                    }
                });
            }

            wq.Stop();

            if (options.Recursive) {
                origin.ForDirectoriesIn(path, (dir) => {
                    mirror(origin, destination, dir);
                });
            }
        }

        private String ProcessPath(string path) {
            var l1 = path.Split('\\');
            var mid = Path.Combine(l1);
            var l2 = path.Split('/');
            return Path.Combine(l2);
        }

        private void processFile(IFileAccessor origin, IFileAccessor destination, string workingFile) {
            workingFile = ProcessPath(workingFile);
            // DRAGONS?
            // this is to avoid same file from being written by 2 or more threads
            // I SHOULD NEVER HAPPEN naturally. But Idk, extra.
            lock (workingFile) {
                origin.Read(workingFile, (input) => {
                    destination.Write(workingFile, (output) => {
                        input.CopyTo(output);
                    });
                });
            }
        }

    }
}
