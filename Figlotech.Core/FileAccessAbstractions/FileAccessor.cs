using Figlotech.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.Core.FileAcessAbstractions {
    public class FileAccessor : IFileSystem {

        private static int gid = 0;
        private static int myid = ++gid;
        private static readonly char S = Path.DirectorySeparatorChar;

        public bool UseCriptography;

        public String RootDirectory { get; set; }
        public int MaxStreamBufferLength { get; set; } = 64 * 1024 * 1024;

        public String FixRel(ref string relative) {
            return relative = relative.Replace('\\', '/')
                .Replace("//", "/")
                .Replace('/', S);
        }
        public String UnFixRel(ref string relative) {
            return relative = relative.Replace(S, '/')
                .Replace($"{S}", "/");
        }

        public FileAccessor(String workingPath) {
            RootDirectory = Path.GetFullPath(workingPath);

            FixRel(ref workingPath);
            try {
                if (!Directory.Exists(RootDirectory)) {
                    absMkDirs(RootDirectory);
                }
            } catch (Exception x) {
                throw x;
            }
        }

        private void absMkDirs(string dir) {
            FixRel(ref dir);
            try {
                if (!Directory.Exists(Path.GetDirectoryName(dir))) {
                    absMkDirs(Path.GetDirectoryName(dir));
                }
            } catch (Exception) { }
            try {
                Directory.CreateDirectory(dir);
            } catch (Exception x) {
                //Fi.Tech.WriteLine(x.Message);
            }
        }

        public void MkDirs(string dir) {
            FixRel(ref dir);
            absMkDirs(Path.Combine(RootDirectory, dir));
        }

        public void Write(String relative, Action<Stream> func) {
            FixRel(ref relative);
            var WorkingDirectory = Path.Combine(RootDirectory, relative);
            // Metodo 1: Convencional File System:
            if (!Directory.Exists(Path.GetDirectoryName(WorkingDirectory))) {
                absMkDirs(Path.GetDirectoryName(WorkingDirectory));
            }
            LockRegion(WorkingDirectory, () => {
                if (!File.Exists(WorkingDirectory)) {
                    using (var f = File.Create(WorkingDirectory)) { }
                }
                using (Stream fs = Open(relative, FileMode.Truncate, FileAccess.Write)) {
                    func(fs);
                }
            });
        }

        private void RenDir(string relative, string newName) {
            FixRel(ref relative);
            foreach (var f in Directory.GetFiles(relative)) {
                LockRegion(f, () => {
                    var fname = f;
                    var fnew = f.Replace(relative, newName);
                    if (File.Exists(fnew))
                        File.Delete(fnew);
                    File.Move(fname, fnew);
                });
            }
            foreach (var f in Directory.GetDirectories(relative)) {
                LockRegion(f, () => {
                    RenDir(f, f.Replace(relative, newName));
                });
            }

            Directory.Delete(relative);
        }

        public string MapPathTo(string relative) {
            FixRel(ref relative);
            return Path.Combine(RootDirectory, relative);
        }

        public void Rename(string relative, string newName) {
            FixRel(ref relative);
            FileAttributes attr = File.GetAttributes(Path.Combine(RootDirectory, relative));
            var WorkingDirectory = Path.Combine(RootDirectory, relative);
            LockRegion(WorkingDirectory, () => {
                //detect whether its a directory or file
                if ((attr & FileAttributes.Directory) == FileAttributes.Directory) {
                    if (Directory.Exists(Path.Combine(RootDirectory, relative))) {
                        if (!Directory.Exists(Path.GetDirectoryName(Path.Combine(RootDirectory, newName))))
                            absMkDirs(Path.GetDirectoryName(Path.Combine(RootDirectory, newName)));
                        if (Directory.Exists(Path.Combine(RootDirectory, newName))) {
                            RenDir(Path.Combine(RootDirectory, relative), Path.Combine(RootDirectory, newName));
                        } else {
                            Directory.Move(Path.Combine(RootDirectory, relative), Path.Combine(RootDirectory, newName));
                        }
                    }
                } else {
                    if (File.Exists(Path.Combine(RootDirectory, relative))) {
                        if (!Directory.Exists(Path.GetDirectoryName(Path.Combine(RootDirectory, newName))))
                            absMkDirs(Path.GetDirectoryName(Path.Combine(RootDirectory, newName)));
                        if (File.Exists(Path.Combine(RootDirectory, newName))) {
                            File.Delete(Path.Combine(RootDirectory, newName));
                        }
                        File.Move(Path.Combine(RootDirectory, relative), Path.Combine(RootDirectory, newName));
                    }
                }
            });
        }

        public DateTime? GetLastModified(string relative) {
            FixRel(ref relative);
            if (Exists(relative)) {
                return new FileInfo(Path.Combine(RootDirectory, relative)).LastWriteTimeUtc;
            }
            return DateTime.MinValue;
        }
        public DateTime? GetLastAccess(string relative) {
            FixRel(ref relative);
            if (Exists(relative)) {
                return new FileInfo(Path.Combine(RootDirectory, relative)).LastAccessTimeUtc;
            }
            return DateTime.MinValue;
        }

        public long GetSize(string relative) {
            FixRel(ref relative);
            var WorkingDirectory = Path.Combine(RootDirectory, relative);
            if (File.Exists(WorkingDirectory)) {
                return new FileInfo(WorkingDirectory).Length;
            }
            return 0;
        }

        public void ParallelForFilesIn(String relative, Action<String> execFunc) {
            FixRel(ref relative);
            var WorkingDirectory = Path.Combine(RootDirectory, relative);
            if (!Directory.Exists(WorkingDirectory)) {
                return;
            }
            Parallel.ForEach(Directory.GetFiles(WorkingDirectory), (Action<string>)((s) => {
                s = s.Substring(RootDirectory.Length);
                if (s.StartsWith("\\")) {
                    s = s.Substring(1);
                }
                s = this.UnFixRel(ref s);
                execFunc(s);
            }));
        }

        public void ForFilesIn(String relative, Action<String> execFunc, Action<String, Exception> handler = null) {
            FixRel(ref relative);
            var WorkingDirectory = Path.Combine(RootDirectory, relative);
            if (!Directory.Exists(WorkingDirectory)) {
                return;
            }
            List<Exception> ex = new List<Exception>();
            foreach (var s1 in Directory.GetFiles(WorkingDirectory)) {
                String s = s1;
                try {
                    s = s1.Substring(RootDirectory.Length);
                    if (s.StartsWith("\\")) {
                        s = s.Substring(1);
                    }
                    s = this.UnFixRel(ref s);
                    execFunc?.Invoke(s);
                } catch (Exception x) {
                    if (handler != null) {
                        handler?.Invoke(s, x);
                    } else {
                        ex.Add(new Exception("Error manipulating {s}", x));
                    }
                }
                if (handler == null && ex.Any()) {
                    throw new AggregateException(ex);
                }
            }
        }
        public IEnumerable<string> GetFilesIn(String relative) {
            relative = relative.Replace('/', '\\');
            var WorkingDirectory = Path.Combine(RootDirectory, relative);
            if (!Directory.Exists(WorkingDirectory)) {
                return new string[0];
            }
            return Directory.GetFiles(WorkingDirectory).Select(
                    ((a) => {
                        var s = a.Substring(RootDirectory.Length);
                        if (s.StartsWith("\\")) {
                            s = s.Substring(1);
                        }
                        return this.UnFixRel(ref s);
                    }));
        }

        public void ParallelForDirectoriesIn(String relative, Action<String> execFunc) {
            FixRel(ref relative);
            var WorkingDirectory = Path.Combine(RootDirectory, relative);
            if (!Directory.Exists(WorkingDirectory)) {
                return;
            }
            Parallel.ForEach(Directory.GetDirectories(WorkingDirectory), (Action<string>)((s) => {
                s = s.Substring(RootDirectory.Length);
                if (s.StartsWith("\\")) {
                    s = s.Substring(1);
                }
                s = this.UnFixRel(ref s);
                execFunc(s);
            }));
        }
        public void ForDirectoriesIn(String relative, Action<String> execFunc) {
            FixRel(ref relative);
            var WorkingDirectory = Path.Combine(RootDirectory, relative);
            if (!Directory.Exists(WorkingDirectory)) {
                return;
            }
            foreach (var s1 in Directory.GetDirectories(WorkingDirectory)) {
                var s = s1.Substring(RootDirectory.Length);
                if (s.StartsWith("\\")) {
                    s = s.Substring(1);
                }
                s = this.UnFixRel(ref s);
                execFunc(s);
            }
        }
        public IEnumerable<string> GetDirectoriesIn(String relative) {
            relative = relative.Replace('/', '\\');
            var WorkingDirectory = Path.Combine(RootDirectory, relative);
            if (!Directory.Exists(WorkingDirectory)) {
                return new string[0];
            }
            return Directory.GetDirectories(WorkingDirectory)
                .Select((Func<string, string>)((a) => {
                    a = a.Replace(RootDirectory, "");
                    return this.FixRel(ref a);
                }));
        }

        public bool Read(String relative, Action<Stream> func) {
            FixRel(ref relative);
            if (!Exists(relative)) {
                return false;
            }
            var WorkingDirectory = Path.Combine(RootDirectory, relative);
            if (!File.Exists(WorkingDirectory)) {
                return false;
            }
            return LockRegion(WorkingDirectory, () => {
                using (Stream fs = Open(relative, FileMode.Open, FileAccess.Read)) {
                    func(fs);
                }
                return true;
            });
        }

        public void SetLastModified(String relative, DateTime dt) {
            FixRel(ref relative);
            var WorkingDirectory = Path.Combine(RootDirectory, relative);
            File.SetLastWriteTimeUtc(WorkingDirectory, dt);
        }
        public void SetLastAccess(String relative, DateTime dt) {
            FixRel(ref relative);
            var WorkingDirectory = Path.Combine(RootDirectory, relative);
            File.SetLastAccessTimeUtc(WorkingDirectory, dt);
        }

        public String ReadAllText(String relative) {
            FixRel(ref relative);
            var WorkingDirectory = Path.Combine(RootDirectory, relative);
            return LockRegion(WorkingDirectory, () => {
                // Metodo 1: Convencional File System:
                if (!Directory.Exists(Path.GetDirectoryName(WorkingDirectory))) {
                    absMkDirs(Path.GetDirectoryName(WorkingDirectory));
                }
                if (!File.Exists(WorkingDirectory)) {
                    return null;
                }
                String text = File.ReadAllText(WorkingDirectory);
                return text;
            });
        }

        public byte[] ReadAllBytes(String relative) {
            FixRel(ref relative);
            var WorkingDirectory = Path.Combine(RootDirectory, relative);
            // Metodo 1: Convencional File System:
            return LockRegion<byte[]>(WorkingDirectory, () => {
                if (!Directory.Exists(Path.GetDirectoryName(WorkingDirectory))) {
                    absMkDirs(Path.GetDirectoryName(WorkingDirectory));
                }
                if (!File.Exists(WorkingDirectory)) {
                    return null;
                }
                return File.ReadAllBytes(WorkingDirectory);
            });
        }

        private T LockRegion<T>(String wd, Func<T> act) {
            lock (wd) {
                return act.Invoke();
            }
        }
        private void LockRegion(String wd, Action act) {
            lock ($"FILE_ACCESSOR_LOCK_REGION:{wd}") {
                act?.Invoke();
            }
        }

        public void WriteAllText(String relative, String content) {
            FixRel(ref relative);
            var WorkingDirectory = Path.Combine(RootDirectory, relative);
            // Metodo 1: Convencional File System:

            LockRegion(WorkingDirectory, () => {
                if (!Directory.Exists(Path.GetDirectoryName(WorkingDirectory))) {
                    absMkDirs(Path.GetDirectoryName(WorkingDirectory));
                }
                File.WriteAllText(WorkingDirectory, content);
            });
        }

        public void WriteAllBytes(String relative, byte[] content) {
            FixRel(ref relative);
            var WorkingDirectory = Path.Combine(RootDirectory, relative);
            // Metodo 1: Convencional File System:
            LockRegion(WorkingDirectory, () => {
                if (!Directory.Exists(Path.GetDirectoryName(WorkingDirectory))) {
                    absMkDirs(Path.GetDirectoryName(WorkingDirectory));
                }
                File.WriteAllBytes(WorkingDirectory, content);
            });
        }

        public bool Delete(String relative) {
            FixRel(ref relative);
            var WorkingDirectory = Path.Combine(RootDirectory, relative);
            // Metodo 1: Convencional File System:
            if (!Directory.Exists(Path.GetDirectoryName(WorkingDirectory))) {
                absMkDirs(Path.GetDirectoryName(WorkingDirectory));
            }
            if (!File.Exists(WorkingDirectory)) {
                return false;
            }
            LockRegion(WorkingDirectory, () => {
                File.Delete(WorkingDirectory);
            });
            return true;
        }

        public bool IsDirectory(string relative) {
            FixRel(ref relative);
            return Directory.Exists(Path.Combine(RootDirectory, relative));
        }
        public bool IsFile(string relative) {
            FixRel(ref relative);
            return File.Exists(Path.Combine(RootDirectory, relative));
        }

        public bool Exists(String relative) {
            FixRel(ref relative);
            return IsFile(relative) || IsDirectory(relative);
        }

        public void AppendAllLines(String relative, IEnumerable<string> content) {
            FixRel(ref relative);
            var WorkingDirectory = Path.Combine(RootDirectory, relative);
            // Metodo 1: Convencional File System:
            if (!Directory.Exists(Path.GetDirectoryName(WorkingDirectory))) {
                absMkDirs(Path.GetDirectoryName(WorkingDirectory));
            }
            LockRegion(WorkingDirectory, () => {
                File.AppendAllLines(WorkingDirectory, content);
            });
        }

        public void AppendAllLinesAsync(String relative, IEnumerable<string> content, Action OnComplete = null) {
            FixRel(ref relative);
            Fi.Tech.RunAndForget(() => {
                var WorkingDirectory = Path.Combine(RootDirectory, relative);
                if (!Directory.Exists(Path.GetDirectoryName(WorkingDirectory))) {
                    absMkDirs(Path.GetDirectoryName(WorkingDirectory));
                }

                LockRegion(WorkingDirectory, () => {
                    File.AppendAllLines(WorkingDirectory, content);
                    OnComplete();
                });
            });

        }

        public Stream Open(string relative, FileMode fileMode, FileAccess fileAccess) {
            FixRel(ref relative);
            var WorkingDirectory = Path.Combine(RootDirectory, relative);
            if (!Directory.Exists(Path.GetDirectoryName(WorkingDirectory))) {
                absMkDirs(Path.GetDirectoryName(WorkingDirectory));
            }
            var bufferLength = new FileInfo(WorkingDirectory).Length * .6;
            bufferLength = Math.Max(256 * 1024, bufferLength);
            bufferLength = Math.Min(bufferLength, MaxStreamBufferLength);

            return new FileStream(WorkingDirectory, fileMode, fileAccess, FileShare.Read, (int)bufferLength);
        }

        public void Hide(string relative) {
            FixRel(ref relative);
            var WorkingDirectory = Path.Combine(RootDirectory, relative);
            if (File.Exists(WorkingDirectory)) {
                File.SetAttributes(WorkingDirectory, File.GetAttributes(WorkingDirectory) | FileAttributes.Hidden);
            }
        }

        public void Show(string relative) {
            FixRel(ref relative);
            var WorkingDirectory = Path.Combine(RootDirectory, relative);
            if (File.Exists(WorkingDirectory)) {
                File.SetAttributes(WorkingDirectory, File.GetAttributes(WorkingDirectory) & ~FileAttributes.Hidden);
            }
        }
    }
}
