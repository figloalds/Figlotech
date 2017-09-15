using Figlotech.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.Core.FileAcessAbstractions {
    public class FileAccessor : IFileAccessor {

        private static int gid = 0;
        private static int myid = ++gid;
        private static readonly char S = Path.DirectorySeparatorChar;

        public bool UseCriptography;

        public String RootDirectory { get; set; }

        public String FixRel(ref string relative) {
            return relative = relative.Replace('\\', '/');
        }
        public String RelToFs(ref string relative) {
            relative = relative.Replace('\\', '/');
            return relative = relative.Replace('/', S);
        }

        public FileAccessor(String workingPath) {
            RootDirectory = Path.GetFullPath(workingPath);

            RelToFs(ref workingPath);
            try {
                if (!Directory.Exists(RootDirectory)) {
                    absMkDirs(RootDirectory);
                }
            } catch (Exception x) {
                throw x;
            }
        }

        private void absMkDirs(string dir) {
            RelToFs(ref dir);
            try {
                if (!Directory.Exists(Path.GetDirectoryName(dir))) {
                    absMkDirs(Path.GetDirectoryName(dir));
                }
            } catch (Exception) { }
            try {
                Directory.CreateDirectory(dir);
            } catch (Exception x) {
                Fi.Tech.WriteLine(x.Message);
            }
        }

        public void MkDirs(string dir) {
            RelToFs(ref dir);
            absMkDirs(Path.Combine(RootDirectory, dir));
        }

        public void Write(String relative, Action<Stream> func) {
            RelToFs(ref relative);
            var WorkingDirectory = Path.Combine(RootDirectory, relative);
            // Metodo 1: Convencional File System:
            if (!Directory.Exists(Path.GetDirectoryName(WorkingDirectory))) {
                absMkDirs(Path.GetDirectoryName(WorkingDirectory));
            }
            LockRegion(WorkingDirectory, () => {
                using (FileStream fs = new FileStream(WorkingDirectory, FileMode.OpenOrCreate, FileAccess.Write)) {
                    try {
                        func(fs);
                    } catch (Exception) { }
                    fs.Flush();
                }
            });
        }

        private void RenDir(string relative, string newName) {
            RelToFs(ref relative);
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

        public void Rename(string relative, string newName) {
            RelToFs(ref relative);
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

        public DateTime? GetLastFileWrite(string relative) {
            RelToFs(ref relative);
            if (Exists(relative)) {
                return new FileInfo(Path.Combine(RootDirectory, relative)).LastWriteTimeUtc;
            }
            return DateTime.MinValue;
        }
        
        public long GetSize(string relative) {
            RelToFs(ref relative);
            var WorkingDirectory = Path.Combine(RootDirectory, relative);
            if (File.Exists(WorkingDirectory)) {
                return new FileInfo(WorkingDirectory).Length;
            }
            return 0;
        }

        public void ParallelForFilesIn(String relative, Action<String> execFunc) {
            RelToFs(ref relative);
            var WorkingDirectory = Path.Combine(RootDirectory, relative);
            if (!Directory.Exists(WorkingDirectory)) {
                return;
            }
            List<Thread> ts = new List<Thread>();
            //foreach(var sa in Directory.GetFiles(WorkingDirectory)) {
            //    var s = sa.Substring(RootDirectory.Length);
            //    if (s.StartsWith("\\")) {
            //        s = s.Substring(1);
            //    }
            //    FixRel(ref s);
            //    var t = new Thread(() => {
            //        execFunc(s);
            //    });
            //    t.Name = $"FileAccessor {s}";
            //    t.Start();
            //    ts.Add(t);

            //}
            Parallel.ForEach(Directory.GetFiles(WorkingDirectory), (s) => {
                s = s.Substring(RootDirectory.Length);
                if (s.StartsWith("\\")) {
                    s = s.Substring(1);
                }
                FixRel(ref s);
                execFunc(s);
            });
        }
        public void ForFilesIn(String relative, Action<String> execFunc) {
            RelToFs(ref relative);
            var WorkingDirectory = Path.Combine(RootDirectory, relative);
            if (!Directory.Exists(WorkingDirectory)) {
                return;
            }
            foreach (var s1 in Directory.GetFiles(WorkingDirectory)) {
                var s = s1.Substring(RootDirectory.Length);
                if (s.StartsWith("\\")) {
                    s = s.Substring(1);
                }
                FixRel(ref s);
                execFunc(s);
            }
        }
        public IEnumerable<string> GetFilesIn(String relative) {
            relative = relative.Replace('/', '\\');
            var WorkingDirectory = Path.Combine(RootDirectory, relative);
            if (!Directory.Exists(WorkingDirectory)) {
                return new string[0];
            }
            return Directory.GetFiles(WorkingDirectory).Select(
                (a) => FixRel(ref a));
        }

        public void ParallelForDirectoriesIn(String relative, Action<String> execFunc) {
            RelToFs(ref relative);
            var WorkingDirectory = Path.Combine(RootDirectory, relative);
            if (!Directory.Exists(WorkingDirectory)) {
                return;
            }
            Parallel.ForEach(Directory.GetDirectories(WorkingDirectory), (s) => {
                s = s.Substring(RootDirectory.Length);
                if (s.StartsWith("\\")) {
                    s = s.Substring(1);
                }
                FixRel(ref s);
                execFunc(s);
            });
        }
        public void ForDirectoriesIn(String relative, Action<String> execFunc) {
            RelToFs(ref relative);
            var WorkingDirectory = Path.Combine(RootDirectory, relative);
            if (!Directory.Exists(WorkingDirectory)) {
                return;
            }
            foreach (var s1 in Directory.GetDirectories(WorkingDirectory)) {
                var s = s1.Substring(RootDirectory.Length);
                if (s.StartsWith("\\")) {
                    s = s.Substring(1);
                }
                FixRel(ref s);
                execFunc(s);
            }
        }
        public IEnumerable<string> GetDirectoriesIn(String relative)  {
            relative = relative.Replace('/', '\\');
            var WorkingDirectory = Path.Combine(RootDirectory, relative);
            if (!Directory.Exists(WorkingDirectory)) {
                return new string[0];
            }
            return Directory.GetDirectories(WorkingDirectory)
                .Select((a) => {
                    a = a.Replace(RootDirectory, "");
                    return FixRel(ref a);
                });
        }

        public bool Read(String relative, Action<Stream> func) {
            RelToFs(ref relative);
            var WorkingDirectory = Path.Combine(RootDirectory, relative);
            // Metodo 1: Convencional File System:
            if (!Directory.Exists(Path.GetDirectoryName(WorkingDirectory))) {
                absMkDirs(Path.GetDirectoryName(WorkingDirectory));
            }
            if (!File.Exists(WorkingDirectory)) {
                return false;
            }
            return LockRegion(WorkingDirectory, () => {
                using (FileStream fs = new FileStream(WorkingDirectory, FileMode.Open)) {
                    if (!fs.CanRead) return false;
                    func(fs);
                }
                return true;
            });
        }

        public void SetLastModified(String relative, DateTime dt) {
            RelToFs(ref relative);
            var WorkingDirectory = Path.Combine(RootDirectory, relative);
            File.SetLastWriteTimeUtc(WorkingDirectory, dt);
        }

        public String ReadAllText(String relative) {
            RelToFs(ref relative);
            var WorkingDirectory = Path.Combine(RootDirectory, relative);
            return LockRegion(WorkingDirectory, () => {
                // Metodo 1: Convencional File System:
                if (!Directory.Exists(Path.GetDirectoryName(WorkingDirectory))) {
                    absMkDirs(Path.GetDirectoryName(WorkingDirectory));
                }
                if (!File.Exists(WorkingDirectory)) {
                    return null;
                }
                String text = string.Empty;
                using (FileStream fs = new FileStream(WorkingDirectory, FileMode.Open)) {
                    using (MemoryStream ms = new MemoryStream()) {
                        fs.CopyTo(ms);
                        var bytes = ms.ToArray();
                        text = Encoding.UTF8.GetString(bytes);
                    }
                }

                return text;
            });
        }

        public byte[] ReadAllBytes(String relative) {
            RelToFs(ref relative);
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
            lock (wd) {
                act?.Invoke();
            }
        }

        public void WriteAllText(String relative, String content) {
            RelToFs(ref relative);
            var WorkingDirectory = Path.Combine(RootDirectory, relative);
            // Metodo 1: Convencional File System:

            LockRegion(WorkingDirectory, () => {
                if (!Directory.Exists(Path.GetDirectoryName(WorkingDirectory))) {
                    absMkDirs(Path.GetDirectoryName(WorkingDirectory));
                }
                using (FileStream fs = new FileStream(WorkingDirectory, FileMode.Create)) {
                    var cbytes = Encoding.UTF8.GetBytes(content);
                    fs.Write(cbytes, 0, cbytes.Length);
                }
            });
        }

        public void WriteAllBytes(String relative, byte[] content) {
            RelToFs(ref relative);
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
            RelToFs(ref relative);
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
            RelToFs(ref relative);
            return Directory.Exists(Path.Combine(RootDirectory, relative));
        }
        public bool IsFile(string relative) {
            RelToFs(ref relative);
            return File.Exists(Path.Combine(RootDirectory, relative));
        }

        public bool Exists(String relative) {
            RelToFs(ref relative);
            var WorkingDirectory = Path.Combine(RootDirectory, relative);
            // Metodo 1: Convencional File System:
            if (!Directory.Exists(Path.GetDirectoryName(WorkingDirectory))) {
                absMkDirs(Path.GetDirectoryName(WorkingDirectory));
            }
            return LockRegion(WorkingDirectory, () => {
                return Directory.Exists(WorkingDirectory) || File.Exists(WorkingDirectory);
            });
        }

        public void AppendAllLines(String relative, IEnumerable<string> content) {
            RelToFs(ref relative);
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
            RelToFs(ref relative);
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

        public Stream Open(string relative, FileMode fileMode) {
            RelToFs(ref relative);
            var WorkingDirectory = Path.Combine(RootDirectory, relative);
            if (!Directory.Exists(Path.GetDirectoryName(WorkingDirectory))) {
                absMkDirs(Path.GetDirectoryName(WorkingDirectory));
            }

            return File.Open(WorkingDirectory, fileMode);
        }
    }
}
