using Figlotech.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.Core.FileAcessAbstractions {
    public sealed class FileAccessor : IFileSystem {

        private static int gid = 0;
        private static int myid = ++gid;
        private static readonly char S = Path.DirectorySeparatorChar;

        public bool IsCaseSensitive => !RuntimeInformation.IsOSPlatform(OSPlatform.Windows);

        public bool UseCriptography;

        public String RootDirectory { get; set; }
        public int MaxStreamBufferLength { get; set; } = 64 * 1024 * 1024;

        public String FixRel(ref string relative) {
            return relative = relative?.Replace(Path.DirectorySeparatorChar, '/')
                .Replace("//", "/")
                .Replace('/', S);
        }
        public String UnFixRel(ref string relative) {
            return relative = relative?.Replace(S, '/')
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
            if(string.IsNullOrEmpty(dir)) {
                return;
            }
            FixRel(ref dir);

            try {
                if (!Directory.Exists(Path.GetDirectoryName(dir))) {
                    absMkDirs(Path.GetDirectoryName(dir));
                }
            } catch (Exception x) {
                Console.Error.WriteLine($"Error creating directory tree for {dir}: {x.Message}");
                throw x;
            }
            try {
                Directory.CreateDirectory(dir);
            } catch (Exception x) {
                //Fi.Tech.WriteLine(x.Message);
            }
        }
        private async Task absMkDirsAsync(string dir) {
            await Task.Run(() => {
                absMkDirs(dir);
            }).ConfigureAwait(false);
        }

        public async Task MkDirsAsync(string dir) {
            FixRel(ref dir);
            await absMkDirsAsync(dir).ConfigureAwait(false);
        }

        public async Task Write(String relative, Func<Stream, Task> func) {
            FixRel(ref relative);
            var WorkingDirectory = AssemblePath(RootDirectory, relative);
            // Metodo 1: Convencional File System:
            if (!Directory.Exists(Path.GetDirectoryName(WorkingDirectory))) {
                absMkDirs(Path.GetDirectoryName(WorkingDirectory));
            }
            if (!File.Exists(WorkingDirectory)) {
                using (var f = File.Create(WorkingDirectory)) { }
            }
            using (Stream fs = await OpenAsync(relative, FileMode.Truncate, FileAccess.Write).ConfigureAwait(false)) {
                await func(fs).ConfigureAwait(false);
            }
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
            return AssemblePath(RootDirectory, relative);
        }

        public async Task RenameAsync(string relative, string newName) {
            FixRel(ref relative);
            FileAttributes attr = File.GetAttributes(AssemblePath(RootDirectory, relative));
            var WorkingDirectory = AssemblePath(RootDirectory, relative);
            await LockRegion(WorkingDirectory, async () => {
                //detect whether its a directory or file
                if ((attr & FileAttributes.Directory) == FileAttributes.Directory) {
                    await Task.Run(() => {
                        if (Directory.Exists(AssemblePath(RootDirectory, relative))) {
                            if (!Directory.Exists(Path.GetDirectoryName(AssemblePath(RootDirectory, newName))))
                                absMkDirs(Path.GetDirectoryName(AssemblePath(RootDirectory, newName)));
                            if (Directory.Exists(AssemblePath(RootDirectory, newName))) {
                                RenDir(AssemblePath(RootDirectory, relative), AssemblePath(RootDirectory, newName));
                            } else {
                                Directory.Move(AssemblePath(RootDirectory, relative), AssemblePath(RootDirectory, newName));
                            }
                        }
                    }).ConfigureAwait(false);
                } else {
                    await Task.Run(() => {
                        if (File.Exists(AssemblePath(RootDirectory, relative))) {
                            if (!Directory.Exists(Path.GetDirectoryName(AssemblePath(RootDirectory, newName))))
                                absMkDirs(Path.GetDirectoryName(AssemblePath(RootDirectory, newName)));
                            if (File.Exists(AssemblePath(RootDirectory, newName))) {
                                File.Delete(AssemblePath(RootDirectory, newName));
                            }
                            File.Move(AssemblePath(RootDirectory, relative), AssemblePath(RootDirectory, newName));
                        }
                    }).ConfigureAwait(false);
                }
            }).ConfigureAwait(false);
        }

        public async Task<DateTime?> GetLastModifiedAsync(string relative) {
            FixRel(ref relative);
            if (await IsFileAsync(relative).ConfigureAwait(false)) {
                return new FileInfo(AssemblePath(RootDirectory, relative)).LastWriteTimeUtc;
            }
            if (await IsDirectoryAsync(relative).ConfigureAwait(false)) {
                return new DirectoryInfo(AssemblePath(RootDirectory, relative)).LastWriteTimeUtc;
            }
            return DateTime.MinValue;
        }
        public async Task<DateTime?> GetLastAccessAsync(string relative) {
            FixRel(ref relative);
            if (await IsFileAsync(relative).ConfigureAwait(false)) {
                return new FileInfo(AssemblePath(RootDirectory, relative)).LastAccessTimeUtc;
            }
            if (await IsDirectoryAsync(relative).ConfigureAwait(false)) {
                return new DirectoryInfo(AssemblePath(RootDirectory, relative)).LastAccessTimeUtc;
            }
            return DateTime.MinValue;
        }

        public async Task<long> GetSizeAsync(string relative) {
            FixRel(ref relative);
            var WorkingDirectory = AssemblePath(RootDirectory, relative);
            return await Task.Run(() => {
                if (File.Exists(WorkingDirectory)) {
                    return new FileInfo(WorkingDirectory).Length;
                }
                return 0;
            }).ConfigureAwait(false);
        }

        public void ParallelForFilesIn(String relative, Action<String> execFunc) {
            FixRel(ref relative);
            var WorkingDirectory = AssemblePath(RootDirectory, relative);
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
            var WorkingDirectory = AssemblePath(RootDirectory, relative);
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
                        ex.Add(new Exception($"Error manipulating file {s}", x));
                    }
                }
                if (handler == null && ex.Any()) {
                    throw new AggregateException(ex);
                }
            }
        }
        public IEnumerable<string> GetFilesIn(String relative) {
            relative = relative.Replace('/', Path.DirectorySeparatorChar);
            var WorkingDirectory = AssemblePath(RootDirectory, relative);
            if (!Directory.Exists(WorkingDirectory)) {
                return new string[0];
            }
            if(!WorkingDirectory.EndsWith(Path.DirectorySeparatorChar.ToString())) {
                WorkingDirectory += Path.DirectorySeparatorChar.ToString();
            }
            return Directory.GetFiles(WorkingDirectory).Select(
                    ((a) => {
                        var s = a.Substring(RootDirectory.Length + 1).Replace(Path.DirectorySeparatorChar, '/');
                        return this.UnFixRel(ref s);
                    }));
        }

        public void ParallelForDirectoriesIn(String relative, Action<String> execFunc) {
            FixRel(ref relative);
            var WorkingDirectory = AssemblePath(RootDirectory, relative);
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
            var WorkingDirectory = AssemblePath(RootDirectory, relative);
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
            relative = relative.Replace('/', Path.DirectorySeparatorChar);
            var WorkingDirectory = AssemblePath(RootDirectory, relative);
            if (!Directory.Exists(WorkingDirectory)) {
                return new string[0];
            }
            return Directory.GetDirectories(WorkingDirectory)
                .Select((Func<string, string>)((a) => {
                    a = a.Substring(RootDirectory.Length + 1).Replace(Path.DirectorySeparatorChar, '/');
                    return a;
                }));
        }

        public async Task<bool> Read(String relative, Func<Stream, Task> func) {
            FixRel(ref relative);
            if (!await ExistsAsync(relative).ConfigureAwait(false)) {
                return false;
            }
            var WorkingDirectory = AssemblePath(RootDirectory, relative);
            if (!File.Exists(WorkingDirectory)) {
                return false;
            }
            using (Stream fs = await OpenAsync(relative, FileMode.Open, FileAccess.Read).ConfigureAwait(false)) {
                await func(fs).ConfigureAwait(false);
            }
            return true;
        }

        public async Task SetLastModifiedAsync(String relative, DateTime dt) {
            FixRel(ref relative);
            var WorkingDirectory = AssemblePath(RootDirectory, relative);
            await Task.Run(() => {
                File.SetLastWriteTimeUtc(WorkingDirectory, dt);
            }).ConfigureAwait(false);
        }
        public async Task SetLastAccessAsync(String relative, DateTime dt) {
            FixRel(ref relative);
            var WorkingDirectory = AssemblePath(RootDirectory, relative);
            await Task.Run(() => {
                File.SetLastAccessTimeUtc(WorkingDirectory, dt);
            });
        }

        public String ReadAllText(String relative) {
            FixRel(ref relative);
            var WorkingDirectory = AssemblePath(RootDirectory, relative);
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
            var WorkingDirectory = AssemblePath(RootDirectory, relative);
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
            lock (FileLocks[wd]) {
                return act.Invoke();
            }
        }
        static FiAsyncMultiLock FileLocks = new FiAsyncMultiLock();
        private void LockRegion(String wd, Action act) {
            using (FileLocks.Lock(wd).GetAwaiter().GetResult()) {
                act?.Invoke();
            }
        }

        public void WriteAllText(String relative, String content) {
            FixRel(ref relative);
            var WorkingDirectory = AssemblePath(RootDirectory, relative);
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
            var WorkingDirectory = AssemblePath(RootDirectory, relative);
            // Metodo 1: Convencional File System:
            LockRegion(WorkingDirectory, () => {
                if (!Directory.Exists(Path.GetDirectoryName(WorkingDirectory))) {
                    absMkDirs(Path.GetDirectoryName(WorkingDirectory));
                }
                File.WriteAllBytes(WorkingDirectory, content);
            });
        }

        public async Task<bool> DeleteAsync(String relative) {
            FixRel(ref relative);
            var WorkingDirectory = AssemblePath(RootDirectory, relative);
            // Metodo 1: Convencional File System:
            if (!Directory.Exists(Path.GetDirectoryName(WorkingDirectory))) {
                return false;
            }
            if(await IsDirectoryAsync(relative).ConfigureAwait(false)) {
                Directory.Delete(WorkingDirectory);
            } else {
                if (!File.Exists(WorkingDirectory)) {
                    return false;
                }
            }
            LockRegion(WorkingDirectory, () => {
                File.Delete(WorkingDirectory);
            });
            return true;
        }

        public async Task<bool> IsDirectoryAsync(string relative) {
            FixRel(ref relative);
            return await Task.Run(()=> Directory.Exists(AssemblePath(RootDirectory, relative))).ConfigureAwait(false);
        }
        public async Task<bool> IsFileAsync(string relative) {
            FixRel(ref relative);
            return await Task.Run(()=> File.Exists(AssemblePath(RootDirectory, relative))).ConfigureAwait(false);
        }

        private string AssemblePath(params string[] segments) {
            var seg = segments.Select(s => s.Split(S))
                .Flatten()
                .Where(s => !string.IsNullOrEmpty(s))
                .ToArray();
            var retv = string.Join(S.ToString(), seg);
            if(segments[0].StartsWith($"{S}")) {
                retv = S + retv;   
            }
            return retv;
        }

        public async Task<bool> ExistsAsync(String relative) {
            FixRel(ref relative);
            return await IsFileAsync(relative).ConfigureAwait(false) || await IsDirectoryAsync(relative).ConfigureAwait(false);
        }

        public void AppendAllLines(String relative, IEnumerable<string> content) {
            FixRel(ref relative);
            var WorkingDirectory = AssemblePath(RootDirectory, relative);
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
            Fi.Tech.FireAndForget(async () => {
                await Task.Yield();
                var WorkingDirectory = AssemblePath(RootDirectory, relative);
                if (!Directory.Exists(Path.GetDirectoryName(WorkingDirectory))) {
                    absMkDirs(Path.GetDirectoryName(WorkingDirectory));
                }

                LockRegion(WorkingDirectory, () => {
                    File.AppendAllLines(WorkingDirectory, content);
                    OnComplete();
                });
            });

        }

        public async Task<Stream> OpenAsync(string relative, FileMode fileMode, FileAccess fileAccess) {
            FixRel(ref relative);
            var workingPath = AssemblePath(RootDirectory, relative);
            if (!Directory.Exists(Path.GetDirectoryName(workingPath))) {
                await absMkDirsAsync(Path.GetDirectoryName(workingPath));
            }

            return await Task.Run(() => File.Open(workingPath, fileMode, fileAccess, FileShare.Read)).ConfigureAwait(false);
        }

        public void Hide(string relative) {
            FixRel(ref relative);
            var WorkingDirectory = AssemblePath(RootDirectory, relative);
            if (File.Exists(WorkingDirectory)) {
                File.SetAttributes(WorkingDirectory, File.GetAttributes(WorkingDirectory) | FileAttributes.Hidden);
            }
        }

        public void Show(string relative) {
            FixRel(ref relative);
            var WorkingDirectory = AssemblePath(RootDirectory, relative);
            if (File.Exists(WorkingDirectory)) {
                File.SetAttributes(WorkingDirectory, File.GetAttributes(WorkingDirectory) & ~FileAttributes.Hidden);
            }
        }

        public async Task<string> ReadAllTextAsync(string relative) {
            if(!await ExistsAsync(relative).ConfigureAwait(false)) {
                return string.Empty;
            }
            using (var fstream = await OpenAsync(relative, FileMode.Open, FileAccess.Read).ConfigureAwait(false)) {
                using (var reader = new StreamReader(fstream)) {
                    return await reader.ReadToEndAsync().ConfigureAwait(false);
                }
            }
        }

        public async Task<byte[]> ReadAllBytesAsync(string relative) {
            if (!await ExistsAsync(relative).ConfigureAwait(false)) {
                return new byte[0];
            }
            using (var fstream = await OpenAsync(relative, FileMode.Open, FileAccess.Read).ConfigureAwait(false)) {
                using (var ms = new MemoryStream()) {
                    await fstream.CopyToAsync(ms).ConfigureAwait(false);
                    ms.Seek(0, SeekOrigin.Begin);
                    return ms.ToArray();
                }
            }
        }

        public async Task WriteAllTextAsync(string relative, string content) {
            using (var fstream = await OpenAsync(relative, FileMode.Create, FileAccess.Write).ConfigureAwait(false)) {
                using (var writer = new StreamWriter(fstream)) {
                    await writer.WriteAsync(content).ConfigureAwait(false);
                }
            }
        }

        public async Task WriteAllBytesAsync(string relative, byte[] content) {
            using (var fstream = await OpenAsync(relative, FileMode.Create, FileAccess.Write).ConfigureAwait(false)) {
                using (var ms = new MemoryStream(content)) {
                    await ms.CopyToAsync(fstream).ConfigureAwait(false);
                }
            }
        }
    }
}
