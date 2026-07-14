using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;

namespace Figlotech.Core.FileAcessAbstractions {
    public sealed class FileAccessor : IFileSystem {

        private static int gid = 0;
        private static readonly int myid = ++gid;
        private static readonly char S = Path.DirectorySeparatorChar;

        public bool IsCaseSensitive => !RuntimeInformation.IsOSPlatform(OSPlatform.Windows);

        public bool UseCriptography;

        private string _rootDirectory;
        public String RootDirectory {
            get => _rootDirectory;
            set => _rootDirectory = NormalizeRootPath(value);
        }
        public int MaxStreamBufferLength { get; set; } = 64 * 1024 * 1024;

        public String FixRel(ref string relative) {
            return relative = NormalizeInputPath(relative);
        }
        public String UnFixRel(ref string relative) {
            return relative = NormalizeOutputPath(relative);
        }

        public FileAccessor(String workingPath) {
            RootDirectory = workingPath ?? throw new ArgumentNullException(nameof(workingPath));
            if (!Directory.Exists(RootDirectory)) {
                absMkDirs(RootDirectory);
            }
        }

        private void absMkDirs(string dir) {
            if (string.IsNullOrEmpty(dir)) {
                return;
            }
            Directory.CreateDirectory(NormalizeInputPath(dir));
        }
        private Task absMkDirsAsync(string dir) {
            absMkDirs(dir);
            return Task.CompletedTask;
        }

        public async Task MkDirsAsync(string dir) {
            var workingDirectory = AssemblePath(RootDirectory, dir);
            await absMkDirsAsync(workingDirectory).ConfigureAwait(false);
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

        public IFileSystem Fork(string relative) {
            string newPath = AssemblePath(RootDirectory, relative);
            FixRel(ref newPath);
            if (!Directory.Exists(newPath)) {
                absMkDirs(newPath);
            }
            return new FileAccessor(newPath);
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
                execFunc(ToSelfRelativePath(s));
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
                    s = ToSelfRelativePath(s1);
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
            FixRel(ref relative);
            var WorkingDirectory = AssemblePath(RootDirectory, relative);
            if (!Directory.Exists(WorkingDirectory)) {
                return new string[0];
            }
            return Directory.EnumerateFiles(WorkingDirectory)
                .Select(ToSelfRelativePath)
                .ToArray();
        }

        public void ParallelForDirectoriesIn(String relative, Action<String> execFunc) {
            FixRel(ref relative);
            var WorkingDirectory = AssemblePath(RootDirectory, relative);
            if (!Directory.Exists(WorkingDirectory)) {
                return;
            }
            Parallel.ForEach(Directory.GetDirectories(WorkingDirectory), (Action<string>)((s) => {
                execFunc(ToSelfRelativePath(s));
            }));
        }
        public void ForDirectoriesIn(String relative, Action<String> execFunc) {

            FixRel(ref relative);
            var WorkingDirectory = AssemblePath(RootDirectory, relative);
            if (!Directory.Exists(WorkingDirectory)) {
                return;
            }
            foreach (var s1 in Directory.GetDirectories(WorkingDirectory)) {
                execFunc(ToSelfRelativePath(s1));
            }
        }
        public IEnumerable<string> GetDirectoriesIn(String relative) {
            FixRel(ref relative);
            var WorkingDirectory = AssemblePath(RootDirectory, relative);
            if (!Directory.Exists(WorkingDirectory)) {
                return new string[0];
            }
            return Directory.EnumerateDirectories(WorkingDirectory)
                .Select(ToSelfRelativePath)
                .ToArray();
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
        static readonly FiAsyncMultiLock FileLocks = new FiAsyncMultiLock();
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
            if (await IsDirectoryAsync(relative).ConfigureAwait(false)) {
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
            return await Task.Run(() => Directory.Exists(AssemblePath(RootDirectory, relative))).ConfigureAwait(false);
        }
        public async Task<bool> IsFileAsync(string relative) {
            FixRel(ref relative);
            return await Task.Run(() => File.Exists(AssemblePath(RootDirectory, relative))).ConfigureAwait(false);
        }

        private static string NormalizeInputPath(string path) {
            return path?.Replace('\\', S).Replace('/', S);
        }

        private static string NormalizeOutputPath(string path) {
            return path?.Replace('\\', '/').Replace(S, '/');
        }

        private static string NormalizeRootPath(string path) {
            if (path == null) {
                throw new ArgumentNullException(nameof(path));
            }

            var fullPath = Path.GetFullPath(NormalizeInputPath(path));
            var pathRoot = Path.GetPathRoot(fullPath);
            if (string.Equals(fullPath, pathRoot, StringComparison.OrdinalIgnoreCase)) {
                return fullPath;
            }
            return fullPath.TrimEnd('\\', '/');
        }

        private string ToSelfRelativePath(string fullPath) {
            return NormalizeOutputPath(Path.GetRelativePath(RootDirectory, fullPath));
        }

        private string AssemblePath(string root, string relative) {
            if (relative == null) {
                throw new ArgumentNullException(nameof(relative));
            }

            var normalizedRoot = NormalizeRootPath(root);
            var normalizedRelative = NormalizeInputPath(relative).TrimStart('\\', '/');
            if (Path.IsPathRooted(normalizedRelative)) {
                throw new ArgumentException("Path must be relative to the FileSystem root.", nameof(relative));
            }

            var fullPath = Path.GetFullPath(Path.Combine(normalizedRoot, normalizedRelative));
            var rootPrefix = normalizedRoot.EndsWith(S.ToString())
                ? normalizedRoot
                : normalizedRoot + S;
            var comparison = RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
                ? StringComparison.OrdinalIgnoreCase
                : StringComparison.Ordinal;
            if (!string.Equals(fullPath, normalizedRoot, comparison) &&
                !fullPath.StartsWith(rootPrefix, comparison)) {
                throw new ArgumentException("Relative path must be within the FileSystem's Root Directory.", nameof(relative));
            }
            return fullPath;
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
            if (!await ExistsAsync(relative).ConfigureAwait(false)) {
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
