using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Figlotech.Core.FileAcessAbstractions {

    public static class IFileSystemExtensions {
        private static IEnumerable<string> __findfile(IFileSystem fs, string relative, string regex) {
            foreach (var file in fs.GetFilesIn(relative)) {
                var f = file;
                if (f.Contains("/")) {
                    f = file.Substring(file.LastIndexOf('/') + 1);
                }
                if(f.RegExp(regex)) {
                    yield return file;
                }
            }
            foreach(var directory in fs.GetDirectoriesIn(relative)) {
                foreach(var f in __findfile(fs, directory, regex)) {
                    yield return f;
                }
            }
        }

        public static IEnumerable<string> Find(this IFileSystem fs, string relative, string regex) {
            return __findfile(fs, relative, regex);
        }

    }

    public interface IFileSystem {

        bool IsCaseSensitive { get; }

        Task Write(string relative, Func<Stream, Task> func);

        void ForFilesIn(string relative, Action<string> execFunc, Action<String, Exception> handler = null);
        IEnumerable<string> GetFilesIn(string relative);

        void ForDirectoriesIn(string relative, Action<string> execFunc);
        IEnumerable<string> GetDirectoriesIn(string relative);

        Task<bool> Read(string relative, Func<Stream, Task> func);

        string ReadAllText(string relative);

        byte[] ReadAllBytes(string relative);

        void WriteAllText(string relative, string content);

        void WriteAllBytes(string relative, byte[] content);

        Task<string> ReadAllTextAsync(string relative);

        Task<byte[]> ReadAllBytesAsync(string relative);

        Task WriteAllTextAsync(string relative, string content);

        Task WriteAllBytesAsync(string relative, byte[] content);

        Stream Open(String relative, FileMode fileMode, FileAccess fileAccess);

        DateTime? GetLastModified(string relative);
        DateTime? GetLastAccess(string relative);

        long GetSize(string relative);

        void SetLastModified(String relative, DateTime dt);
        void SetLastAccess(String relative, DateTime dt);

        void Rename(string relative, string newName);

        void MkDirs(string relative);

        bool Delete(string relative);

        bool Exists(string relative);

        bool IsDirectory(string relative);
        bool IsFile(string relative);
        
        void AppendAllLines(string relative, IEnumerable<string> content);
        void Hide(string relative);
        void Show(string relative);
    }
}