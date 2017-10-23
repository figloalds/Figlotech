using System;
using System.Collections.Generic;
using System.IO;

namespace Figlotech.Core.FileAcessAbstractions {
    public interface IFileAccessor {

        void Write(string relative, Action<Stream> func);

        void ForFilesIn(string relative, Action<string> execFunc);
        IEnumerable<string> GetFilesIn(string relative);

        void ForDirectoriesIn(string relative, Action<string> execFunc);
        IEnumerable<string> GetDirectoriesIn(string relative);

        bool Read(string relative, Action<Stream> func);

        string ReadAllText(string relative);

        byte[] ReadAllBytes(string relative);

        void WriteAllText(string relative, string content);

        void WriteAllBytes(string relative, byte[] content);

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

    }
}