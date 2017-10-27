using Figlotech.Core.FileAcessAbstractions;
using System;

namespace Figlotech.Core.Interfaces {
    public interface ILogger {
        void WriteLog(String log);

        IFileSystem FileAccessor { get; set; }

        bool Enabled { get; set; }
        
    }
}
