using Figlotech.BDados.FileAcessAbstractions;
using System;

namespace Figlotech.BDados.Interfaces {
    public interface ILogger {
        void WriteLog(String log);

        IFileAccessor FileAccessor { get; set; }

        bool Enabled { get; set; }
        
    }
}
