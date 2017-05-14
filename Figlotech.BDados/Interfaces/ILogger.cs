using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Interfaces {
    public interface ILogger {
        void WriteLog(String log);

        IFileAccessor FileAccessor { get; set; }

        bool Enabled { get; set; }
    }
}
