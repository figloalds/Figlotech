using System;
using System.IO;
using System.Threading.Tasks;

namespace Figlotech.Core.Helpers {
    public interface IStreamProcessor {
        Task Process(Stream input, Func<Stream, Task> act);
    }
}
