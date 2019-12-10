using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core.Helpers
{
    public interface IStreamProcessor
    {
        Task Process(Stream input, Func<Stream, Task> act);
    }
}
