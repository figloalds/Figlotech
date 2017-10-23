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
        bool Enable { get; set; }
        void Process(Stream input, Action<Stream> act);
    }
}
