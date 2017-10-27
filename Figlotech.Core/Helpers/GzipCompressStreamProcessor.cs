using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core.Helpers {
    public class GzipCompressStreamProcessor : IStreamProcessor {

        public GzipCompressStreamProcessor() {
        }

        public void Process(Stream input, Action<Stream> act) {
            using (var gzs = new GZipStream(input, CompressionLevel.Optimal))
                act?.Invoke(gzs);
        }
    }
}
