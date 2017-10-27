using System;
using System.IO;
using System.IO.Compression;

namespace Figlotech.Core.Helpers {
    public class GzipDecompressStreamProcessor : IStreamProcessor {
        public GzipDecompressStreamProcessor() {
        }

        public void Process(Stream input, Action<Stream> act) {
            using (var gzs = new GZipStream(input, CompressionMode.Decompress))
                act?.Invoke(gzs);
        }
    }
}
