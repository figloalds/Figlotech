using System;
using System.IO;
using System.IO.Compression;

namespace Figlotech.Core.Helpers {
    public class GzipDecompressStreamProcessor : IStreamProcessor {
        public bool Enable { get; set; } = false;
        public GzipDecompressStreamProcessor(bool enableCompression) {
            Enable = enableCompression;
        }

        public void Process(Stream input, Action<Stream> act) {
            if (Enable)
                using (var gzs = new GZipStream(input, CompressionMode.Decompress))
                    act?.Invoke(gzs);

            else
                act?.Invoke(input);
        }
    }
}
