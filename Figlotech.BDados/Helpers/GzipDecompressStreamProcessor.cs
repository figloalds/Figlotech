using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Helpers {
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
