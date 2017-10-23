using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core.Helpers {
    public class GzipCompressStreamProcessor : IStreamProcessor {
        public bool Enable { get; set; } = false;

        public GzipCompressStreamProcessor(bool enableCompression) {
            Enable = enableCompression;
        }

        public void Process(Stream input, Action<Stream> act) {
            if (Enable)
                using (var gzs = new GZipStream(input, CompressionLevel.Optimal))
                    act?.Invoke(gzs);

            else
                act?.Invoke(input);
        }
    }
}
