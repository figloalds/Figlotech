using System;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;

namespace Figlotech.Core.Helpers {
    public sealed class GzipDecompressStreamProcessor : IStreamProcessor {
        public GzipDecompressStreamProcessor() {
        }

        public async Task Process(Stream input, Func<Stream, Task> act) {
            if(act == null) {
                return;
            }
            using (var gzs = new GZipStream(input, CompressionMode.Decompress))
                await (act.Invoke(gzs)).ConfigureAwait(false);
        }
    }
}
