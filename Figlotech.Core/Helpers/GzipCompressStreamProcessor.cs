﻿using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core.Helpers {
    public sealed class GzipCompressStreamProcessor : IStreamProcessor {

        public GzipCompressStreamProcessor() {
        }

        public async Task Process(Stream input, Func<Stream, Task> act) {
            using (var outstream = new MemoryStream()) {
                using (var gzs = new GZipStream(outstream, CompressionLevel.Optimal, true)) {
                    input.CopyTo(gzs);
                }
                outstream.Seek(0, SeekOrigin.Begin);
                await act?.Invoke(outstream);
            }
        }
    }
}
