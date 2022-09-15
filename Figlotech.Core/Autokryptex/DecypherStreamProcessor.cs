using Figlotech.Core.Helpers;
using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using Figlotech.Core.Autokryptex;
using System.Threading.Tasks;

namespace Figlotech.Core.Autokryptex
{
    public sealed class DecypherStreamProcessor : IStreamProcessor {
        public bool Enable { get; set; }

        IEncryptionMethod _method;

        public DecypherStreamProcessor(IEncryptionMethod method) {
            _method = method;
        }

        public async Task Process(Stream input, Func<Stream, Task> act) {
            using (MemoryStream ms = new MemoryStream()) {
                input.CopyTo(ms);
                ms.Seek(0, SeekOrigin.Begin);
                var bytes = ms.ToArray();
                var cypheredBytes = _method.Decrypt(bytes);
                using (MemoryStream output = new MemoryStream(cypheredBytes)) {

                    await act(output);
                }
            }
        }
    }
}
