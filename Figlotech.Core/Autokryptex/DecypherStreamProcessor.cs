using Figlotech.Core.Helpers;
using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using Figlotech.Core.Autokryptex;

namespace Figlotech.Core.Autokryptex
{
    public class DecypherStreamProcessor : IStreamProcessor {
        public bool Enable { get; set; }

        IEncryptionMethod _method;

        public DecypherStreamProcessor(IEncryptionMethod method) {
            _method = method;
        }

        public void Process(Stream input, Action<Stream> act) {
            using (MemoryStream ms = new MemoryStream()) {
                input.CopyTo(ms);
                var bytes = ms.ToArray();
                var cypheredBytes = _method.Decrypt(bytes);
                using (MemoryStream output = new MemoryStream(cypheredBytes)) {
                    act(output);
                }
            }
        }
    }
}
