using Figlotech.Core.Helpers;
using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using Figlotech.Core.Autokryptex;

namespace Figlotech.Core.Autokryptex
{
    public class CypherStreamProcessor : IStreamProcessor {

        IEncryptionMethod _method;

        public CypherStreamProcessor(IEncryptionMethod method) {
            _method = method;
        }

        public void Process(Stream input, Action<Stream> act) {
            using (MemoryStream ms = new MemoryStream()) {
                input.CopyTo(ms);
                ms.Seek(0, SeekOrigin.Begin);
                var bytes = ms.ToArray();
                var cypheredBytes = _method.Encrypt(bytes);
                using (MemoryStream output = new MemoryStream(cypheredBytes)) {
                    act(output);
                }
            }
        }
    }
}
