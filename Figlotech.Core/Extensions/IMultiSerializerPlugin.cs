using Figlotech.Core.Autokryptex;
using Figlotech.Core.FileAcessAbstractions;
using Figlotech.Core.Helpers;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core.Extensions {
    public class FTHSerializableOptions {
        public bool UseGzip { get; set; }
        public bool Formatted { get; set; }
        public IEncryptionMethod UseEncryption { get; set; }
    }
    public interface IMultiSerializerPlugin {
        void Serialize(object o, Stream stream, FTHSerializableOptions options);
        void Deserialize(Stream s, Stream stream, FTHSerializableOptions options);
    }

}
