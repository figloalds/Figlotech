using Figlotech.Core.Autokryptex;
using System.IO;

namespace Figlotech.Core.Extensions {
    public sealed class FTHSerializableOptions {
        public bool UseGzip { get; set; }
        public bool Formatted { get; set; }
        public IEncryptionMethod UseEncryption { get; set; }
        public Newtonsoft.Json.JsonSerializerSettings JsonSettings { get; set; } = null;
    }
    public interface IMultiSerializerPlugin {
        void Serialize(object o, Stream stream, FTHSerializableOptions options);
        void Deserialize(Stream s, Stream stream, FTHSerializableOptions options);
    }

}
