using System.Runtime.Serialization;

namespace System {
    public class EndUserGeneratedException : MainLogicGeneratedException {

        public EndUserGeneratedException(int code, string message) : base(code, message) {
        }

        public EndUserGeneratedException(int code, string message, Exception innerException) : base(code, message, innerException) {
        }

        protected EndUserGeneratedException(int code, SerializationInfo info, StreamingContext context) : base(code, info, context) {
        }
    }
}
