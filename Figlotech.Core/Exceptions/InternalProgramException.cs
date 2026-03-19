using System.Runtime.Serialization;

namespace System {
    public sealed class InternalProgramException : MainLogicGeneratedException {

        public InternalProgramException(string message) : base(500, message) {
        }

        public InternalProgramException(string message, Exception innerException) : base(500, message, innerException) {
        }

        protected InternalProgramException(SerializationInfo info, StreamingContext context) : base(500, info, context) {
        }
    }
}
