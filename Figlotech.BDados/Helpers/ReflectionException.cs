using System;
using System.Runtime.Serialization;

namespace Figlotech.BDados.Helpers {
    [Serializable]
    internal class ReflectionException : Exception {
        public ReflectionException() {
        }

        public ReflectionException(string message) : base(message) {
        }

        public ReflectionException(string message, Exception innerException) : base(message, innerException) {
        }

        protected ReflectionException(SerializationInfo info, StreamingContext context) : base(info, context) {
        }
    }
}