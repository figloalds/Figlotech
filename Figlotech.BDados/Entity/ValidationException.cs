using System;
using System.Runtime.Serialization;

namespace Figlotech.BDados.Entity {
    [Serializable]
    public class ValidationException : Exception {
        public ValidationErrors Errors;

        public ValidationException(ValidationErrors errors) {
            Errors = errors;
        }

        public ValidationException(string message, ValidationErrors errors = null) : base(message) {
            Errors = errors;
        }

        public ValidationException(string message, Exception innerException) : base(message, innerException) {
        }

        protected ValidationException(SerializationInfo info, StreamingContext context) : base(info, context) {
        }
    }
}