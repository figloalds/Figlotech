using Figlotech.Core;
using System;
using System.Collections;
using System.Runtime.Serialization;

namespace System {
    public class MainLogicGeneratedException : Exception {
        public int StatusCode { get; set; }
        public LenientDictionary<string, object> AdditionalInfo { get; private set; } = new LenientDictionary<string, object>();

        public MainLogicGeneratedException(int CommonHttpError, string message) : base(message) {
            StatusCode = CommonHttpError;
        }

        public MainLogicGeneratedException(int CommonHttpError, string message, Exception innerException) : base(message, innerException) {
            StatusCode = CommonHttpError;
        }

        protected MainLogicGeneratedException(int CommonHttpError, SerializationInfo info, StreamingContext context) : base(info, context) {
            StatusCode = CommonHttpError;
        }
    }
}
