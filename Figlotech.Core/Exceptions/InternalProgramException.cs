using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;

namespace System {
    public class InternalProgramException : MainLogicGeneratedException {

        public InternalProgramException(string message) : base(500, message) {
        }

        public InternalProgramException(string message, Exception innerException) : base(500, message, innerException) {
        }

        protected InternalProgramException(SerializationInfo info, StreamingContext context) : base(500, info, context) {
        }
    }
}
