using System;
using System.Collections.Generic;
using System.Text;

namespace Figlotech.Core
{
    public struct ValueBox<T>
    {
        public T Value { get; set; }
        public ValueBox(T value) {
            this.Value = value;
        }
        
        public static implicit operator T(ValueBox<T> self) {
            return self.Value;
        }

        public static implicit operator ValueBox<T>(T other) {
            var retv = new ValueBox<T>(other);
            retv.Value = other;
            return retv;
        }
    }
}
