using System;
using System.Collections.Generic;
using System.Text;

namespace Figlotech.Core
{
    public class Any<T> {
        T val;
        public Any(T startingValue) {
            val = startingValue;
        }

        public static implicit operator Any<T>(T a) {
            return new Any<T>(a);
        }
        public static implicit operator T(Any<T> a) {
            return a.val;
        }

        public static implicit operator Any<T>(Any<object> a) {
            if (a.val is IConvertible conv && typeof(T).IsPrimitive) {
            
                var newVal = (T) Convert.ChangeType(a.val, typeof(T));
                return new Any<T>(newVal);
            }
            if (typeof(T).IsAssignableFrom(a.val?.GetType())) {
                return new Any<T>((T) a.val);
            }
            return new Any<T>(default(T));
        }
        public Any<A> To<A>() {
            return (Any<A>)(Any<object>)this;
        }

        public static implicit operator Any<object>(Any<T> a) {
            return new Any<object>(a.val);
        }
    }

    public class Any : Any<object>  {
        public Any(object o) : base(o) {

        }
        
        public static Any<T> From<T>(T o) {
            return new Any<T>(o);
        }
    }
}
