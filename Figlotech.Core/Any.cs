using System;
using System.Collections.Generic;
using System.Text;

namespace Figlotech.Core
{
    public sealed class Any<T> : Any {
        T val;

        public T Value { get; }

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
            if (a.val is IConvertible conv) {
                try {
                    var newVal = (T)Convert.ChangeType(a.val, typeof(T));
                    return new Any<T>(newVal);
                } catch(Exception x) {
                    //return new Any<T>(default(T));
                }
            }
            if (typeof(T).IsAssignableFrom(a.val?.GetType())) {
                return new Any<T>((T) a.val);
            }
            return new Any<T>(default(T));
        }
        public override Any<A> To<A>() {
            return (Any<A>)(Any<object>)this;
        }

        public static implicit operator Any<object>(Any<T> a) {
            return new Any<object>(a.val);
        }
    }

    public abstract class Any {

        public abstract Any<A> To<A>();

        public static Any<T> From<T>(T o) {
            return new Any<T>(o);
        }
    }
}
