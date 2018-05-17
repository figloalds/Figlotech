using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace Figlotech.Core
{
    public class FnVal {
        public static FnVal<A> From<A>(Func<A> fn) => new FnVal<A>(fn);
    }
    public class FnVal<T>
    {
        public Func<T> Accessor { get; private set; }
        public T Value => this.Accessor();
        public FnVal(Func<T> accessor) {
            this.Accessor = accessor;
        }

        public static implicit operator T(FnVal<T> self) {
            if (self.Accessor != null) {
                return self.Accessor.Invoke();
            }
            return default(T);
        }

        public static implicit operator List<T>(FnVal<T> self) {
            if (self.Accessor != null) {
                return new List<T> { self.Accessor.Invoke() };
            }
            return new List<T>();
        }

        public static implicit operator FnVal<T>(List<T> other) {
            return new FnVal<T>(()=> other.FirstOrDefault<T>());
        }

        public static implicit operator FnVal<T>(Func<T> other) {
            return new FnVal<T>(other);
        }

        public static implicit operator FnVal<T>(Expression<Func<T>> other) {
            return new FnVal<T>(other.Compile());
        }

        public static implicit operator FnVal<T>(T other) {
            return new FnVal<T>(()=> other);
        }

    }
}
