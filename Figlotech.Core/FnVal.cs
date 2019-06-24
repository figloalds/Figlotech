using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace Figlotech.Core {
    public class FnVal {
        public static FnVal<A> From<A>(Func<A> fn) => new FnVal<A>(fn);
    }

    public class FnVal<T> {
        public Func<T> Accessor { get; private set; }
        public T Value { get; set; } = default(T);
        public FnVal(Func<T> accessor) {
            this.Accessor = accessor;
            this.Refresh();
        }

        public void Refresh() {
            if (Accessor != null) {
                Value = Accessor.Invoke();
            }
        }

        public static implicit operator T(FnVal<T> self) {
            self.Refresh();
            return self.Value;
        }

        public static implicit operator List<T>(FnVal<T> self) {
            self.Refresh();
            return new List<T> { self.Value };
        }

        public static implicit operator FnVal<T>(List<T> other) {
            return new FnVal<T>(() => other.FirstOrDefault<T>());
        }

        public static implicit operator FnVal<T>(Func<T> other) {
            return new FnVal<T>(other);
        }

        public static implicit operator FnVal<T>(Expression<Func<T>> other) {
            return new FnVal<T>(other.Compile());
        }

        public static implicit operator FnVal<T>(T other) {
            var retv = new FnVal<T>(() => other);
            retv.Value = other;
            return retv;
        }

        public override string ToString() {
            Refresh();
            return Value?.ToString();
        }

    }
}
