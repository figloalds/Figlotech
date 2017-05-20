using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados
{
    /// <summary>
    /// This class warps a reference and only initializes it when
    /// it's first read.
    /// This can be useful for data that MAY OR NOT be needed at run time,
    /// but has a heavy initialization that could otherwise be skipped.
    /// also serves as a syntax suggar, because it's easy to do what this class does with a 
    /// property and a field, but (to me) the code ends up looking like trash.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class LazyInit<T>
    {
        T _value;
        Func<T> initFun;
        public LazyInit(Func<T> initFunction) {
            initFun = initFunction;
        }

        public static implicit operator T(LazyInit<T> input) {
            return input._value;
        }
        public static implicit operator LazyInit<T>(T input) {
            return new LazyInit<T>(()=> input);
        }
    }
}
