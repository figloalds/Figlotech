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
    public class Lazy<T>
    {
        T _value;
        Func<T> initFun;
        bool isInit = false;
        public T Value {
            get {
                if (!isInit) {
                    _value = initFun();
                    isInit = true;
                    initFun = null;
                }
                return _value;
            } set {
                if (!isInit) {
                    isInit = true;
                    initFun = null;
                }
                _value = value;
            }
        }
        public Lazy(Func<T> initFunction) {
            initFun = initFunction;
        }

        public static explicit operator T(Lazy<T> input) {
            return input.Value;
        }

        public static implicit operator Lazy<T>(T input) {
            return new Lazy<T>(() => input);
        }
    }
}
