using System;
using System.Collections.Generic;
using System.Text;

namespace Figlotech.Core
{
    public class DynaLocks
    {
        SelfInitializerDictionary<string, object> DynaLockMutexes = new SelfInitializerDictionary<string, object>(
            s => new object(), true
        );
        public object this[string key] {
            get {
                lock(string.Intern($"{this.GetHashCode()}_{key}")) {
                    return DynaLockMutexes[key];
                }
            }
        }
    }
}
