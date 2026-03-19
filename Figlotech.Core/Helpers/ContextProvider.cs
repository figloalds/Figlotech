using Figlotech.Core.Interfaces;
using System;
using System.Collections.Generic;

namespace Figlotech.Core.Helpers {
    public sealed class ContextProvider : IContextProvider {
        readonly Dictionary<String, object> obj = new Dictionary<string, object>();

        public object Get(string name) {
            if (obj.ContainsKey(name)) {
                return obj[name];
            }
            return null;
        }

        public T Get<T>(string name) {
            return (T)Get(name);
        }

        public void Set(string name, object value) {
            obj[name] = value;
        }
    }
}
