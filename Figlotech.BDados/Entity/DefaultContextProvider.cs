using Figlotech.BDados.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Entity {
    public class DefaultContextProvider : IContextProvider {
        Dictionary<String, Object> context = new Dictionary<string, object>();
        public object Get(string name) {
            if(context.ContainsKey(name)) {
                return context[name];
            }
            return null;
        }

        public void Set(string name, object value) {
            if (context.ContainsKey(name)) {
                context.Remove(name);
            }
            context.Add(name, value);
        }

        public T Get<T>(string name) {
            if (context.ContainsKey(name)) {
                var v = context[name];
                if((typeof(T).IsInterface && v.GetType().GetInterfaces().Contains(typeof(T))) ||
                    (v.GetType().IsAssignableFrom(typeof(T)))) {
                    return (T)v; // Return a TV!
                }
            }
            return default(T);
        }

        public Object Get(Type t) {
            if (t.IsInterface) {
                foreach (var a in context) {
                    if(a.Value.GetType().GetInterfaces().Contains(t)) {
                        return a.Value;
                    }
                }
            } else {
                foreach (var a in context) {
                    if (a.Value.GetType() == t) {
                        return a.Value;
                    }
                }
            }

            return null;
        }
    }
}
