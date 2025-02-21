using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using System.Text;

namespace Figlotech.Core.Extensions {
    public static class ConcurrentDictionaryExtensions {
        public static TValue GetOrAddWithLocking<TKey, TValue>(this ConcurrentDictionary<TKey, TValue> self, TKey key, Func<TKey, TValue> factory) {
            if(self.TryGetValue(key, out var value)) {
                return value;
            }
            var factoryMethodInfo = factory.GetMethodInfo();
            var factoryName = $"{factoryMethodInfo.DeclaringType.FullName}::{factoryMethodInfo.Name}"; 
            lock(string.Intern(factoryName)) {
                return self.GetOrAdd(key, factory);
            }
        }
    }
}
