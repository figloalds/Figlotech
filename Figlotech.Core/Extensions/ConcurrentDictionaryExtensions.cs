using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core.Extensions {
    public static class ConcurrentDictionaryExtensions {
        public static TValue GetOrAddWithLocking<TKey, TValue>(this ConcurrentDictionary<TKey, TValue> self, TKey key, Func<TKey, TValue> factory) {
            if (self.TryGetValue(key, out var value)) {
                return value;
            }
            var factoryMethodInfo = factory.GetMethodInfo();
            var factoryName = $"{factoryMethodInfo.DeclaringType.FullName}::{factoryMethodInfo.Name}";
            using(var handle = _getOrAddAsyncLock.LockSync(string.Intern(factoryName))) {
                return self.GetOrAdd(key, factory);
            }
        }

        static internal FiAsyncMultiLock _getOrAddAsyncLock = new FiAsyncMultiLock();
        public static async Task<TValue> GetOrAddWithLocking<TKey, TValue>(this ConcurrentDictionary<TKey, TValue> self, TKey key, Func<TKey, Task<TValue>> factory) {
            if (self.TryGetValue(key, out var value)) {
                return value;
            }
            var factoryMethodInfo = factory.GetMethodInfo();
            var factoryName = $"{factoryMethodInfo.DeclaringType.FullName}::{factoryMethodInfo.Name}";
            await using (var handle = await _getOrAddAsyncLock.Lock(string.Intern(factoryName))) {
                var v = await factory(key);
                return self.GetOrAdd(key, v);
            }
        }
    }
}
