using System;
using System.Collections.Concurrent;
using System.IO;
using System.Reflection;
using Figlotech.Core.Interfaces;

namespace Figlotech.BDados.Helpers {
    // Resolve static interface dispatch and construction once per CLR type, including explicit implementations.
    internal sealed class BDadosBackupBinaryType {
        private static readonly ConcurrentDictionary<Type, BDadosBackupBinaryType> Cache = new();
        internal int? FixedLength { get; private init; }
        internal Func<IBinarySerializable> CreateInstance { get; private init; }

        internal static BDadosBackupBinaryType Get(Type type) {
            return Cache.GetOrAdd(type, t => {
                if (!typeof(IBinarySerializable).IsAssignableFrom(t) || t.IsAbstract || t.IsInterface
                    || t.ContainsGenericParameters || (!t.IsValueType && t.GetConstructor(Type.EmptyTypes) == null)) {
                    throw new NotSupportedException($"Binary backup type '{t.FullName}' must be concrete with a public parameterless constructor (or be a struct).");
                }
                return typeof(BDadosBackupBinaryType).GetMethod(nameof(Create), BindingFlags.NonPublic | BindingFlags.Static)
                    .MakeGenericMethod(t).CreateDelegate<Func<BDadosBackupBinaryType>>()();
            });
        }

        private static BDadosBackupBinaryType Create<T>() where T : IBinarySerializable, new() {
            int? length = T.IsFixedLength ? T.Length : null;
            if (length < 0) throw new InvalidDataException($"Binary backup type '{typeof(T).FullName}' has a negative fixed length.");
            return new BDadosBackupBinaryType { FixedLength = length, CreateInstance = () => new T() };
        }
    }
}
