using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace System.Text {
    public static class ListOfStringsBuilderConcatExtensions {
        
        public static string ConcatAll(this IEnumerable<string> stringsEnum) {
            var Strings = stringsEnum.ToList();
            using var builder = new ValueStringBuilder(Strings.Sum(x => x.Length));
            foreach (var str in Strings) {
                builder.Append(str);
            }
            return builder.ToString();
        }
        public static string JoinWith(this IEnumerable<string> stringsEnum, string separator) {
            var Strings = stringsEnum.ToList();
            if(Strings.Count == 0) {
                return string.Empty;
            }
            using var builder = new ValueStringBuilder(Strings.Sum(x => x.Length) + (separator.Length * Strings.Count - 1));
            var first = true;
            foreach (var str in Strings) {
                if(!first) {
                    builder.Append(separator);
                } else {
                    first = false;
                }
                builder.Append(str);
            }
            return builder.ToString();
        }
    }

    public ref struct ValueStringBuilder {
        private char[]? _arrayFromPool;
        private Span<char> _chars;
        private int _pos;

        public ValueStringBuilder(int initialCapacity) {
            _arrayFromPool = ArrayPool<char>.Shared.Rent(initialCapacity);
            _chars = _arrayFromPool;
            _pos = 0;
        }

        // Stackalloc constructor - no allocation at all for small strings
        public ValueStringBuilder(Span<char> initialBuffer) {
            _arrayFromPool = null;
            _chars = initialBuffer;
            _pos = 0;
        }

        public int Length => _pos;
        public int Capacity => _chars.Length;

        public void Append(string? value) {
            if (string.IsNullOrEmpty(value)) return;

            EnsureCapacity(value.Length);
            value.AsSpan().CopyTo(_chars.Slice(_pos));
            _pos += value.Length;
        }

        public void AppendLine(string? value) {
            Append(value);
            Append(Environment.NewLine);
        }

        public void Append(char value) {
            EnsureCapacity(1);
            _chars[_pos++] = value;
        }

        public void Append(ReadOnlySpan<char> value) {
            EnsureCapacity(value.Length);
            value.CopyTo(_chars.Slice(_pos));
            _pos += value.Length;
        }

        public void Clear() {
            _pos = 0;
        }

        private void EnsureCapacity(int required) {
            int newTotal = _pos + required;
            if (newTotal <= _chars.Length) return;

            // Growth strategy: double or at least 256
            int newSize = Math.Max(_chars.Length * 2, newTotal);
            newSize = Math.Max(newSize, 256);

            var newArray = ArrayPool<char>.Shared.Rent(newSize);
            _chars.Slice(0, _pos).CopyTo(newArray);

            // Return old buffer if it came from pool
            ReturnToPool();

            _arrayFromPool = newArray;
            _chars = newArray;
        }

        public override string ToString() {
            return _chars.Slice(0, _pos).ToString();
        }

        // Critical: prevents leaks
        public void Dispose() {
            ReturnToPool();
            this = default; // Clear refs
        }

        private void ReturnToPool() {
            if (_arrayFromPool != null) {
                ArrayPool<char>.Shared.Return(_arrayFromPool, clearArray: false);
                _arrayFromPool = null;
            }
        }
    }
}
