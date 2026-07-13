using System;
using System.Collections.Generic;
using System.Collections.Immutable;

namespace Figlotech.BDados.DataAccessAbstractions {
    public readonly struct AggregatePath : IEquatable<AggregatePath>, IComparable<AggregatePath> {
        private readonly ImmutableArray<string> _segments;

        public AggregatePath(IEnumerable<string> segments) {
            if (segments == null) {
                throw new ArgumentNullException(nameof(segments));
            }

            _segments = ImmutableArray.CreateRange(segments);
            for (int i = 0; i < Segments.Length; i++) {
                if (String.IsNullOrWhiteSpace(Segments[i])) {
                    throw new ArgumentException("Aggregate path segments must be non-empty.", nameof(segments));
                }
            }
        }

        public ImmutableArray<string> Segments => _segments.IsDefault ? ImmutableArray<string>.Empty : _segments;

        public bool Equals(AggregatePath other) {
            ImmutableArray<string> segments = Segments;
            ImmutableArray<string> otherSegments = other.Segments;
            if (segments.Length != otherSegments.Length) {
                return false;
            }

            for (int i = 0; i < segments.Length; i++) {
                if (!String.Equals(segments[i], otherSegments[i], StringComparison.Ordinal)) {
                    return false;
                }
            }
            return true;
        }

        public int CompareTo(AggregatePath other) {
            ImmutableArray<string> segments = Segments;
            ImmutableArray<string> otherSegments = other.Segments;
            int commonLength = Math.Min(segments.Length, otherSegments.Length);
            for (int i = 0; i < commonLength; i++) {
                int comparison = StringComparer.Ordinal.Compare(segments[i], otherSegments[i]);
                if (comparison != 0) {
                    return comparison;
                }
            }

            return segments.Length.CompareTo(otherSegments.Length);
        }

        public override bool Equals(object obj) {
            return obj is AggregatePath other && Equals(other);
        }

        public override int GetHashCode() {
            ImmutableArray<string> segments = Segments;
            unchecked {
                int hash = 17;
                for (int i = 0; i < segments.Length; i++) {
                    hash = (hash * 31) + StringComparer.Ordinal.GetHashCode(segments[i]);
                }
                return hash;
            }
        }

        public override string ToString() {
            return String.Join(".", Segments);
        }

        public static bool operator ==(AggregatePath left, AggregatePath right) {
            return left.Equals(right);
        }

        public static bool operator !=(AggregatePath left, AggregatePath right) {
            return !left.Equals(right);
        }
    }
}
