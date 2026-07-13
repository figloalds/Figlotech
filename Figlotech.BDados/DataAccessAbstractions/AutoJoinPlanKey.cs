using System;

namespace Figlotech.BDados.DataAccessAbstractions {
    public readonly struct AutoJoinPlanKey : IEquatable<AutoJoinPlanKey> {
        public AutoJoinPlanKey(Type rootType, AggregateJoinShape shape, int formatVersion) {
            RootType = rootType ?? throw new ArgumentNullException(nameof(rootType));
            Shape = shape;
            if (formatVersion < 0) {
                throw new ArgumentOutOfRangeException(nameof(formatVersion), formatVersion, "Format version must be non-negative.");
            }
            FormatVersion = formatVersion;
        }

        public Type RootType { get; }
        public AggregateJoinShape Shape { get; }
        public int FormatVersion { get; }

        public bool Equals(AutoJoinPlanKey other) {
            return RootType == other.RootType
                && Shape == other.Shape
                && FormatVersion == other.FormatVersion;
        }

        public override bool Equals(object obj) {
            return obj is AutoJoinPlanKey other && Equals(other);
        }

        public override int GetHashCode() {
            unchecked {
                int hash = RootType == null ? 0 : RootType.GetHashCode();
                hash = (hash * 397) ^ (int)Shape;
                hash = (hash * 397) ^ FormatVersion;
                return hash;
            }
        }

        public static bool operator ==(AutoJoinPlanKey left, AutoJoinPlanKey right) {
            return left.Equals(right);
        }

        public static bool operator !=(AutoJoinPlanKey left, AutoJoinPlanKey right) {
            return !left.Equals(right);
        }
    }
}
