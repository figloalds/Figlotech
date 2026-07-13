using System;
using System.Reflection;

namespace Figlotech.BDados.DataAccessAbstractions {
    public sealed class DefinitiveIdentifier {
        public DefinitiveIdentifier(MemberInfo member, string columnName, int projectionOrdinal, string resultAlias) {
            Member = member ?? throw new ArgumentNullException(nameof(member));
            ColumnName = DefinitivePlanValidation.RequireText(columnName, nameof(columnName));
            if (projectionOrdinal < 0) {
                throw new ArgumentOutOfRangeException(nameof(projectionOrdinal), projectionOrdinal, "Identifier projection ordinal must be non-negative.");
            }
            ProjectionOrdinal = projectionOrdinal;
            ResultAlias = DefinitivePlanValidation.RequireText(resultAlias, nameof(resultAlias));
            ClrType = DefinitivePlanValidation.GetMemberType(Member, nameof(member));
        }

        public MemberInfo Member { get; }
        public string ColumnName { get; }
        public Type ClrType { get; }
        public int ProjectionOrdinal { get; }
        public string ResultAlias { get; }
    }
}
