using System;
using System.Reflection;

namespace Figlotech.BDados.DataAccessAbstractions {
    public sealed class DefinitiveProjectionColumn {
        public DefinitiveProjectionColumn(int ordinal, int tableIndex, string sourceColumn, string resultAlias, MemberInfo destinationMember) {
            if (ordinal < 0) {
                throw new ArgumentOutOfRangeException(nameof(ordinal), ordinal, "Projection ordinal must be non-negative.");
            }
            if (tableIndex < 0) {
                throw new ArgumentOutOfRangeException(nameof(tableIndex), tableIndex, "Projection table index must be non-negative.");
            }

            Ordinal = ordinal;
            TableIndex = tableIndex;
            SourceColumn = DefinitivePlanValidation.RequireText(sourceColumn, nameof(sourceColumn));
            ResultAlias = DefinitivePlanValidation.RequireText(resultAlias, nameof(resultAlias));
            DestinationMember = destinationMember;
            DestinationType = destinationMember == null ? null : DefinitivePlanValidation.GetMemberType(destinationMember, nameof(destinationMember));
        }

        public int Ordinal { get; }
        public int TableIndex { get; }
        public string SourceColumn { get; }
        public string ResultAlias { get; }
        public MemberInfo DestinationMember { get; }
        public Type DestinationType { get; }
    }
}
