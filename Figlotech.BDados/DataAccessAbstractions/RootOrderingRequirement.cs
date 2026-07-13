using System;

namespace Figlotech.BDados.DataAccessAbstractions {
    public sealed class RootOrderingRequirement {
        public RootOrderingRequirement(int tableIndex, string columnName, int projectionOrdinal, string resultAlias) {
            if (tableIndex < 0) {
                throw new ArgumentOutOfRangeException(nameof(tableIndex), tableIndex, "Root ordering table index must be non-negative.");
            }
            if (projectionOrdinal < 0) {
                throw new ArgumentOutOfRangeException(nameof(projectionOrdinal), projectionOrdinal, "Root ordering projection ordinal must be non-negative.");
            }

            TableIndex = tableIndex;
            ColumnName = DefinitivePlanValidation.RequireText(columnName, nameof(columnName));
            ProjectionOrdinal = projectionOrdinal;
            ResultAlias = DefinitivePlanValidation.RequireText(resultAlias, nameof(resultAlias));
        }

        public int TableIndex { get; }
        public string ColumnName { get; }
        public int ProjectionOrdinal { get; }
        public string ResultAlias { get; }
    }
}
