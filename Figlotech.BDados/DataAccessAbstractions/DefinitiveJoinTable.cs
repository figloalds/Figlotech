using System;
using System.Collections.Generic;
using System.Collections.Immutable;

namespace Figlotech.BDados.DataAccessAbstractions {
    public sealed class DefinitiveJoinTable {
        public DefinitiveJoinTable(
            Type entityType,
            string tableName,
            string alias,
            string prefix,
            JoinType joinKind,
            string joinPredicate,
            DefinitiveIdentifier identifier,
            IEnumerable<string> projectedColumns) {
            EntityType = entityType ?? throw new ArgumentNullException(nameof(entityType));
            TableName = DefinitivePlanValidation.RequireText(tableName, nameof(tableName));
            Alias = DefinitivePlanValidation.RequireText(alias, nameof(alias));
            Prefix = DefinitivePlanValidation.RequireText(prefix, nameof(prefix));
            JoinKind = joinKind;
            JoinPredicate = joinPredicate;
            Identifier = identifier ?? throw new ArgumentNullException(nameof(identifier));
            if (projectedColumns == null) {
                throw new ArgumentNullException(nameof(projectedColumns));
            }

            ProjectedColumns = ImmutableArray.CreateRange(projectedColumns);
            for (int i = 0; i < ProjectedColumns.Length; i++) {
                DefinitivePlanValidation.RequireText(ProjectedColumns[i], nameof(projectedColumns));
            }
        }

        public Type EntityType { get; }
        public string TableName { get; }
        public string Alias { get; }
        public string Prefix { get; }
        public JoinType JoinKind { get; }
        public string JoinPredicate { get; }
        public DefinitiveIdentifier Identifier { get; }
        public ImmutableArray<string> ProjectedColumns { get; }
    }
}
