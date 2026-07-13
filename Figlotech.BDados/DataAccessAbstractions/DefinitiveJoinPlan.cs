using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Reflection;
using System.Security.Cryptography;
using System.Text;

namespace Figlotech.BDados.DataAccessAbstractions {
    public sealed class DefinitiveJoinPlan {
        public DefinitiveJoinPlan(
            Type rootType,
            AggregateJoinShape shape,
            int rootTableIndex,
            IEnumerable<DefinitiveJoinTable> tables,
            IEnumerable<DefinitiveJoinRelation> relations,
            IEnumerable<DefinitiveProjectionColumn> projection,
            IEnumerable<KeyValuePair<AggregatePath, string>> aliasByPath,
            IEnumerable<KeyValuePair<string, int>> tableIndexByAlias,
            RootOrderingRequirement rootOrdering,
            int formatVersion) {
            RootType = rootType ?? throw new ArgumentNullException(nameof(rootType));
            if (formatVersion < 0) {
                throw new ArgumentOutOfRangeException(nameof(formatVersion), formatVersion, "Format version must be non-negative.");
            }
            if (tables == null) {
                throw new ArgumentNullException(nameof(tables));
            }
            if (relations == null) {
                throw new ArgumentNullException(nameof(relations));
            }
            if (projection == null) {
                throw new ArgumentNullException(nameof(projection));
            }
            if (aliasByPath == null) {
                throw new ArgumentNullException(nameof(aliasByPath));
            }
            if (tableIndexByAlias == null) {
                throw new ArgumentNullException(nameof(tableIndexByAlias));
            }
            RootOrdering = rootOrdering ?? throw new ArgumentNullException(nameof(rootOrdering));

            if (!Enum.IsDefined(typeof(AggregateJoinShape), shape)) {
                throw new ArgumentOutOfRangeException(nameof(shape), shape, "Aggregate join shape must be a defined value.");
            }
            Shape = shape;
            FormatVersion = formatVersion;
            Tables = ImmutableArray.CreateRange(tables);
            Relations = ImmutableArray.CreateRange(relations);
            Projection = ImmutableArray.CreateRange(projection);

            ValidateTables();
            if (rootTableIndex < 0 || rootTableIndex >= Tables.Length) {
                throw new ArgumentOutOfRangeException(nameof(rootTableIndex), rootTableIndex, "Root table index must refer to a table in the plan.");
            }
            if (Tables[rootTableIndex].EntityType != RootType) {
                throw new ArgumentException($"Root table at index {rootTableIndex} has entity type '{Tables[rootTableIndex].EntityType}', which must match root type '{RootType}'.", nameof(rootTableIndex));
            }
            RootTableIndex = rootTableIndex;

            ValidateRelations();
            ValidateProjection();
            TableIndexByAlias = SnapshotTableIndexByAlias(tableIndexByAlias);
            AliasByPath = SnapshotAliasByPath(aliasByPath);
            ValidateRootOrdering();
            StructuralSignature = ComputeStructuralSignature();
        }

        public Type RootType { get; }
        public AggregateJoinShape Shape { get; }
        public int RootTableIndex { get; }
        public ImmutableArray<DefinitiveJoinTable> Tables { get; }
        public ImmutableArray<DefinitiveJoinRelation> Relations { get; }
        public ImmutableArray<DefinitiveProjectionColumn> Projection { get; }
        public ImmutableDictionary<AggregatePath, string> AliasByPath { get; }
        public ImmutableDictionary<string, int> TableIndexByAlias { get; }
        public RootOrderingRequirement RootOrdering { get; }
        public int FormatVersion { get; }

        /// <summary>
        /// Deterministic diagnostics and cache metadata. It is not a collision-proof substitute for structural equality.
        /// </summary>
        public string StructuralSignature { get; }

        private void ValidateTables() {
            if (Tables.Length == 0) {
                throw new ArgumentException("A definitive join plan must contain at least one table.", nameof(Tables));
            }

            var aliases = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var prefixes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < Tables.Length; i++) {
                DefinitiveJoinTable table = Tables[i];
                if (table == null) {
                    throw new ArgumentException("Tables cannot contain null values.", nameof(Tables));
                }
                if (!Enum.IsDefined(typeof(JoinType), table.JoinKind)) {
                    throw new ArgumentOutOfRangeException(nameof(Tables), table.JoinKind, $"Table alias '{table.Alias}' has an undefined join kind.");
                }
                if (!aliases.Add(table.Alias)) {
                    throw new ArgumentException($"Table alias '{table.Alias}' is duplicated ignoring SQL identifier case.", nameof(Tables));
                }
                if (!prefixes.Add(table.Prefix)) {
                    throw new ArgumentException($"Table prefix '{table.Prefix}' is duplicated ignoring SQL identifier case.", nameof(Tables));
                }
            }
        }

        private void ValidateRelations() {
            for (int i = 0; i < Relations.Length; i++) {
                DefinitiveJoinRelation relation = Relations[i];
                if (relation == null) {
                    throw new ArgumentException("Relations cannot contain null values.", nameof(Relations));
                }
                if (!Enum.IsDefined(typeof(AggregateBuildOptions), relation.BuildKind)) {
                    throw new ArgumentOutOfRangeException(nameof(Relations), relation.BuildKind, $"Relation at index {i} has an undefined aggregate build option.");
                }
                if (relation.ParentTableIndex >= Tables.Length) {
                    throw new ArgumentOutOfRangeException(nameof(Relations), $"Relation at index {i} has parent table index {relation.ParentTableIndex}, which is outside the plan table range.");
                }
                if (relation.ChildTableIndex >= Tables.Length) {
                    throw new ArgumentOutOfRangeException(nameof(Relations), $"Relation at index {i} has child table index {relation.ChildTableIndex}, which is outside the plan table range.");
                }
            }
        }

        private void ValidateProjection() {
            var resultAliases = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < Projection.Length; i++) {
                DefinitiveProjectionColumn column = Projection[i];
                if (column == null) {
                    throw new ArgumentException("Projection cannot contain null values.", nameof(Projection));
                }
                if (column.Ordinal != i) {
                    throw new ArgumentException("Projection ordinals must be contiguous and begin at zero in projection order.", nameof(Projection));
                }
                if (column.TableIndex >= Tables.Length) {
                    throw new ArgumentOutOfRangeException(nameof(Projection), $"Projection column at ordinal {column.Ordinal} refers to table index {column.TableIndex}, which is outside the plan table range.");
                }
                if (!resultAliases.Add(column.ResultAlias)) {
                    throw new ArgumentException($"Projection result alias '{column.ResultAlias}' is duplicated.", nameof(Projection));
                }
            }

            for (int i = 0; i < Tables.Length; i++) {
                DefinitiveIdentifier identifier = Tables[i].Identifier;
                if (identifier.ProjectionOrdinal >= Projection.Length) {
                    throw new ArgumentOutOfRangeException(nameof(Projection), $"Identifier for table alias '{Tables[i].Alias}' refers to projection ordinal {identifier.ProjectionOrdinal}, which is outside the projection range.");
                }

                DefinitiveProjectionColumn identifierProjection = Projection[identifier.ProjectionOrdinal];
                if (identifierProjection.TableIndex != i
                    || !String.Equals(identifierProjection.SourceColumn, identifier.ColumnName, StringComparison.Ordinal)
                    || !String.Equals(identifierProjection.ResultAlias, identifier.ResultAlias, StringComparison.Ordinal)) {
                    throw new ArgumentException($"Identifier for table alias '{Tables[i].Alias}' must match its projection column.", nameof(Projection));
                }
            }
        }

        private ImmutableDictionary<AggregatePath, string> SnapshotAliasByPath(IEnumerable<KeyValuePair<AggregatePath, string>> aliasByPath) {
            var aliases = ImmutableDictionary.CreateBuilder<AggregatePath, string>();
            foreach (KeyValuePair<AggregatePath, string> entry in aliasByPath) {
                string alias = DefinitivePlanValidation.RequireText(entry.Value, nameof(aliasByPath));
                if (!TableIndexByAlias.ContainsKey(alias)) {
                    throw new ArgumentException($"Aggregate path alias '{alias}' does not refer to a table alias in this plan.", nameof(aliasByPath));
                }
                if (aliases.ContainsKey(entry.Key)) {
                    throw new ArgumentException($"Aggregate path '{entry.Key}' is duplicated.", nameof(aliasByPath));
                }
                aliases.Add(entry.Key, alias);
            }
            return aliases.ToImmutable();
        }

        private ImmutableDictionary<string, int> SnapshotTableIndexByAlias(IEnumerable<KeyValuePair<string, int>> tableIndexByAlias) {
            var indices = ImmutableDictionary.CreateBuilder<string, int>(StringComparer.OrdinalIgnoreCase);
            foreach (KeyValuePair<string, int> entry in tableIndexByAlias) {
                string alias = DefinitivePlanValidation.RequireText(entry.Key, nameof(tableIndexByAlias));
                if (entry.Value < 0 || entry.Value >= Tables.Length) {
                    throw new ArgumentOutOfRangeException(nameof(tableIndexByAlias), $"Table alias '{alias}' maps to index {entry.Value}, which is outside the plan table range.");
                }
                if (indices.ContainsKey(alias)) {
                    throw new ArgumentException($"Table index mapping contains duplicate alias '{alias}'.", nameof(tableIndexByAlias));
                }
                indices.Add(alias, entry.Value);
            }

            if (indices.Count != Tables.Length) {
                throw new ArgumentException("Table index mappings must contain exactly one entry for every table alias.", nameof(tableIndexByAlias));
            }
            for (int i = 0; i < Tables.Length; i++) {
                if (!indices.TryGetValue(Tables[i].Alias, out int mappedIndex) || mappedIndex != i) {
                    throw new ArgumentException($"Table index mapping for alias '{Tables[i].Alias}' must equal table index {i}.", nameof(tableIndexByAlias));
                }
            }
            return indices.ToImmutable();
        }

        private void ValidateRootOrdering() {
            if (RootOrdering.TableIndex != RootTableIndex) {
                throw new ArgumentException("Root ordering must refer to the root table index.", nameof(RootOrdering));
            }
            if (RootOrdering.ProjectionOrdinal >= Projection.Length) {
                throw new ArgumentOutOfRangeException(nameof(RootOrdering), $"Root ordering projection ordinal {RootOrdering.ProjectionOrdinal} is outside the projection range.");
            }

            DefinitiveProjectionColumn orderingProjection = Projection[RootOrdering.ProjectionOrdinal];
            if (orderingProjection.TableIndex != RootTableIndex
                || !String.Equals(orderingProjection.SourceColumn, RootOrdering.ColumnName, StringComparison.Ordinal)
                || !String.Equals(orderingProjection.ResultAlias, RootOrdering.ResultAlias, StringComparison.Ordinal)) {
                throw new ArgumentException("Root ordering must match a projection column on the root table.", nameof(RootOrdering));
            }
        }

        private string ComputeStructuralSignature() {
            var builder = new DefinitivePlanSignatureBuilder();
            builder.Add(RootType);
            builder.Add((int)Shape);
            builder.Add(RootTableIndex);
            builder.Add(FormatVersion);

            builder.Add(Tables.Length);
            for (int i = 0; i < Tables.Length; i++) {
                DefinitiveJoinTable table = Tables[i];
                builder.Add(table.EntityType);
                builder.Add(table.TableName);
                builder.Add(table.Alias);
                builder.Add(table.Prefix);
                builder.Add((int)table.JoinKind);
                builder.Add(table.JoinPredicate);
                AddIdentifier(builder, table.Identifier);
                builder.Add(table.ProjectedColumns.Length);
                for (int j = 0; j < table.ProjectedColumns.Length; j++) {
                    builder.Add(table.ProjectedColumns[j]);
                }
            }

            builder.Add(Relations.Length);
            for (int i = 0; i < Relations.Length; i++) {
                DefinitiveJoinRelation relation = Relations[i];
                builder.Add(relation.ParentTableIndex);
                builder.Add(relation.ChildTableIndex);
                builder.Add(relation.ParentKey);
                builder.Add(relation.ChildKey);
                builder.Add((int)relation.BuildKind);
                builder.Add(relation.TargetMember);
                builder.Add(relation.SourceFields.Length);
                for (int j = 0; j < relation.SourceFields.Length; j++) {
                    builder.Add(relation.SourceFields[j]);
                }
            }

            builder.Add(Projection.Length);
            for (int i = 0; i < Projection.Length; i++) {
                DefinitiveProjectionColumn column = Projection[i];
                builder.Add(column.Ordinal);
                builder.Add(column.TableIndex);
                builder.Add(column.SourceColumn);
                builder.Add(column.ResultAlias);
                builder.Add(column.DestinationMember);
                builder.Add(column.DestinationType);
            }

            var aliasEntries = new List<KeyValuePair<AggregatePath, string>>(AliasByPath);
            aliasEntries.Sort((left, right) => left.Key.CompareTo(right.Key));
            builder.Add(aliasEntries.Count);
            for (int i = 0; i < aliasEntries.Count; i++) {
                AggregatePath path = aliasEntries[i].Key;
                builder.Add(path.Segments.Length);
                for (int j = 0; j < path.Segments.Length; j++) {
                    builder.Add(path.Segments[j]);
                }
                builder.Add(aliasEntries[i].Value);
            }

            var indexEntries = new List<KeyValuePair<string, int>>(TableIndexByAlias);
            indexEntries.Sort((left, right) => StringComparer.Ordinal.Compare(left.Key, right.Key));
            builder.Add(indexEntries.Count);
            for (int i = 0; i < indexEntries.Count; i++) {
                builder.Add(indexEntries[i].Key);
                builder.Add(indexEntries[i].Value);
            }

            builder.Add(RootOrdering.TableIndex);
            builder.Add(RootOrdering.ColumnName);
            builder.Add(RootOrdering.ProjectionOrdinal);
            builder.Add(RootOrdering.ResultAlias);
            return builder.ComputeHash();
        }

        private static void AddIdentifier(DefinitivePlanSignatureBuilder builder, DefinitiveIdentifier identifier) {
            builder.Add(identifier.Member);
            builder.Add(identifier.ColumnName);
            builder.Add(identifier.ClrType);
            builder.Add(identifier.ProjectionOrdinal);
            builder.Add(identifier.ResultAlias);
        }
    }

    internal static class DefinitivePlanValidation {
        public static string RequireText(string value, string parameterName) {
            if (String.IsNullOrWhiteSpace(value)) {
                throw new ArgumentException("Value must be non-empty.", parameterName);
            }
            return value;
        }

        public static Type GetMemberType(MemberInfo member, string parameterName) {
            if (member is PropertyInfo property) {
                return property.PropertyType;
            }
            if (member is FieldInfo field) {
                return field.FieldType;
            }
            throw new ArgumentException("Member must be a field or property.", parameterName);
        }
    }

    internal sealed class DefinitivePlanSignatureBuilder {
        private readonly StringBuilder _builder = new StringBuilder();

        public void Add(string value) {
            if (value == null) {
                _builder.Append("-1:");
                return;
            }
            _builder.Append(value.Length).Append(':').Append(value);
        }

        public void Add(int value) {
            Add(value.ToString(System.Globalization.CultureInfo.InvariantCulture));
        }

        public void Add(Type value) {
            Add(value == null ? null : value.AssemblyQualifiedName ?? value.FullName ?? value.Name);
        }

        public void Add(MemberInfo value) {
            if (value == null) {
                Add((string)null);
                return;
            }
            Add(value.DeclaringType);
            Add((int)value.MemberType);
            Add(value.Name);
            if (value is PropertyInfo property) {
                Add(property.PropertyType);
            } else if (value is FieldInfo field) {
                Add(field.FieldType);
            }
        }

        public string ComputeHash() {
            using (SHA256 sha256 = SHA256.Create()) {
                byte[] bytes = Encoding.UTF8.GetBytes(_builder.ToString());
                byte[] hash = sha256.ComputeHash(bytes);
                var result = new StringBuilder(hash.Length * 2);
                for (int i = 0; i < hash.Length; i++) {
                    result.Append(hash[i].ToString("x2", System.Globalization.CultureInfo.InvariantCulture));
                }
                return result.ToString();
            }
        }
    }
}
