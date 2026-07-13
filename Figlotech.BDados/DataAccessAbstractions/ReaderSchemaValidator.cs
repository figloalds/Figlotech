using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace Figlotech.BDados.DataAccessAbstractions {
    /// <summary>
    /// Validates that a data reader schema exactly matches a definitive join plan projection.
    /// SQL result aliases are compared without case because unquoted SQL identifiers may be case-folded by providers.
    /// </summary>
    public static class ReaderSchemaValidator {
        public static void Validate(IDataRecord record, DefinitiveJoinPlan plan) {
            if (record == null) {
                throw new ArgumentNullException(nameof(record));
            }
            if (plan == null) {
                throw new ArgumentNullException(nameof(plan));
            }

            string[] expectedAliases = SnapshotExpectedAliases(plan);
            string[] actualAliases = SnapshotActualAliases(record);

            for (int i = 0; i < actualAliases.Length; i++) {
                if (String.IsNullOrWhiteSpace(actualAliases[i])) {
                    ThrowValidationFailure($"Actual result alias at index {i} is null or blank.", expectedAliases, actualAliases);
                }
            }

            var actualIndicesByAlias = new Dictionary<string, List<int>>(StringComparer.OrdinalIgnoreCase);
            var duplicateAliases = new List<string>();
            for (int i = 0; i < actualAliases.Length; i++) {
                string actualAlias = actualAliases[i]!;
                if (!actualIndicesByAlias.TryGetValue(actualAlias, out List<int> indices)) {
                    indices = new List<int>();
                    actualIndicesByAlias.Add(actualAlias, indices);
                }
                indices.Add(i);
                if (indices.Count == 2) {
                    duplicateAliases.Add(actualAlias);
                }
            }

            if (duplicateAliases.Count > 0) {
                var duplicates = new List<string>();
                for (int i = 0; i < duplicateAliases.Count; i++) {
                    string duplicateAlias = duplicateAliases[i];
                    duplicates.Add(duplicateAlias + " (indices " + String.Join(", ", actualIndicesByAlias[duplicateAlias]) + ")");
                }
                ThrowValidationFailure("Duplicate actual result aliases ignoring SQL identifier case: " + String.Join("; ", duplicates) + ".", expectedAliases, actualAliases);
            }

            var expectedAliasesSet = new HashSet<string>(expectedAliases, StringComparer.OrdinalIgnoreCase);
            var actualAliasesSet = new HashSet<string>(actualAliases!, StringComparer.OrdinalIgnoreCase);
            var missingAliases = new List<string>();
            for (int i = 0; i < expectedAliases.Length; i++) {
                if (!actualAliasesSet.Contains(expectedAliases[i])) {
                    missingAliases.Add(expectedAliases[i]);
                }
            }

            var unexpectedAliases = new List<string>();
            for (int i = 0; i < actualAliases.Length; i++) {
                string actualAlias = actualAliases[i]!;
                if (!expectedAliasesSet.Contains(actualAlias)) {
                    unexpectedAliases.Add(actualAlias);
                }
            }

            if (missingAliases.Count > 0 || unexpectedAliases.Count > 0) {
                var message = new StringBuilder();
                message.Append("Reader schema has ").Append(actualAliases.Length).Append(" aliases, but the plan requires ").Append(expectedAliases.Length).Append(".");
                if (missingAliases.Count > 0) {
                    message.Append(" Missing expected aliases: ").Append(String.Join(", ", missingAliases)).Append('.');
                }
                if (unexpectedAliases.Count > 0) {
                    message.Append(" Unexpected actual aliases: ").Append(String.Join(", ", unexpectedAliases)).Append('.');
                }
                ThrowValidationFailure(message.ToString(), expectedAliases, actualAliases);
            }

            for (int i = 0; i < expectedAliases.Length; i++) {
                if (!String.Equals(expectedAliases[i], actualAliases[i], StringComparison.OrdinalIgnoreCase)) {
                    ThrowValidationFailure($"Reader schema order mismatch at index {i}: expected '{expectedAliases[i]}', actual '{actualAliases[i]}'.", expectedAliases, actualAliases);
                }
            }
        }

        private static string[] SnapshotExpectedAliases(DefinitiveJoinPlan plan) {
            var expectedAliases = new string[plan.Projection.Length];
            for (int i = 0; i < plan.Projection.Length; i++) {
                expectedAliases[i] = plan.Projection[i].ResultAlias;
            }
            return expectedAliases;
        }

        private static string[] SnapshotActualAliases(IDataRecord record) {
            var actualAliases = new string[record.FieldCount];
            for (int i = 0; i < actualAliases.Length; i++) {
                actualAliases[i] = record.GetName(i);
            }
            return actualAliases;
        }

        private static void ThrowValidationFailure(string message, IReadOnlyList<string> expectedAliases, IReadOnlyList<string> actualAliases) {
            throw new ArgumentException(message
                + " Expected ordered aliases: " + FormatSchema(expectedAliases)
                + " Actual ordered aliases: " + FormatSchema(actualAliases), "record");
        }

        private static string FormatSchema(IReadOnlyList<string> aliases) {
            var builder = new StringBuilder("[");
            for (int i = 0; i < aliases.Count; i++) {
                if (i > 0) {
                    builder.Append(", ");
                }
                builder.Append(i).Append(":");
                if (aliases[i] == null) {
                    builder.Append("<null>");
                } else if (String.IsNullOrWhiteSpace(aliases[i])) {
                    builder.Append("<blank>");
                } else {
                    builder.Append(aliases[i]);
                }
            }
            return builder.Append(']').ToString();
        }
    }
}
