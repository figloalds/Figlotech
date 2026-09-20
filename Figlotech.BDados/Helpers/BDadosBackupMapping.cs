using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Figlotech.BDados.Helpers {
    internal static class BDadosBackupMapping {
        // Bind current names first, then historical names. A current column wins if both it and
        // its old name exist in an archive; never let reflection or archive order decide the value.
        internal static T[] Bind<T>(string[] sourceNames, T[] targets, Func<T, string> name, Func<T, string> oldName) where T : class {
            var currentNames = new Dictionary<string, T>(StringComparer.OrdinalIgnoreCase);
            foreach (var target in targets) {
                if (!currentNames.TryAdd(name(target), target)) {
                    throw new InvalidDataException($"Ambiguous restore name '{name(target)}'.");
                }
            }
            if (sourceNames.Distinct(StringComparer.OrdinalIgnoreCase).Count() != sourceNames.Length) {
                throw new InvalidDataException("Backup names differ only by casing and cannot be mapped unambiguously.");
            }
            var result = new T[sourceNames.Length];
            var used = new HashSet<T>();
            for (int i = 0; i < sourceNames.Length; i++) {
                if (currentNames.TryGetValue(sourceNames[i], out var target)) {
                    result[i] = target;
                    used.Add(target);
                }
            }
            var aliases = targets.Where(t => !used.Contains(t) && !string.IsNullOrWhiteSpace(oldName(t)))
                .ToLookup(oldName, StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < sourceNames.Length; i++) {
                if (result[i] != null) continue;
                var matches = aliases[sourceNames[i]].ToArray();
                if (matches.Length > 1) {
                    throw new InvalidDataException($"Ambiguous OldName mapping for '{sourceNames[i]}'.");
                }
                if (matches.Length == 1) result[i] = matches[0];
            }
            return result;
        }
    }
}
