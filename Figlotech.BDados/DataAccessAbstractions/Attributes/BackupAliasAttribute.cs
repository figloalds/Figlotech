using System;

namespace Figlotech.BDados.DataAccessAbstractions.Attributes {
    /// <summary>
    /// A permanent historical backup name. Unlike OldName, aliases do not affect DDL migrations.
    /// Keep every historical alias when a table, member or custom binary type is renamed again.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct | AttributeTargets.Property | AttributeTargets.Field,
        AllowMultiple = true, Inherited = true)]
    public sealed class BackupAliasAttribute : Attribute {
        public string Name { get; }

        public BackupAliasAttribute(string name) {
            if (string.IsNullOrWhiteSpace(name)) throw new ArgumentException("A backup alias must not be blank.", nameof(name));
            Name = name;
        }
    }
}
