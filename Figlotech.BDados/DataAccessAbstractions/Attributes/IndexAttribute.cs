
using System;

namespace Figlotech.BDados.DataAccessAbstractions.Attributes {
    /// <summary>
    /// Instructs the StructureChecker to create an index on this column.
    /// When used with no parameters, creates a simple single-column index.
    /// Use CompositeWith to specify additional column names for a composite index.
    /// </summary>
    [AttributeUsage(AttributeTargets.Field | AttributeTargets.Property, AllowMultiple = true)]
    public sealed class IndexAttribute : Attribute {
        public bool IsUnique { get; set; }
        public string[] CompositeWith { get; set; }
        public string Name { get; set; }

        public IndexAttribute() { }

        public IndexAttribute(params string[] compositeWith) {
            CompositeWith = compositeWith;
        }
    }
}
