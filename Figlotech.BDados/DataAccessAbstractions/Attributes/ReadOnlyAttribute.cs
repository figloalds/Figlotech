using System;

namespace Figlotech.BDados.DataAccessAbstractions.Attributes {
    /// <summary>
    /// This attribute works exactly the same as NoUpdateAttribute
    /// </summary>
    public sealed class ReadOnlyAttribute : Attribute, IPersistencePolicyAttribute {
        public ReadOnlyAttribute() { }

        public bool ShouldPersist() {
            return false;
        }
    }
}
