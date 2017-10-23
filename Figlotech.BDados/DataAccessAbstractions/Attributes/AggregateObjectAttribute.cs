using System;

namespace Figlotech.BDados.DataAccessAbstractions.Attributes {
    /// <summary>
    /// Uses the specified relation to load an entire other dataobject into 
    /// the annotated field/property
    /// </summary>
    public class AggregateObjectAttribute : Attribute {
        public String ObjectKey;

        public AggregateObjectAttribute(string keyField) {
            ObjectKey = keyField;
        }
    }
}