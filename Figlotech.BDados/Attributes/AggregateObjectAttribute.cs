using System;

namespace Figlotech.BDados.Attributes {
    public class AggregateObjectAttribute : Attribute {
        public String ObjectKey;

        public AggregateObjectAttribute(string keyField) {
            ObjectKey = keyField;
        }
    }
}