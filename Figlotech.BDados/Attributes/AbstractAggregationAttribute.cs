using System;

namespace Figlotech.BDados.Attributes {
    public abstract class AbstractAggregationAttribute : Attribute {
        public string ObjectKey;
        public Type RemoteObjectType; 

        public AbstractAggregationAttribute(Type remoteObjectType, string keyField) {
            ObjectKey = keyField;
            RemoteObjectType = remoteObjectType;
        }
    }
}