using System;

namespace Figlotech.BDados.DataAccessAbstractions.Attributes {
    /// <summary>
    /// This is used by AggregateFieldAttribute
    /// </summary>
    public abstract class AbstractAggregationAttribute : Attribute {
        public string ObjectKey;
        public Type RemoteObjectType; 

        public AbstractAggregationAttribute(Type remoteObjectType, string keyField) {
            ObjectKey = keyField;
            RemoteObjectType = remoteObjectType;
        }
    }
}