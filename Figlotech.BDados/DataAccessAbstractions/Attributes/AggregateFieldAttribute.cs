using System;

namespace Figlotech.BDados.DataAccessAbstractions.Attributes {
    /// <summary>
    /// Aggregates a field from a directly related dataobject
    /// </summary>
    public class AggregateFieldAttribute : AbstractAggregationAttribute {
        public string RemoteField;
        public string ObjectKey;
        public Type RemoteObjectType;

        public AggregateFieldAttribute(Type remoteObjectType, string keyField, string remoteField) {
            RemoteField = remoteField;
            ObjectKey = keyField;
            RemoteObjectType = remoteObjectType;
        }
    }
}