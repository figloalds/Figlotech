using System;

namespace Figlotech.BDados.Attributes {
    /// <summary>
    /// Aggregates a field from a directly related dataobject
    /// </summary>
    public class AggregateFieldAttribute : AbstractAggregationAttribute {
        public string RemoteField;

        public AggregateFieldAttribute(Type remoteObjectType, string keyField, string remoteField) : base(remoteObjectType, keyField) {
            RemoteField = remoteField;
        }
    }
}