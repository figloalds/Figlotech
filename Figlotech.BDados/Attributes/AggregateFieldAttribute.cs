using System;

namespace Figlotech.BDados.Attributes {
    public class AggregateFieldAttribute : AbstractAggregationAttribute {
        public string RemoteField;

        public AggregateFieldAttribute(Type remoteObjectType, string keyField, string remoteField) : base(remoteObjectType, keyField) {
            RemoteField = remoteField;
        }
    }
}