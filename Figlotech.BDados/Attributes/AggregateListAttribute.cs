using System;

namespace Figlotech.BDados.Attributes {
    public class AggregateListAttribute : Attribute {
        public string RemoteField;
        public Type RemoteObjectType;

        public AggregateListAttribute(Type remoteObjectType, string remoteField) {
            RemoteObjectType = remoteObjectType;
            RemoteField = remoteField;
        }
    }
}