using System;

namespace Figlotech.BDados.Attributes {
    public class AggregateNatural : Attribute {
        public string RemoteField;
        public Type RemoteObjectType;

        public AggregateNatural(Type remoteObjectType, string remoteField) {
            RemoteObjectType = remoteObjectType;
            RemoteField = remoteField;
        }
    }
}