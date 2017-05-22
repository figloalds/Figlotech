using System;

namespace Figlotech.BDados.DataAccessAbstractions.Attributes {
    /// <summary>
    /// Aggregates a list of given type into this field/property based on a
    /// relation.
    /// </summary>
    public class AggregateListAttribute : Attribute {
        public string RemoteField;
        public Type RemoteObjectType;

        public AggregateListAttribute(Type remoteObjectType, string remoteField) {
            RemoteObjectType = remoteObjectType;
            RemoteField = remoteField;
        }
    }
}