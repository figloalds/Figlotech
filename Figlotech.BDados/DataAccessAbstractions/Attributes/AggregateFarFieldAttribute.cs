using System;

namespace Figlotech.BDados.DataAccessAbstractions.Attributes {
    /// <summary>
    /// Specifying this attribute allows you to tell BDados that said field/property in a class 
    /// should be loaded from a DataObject that is not directly related to this one.
    /// ImediateType = The type to which this one is related.
    /// ImmediateKey = The key that connects this object to its immediate
    /// FarType = The far data object who contains the field you want
    /// FarKey = The key that relates your immediate type with the far one
    /// FarField = The far field from which to load information.
    /// </summary>
    public class AggregateFarFieldAttribute : Attribute {
        public Type ImediateType;
        public string ImediateKey;
        public Type FarType;
        public string FarKey;
        public string FarField;

        public AggregateFarFieldAttribute(Type imediateType, string imediateKey, Type farType, string farkey, string farField){
            ImediateType = imediateType;
            FarType = farType;
            FarKey = farkey;
            ImediateKey = imediateKey;
            FarField = farField;
        }
    }
}