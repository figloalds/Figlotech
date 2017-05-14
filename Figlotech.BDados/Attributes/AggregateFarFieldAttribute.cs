using System;

namespace Figlotech.BDados.Attributes {
    public class AggregateFarFieldAttribute : Attribute {
        public Type ImediateType;
        public Type FarType;
        public string FarKey;
        public string ImediateKey;
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