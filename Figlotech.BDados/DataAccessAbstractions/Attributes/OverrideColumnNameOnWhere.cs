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
    public sealed class OverrideColumnNameOnWhere : Attribute {
        public string Name { get; set; }
        public OverrideColumnNameOnWhere(string name){
            this.Name = name;
        }
    }
}