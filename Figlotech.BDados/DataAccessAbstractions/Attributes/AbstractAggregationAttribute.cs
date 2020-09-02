using System;

namespace Figlotech.BDados.DataAccessAbstractions.Attributes {
    /// <summary>
    /// This is used by AggregateFieldAttribute
    /// </summary>
    public abstract class AbstractAggregationAttribute : Attribute {
        public string Flags { get; set; } = "full";
        string[] _explodedFlags = null;
        public string[] ExplodedFlags => _explodedFlags ?? (_explodedFlags = Flags.ToLower().Split(','));

        public AbstractAggregationAttribute() {
        }
    }
}