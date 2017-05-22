using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.DataAccessAbstractions.Attributes {
    /// <summary>
    /// This attribute works exactly the same as NoUpdateAttribute
    /// </summary>
    public class ReadOnlyAttribute : Attribute, IPersistancePolicyAttribute {
        public ReadOnlyAttribute() { }

        public bool ShouldPersist() {
            return false;
        }
    }
}
