using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.DataAccessAbstractions.Attributes {
    /// <summary>
    /// This attribute works exactly the same as NoUpdateAttribute
    /// </summary>
    public class ReadOnlyAttribute : Attribute, IPersistencePolicyAttribute {
        public ReadOnlyAttribute() { }

        public bool ShouldPersist() {
            return false;
        }
    }
}
