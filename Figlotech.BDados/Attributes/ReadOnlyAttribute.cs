using Figlotech.BDados.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Attributes {
    public class ReadOnlyAttribute : Attribute, IPersistancePolicyAttribute {
        public ReadOnlyAttribute() { }

        public bool ShouldPersist() {
            return false;
        }
    }
}
