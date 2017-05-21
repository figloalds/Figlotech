using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.DataAccessAbstractions {
    public interface IPersistancePolicyAttribute {

        bool ShouldPersist();
    }
}
