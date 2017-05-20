using Figlotech.BDados.DataAccessAbstractions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Business {
    public interface  IBusinessRule {

        List<ValidationErrors> Apply();
    }
}
