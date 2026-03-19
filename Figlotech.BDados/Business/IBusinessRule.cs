using Figlotech.Core.BusinessModel;
using System.Collections.Generic;

namespace Figlotech.BDados.Business {
    public interface IBusinessRule {

        List<ValidationErrors> Apply();
    }
}
