using Figlotech.Core.BusinessModel;
using System.Collections.Generic;

namespace Figlotech.BDados.Business {
    public interface IBusinessValidation {

        List<ValidationErrors> Validate();
    }
}
