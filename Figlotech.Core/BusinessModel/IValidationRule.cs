using System.Collections.Generic;

namespace Figlotech.Core.BusinessModel {

    public interface IValidationRule  {
        IEnumerable<ValidationError> Validate(IBusinessObject ObjectToValidate);
    }

    public interface IValidationRule<T> : IValidationRule where T : IBusinessObject, new() {
        IEnumerable<ValidationError> Validate(T ObjectToValidate);
    }
}
