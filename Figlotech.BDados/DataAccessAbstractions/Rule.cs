using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.DataAccessAbstractions
{
    public class Rule<T> : IValidationRule<T> where T : IDataObject, new()
    {
        Func<IBusinessObject<T>, ValidationErrors, ValidationErrors> validationFunction;

        public Rule(Func<IBusinessObject<T>, ValidationErrors, ValidationErrors> rule) {
            validationFunction = rule;
        }

        public ValidationErrors Validate(IBusinessObject<T> ObjectToValidate, ValidationErrors errs) {
            return validationFunction(ObjectToValidate, errs);
        }
    }
}
