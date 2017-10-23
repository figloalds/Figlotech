using Figlotech.Core.BusinessModel;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.DataAccessAbstractions
{
    public class Rule<T> : IValidationRule<T> where T : IBusinessObject, new()
    {
        Func<T, ValidationErrors, ValidationErrors> validationFunction;

        public Rule(Func<T, ValidationErrors, ValidationErrors> rule) {
            validationFunction = rule;
        }

        public ValidationErrors Validate(T ObjectToValidate, ValidationErrors errs) {
            return validationFunction(ObjectToValidate, errs);
        }
    }
}
