using Figlotech.Core.BusinessModel;
using System;
using System.Collections.Generic;

namespace Figlotech.BDados.DataAccessAbstractions {
    public sealed class Rule<T> : IValidationRule<T> where T : IBusinessObject, new() {
        readonly Func<T, IEnumerable<ValidationError>> validationFunction;

        public Rule(Func<T, IEnumerable<ValidationError>> rule) {
            validationFunction = rule;
        }

        public Rule(Func<T, ValidationError> rule) {
            validationFunction = (input) => new[] { rule?.Invoke(input) };
        }

        public Rule(Action<T, ValidationErrors> rule) {
            validationFunction = (input) => {
                var errs = new ValidationErrors();
                rule?.Invoke(input, errs);
                return errs;
            };
        }

        public IEnumerable<ValidationError> Validate(T ObjectToValidate) {
            return validationFunction(ObjectToValidate);
        }

        public IEnumerable<ValidationError> Validate(IBusinessObject ObjectToValidate) {
            var errors = new ValidationErrors();
            if (ObjectToValidate is T validationTarget) {
                errors.AddRange(Validate(validationTarget));
            }
            return errors;
        }
    }
}
