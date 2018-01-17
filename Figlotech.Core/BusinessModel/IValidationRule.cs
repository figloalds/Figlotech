namespace Figlotech.Core.BusinessModel {

    public interface IValidationRule {
        ValidationErrors Validate(IBusinessObject ObjectToValidate, ValidationErrors errors);
    }

    public interface IValidationRule<T> : IValidationRule where T : IBusinessObject, new() {
        ValidationErrors Validate(T ObjectToValidate, ValidationErrors errors);
    }
}
