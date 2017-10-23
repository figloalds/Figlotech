namespace Figlotech.Core.BusinessModel {
    public interface IValidationRule<T> where T : IBusinessObject, new() {
        ValidationErrors Validate(T ObjectToValidate, ValidationErrors errors);
    }
}
