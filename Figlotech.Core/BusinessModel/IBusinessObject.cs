namespace Figlotech.Core.BusinessModel {
    public interface IBusinessObject {
        ValidationErrors ValidateInput();

        ValidationErrors ValidateBusiness();

        string RunValidations();

        bool ValidateAndPersist(string iaToken);

        void OnBeforePersist();

        void OnAfterPersist();

        void OnAfterLoad();
    }
	
    public interface IBusinessObject<T> : IBusinessObject where T : IBusinessObject, new() {
        //List<IValidationRule<T>> ValidationRules { get; }
    }
}
