using Figlotech.BDados.Entity;

namespace Figlotech.BDados.Interfaces {
    public interface IBusinessObject {
        ValidationErrors ValidateInput();

        ValidationErrors ValidateBusiness();

        void OnBeforePersist();

        void OnAfterPersist();
    }
}
