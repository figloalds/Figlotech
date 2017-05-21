using Figlotech.BDados.DataAccessAbstractions;
using System.Collections.Generic;

namespace Figlotech.BDados.DataAccessAbstractions {
    public interface IBusinessObject {
        ValidationErrors ValidateInput();

        ValidationErrors ValidateBusiness();

        void OnBeforePersist();

        void OnAfterPersist();
    }
	
    public interface IBusinessObject<T> : IDataObject, IBusinessObject where T : IDataObject,new() {
        List<IValidationRule<T>> ValidationRules { get; }
    }
}
