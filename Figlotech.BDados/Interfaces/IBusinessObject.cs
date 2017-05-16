using Figlotech.BDados.Entity;
using System.Collections.Generic;

namespace Figlotech.BDados.Interfaces {
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
