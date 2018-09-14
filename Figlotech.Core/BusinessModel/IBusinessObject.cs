

using Figlotech.Core.Interfaces;
using System.Collections.Generic;

namespace Figlotech.Core.BusinessModel {
    public class DataLoadContext {
        public IDataAccessor DataAccessor { get; set; }
        public bool IsAggregateLoad { get; set; }
    }

    public interface IBusinessObject {
        ValidationErrors ValidateInput();

        ValidationErrors ValidateBusiness();

        string RunValidations();

        bool ValidateAndPersist(string iaToken);

        void OnBeforePersist();

        void OnAfterPersist();

        void OnAfterLoad(DataLoadContext ctx);
        
    }
	
    public interface IBusinessObject<T> : IBusinessObject where T : IDataObject, new() {
        //List<IValidationRule<T>> ValidationRules { get; }

        void OnAfterAggregateLoad(DataLoadContext ctx, IList<T> AggregateLoadResult);
    }
}
