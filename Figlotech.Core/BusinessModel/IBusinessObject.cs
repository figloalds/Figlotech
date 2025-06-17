

using Figlotech.Core.Interfaces;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Figlotech.Core.BusinessModel {
    public sealed class DataLoadContext {
        public IDataAccessor DataAccessor { get; set; }
        public bool IsAggregateLoad { get; set; }
        public object ContextTransferObject { get; set; }

        public T ContextAs<T>() {
            return (T) ContextTransferObject;
        }
    }

    public interface IBusinessObject {
        Task<ValidationErrors> ValidateInput();
        Task<ValidationErrors> ValidateBusiness();

        Task<string> RunValidations();

        Task<bool> ValidateAndPersistAsync(string iaToken);

        Task OnBeforePersistAsync();

        Task OnAfterPersistAsync();

        void OnAfterLoad(DataLoadContext ctx);
        
    }
	
    public interface IBusinessObject<T> : IBusinessObject where T : IDataObject, new() {
        //List<IValidationRule<T>> ValidationRules { get; }

        Task OnAfterAggregateLoadAsync(DataLoadContext ctx);
        Task OnAfterListAggregateLoadAsync(DataLoadContext ctx, List<T> AggregateLoadResult);
    }
}
