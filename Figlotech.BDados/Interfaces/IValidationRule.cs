using Figlotech.BDados.Entity;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Interfaces {
    public interface IValidationRule<T> where T : IDataObject, new() {
        ValidationErrors Validate(IBusinessObject<T> ObjectToValidate);
    }
}
