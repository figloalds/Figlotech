using Figlotech.BDados.Entity;
using Figlotech.BDados.Requirements;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Interfaces {

    public interface IDataObject : IRequiresDataAccessor, ISaveable  {
        void ForceId(long newId);
        void ForceRID(RID newRid);

        void SelfCompute(object Previous = null);

        long Id { get; set; }
        DateTime? UpdatedTime { get; set; }
        DateTime CreatedTime { get; set; }
        bool IsActive { get; set; }
        RID RID { get; set; }

        bool IsPersisted();

        IContextProvider Context { get; }
    }

    public interface IDataObject<T> : IDataObject where T : IDataObject, new() {
        List<IValidationRule<T>> ValidationRules { get; set; }
    }
}
