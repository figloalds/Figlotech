using Figlotech.BDados.Entity;
using Figlotech.BDados.Requirements;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Interfaces {

    public interface IDataObject : ISaveable  {
        void ForceId(long newId);
        void ForceRID(RID newRid);

        IDataAccessor DataAccessor { get; set; }

        void SelfCompute(object Previous = null);

        long Id { get; set; }
        DateTime? UpdatedTime { get; set; }
        DateTime CreatedTime { get; set; }
        bool IsActive { get; set; }
        RID RID { get; set; }

        bool IsPersisted();
    }
}
