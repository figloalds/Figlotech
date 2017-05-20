using Figlotech.BDados.Interfaces;
using System;

namespace Figlotech.BDados.DataAccessAbstractions {

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
