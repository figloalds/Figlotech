using Figlotech.BDados.Interfaces;
using System;

namespace Figlotech.BDados.DataAccessAbstractions {

    public interface IDataObject : ISaveable
    {
        void ForceId(long novoId);
        void ForceRID(String novoRid);

        void SelfCompute(object Previous = null);

        long Id { get; set; }
        DateTime? UpdatedTime { get; set; }
        DateTime CreatedTime { get; set; }
        String RID { get; set; }
        bool IsPersisted { get; }

        void Init();

    }
}
