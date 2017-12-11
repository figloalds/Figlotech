using Figlotech.Core.BusinessModel;
using Figlotech.Core.Interfaces;
using System;

namespace Figlotech.BDados.DataAccessAbstractions {

    public interface IDataObject : ISaveable
    {
        //void ForceId(long novoId);
        //void ForceRID(String novoRid);

        long Id { get; set; }
        DateTime? UpdatedTime { get; set; }
        DateTime CreatedTime { get; set; }
        String RID { get; set; }
        bool IsPersisted { get; set; }
        
    }
}
