using Figlotech.Core;
using Figlotech.Core.BusinessModel;
using Figlotech.Core.Interfaces;
using System;

namespace Figlotech.BDados.DataAccessAbstractions {

    public static class IDataObjectExtensions {
        public static bool WasCreatedLocally (this IDataObject o) => o.CreatedBy == RID.MachineRID.AsULong;
        public static bool WasAlteredLocally (this IDataObject o) => o.AlteredBy == RID.MachineRID.AsULong;
    }

    public interface IDataObject : ISaveable
    {
        //void ForceId(long novoId);
        //void ForceRID(String novoRid);

        long Id { get; set; }
        DateTime? UpdatedTime { get; set; }
        DateTime CreatedTime { get; set; }
        String RID { get; set; }
        bool IsPersisted { get; set; }

        int PersistedHash { get; set; }
        ulong AlteredBy { get; set; }
        ulong CreatedBy { get; set; }

    }
}
