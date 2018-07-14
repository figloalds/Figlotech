using Figlotech.Core;
using Figlotech.Core.BusinessModel;
using Figlotech.Core.Helpers;
using Figlotech.Core.Interfaces;
using System;

namespace Figlotech.BDados.DataAccessAbstractions {

    public static class IDataObjectExtensions {
        public static bool WasCreatedLocally (this IDataObject o) => o.CreatedBy == RID.MachineRID.AsULong;
        public static bool WasAlteredLocally (this IDataObject o) => o.AlteredBy == RID.MachineRID.AsULong;

        public static T Duplicate<T>(this T origin) where T: IDataObject, new() {
            if (origin == null)
                return default(T);
            var t = typeof(T);
            var ridCol = FiTechBDadosExtensions.RidColumnOf[t];
            var idCol = FiTechBDadosExtensions.IdColumnOf[t];
            T destination = new T();
            ObjectReflector.Open(origin, (objA) => {
                ObjectReflector.Open(destination, (objB) => {
                    foreach (var field in objB) {
                        if(field.Key.Name == ridCol) {
                            continue;
                        }
                        if (objA.ContainsKey(field.Key.Name)) {
                            objB[field.Key] = objA[field.Key.Name];
                        }
                    }
                });
            });

            return destination;
        }
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
