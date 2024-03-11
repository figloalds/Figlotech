using Figlotech.Core;
using Figlotech.Core.BusinessModel;
using Figlotech.Core.Helpers;
using System;
using System.Linq;
using System.Reflection;

namespace Figlotech.Core.Interfaces {

    public static class IDataObjectExtensions {
        public static ulong localInstanceId { get; private set; } = RID.MachineRID2.AsULong;

        public static bool WasCreatedLocally (this IDataObject o) => o.CreatedBy == localInstanceId;
        public static bool WasAlteredLocally (this IDataObject o) => o.AlteredBy == localInstanceId;

        static SelfInitializerDictionary<Type, MemberInfo[]> membersList = new SelfInitializerDictionary<Type, MemberInfo[]>(
            (t) => ReflectionTool.FieldsAndPropertiesOf(t).Where(f=> f.Name != "PersistedHash").ToArray()
        );
        public static int SpFthComputeDataFieldsHash(this IDataObject self) {
            if (self == null) {
                return 0;
            }
            var retv = 0;
            var t = self.GetType();
            MemberInfo[] li = membersList[t];
            li.ForEach(i => retv ^= i?.GetHashCode() ?? 0);

            return retv;
        }

        public static T CloneIntoNewDataObject<T>(this T self) where T : IDataObject, new() {
            if(self == null) {
                return default(T);
            }
            var retv = new T();
            retv.CopyFrom(self);
            retv.Id = 0;
            retv.UpdatedTime = DateTime.UtcNow;
            retv.CreatedTime = DateTime.UtcNow;
            retv.RID = null;
            if(retv.RID == null) {
                retv.RID = new RID().AsBase36;
            }
            return retv;
        }
    }

    public interface IDataObject 
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

        bool IsReceivedFromSync { get; set; }
    }
}
