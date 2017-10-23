using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using Figlotech.Core;
using Figlotech.Core.Helpers;
using System;
using System.Linq;
using System.Reflection;

namespace AppRostoJovem.WebCore {
    [ViewOnly]
    public class AppDataObject : IDataObject {
        [PrimaryKey]
        [Field()]
        public long Id {
            get; set;
        }

        [Field(Type = "TIMESTAMP")]
        public DateTime Registro { get; set; } = DateTime.UtcNow;
        [Field(AllowNull = true)]
        public DateTime? DataUpdate { get; set; } = DateTime.UtcNow;

        public DateTime? UpdatedTime { get { return DataUpdate; } set { DataUpdate = value; } }
        public DateTime CreatedTime { get { return Registro; } set { Registro = value; } }

        private string _rid;
        [Field(Size = 64, Unique = true, AllowNull = false)]
        public string RID {
            get {
                return _rid ?? new RID().ToString();
            }
            set {
                _rid = value;
            }
        }

        public MemberInfo GetPriKey() {
            return ReflectionTool.FieldsAndPropertiesOf(this.GetType())
                .FirstOrDefault(m => m.GetCustomAttribute<PrimaryKeyAttribute>() != null);
        }


        public bool IsPersisted => Id > 0;

        public void ForceId(long novoId) {
            Id = novoId;
        }

        public void ForceRID(string novoRid) {

        }

        public bool Load(Action fn = null) {
            return false;
        }

        public bool Save(Action fn = null) {
            return false;
        }

    }
}
