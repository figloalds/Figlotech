using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using Figlotech.BDados.Helpers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace SoftLeader.Sistema.Models
{
    public class SoftLeaderDataObject : IDataObject
    {
        public long Id {
            get {
                var priKey = GetPriKey();
                return (long)new ObjectReflector(this)[priKey];
            }
            set {
                new ObjectReflector(this)[GetPriKey()] = value;
            }
        }
        public DateTime? UpdatedTime { get; set; }
        public DateTime CreatedTime { get; set; }
        public string RID { get; set; }
        
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
