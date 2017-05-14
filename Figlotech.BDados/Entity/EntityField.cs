
/**
 * Figlotech.BDados.Entity.EntityField
 * Deprecated
 * Was used by RepositoryValueObject to return an object-like ValueObject definition.
 * 
 * @Author: Felype Rennan Alves dos Santos
 * August/2014
 * 
**/


using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Entity {
    public class EntityField {
        public String Name;
        public String Label;
        public Type Type;
        public int Size;
        public Object DefaultValue;
        public bool AllowNull;
        public String Options { get; set; }
        public bool PrimaryKey { get; set; }
        public bool Unique { get; set; }
    }
}
