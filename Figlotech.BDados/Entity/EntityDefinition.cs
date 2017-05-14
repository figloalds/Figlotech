
/**
 * Figlotech.BDados.Entity.EntityDefinition
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
    public class EntityDefinition {
        public String Name;
        public String Label;
        public String TableName;
        public String ShortName;
        public String Description;
        public List<EntityField> Fields = new List<EntityField>();
        public EntityDefinition() {
        }
    }
}
