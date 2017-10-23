
/**
 * Figlotech::Database::Entity::FieldAttribute
 * This is used by reflection by RepositoryValueObject and BDados 
 * for figuring out how the inherited RepositoryValueObject should treat a given field
 * This is also heavily used by the BDados.CheckStructure method to figure out how
 * any given field is represented in the database as a Column.
 * 
 *@Author: Felype Rennan Alves dos Santos
 * August/2014
 * 
**/

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.DataAccessAbstractions.Attributes {
    /// <summary>
    /// Tells BDados IRdbmsDataAccessors that this column had a previous name
    /// this can be useful for structure checkers to rename columns instead of creating new ones
    /// and transfer data over in the process avoiding data loss and extra management hassle.
    /// </summary>
    public class OldNameAttribute : Attribute
    {
        public String Name { get; set; }

        public OldNameAttribute(String name)
        {
            Name = name;
        }
    }
}
