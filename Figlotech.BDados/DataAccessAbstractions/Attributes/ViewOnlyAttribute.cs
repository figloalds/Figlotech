
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
    /// Decorate a dataobject with this to stop 
    /// the IRdbmsDataAccessor structure checker from creating a table
    /// for this data object (in future I might add a functionality for the structurechecker to create and use 
    /// a view, based on the definition of the class.)
    /// </summary>
    public class ViewOnlyAttribute : Attribute {
        public ViewOnlyAttribute() {
        }
    }
}
