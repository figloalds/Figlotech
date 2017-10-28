
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
    /// Tells BDados IRDBMS structure checkers that this field must be represented
    /// in the rdbms database, generally as a column.
    /// </summary>
    public class FieldAttribute : Attribute {
        public String Type { get; set; }
        public String Options { get; set; }
        public bool PrimaryKey { get; set; }
        public int Size { get; set; } = 128;
        public object DefaultValue { get; set; }
        public bool AllowNull { get; set; }
        public bool Unique { get; set; }
        // --
        public string Table { get; set; }
        public string Name { get; set; }
        public string Precision { get; set; }
        public string Charset { get; set; }
        public string Collation { get; set; }
        public string Comment { get; set; }
        public string GenerationExpression { get; set; }
        // --
        public FieldAttribute(string tipo, string opcoes)
        {
            Type = tipo;
            Options = opcoes;
        }
        public FieldAttribute() { }
    }
}
