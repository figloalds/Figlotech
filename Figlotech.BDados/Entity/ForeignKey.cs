
/**
 * Figlotech::Database::Entity::ForeignKey
 * Deprecated
 * Was used in the generation of object-like class definitions.
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

namespace Figlotech.BDados.Entity
{
    public class ForeignKeyDefinition
    {
        public String TableName;
        public String ColumnName;
        public String ReferencedTable;
        public String ReferencedColumn;

        public ForeignKeyDefinition(String TabelaOrigem, String ColunaOrigem, String TabelaEstrangeira, String ColunaEstrangeira)
        {
            TableName = TabelaOrigem;
            ColumnName = ColunaOrigem;
            ReferencedTable = TabelaEstrangeira;
            ReferencedColumn = ColunaEstrangeira;
        }

        public String ObterComandoCriacao()
        {
            String ComandoCriação = String.Format(@"ALTER TABLE {0} ADD FOREIGN KEY ({1}) REFERENCES {2} ({3});", TableName, ColumnName, ReferencedTable, ReferencedColumn);
            return ComandoCriação;
        }
    }
}
