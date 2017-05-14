
/**
 * Figlotech.BDados.Entity.Column
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

namespace Figlotech.BDados.Entity
{
    public class Column
    {
        public String Nome;
        public String Tipo;
        public String Opcoes;
        public Column(String BDNome, String BDTipo, String BDOpcoes)
        {
            Nome = BDNome;
            Tipo = BDTipo;
            Opcoes = BDOpcoes;
        }
    }
}
