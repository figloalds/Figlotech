/**
 * Figlotech::Database::Entity::AttColuna
 * Atributo pra marcar em Fields/Properties de classes derivadas da BDTabela
 * isso vai adicionar tal campo aos metadados do BDados e permitir que a API vincule esses campos a colunas do banco.
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
    /// This attribute could be AMAZING if C# allowed functions in attributes.
    /// </summary>
    public class NoUpdateIfAttribute : Attribute
    {
        public Func<bool> Condition;
        public NoUpdateIfAttribute(Func<bool> condition) {
            Condition = condition;
        }
    }
}
