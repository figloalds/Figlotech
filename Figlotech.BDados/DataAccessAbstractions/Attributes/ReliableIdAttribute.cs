/**
 * Figlotech::Database::Entity::AttPrimaryKey
 * Atributo pra marcar em Fields/Properties de classes derivadas da BDTabela
 * isso vai adicionar tal campo aos metadados do BDados e permitir que a API vincule esses campos a colunas do banco.
 * Inutilizado, a Primary Key é interpretada internamente pelo BDTabela como sendo a coluna Id;
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
    /// <para>
    /// Tells DataAccessors that decorated field is a ReliableId
    /// ReliableIds (or RIDs) in BDados are used to copy databases without losing their relations
    /// despite auto-increments.
    /// </para>
    /// <para>
    /// This is not mandatory, I personally decided to relate objects by runtime-generated strings
    /// because of problems when copying data between different data repositories.
    /// To my work it became really necessary to have on-demand non-clustered data synchronization, and 
    /// auto-incremental Ids were giving me a PUNK headache.
    /// </para>
    /// </summary>
    public class ReliableIdAttribute : Attribute
    {
        public ReliableIdAttribute()
        {
        }
    }
}
