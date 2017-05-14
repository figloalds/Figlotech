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

namespace Figlotech.BDados.Attributes {
    public class ReliableIdAttribute : Attribute
    {
        public ReliableIdAttribute()
        {
        }
    }
}
