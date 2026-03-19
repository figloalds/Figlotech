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

namespace Figlotech.BDados.DataAccessAbstractions.Attributes {
    /// <summary>
    /// Tells BDados DataAccessors to ignore this field when persisting updates
    /// </summary>
    public sealed class NoUpdateAttribute : Attribute {
        public NoUpdateAttribute() { }
    }
}
