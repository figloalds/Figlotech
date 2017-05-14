/**
 * Figlotech::Database::Entity::ForeignKeyAttribute
 * This attribute
 * Atributo pra marcar em Fields/Properties de classes derivadas da BDTabela
 * isso vai adicionar tal campo aos metadados do BDados e permitir que a API vincule esses campos a colunas do banco.
 * Ainda não tem nada muito util sobre isso aqui (incompleto)
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
    public class ForeignKeyAttribute : Attribute
    {
        public Type referencedType;
        public String referencedColumn;

        public ForeignKeyAttribute(Type foreignType, String foreignColumn = "RID")
        {
            referencedType = foreignType;
            referencedColumn = foreignColumn;
        }
    }
}
