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
using Figlotech.BDados.Builders;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.DataAccessAbstractions.Attributes {
    /// <summary>
    /// Tells IRdbmsDataAccessors that the decorated field should be a primary key in the database.
    /// </summary>
    public class PreemptiveCounter : Attribute
    {
        public PreemptiveCounter()
        {
        }

        public IQueryBuilder OnInsertSubQuery(Type t, MemberInfo mi) {
            return Qb.Fmt($@"(
                SELECT a.{mi.Name} + 1 as PCount From {t.Name} a
	                LEFT JOIN {t.Name} b
		                ON b.{mi.Name} = a.{mi.Name} + 1
                WHERE b.{mi.Name} IS NULL
                LIMIT 1
            )");
        }
    }
}
