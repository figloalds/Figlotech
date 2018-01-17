﻿/**
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

namespace Figlotech.BDados.DataAccessAbstractions.Attributes {
    /// <summary>
    /// Tells BDados IRDBMS structure checkers that this field should 
    /// have a foreign key attached to it referencing a field (column) of other
    /// valueobject (table)
    /// </summary>
    public class ForeignKeyAttribute : Attribute {
        public String Table { get; set; }
        public String Column { get; set; }
        public String RefTable { get; set; }
        public String RefColumn { get; set; }
        public String ConstraintName { get; set; }
        public String FTechConstraintName => $"fk_{Column}_{RefTable}_{RefColumn}";
        internal Type RefType { get; set; }

        public ForeignKeyAttribute() { }
        public ForeignKeyAttribute(Type foreignType, String foreignColumn = "RID")
        {
            RefType = foreignType;
            RefTable = RefType.Name;
            RefColumn = foreignColumn;
        }

        public override bool Equals(object obj) {
            return this == obj || (obj is ForeignKeyAttribute fkatt && fkatt.ToString() == this.ToString());
        }

        public override string ToString() {
            return $"{Table}_{Column}_{RefTable}_{RefColumn}".ToLower();
        }
    }
}
