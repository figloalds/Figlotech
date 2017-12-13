﻿using Figlotech.BDados.Builders;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.DataAccessAbstractions {
    public delegate void BuildHelper(IQueryBuildHelper qh);
    public interface IQueryGenerator
    {
        IQueryBuilder GenerateInsertQuery(IDataObject tabelaInput);
        IQueryBuilder GenerateUpdateQuery(IDataObject tabelaInput);
        IQueryBuilder GenerateSaveQuery(IDataObject tabelaInput);
        IQueryBuilder GenerateSelectAll<T>() where T : IDataObject, new();
        IQueryBuilder GenerateSelect<T>(IQueryBuilder condicoes, MemberInfo orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new();
        IQueryBuilder GenerateJoinQuery(JoinDefinition juncaoInput, IQueryBuilder condicoes, MemberInfo orderingMember = null, OrderingType otype = OrderingType.Asc, int? p = 1, int? limit = 100, IQueryBuilder condicoesRoot = null);
        IQueryBuilder GenerateMultiInsert<T>(RecordSet<T> conjuntoInput, bool OmmitPk = true) where T : IDataObject, new();
        IQueryBuilder GenerateMultiUpdate<T>(RecordSet<T> inputRecordset) where T : IDataObject, new();
        IQueryBuilder GenerateCallProcedure(string name, object[] args);
        IQueryBuilder InformationSchemaQueryTables(string schema);
        IQueryBuilder InformationSchemaQueryColumns(string schema);
        IQueryBuilder InformationSchemaQueryKeys(string schema);
        IQueryBuilder RenameTable(string tabName, string v);
        IQueryBuilder DropForeignKey(string target, string constraint);
        IQueryBuilder AddColumn(string tableName, string columnDefinition);
        IQueryBuilder AddForeignKey(string table, string column, string refTable, string refColumn);
        IQueryBuilder RenameColumn(string table, string column, string newName);
        IQueryBuilder Purge(string table, string column, string refTable, string refColumn);
        IQueryBuilder GetLastInsertId<T>() where T : IDataObject, new();
        IQueryBuilder GetIdFromRid<T>(object Rid) where T : IDataObject, new();
        IQueryBuilder GetCreationCommand(Type t);
        IQueryBuilder GetCreationCommand(ForeignKeyAttribute fkd);
    }
}
