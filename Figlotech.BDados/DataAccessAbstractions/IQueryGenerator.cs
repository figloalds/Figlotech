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
using Figlotech.Core.Interfaces;
using Figlotech.Core;
using Figlotech.Data;

namespace Figlotech.BDados.DataAccessAbstractions {
    public delegate void BuildHelper(IQueryBuildHelper qh);
    public interface IQueryGenerator {
        IQueryBuilder CreateDatabase(string schemaName);
        IQueryBuilder GenerateInsertQuery(IDataObject tabelaInput);
        IQueryBuilder GenerateUpdateQuery(IDataObject tabelaInput);
        IQueryBuilder GenerateUpdateQuery<T>(T input, params (Expression<Func<T,object>> parameterExpression, object Value)[] updates) where T : IDataObject;
        IQueryBuilder GenerateSaveQuery(IDataObject tabelaInput);
        IQueryBuilder GenerateSelectAll<T>() where T : IDataObject, new();
        IQueryBuilder GenerateSelect<T>(IQueryBuilder condicoes, int? skip, int? limit, MemberInfo orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new();
        IQueryBuilder GenerateJoinQuery(JoinDefinition juncaoInput, IQueryBuilder condicoes, int? p = 0, int? limit = 100, MemberInfo orderingMember = null, OrderingType otype = OrderingType.Asc, IQueryBuilder condicoesRoot = null);
        IQueryBuilder GenerateMultiInsert<T>(List<T> conjuntoInput, bool OmmitPk = true) where T : IDataObject;
        IQueryBuilder GenerateMultiUpdate<T>(List<T> inputRecordset) where T : IDataObject;
        IQueryBuilder GenerateCallProcedure(string name, object[] args);
        IQueryBuilder GenerateGetStateChangesQuery(List<Type> workingTypes, Dictionary<Type, MemberInfo[]> fields, DateTime moment);
        IQueryBuilder InformationSchemaQueryTables(string schema);
        IQueryBuilder InformationSchemaQueryColumns(string schema);
        IQueryBuilder InformationSchemaQueryKeys(string schema);
        IQueryBuilder InformationSchemaIndexes(string schema);
        IQueryBuilder RenameTable(string tabName, string v);
        IQueryBuilder DropForeignKey(string target, string constraint);
        IQueryBuilder DropUnique(string target, string constraint);
        IQueryBuilder DropColumn(string table, string column);
        IQueryBuilder DropIndex(string target, string constraint);
        IQueryBuilder DropPrimary(string target, string constraint);
        IQueryBuilder AddColumn(string tableName, string columnDefinition);
        IQueryBuilder AddForeignKey(string table, string column, string refTable, string refColumn, string constraintName);
        IQueryBuilder AddIndex(string table, string column, string constraintName);
        IQueryBuilder AddIndexForUniqueKey(string table, string column, string constraintName);
        IQueryBuilder AddUniqueKey(string table, string column, string constraintName);
        IQueryBuilder AddPrimaryKey(string table, string column, string constraintName);
        IQueryBuilder UpdateColumn(string table, string column, object value, IQueryBuilder conditions);
        IQueryBuilder RenameColumn(string table, string column, MemberInfo newDefinition, FieldAttribute info);
        IQueryBuilder Purge(string table, string column, string refTable, string refColumn, bool isNullable);
        IQueryBuilder GetLastInsertId<T>() where T : IDataObject, new();
        IQueryBuilder GetIdFromRid<T>(object Rid) where T : IDataObject, new();
        IQueryBuilder GetCreationCommand(Type t);
        IQueryBuilder GetCreationCommand(ForeignKeyAttribute fkd);
        IQueryBuilder QueryIds<T>(List<T> rs) where T : IDataObject;
        IQueryBuilder CheckExistsById<T>(long Id) where T : IDataObject;
        IQueryBuilder CheckExistsByRID<T>(string RID) where T : IDataObject;
        IQueryBuilder DisableForeignKeys();
        IQueryBuilder EnableForeignKeys();
        string GetDatabaseType(MemberInfo field, FieldAttribute fieldAtt);
        string GetDatabaseTypeWithLength(MemberInfo field, FieldAttribute fieldAtt);
        string GetColumnDefinition(MemberInfo columnMember, FieldAttribute info = null);
        IQueryBuilder AlterColumnDataType(string table, MemberInfo member, FieldAttribute fieldAttribute);
        IQueryBuilder AlterColumnNullability(string table, MemberInfo member, FieldAttribute fieldAttribute);
    }
}
