
using Figlotech.BDados.Builders;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using Figlotech.BDados.Helpers;
using Figlotech.Core;
using Figlotech.Core.Helpers;
using Figlotech.Core.Interfaces;
/**
 * Figlotech.BDados.Builders.PgSQLQueryGenerator
 * This is a blatant copy from MySQL Query Generator
 * Will fix in time, maybe.
 * 
 * @Author: Felype Rennan Alves dos Santos
 * August/2014
 * 
**/

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace Figlotech.BDados.PgSQLDataAccessor {
    public class PgSQLQueryGenerator : IQueryGenerator {

        public IQueryBuilder CreateDatabase(string schemaName) {
            return new QueryBuilder($"CREATE DATABASE IF NOT EXISTS {schemaName}");
        }

        public IQueryBuilder GenerateInsertQuery(IDataObject inputObject) {
            var type = inputObject.GetType();
            QueryBuilder query = new QbFmt($"INSERT INTO {inputObject.GetType().Name}");
            query.Append("(");
            query.Append(GenerateFieldsString(type, false));
            query.Append(")");
            query.Append("VALUES (");
            var members = GetMembers(type);
            // members.RemoveAll(m => m.GetCustomAttribute<PrimaryKeyAttribute>() != null);
            for (int i = 0; i < members.Count; i++) {
                query.Append($"@{i + 1}", ReflectionTool.GetMemberValue(members[i], inputObject));
                if (i < members.Count - 1) {
                    query.Append(",");
                }
            }
            query.Append(")");
            //query.Append("ON CONFLICT (" + GenerateKeysString(type) + ") DO UPDATE SET ");
            //var Fields = GetMembers(inputObject.GetType());
            //for (int i = 0; i < Fields.Count; ++i) {
            //    if (Fields[i].GetCustomAttribute(typeof(PrimaryKeyAttribute)) != null)
            //        continue;
            //    query.Append(String.Format("{0} = VALUES({0})", Fields[i].Name));
            //    if (i < Fields.Count - 1) {
            //        query.Append(",");
            //    }
            //}
            return query;
        }

        public IQueryBuilder GetCreationCommand(Type t) {
            String objectName = t.Name;

            var members = ReflectionTool.FieldsAndPropertiesOf(t)
                .Where((m) => m?.GetCustomAttribute<FieldAttribute>() != null).ToArray();
            if (objectName == null || objectName.Length == 0)
                return null;
            QueryBuilder CreateTable = new QbFmt($"CREATE TABLE IF NOT EXISTS {objectName} (\n");
            for (int i = 0; i < members.Length; i++) {
                var info = members[i].GetCustomAttribute<FieldAttribute>();
                CreateTable.Append(GetColumnDefinition(members[i], info));
                CreateTable.Append(" ");
                CreateTable.Append(info.Options ?? "");
                if (i != members.Length - 1) {
                    CreateTable.Append(", \n");
                }
            }
            CreateTable.Append(" );");
            return CreateTable;
        }
        /// <summary>
        /// deprecated
        /// this is responsibility of the rdbms query generator
        /// </summary>
        /// <param name="field"></param>
        /// <param name="info"></param>
        /// <returns></returns>
        public String GetColumnDefinition(MemberInfo field, FieldAttribute info = null) {
            if (info == null)
                info = field.GetCustomAttribute<FieldAttribute>();
            if (info == null)
                return "VARCHAR(128)";
            var typeOfField = ReflectionTool.GetTypeOf(field);
            var nome = field.Name;
            String tipo = GetDatabaseTypeWithLength(field, info);
            var options = "";
            if (info.Options != null && info.Options.Length > 0) {
                options = info.Options;
            } else {
                if (!info.AllowNull) {
                    options += " NOT NULL";
                } else if (Nullable.GetUnderlyingType(typeOfField) == null && typeOfField.IsValueType && !info.AllowNull) {
                    options += " NOT NULL";
                }
                if (info.Unique)
                    options += " UNIQUE";
                if ((info.AllowNull && info.DefaultValue == null))
                    options += " DEFAULT NULL";
                else if (info.DefaultValue != null)
                    options += $" DEFAULT {ConvertDefaultOption(info.DefaultValue, typeOfField)}";
                foreach (var att in field.GetCustomAttributes())
                    if (att is PrimaryKeyAttribute) {
                        options = " PRIMARY KEY";
                        tipo = "SERIAL";
                    }
            }

            return $"{nome} {tipo} {options}";
        }

        public string ConvertDefaultOption(object input, Type type) {
            if (input == null) {
                return "NULL";
            }
            if (type == typeof(string)) {
                return Fi.Tech.CheapSanitize(input);
            }
            if (type == typeof(Boolean) &&
                (input.GetType() == typeof(Int16) ||
                input.GetType() == typeof(Int32) ||
                input.GetType() == typeof(Int64) ||
                input.GetType() == typeof(Single) ||
                input.GetType() == typeof(Double) ||
                input.GetType() == typeof(Decimal))
            ) {
                return ((int)input != 0).ToString().ToUpper();
            }
            return Fi.Tech.CheapSanitize(input);
        }

        public String GetDatabaseTypeWithLength(MemberInfo field, FieldAttribute info = null) {
            String retv = GetDatabaseType(field, info);
            if (info.Type != null && info.Type.Length > 0)
                retv = info.Type;
            if (retv == "VARCHAR" || retv == "VARBINARY") {
                retv += $"({(info.Size > 0 ? info.Size : 100)})";
            }
            if (retv == "FLOAT" || retv == "DOUBLE" || retv == "DECIMAL" || retv == "NUMERIC") {
                retv += "(16,3)";
            }

            return retv;
        }

        /// <summary>
        /// deprecated
        /// Must implement this on each rdbms query generator
        /// </summary>
        /// <param name="field"></param>
        /// <param name="info"></param>
        /// <returns></returns>
        public String GetDatabaseType(MemberInfo field, FieldAttribute info = null) {
            if (info == null)
                foreach (var att in field.GetCustomAttributes())
                    if (att is FieldAttribute) {
                        info = (FieldAttribute)att; break;
                    }
            if (info == null)
                return "VARCHAR";
            var typeOfField = ReflectionTool.GetTypeOf(field);
            string tipoDados;
            if (Nullable.GetUnderlyingType(typeOfField) != null)
                tipoDados = Nullable.GetUnderlyingType(typeOfField).Name;
            else
                tipoDados = typeOfField.Name;
            if (typeOfField.IsEnum) {
                return "INT4";
            }
            String type = "VARCHAR";
            if (info.Type != null && info.Type.Length > 0) {
                type = info.Type;
            } else {
                switch (tipoDados.ToLower()) {
                    case "string":
                        type = $"VARCHAR";
                        break;
                    case "int":
                    case "int32":
                        type = $"INT4";
                        break;
                    case "short":
                    case "int16":
                        type = $"INT2";
                        break;
                    case "long":
                    case "int64":
                        type = $"INT8";
                        break;
                    case "bool":
                    case "boolean":
                        type = $"BOOL";
                        break;
                    case "float":
                    case "double":
                    case "single":
                    case "decimal":
                        type = $"NUMERIC";
                        break;
                    case "byte[]":
                        type = $"BYTEA";
                        break;
                    case "datetime":
                        type = $"TIMESTAMP";
                        break;
                }
            }
            return type;
        }

        public IQueryBuilder GenerateCallProcedure(string name, object[] args) {
            QueryBuilder query = new QbFmt($"CALL {name}");
            for (int i = 0; i < args.Length; i++) {
                if (i == 0) {
                    query.Append("(");
                }
                query.Append($"@{i + 1}", args[i]);
                if (i < args.Length - 1)
                    query.Append(",");
                if (i == args.Length - 1) {
                    query.Append(")");
                }
            }
            return query;
        }

        private bool RelatesWithMain(string a) {
            var retv = (a.StartsWith("a.") || a.Contains("=a."));
            return retv;
        }

        public IQueryBuilder GenerateJoinQuery(JoinDefinition inputJoin, IQueryBuilder conditions, int? skip = null, int? limit = null, MemberInfo orderingMember = null, OrderingType otype = OrderingType.Asc, IQueryBuilder rootConditions = null) {
            if (rootConditions == null)
                rootConditions = new QbFmt("true");
            if (inputJoin.Joins.Count < 1)
                throw new BDadosException("This join needs 1 or more tables.");

            List<Type> tables = (from a in inputJoin.Joins select a.ValueObject).ToList();
            List<String> tableNames = (from a in inputJoin.Joins select a.ValueObject.Name).ToList();
            List<String> prefixes = (from a in inputJoin.Joins select a.Prefix).ToList();
            List<String> aliases = (from a in inputJoin.Joins select a.Alias).ToList();
            List<String> onclauses = (from a in inputJoin.Joins select a.Args).ToList();
            List<List<String>> columns = (from a in inputJoin.Joins select a.Columns).ToList();
            List<JoinType> joinTypes = (from a in inputJoin.Joins select a.Type).ToList();

            QueryBuilder Query = new QbFmt("SELECT sub.*\n");
            Query.Append($"\t FROM (SELECT\n");
            for (int i = 0; i < tables.Count; i++) {
                Query.Append($"\t\t-- Table {tableNames[i]}\n");
                var fields = ReflectionTool.FieldsAndPropertiesOf(
                    tables[i])
                    .Where((a) => a.GetCustomAttribute(typeof(FieldAttribute)) != null)
                    .ToArray();
                if (!columns[i].Contains("RID"))
                    columns[i].Add("RID");
                var nonexcl = columns[i];
                for (int j = 0; j < nonexcl.Count; j++) {
                    Query.Append($"\t\t{prefixes[i]}.{nonexcl[j]} AS {prefixes[i]}_{nonexcl[j]}");
                    if (true || j < nonexcl.Count - 1 || i < tables.Count - 1) {
                        Query.Append(",");
                    }
                    Query.Append("\n");
                }
                Query.Append("\n");
            }

            Query.Append($"\t\t1 FROM (SELECT * FROM {tableNames[0]}");
            if (rootConditions != null) {
                Query.Append("WHERE ");
                Query.Append(rootConditions);
            }
            if (orderingMember != null) {
                Query.Append($"ORDER BY {orderingMember.Name} {otype.ToString().ToUpper()}");
            }
            if (limit != null) {
                Query.Append($"LIMIT");
                if (skip != null)
                    Query.Append($"{skip},");
                Query.Append($"{limit}");
            }
            Query.Append($"");
            Query.Append($") AS {prefixes[0]}\n");

            for (int i = 1; i < tables.Count; i++) {

                Query.Append($"\t\t{"LEFT"} JOIN {tableNames[i]} AS {prefixes[i]} ON {onclauses[i]}\n");
            }

            if (conditions != null && !conditions.IsEmpty) {
                Query.Append("\tWHERE");
                Query.Append(conditions);

            }
            if (orderingMember != null) {
                Query.Append($"ORDER BY {prefixes[0]}.{orderingMember.Name} {otype.ToString().ToUpper()}");
            }
            if (limit != null) {
                Query.Append($"LIMIT");
                if ((skip ?? 0) > 0)
                    Query.Append($"{skip}, ");
                Query.Append($"{limit}");
            }
            Query.Append(") AS sub\n");

            return Query;
        }

        public IQueryBuilder GenerateSaveQuery(IDataObject tabelaInput) {
            return new QueryBuilder();
        }

        public IQueryBuilder GenerateSelectAll<T>() where T : IDataObject, new() {
            var type = typeof(T);
            QueryBuilder Query = new QbFmt("SELECT ");
            Query.Append(GenerateFieldsString(type, false));
            Query.Append($"FROM {type.Name} AS {new PrefixMaker().GetAliasFor("root", typeof(T).Name, String.Empty)};");
            return Query;
        }

        public IQueryBuilder GenerateSelect<T>(IQueryBuilder condicoes = null, int? skip = null, int? limit = null, MemberInfo orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new() {
            var type = typeof(T);
            var alias = new PrefixMaker().GetAliasFor("root", typeof(T).Name, String.Empty);
            Fi.Tech.WriteLine($"Generating SELECT {condicoes} {skip} {limit} {orderingMember?.Name} {ordering}");
            QueryBuilder Query = new QbFmt("SELECT ");
            Query.Append(GenerateFieldsString(type, false));
            Query.Append(String.Format($"FROM {type.Name} AS { alias }"));
            if (condicoes != null && !condicoes.IsEmpty) {
                Query.Append("WHERE");
                Query.Append(condicoes);
            }
            if (orderingMember != null) {
                Query.Append($"ORDER BY {alias}.{orderingMember.Name} {ordering.ToString().ToUpper()}");
            }
            if (limit != null || skip != null) {
                Query.Append($"LIMIT {(skip != null ? $"{skip}," : "")} {limit ?? Int32.MaxValue}");
            }
            Query.Append(";");
            return Query;
        }

        public IQueryBuilder GenerateUpdateQuery(IDataObject tabelaInput) {
            var type = tabelaInput.GetType();
            var rid = FiTechBDadosExtensions.RidColumnOf[type];
            var ridType = ReflectionTool.GetTypeOf(ReflectionTool.FieldsAndPropertiesOf(type).FirstOrDefault(x => x.GetCustomAttribute<ReliableIdAttribute>() != null));

            QueryBuilder Query = new QbFmt(String.Format("UPDATE {0} ", tabelaInput.GetType().Name));
            Query.Append("SET");
            Query.Append(GenerateUpdateValueParams(tabelaInput, false));
            Query.Append($" WHERE {rid} = @rid;", Convert.ChangeType(tabelaInput.RID, ridType));
            return Query;
        }

        internal QueryBuilder GenerateUpdateValueParams(IDataObject tabelaInput, bool OmmitPk = true) {
            QueryBuilder Query = new QueryBuilder();
            var lifi = GetMembers(tabelaInput.GetType());
            int k = 0;
            for (int i = 0; i < lifi.Count; i++) {
                if (OmmitPk && lifi[i].GetCustomAttribute(typeof(PrimaryKeyAttribute)) != null)
                    continue;
                foreach (CustomAttributeData att in lifi[i].CustomAttributes) {
                    if (att.AttributeType == typeof(FieldAttribute)) {
                        Object val = ReflectionTool.GetMemberValue(lifi[i], tabelaInput);
                        Query.Append($"{lifi[i].Name} = @{(++k)}", val);
                        if (i < lifi.Count - 1)
                            Query.Append(", ");
                    }
                }
            }
            return Query;
        }

        public IQueryBuilder GenerateValuesString(IDataObject tabelaInput, bool OmmitPK = true) {
            var cod = IntEx.GenerateShortRid();
            QueryBuilder Query = new QueryBuilder();
            var fields = GetMembers(tabelaInput.GetType());
            if (OmmitPK) {
                fields.RemoveAll(m => m.GetCustomAttribute<PrimaryKeyAttribute>() != null && m.GetCustomAttribute<ReliableIdAttribute>() == null);
            }
            for (int i = 0; i < fields.Count; i++) {
                Object val = ReflectionTool.GetMemberValue(fields[i], tabelaInput);
                if (!Query.IsEmpty)
                    Query.Append(", ");
                Query.Append($"@{cod}{i + 1}", val);
            }
            return Query;

        }

        public IQueryBuilder GenerateMultiUpdate<T>(List<T> inputRecordset) where T : IDataObject {
            // -- 
            List<T> workingSet = new List<T>();

            var rid = FiTechBDadosExtensions.RidColumnOf[typeof(T)];

            workingSet.AddRange(inputRecordset.Where((record) => record.IsPersisted));
            if (workingSet.Count < 1) {
                return null;
            }
            QueryBuilder Query = new QueryBuilder();
            Query.Append($"UPDATE {typeof(T).Name} ");
            Query.Append("SET ");

            // -- 
            var members = GetMembers(typeof(T));
            members.RemoveAll(m => m.GetCustomAttribute<PrimaryKeyAttribute>() != null);
            int x = 0;
            for (var i = 0; i < members.Count; i++) {
                var memberType = ReflectionTool.GetTypeOf(members[i]);
                Query.Append($"{members[i].Name}=(CASE ");
                foreach (var a in inputRecordset) {
                    string sid = IntEx.GenerateShortRid();
                    Query.Append($"WHEN {rid}=@{sid}{x++} THEN @{sid}{x++}", Convert.ChangeType(a.RID, FiTechBDadosExtensions.RidFieldType[a.GetType()]), ReflectionTool.GetMemberValue(members[i], a));
                }
                Query.Append($"ELSE {members[i].Name} END)");
                if (i < members.Count - 1) {
                    Query.Append(",");
                }
            }

            Query.Append($"WHERE {rid} IN (")
                .Append(Fi.Tech.ListRids(workingSet))
                .Append(");");
            // --
            return Query;
        }

        public IQueryBuilder GenerateMultiInsert<T>(List<T> inputRecordset, bool OmmitPk = false) where T : IDataObject {
            List<T> workingSet = new List<T>();
            workingSet.AddRange(inputRecordset.Where((r) => !r.IsPersisted));
            if (workingSet.Count < 1) return null;
            // -- 
            QueryBuilder Query = new QueryBuilder();
            Query.Append($"INSERT INTO {typeof(T).Name} (");
            Query.Append(GenerateFieldsString(typeof(T), OmmitPk));
            Query.Append(") VALUES");
            // -- 
            for (int i = 0; i < workingSet.Count; i++) {
                Query.Append("(");
                Query.Append(GenerateValuesString(workingSet[i], OmmitPk));
                Query.Append(")");
                if (i < workingSet.Count - 1)
                    Query.Append(",");
            }
            // -- 
            //Query.Append("ON CONFLICT ("+ GenerateKeysString(typeof(T)) + ") DO UPDATE SET ");
            //var Fields = GetMembers(typeof(T));
            //for (int i = 0; i < Fields.Count; ++i) {
            //    if (OmmitPk && Fields[i].GetCustomAttribute(typeof(PrimaryKeyAttribute)) != null)
            //        continue;
            //    Query.Append(String.Format("{0} = VALUES({0})", Fields[i].Name));
            //    if (i < Fields.Count - 1) {
            //        Query.Append(",");
            //    }
            //}
            // -- 
            return Query;
        }

        public IQueryBuilder GenerateKeysString(Type type) {
            QueryBuilder sb = new QueryBuilder();
            var fields = GetMembers(type);
            for (int i = 0; i < fields.Count; i++) {
                if (
                    fields[i].GetCustomAttribute(typeof(PrimaryKeyAttribute)) != null ||
                    fields[i].GetCustomAttribute<FieldAttribute>()?.Unique == true
                )
                    sb.Append(", ");
            }
            return sb;
        }

        public IQueryBuilder GenerateFieldsString(Type type, bool ommitPk = false) {
            QueryBuilder sb = new QueryBuilder();
            var fields = GetMembers(type);
            for (int i = 0; i < fields.Count; i++) {
                if (ommitPk && fields[i].GetCustomAttribute(typeof(PrimaryKeyAttribute)) != null && fields[i].GetCustomAttribute<ReliableIdAttribute>() == null)
                    continue;
                if (!sb.IsEmpty)
                    sb.Append(", ");
                sb.Append(fields[i].Name);
            }
            return sb;
        }

        public IQueryBuilder InformationSchemaQueryTables(String schema) {
            return new QueryBuilder().Append("SELECT * FROM information_schema.tables WHERE TABLE_CATALOG=@1 AND TABLE_SCHEMA='public';", schema);
        }
        public IQueryBuilder InformationSchemaQueryColumns(String schema) {
            return new QueryBuilder().Append("SELECT * FROM information_schema.columns WHERE TABLE_CATALOG=@1 AND TABLE_SCHEMA='public';", schema);
        }
        public IQueryBuilder InformationSchemaQueryKeys(string schema) {
            return new QueryBuilder().Append(
                @"SELECT
	                tc.*,
                    kcu.COLUMN_NAME, 
                    kcu.table_schema AS REFERENCED_TABLE_SCHEMA,
                    kcu.table_name AS REFERENCED_TABLE_NAME,
                    kcu.column_name AS REFERENCED_COLUMN_NAME
                FROM 
                    information_schema.table_constraints AS tc 
                    LEFT JOIN information_schema.key_column_usage AS kcu
                      ON tc.constraint_name = kcu.constraint_name
                      AND tc.table_schema = kcu.table_schema
                    LEFT JOIN information_schema.constraint_column_usage AS ccu
                      ON ccu.constraint_name = tc.constraint_name AND
                        ccu.table_name = tc.table_name
                        AND ccu.table_schema = tc.table_schema AND tc.CONSTRAINT_TYPE='FOREIGN KEY'
  
                WHERE tc.CONSTRAINT_CATALOG=@1 AND tc.CONSTRAINT_SCHEMA='public' AND tc.CONSTRAINT_TYPE!='CHECK';", schema);
        }
        public IQueryBuilder InformationSchemaIndexes(string schema) {
            return new QueryBuilder().Append(
                @"SELECT *, relname AS TABLE_NAME, indexrelname AS CONSTRAINT_NAME, pg_size_pretty(pg_relation_size(indexrelname::text))
                FROM pg_stat_all_indexes
                WHERE schemaname = 'public';");
        }

        public IQueryBuilder RenameTable(string tabName, string newName) {
            return new QueryBuilder().Append($"ALTER TABLE {tabName} RENAME TO {newName};");
        }

        public IQueryBuilder UpdateColumn(string table, string column, object value, IQueryBuilder conditions) {
            return new QueryBuilder().Append($"UPDATE {table} SET {column}=@value WHERE ").Append(conditions);
        }
        public IQueryBuilder RenameColumn(string table, string column, string newName) {
            return new QueryBuilder().Append($"ALTER TABLE {table} RENAME COLUMN {column} TO {newName};");
        }
        public IQueryBuilder AlterColumnDataType(string table, MemberInfo member, FieldAttribute fieldAttribute) {
            return new QueryBuilder().Append($"ALTER TABLE {table} ALTER COLUMN {member.Name} TYPE {GetDatabaseTypeWithLength(member, fieldAttribute)};");
        }
        public IQueryBuilder AlterColumnNullability(string table, MemberInfo member, FieldAttribute fieldAttribute) {
            return new QueryBuilder().Append($"ALTER TABLE {table} ALTER COLUMN {member.Name} {(fieldAttribute.AllowNull ? "DROP" : "SET")} NOT NULL;");
        }

        public IQueryBuilder DropForeignKey(string target, string constraint) {
            return new QueryBuilder().Append($"ALTER TABLE {target} DROP CONSTRAINT {constraint};");
        }
        public IQueryBuilder DropColumn(string table, string column) {
            return new QueryBuilder().Append($"ALTER TABLE {table} DROP COLUMN {column};");
        }
        public IQueryBuilder DropUnique(string target, string constraint) {
            return new QueryBuilder().Append($"ALTER TABLE {target} DROP KEY {constraint};");
        }
        public IQueryBuilder DropIndex(string target, string constraint) {
            return new QueryBuilder().Append($"DROP INDEX {constraint};");
        }
        public IQueryBuilder DropPrimary(string target, string constraint) {
            return new QueryBuilder().Append($"ALTER TABLE {target} DROP PRIMARY KEY;");
        }

        public IQueryBuilder AddColumn(string table, string columnDefinition) {
            return new QueryBuilder().Append($"ALTER TABLE {table} ADD COLUMN {columnDefinition};");
        }

        public IQueryBuilder AddIndex(string table, string column, string constraintName) {
            return new QueryBuilder().Append($"CREATE INDEX {constraintName} ON {table} ({column});");
        }
        public IQueryBuilder AddForeignKey(string table, string column, string refTable, string refColumn, string constraintName) {
            return new QueryBuilder().Append($"ALTER TABLE {table} ADD CONSTRAINT {constraintName} FOREIGN KEY ({column}) REFERENCES {refTable}({refColumn})");
        }

        public IQueryBuilder AddIndexForUniqueKey(string table, string column, string constraintName) {
            table = table.ToLower();
            column = column.ToLower();
            return new QueryBuilder().Append($"ALTER TABLE {table} ADD INDEX {column} ({column});");
        }
        public IQueryBuilder AddUniqueKey(string table, string column, string constraintName) {
            table = table.ToLower();
            column = column.ToLower();
            return new QueryBuilder().Append($"CREATE UNIQUE INDEX {constraintName} ON {table} ({column});");
        }
        public IQueryBuilder AddPrimaryKey(string table, string column, string constraintName) {
            table = table.ToLower();
            column = column.ToLower();
            return new QueryBuilder().Append($"ALTER TABLE {table} ADD CONSTRAINT {constraintName} PRIMARY KEY ({column})");
        }

        public IQueryBuilder Purge(string table, string column, string refTable, string refColumn, bool isNullable) {
            if (!isNullable) {
                return new QueryBuilder().Append($"UPDATE {table} SET {column}=NULL WHERE {column} NOT IN (SELECT {refColumn} FROM {refTable})");
            } else {
                return new QueryBuilder().Append($"DELETE FROM {table} WHERE {column} IS NOT NULL AND {column} NOT IN (SELECT {refColumn} FROM {refTable})");
            }
        }

        private List<MemberInfo> GetMembers(Type t) {
            List<MemberInfo> lifi = new List<MemberInfo>();
            var members = ReflectionTool.FieldsAndPropertiesOf(t);
            foreach (var fi in members
                .Where((a) => a.GetCustomAttribute(typeof(FieldAttribute)) != null)
                .ToArray()) {
                foreach (var at in fi.CustomAttributes) {
                    if (at.AttributeType == typeof(FieldAttribute)) {
                        lifi.Add(fi);
                    }
                }
            }
            return lifi;
        }

        public IQueryBuilder GetLastInsertId<T>() where T : IDataObject, new() {
            return new QbFmt("SELECT last_insert_id()");
        }

        public IQueryBuilder GetIdFromRid<T>(object Rid) where T : IDataObject, new() {
            var id = ReflectionTool.FieldsAndPropertiesOf(typeof(T)).FirstOrDefault(f => f.GetCustomAttribute<PrimaryKeyAttribute>() != null);
            var rid = ReflectionTool.FieldsAndPropertiesOf(typeof(T)).FirstOrDefault(f => f.GetCustomAttribute<ReliableIdAttribute>() != null);
            return new QueryBuilder().Append($"SELECT {id.Name} FROM {typeof(T).Name} WHERE {rid.Name}=@???", Rid);
        }

        public IQueryBuilder GetCreationCommand(ForeignKeyAttribute fkd) {
            var cname = $"fk_{fkd.Column}_{fkd.RefTable}_{fkd.RefColumn}";
            String creationCommand = $"ALTER TABLE {fkd.Table} ADD CONSTRAINT {cname} FOREIGN KEY ({fkd.Column}) REFERENCES {fkd.RefTable} ({fkd.RefColumn});";
            return new QbFmt(creationCommand);
        }

        public IQueryBuilder QueryIds<T>(List<T> rs) where T : IDataObject {
            var type = rs.FirstOrDefault()?.GetType()??typeof(T);
            var id = FiTechBDadosExtensions.IdColumnOf[type];
            var rid = FiTechBDadosExtensions.RidColumnOf[type];
            var ridType = ReflectionTool.GetTypeOf(ReflectionTool.FieldsAndPropertiesOf(type).FirstOrDefault(x => x.GetCustomAttribute<ReliableIdAttribute>() != null));

            return Qb.Fmt($"SELECT {id} AS Id, {rid} AS RID FROM {type.Name} WHERE") + Qb.In(rid, rs, i => Convert.ChangeType(i.RID, ridType));
        }

        public IQueryBuilder GenerateGetStateChangesQuery(List<Type> workingTypes, Dictionary<Type, MemberInfo[]> fields, DateTime moment) {
            throw new NotImplementedException();
        }

        public IQueryBuilder DisableForeignKeys() {
            return Qb.Fmt("SET session_replication_role = 'replica';");
        }
        public IQueryBuilder EnableForeignKeys() {
            return Qb.Fmt("SET session_replication_role = 'origin';");
        }
    }
}
