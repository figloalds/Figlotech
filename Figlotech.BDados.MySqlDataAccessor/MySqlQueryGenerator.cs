
/**
 * Figlotech.BDados.Builders.MySqlQueryGenerator
 * MySQL implementation for IQueryGEnerator, used by the MySqlDataAccessor
 * For generating SQL Queries.
 * 
 * @Author: Felype Rennan Alves dos Santos
 * August/2014
 * 
**/

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.Core.Interfaces;
using System.Reflection;
using System.Linq.Expressions;
using Figlotech.BDados.Helpers;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using Figlotech.Core;
using System.Text.RegularExpressions;
using Figlotech.BDados.Builders;
using Figlotech.Core.Helpers;
using System.Diagnostics;

namespace Figlotech.BDados.MySqlDataAccessor {
    public class MySqlQueryGenerator : IQueryGenerator {

        public IQueryBuilder CreateDatabase(string schemaName) {
            return new QueryBuilder($"CREATE DATABASE IF NOT EXISTS {schemaName}");
        }

        public IQueryBuilder GenerateInsertQuery(IDataObject inputObject) {
            QueryBuilder query = new QbFmt($"INSERT INTO {inputObject.GetType().Name}");
            query.Append("(");
            query.Append(GenerateFieldsString(inputObject.GetType(), true));
            query.Append(")");
            query.Append("VALUES (");
            var members = GetMembers(inputObject.GetType());
            members.RemoveAll(m => m.GetCustomAttribute<PrimaryKeyAttribute>() != null);
            for (int i = 0; i < members.Count; i++) {
                var val = ReflectionTool.GetMemberValue(members[i], inputObject);
                if (val == null) {
                    var pc = members[i].GetCustomAttribute<PreemptiveCounter>();
                    var ic = members[i].GetCustomAttribute<IncrementalCounter>();

                    if (pc != null) {
                        query.Append(pc.OnInsertSubQuery(inputObject.GetType(), members[i]));
                    } else
                    if (ic != null) {
                        query.Append(ic.OnInsertSubQuery(inputObject.GetType(), members[i]));
                    } else {
                        query.Append($"@{members[i].Name}_{i + 1}", val);
                    }
                } else {
                    query.Append($"@{members[i].Name}_{i + 1}", val);
                }

                if (i < members.Count - 1) {
                    query.Append(",");
                }
            }
            query.Append(")");
            //query.Append("ON DUPLICATE KEY UPDATE ");
            //var Fields = GetMembers(inputObject.GetType()).Where(field =>
            //    field.GetCustomAttribute(typeof(PrimaryKeyAttribute)) == null &&
            //    field.GetCustomAttribute(typeof(ReliableIdAttribute)) == null
            //).ToList();
            //for (int i = 0; i < Fields.Count; ++i) {
            //    query.Append(String.Format("{0} = VALUES({0})", Fields[i].Name));
            //    if (i < Fields.Count - 1) {
            //        query.Append(",");
            //    }
            //}
            return query;
        }
        public IQueryBuilder GenerateGetStateChangesQuery(List<Type> workingTypes, Dictionary<Type, MemberInfo[]> fields, DateTime moment) {
            var dataLen = fields.Max(f => f.Value.Length);
            Qb query = new Qb("");
            workingTypes.ForEachIndexed((type, ti) => {
                var cTimeField = fields[type].FirstOrDefault(f => f.GetCustomAttribute<CreationTimeStampAttribute>() != null);
                var uTimeField = fields[type].FirstOrDefault(f => f.GetCustomAttribute<UpdateTimeStampAttribute>() != null);
                query.Append($"(SELECT \r\n\t'{type.Name}' AS TypeName, ");
                for(int i = 0; i < dataLen; i++) {
                    if(fields[type].Length > i) {
                        query.Append($"\r\n\tCAST({fields[type][i].Name} AS BINARY)");
                    } else {
                        query.Append("\r\n\tNULL");
                    }
                    if(ti == 0)
                        query.Append($"AS data_{i}");
                    if(i < dataLen -1) {
                        query.Append(",");
                    }
                }
                query.Append("\r\nFROM");
                query.Append(type.Name);
                if(moment != DateTime.MinValue && (cTimeField != null || uTimeField != null)) {
                    query.Append("\r\nWHERE");
                    if(cTimeField != null) {
                        query.Append($"{cTimeField.Name} > @m", moment);
                    }
                    if (cTimeField != null && uTimeField != null)
                        query.Append("OR");
                    if (uTimeField != null)
                        query.Append($"{uTimeField.Name} > @m", moment);
                }
                query.Append(")\r\n");

                if(ti < workingTypes.Count - 1) {
                    query.Append("UNION ALL\r\n");
                }
            });

            return query;
        }

        public IQueryBuilder GetCreationCommand(Type t) {
            String objectName = t.Name;

            var members = 
                ReflectionTool
                    .FieldsAndPropertiesOf(t).Where((m) => m?.GetCustomAttribute<FieldAttribute>() != null)
                    .ToArray();
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
                if(info.Unsigned) {
                    options += " UNSIGNED";
                }
                if (info.Unique)
                    options += " UNIQUE";
                if ((info.AllowNull && info.DefaultValue == null) || info.DefaultValue != null)
                    options += $" DEFAULT {Fi.Tech.CheapSanitize(info.DefaultValue)}";
                foreach (var att in field.GetCustomAttributes())
                    if (att is PrimaryKeyAttribute)
                        options += " AUTO_INCREMENT PRIMARY KEY";
            }

            return $"{nome} {tipo} {options}";
        }

        public String GetDatabaseTypeWithLength(MemberInfo field, FieldAttribute info = null) {
            String retv = GetDatabaseType(field, info);
            if (info.Type != null && info.Type.Length > 0)
                retv = info.Type;
            if (retv == "VARCHAR" || retv == "VARBINARY") {
                retv += $"({(info.Size > 0 ? info.Size : 100)})";
            }
            if (retv == "FLOAT" || retv == "DOUBLE" || retv == "DECIMAL") {
                retv += $"(20,{info.Precision})";
            }
            return retv;
        }

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
                return "INT";
            }
            String type = "VARCHAR";
            if (info.Type != null && info.Type.Length > 0) {
                type = info.Type;
            } else {
                switch (tipoDados.ToLower()) {
                    case "string":
                        type = $"VARCHAR";
                        break;
                    case "short":
                    case "int16":
                        type = $"SMALLINT";
                        break;
                    case "ushort":
                    case "uint16":
                        type = $"SMALLINT";
                        break;
                    case "int":
                    case "int32":
                        type = $"INT";
                        break;
                    case "uint":
                    case "uint32":
                        type = $"INT";
                        break;
                    case "long":
                    case "int64":
                        type = $"BIGINT";
                        break;
                    case "ulong":
                    case "uint64":
                        type = $"BIGINT";
                        break;
                    case "bool":
                    case "boolean":
                        type = $"TINYINT";
                        break;
                    case "float":
                    case "double":
                    case "single":
                    case "decimal":
                        type = $"DECIMAL";
                        break;
                    case "byte[]":
                        type = $"BLOB";
                        break;
                    case "datetime":
                        type = $"DATETIME";
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

        static Dictionary<JoinDefinition, QueryBuilder> AutoJoinCache = new Dictionary<JoinDefinition, QueryBuilder>();
        public IQueryBuilder GenerateJoinQuery(JoinDefinition inputJoin, IQueryBuilder conditions, int? skip = null, int? take = null, MemberInfo orderingMember = null, OrderingType otype = OrderingType.Asc, IQueryBuilder rootConditions = null) {
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

            var isLinedAggregateJoin = false; // && conditions.GetCommandText() == rootConditions.GetCommandText() && conditions.GetParameters().SequenceEqual(rootConditions.GetParameters());

            QueryBuilder Query = new QueryBuilder();

            if (!AutoJoinCache.ContainsKey(inputJoin)) {
                lock (AutoJoinCache) {
                    if (!AutoJoinCache.ContainsKey(inputJoin)) {
                        // By caching this heavy process I might gain loads of performance
                        // When redoing the same queries.
                        QueryBuilder autoJoinMain = new QbFmt("SELECT sub.*\n");
                        autoJoinMain.Append($"\t FROM (SELECT\n");
                        for (int i = 0; i < tables.Count; i++) {
                            autoJoinMain.Append($"\t\t-- Table {tableNames[i]}\n");
                            var ridF = FiTechBDadosExtensions.RidColumnOf[tables[i]];
                            if (!columns[i].Any(c => c.ToUpper() == ridF.ToUpper()))
                                columns[i].Add(ridF);
                            var nonexcl = columns[i];
                            for (int j = 0; j < nonexcl.Count; j++) {
                                autoJoinMain.Append($"\t\t{prefixes[i]}.{nonexcl[j]} AS {prefixes[i]}_{nonexcl[j]},\n");
                            }
                            autoJoinMain.Append("\n");
                        }

                        autoJoinMain.Append($"\t\t1 FROM (SELECT * FROM {tableNames[0]}");

                        if (isLinedAggregateJoin) {
                            if (rootConditions != null) {
                                autoJoinMain.Append("WHERE ");
                                autoJoinMain.Append(rootConditions);
                            }
                            if (orderingMember != null) {
                                autoJoinMain.Append($"ORDER BY {orderingMember.Name} {otype.ToString().ToUpper()}");
                            }
                            if (skip != null || take != null) {
                                autoJoinMain.Append("LIMIT ");
                                autoJoinMain.Append(
                                    skip != null ? $"{skip},{take ?? Int32.MaxValue}" : $"{take ?? Int32.MaxValue}"
                                );
                            }
                            autoJoinMain.Append($"");
                        }
                        autoJoinMain.Append($") AS {prefixes[0]}\n");

                        for (int i = 1; i < tables.Count; i++) {
                            autoJoinMain.Append($"\t\t{"LEFT"} JOIN {tableNames[i]} AS {prefixes[i]} ON {onclauses[i]}\n");
                        }
                        AutoJoinCache[inputJoin] = (autoJoinMain);
                    }
                }
            }
            Query.Append(AutoJoinCache[inputJoin]);

            if (!isLinedAggregateJoin) {
                if (conditions != null && !conditions.IsEmpty) {
                    Query.Append("\tWHERE");
                    Query.Append(conditions);
                }
                if (orderingMember != null) {
                    Query.Append($"ORDER BY {prefixes[0]}.{orderingMember.Name} {otype.ToString().ToUpper()}");
                }
                if (skip != null || take != null) {
                    Query.Append("LIMIT ");
                    Query.Append(
                        skip != null ? $"{skip},{take ?? Int32.MaxValue}" : $"{take ?? Int32.MaxValue}"
                    );
                }
            }


            Query.Append(") AS sub\n");

            return Query;
        }

        public IQueryBuilder GenerateSaveQuery(IDataObject tabelaInput) {
            return new QueryBuilder();
        }

        public IQueryBuilder GenerateSelectAll<T>() where T : IDataObject, new() {
            var alias = "tba";
            QueryBuilder Query = new QueryBuilder();
            if (!AutoSelectCache.ContainsKey(typeof(T))) {
                QueryBuilder baseSelect = new QbFmt("SELECT ");
                baseSelect.Append(GenerateFieldsString(typeof(T), false));
                baseSelect.Append(String.Format($"FROM {typeof(T).Name} AS { alias }"));
                AutoSelectCache[typeof(T)] = baseSelect;
            }

            Query.Append(AutoSelectCache[typeof(T)]);
            return Query;
        }

        static DynaLocks LocksAutoSelectCache = new DynaLocks();
        static Dictionary<Type, QueryBuilder> AutoSelectCache = new Dictionary<Type, QueryBuilder>();
        public IQueryBuilder GenerateSelect<T>(IQueryBuilder condicoes = null, int? skip = null, int? limit = null, MemberInfo orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new() {
            
            var alias = "tba";
            QueryBuilder Query = new QueryBuilder();

            lock(LocksAutoSelectCache[$"MYSQL_SELECTS_{typeof(T).Name}"]) {
                if (!AutoSelectCache.ContainsKey(typeof(T))) {
                    Fi.Tech.WriteLine($"Generating SELECT {condicoes} {skip} {limit} {orderingMember?.Name} {ordering}");
                    QueryBuilder baseSelect = new QbFmt("SELECT ");
                    baseSelect.Append(GenerateFieldsString(typeof(T), false));
                    baseSelect.Append(String.Format($"FROM {typeof(T).Name} AS { alias }"));
                    AutoSelectCache[typeof(T)] = baseSelect;
                }

                Query.Append(AutoSelectCache[typeof(T)]);
            }


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
            var rid = FiTechBDadosExtensions.RidColumnOf[tabelaInput.GetType()];
            QueryBuilder Query = new QbFmt(String.Format("UPDATE {0} ", tabelaInput.GetType().Name));
            Query.Append("SET");
            Query.Append(GenerateUpdateValueParams(tabelaInput, true));
            Query.Append($" WHERE {rid} = @rid;", tabelaInput.RID);
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
            QueryBuilder Query = new QueryBuilder();
            var fields = GetMembers(tabelaInput.GetType());
            if (OmmitPK) {
                fields.RemoveAll(m => m.GetCustomAttribute<PrimaryKeyAttribute>() != null);
            }
            for (int i = 0; i < fields.Count; i++) {
                Object val = ReflectionTool.GetMemberValue(fields[i], tabelaInput);
                if (!Query.IsEmpty)
                    Query.Append(", ");
                Query.Append($"@{gid++}_{fields[i].Name}", val);
            }
            return Query;
        }

        static int gid = 0;

        public IQueryBuilder GenerateMultiUpdate<T>(List<T> inputRecordset) where T : IDataObject {
            // -- 
            var t = inputRecordset?.FirstOrDefault()?.GetType();
            if(t == null) {
                return Qb.Fmt("SELECT 1");
            }

            List<T> workingSet = new List<T>();

            var rid = FiTechBDadosExtensions.RidColumnOf[t];
            var upd = FiTechBDadosExtensions.UpdateColumnOf[t];

            workingSet.AddRange(inputRecordset.Where((record) => record.IsPersisted));
            if (workingSet.Count < 1) {
                return null;
            }
            QueryBuilder Query = new QueryBuilder();
            Query.Append($"UPDATE {t.Name} ");
            Query.Append("SET \r\n");

            // -- 
            var members = GetMembers(t);
            members.RemoveAll(
                m => 
                    m.GetCustomAttribute<PrimaryKeyAttribute>() != null ||
                    m.GetCustomAttribute<ReliableIdAttribute>() != null
                    );

            int x = 0;
            int ggid = ++gid;
            Query.PrepareForQueryLength(inputRecordset.Count * 512);
            for (var i = 0; i < members.Count; i++) {
                var memberType = ReflectionTool.GetTypeOf(members[i]);
                Query.Append($"\t{members[i].Name}=(CASE ");
                for(int ridx = 0; ridx < inputRecordset.Count; ridx++) {
                    Query.Append($"WHEN {rid}=@r_{ridx}", inputRecordset[ridx].RID);
                    if(inputRecordset[ridx].IsReceivedFromSync) {
                        Query.Append($"AND {upd}<@u_{ridx}", inputRecordset[ridx].UpdatedTime);
                    }
                    Query.Append($"THEN @{ggid}_{++x}", ReflectionTool.GetMemberValue(members[i], inputRecordset[ridx]));
                }
                Query.Append($"ELSE {members[i].Name} END)");
                if (i < members.Count - 1) {
                    Query.Append(",\r\n");
                }
            }
            
            Query.Append($"WHERE {rid} IN (")
                .Append(Fi.Tech.ListRids(workingSet))
                .Append(");");
            // --
            return Query;
        }

        public IQueryBuilder GenerateMultiInsert<T>(List<T> inputRecordset, bool OmmitPk = false) where T : IDataObject {

            var t = inputRecordset.FirstOrDefault()?.GetType();
            if(t == null) {
                return Qb.Fmt("SELECT 1");
            }

            List<T> workingSet = new List<T>();
            workingSet.AddRange(inputRecordset.Where((r) => !r.IsPersisted));
            if (workingSet.Count < 1) return null;
            // -- 
            QueryBuilder Query = new QueryBuilder();
            Query.Append($"INSERT INTO {t.Name} (");
            Query.Append(GenerateFieldsString(t, OmmitPk));
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
            //Query.Append("ON DUPLICATE KEY UPDATE ");
            //var Fields = GetMembers(typeof(T)).Where(
            //    field=> 
            //    (OmmitPk || field.GetCustomAttribute<PrimaryKeyAttribute>() == null) && 
            //    field.GetCustomAttribute<ReliableIdAttribute>() == null
            //).ToList();
            //for (int i = 0; i < Fields.Count; ++i) {
            //    Query.Append($"{Fields[i].Name} = VALUES({Fields[i].Name})");
            //    if (i < Fields.Count - 1) {
            //        Query.Append(",");
            //    }
            //}
            // -- 
            return Query;
        }

        public IQueryBuilder GenerateFieldsString(Type type, bool ommitPk = false) {
            QueryBuilder sb = new QueryBuilder();
            var fields = GetMembers(type);
            for (int i = 0; i < fields.Count; i++) {
                if (ommitPk && fields[i].GetCustomAttribute(typeof(PrimaryKeyAttribute)) != null)
                    continue;
                if (!sb.IsEmpty)
                    sb.Append(", ");
                sb.Append(fields[i].Name);
            }
            return sb;
        }

        public IQueryBuilder InformationSchemaQueryTables(String schema) {
            return new QueryBuilder().Append("SELECT * FROM information_schema.tables WHERE TABLE_SCHEMA=@1;", schema);
        }
        public IQueryBuilder InformationSchemaQueryColumns(String schema) {
            return new QueryBuilder().Append("SELECT * FROM information_schema.columns WHERE TABLE_SCHEMA=@1;", schema);
        }
        public IQueryBuilder InformationSchemaQueryKeys(string schema) {
            return new QueryBuilder().Append(
                @"SELECT
                    tc.*,
                    kcu.COLUMN_NAME, 
	                (CASE WHEN tc.CONSTRAINT_TYPE='FOREIGN KEY' THEN kcu.table_schema ELSE NULL END) AS REFERENCED_TABLE_SCHEMA,
	                (CASE WHEN tc.CONSTRAINT_TYPE='FOREIGN KEY' THEN kcu.table_name ELSE NULL END) AS REFERENCED_TABLE_NAME,
	                (CASE WHEN tc.CONSTRAINT_TYPE='FOREIGN KEY' THEN kcu.column_name ELSE NULL END) AS REFERENCED_COLUMN_NAME
                FROM (
                    SELECT * FROM information_schema.table_constraints
                    WHERE TABLE_SCHEMA=@1
                ) AS tc
                LEFT JOIN (
                    SELECT * FROM information_schema.key_column_usage AS kcu
                    WHERE TABLE_SCHEMA=@1
                ) kcu
                    ON tc.constraint_name = kcu.constraint_name
                    AND tc.table_schema = kcu.table_schema
                    AND tc.table_name = kcu.table_name
                WHERE tc.TABLE_SCHEMA=@1 AND kcu.TABLE_SCHEMA=@1;", schema
            );
        }
        public IQueryBuilder InformationSchemaIndexes(string schema) {
            return new QueryBuilder().Append("SELECT * FROM information_schema.statistics WHERE TABLE_SCHEMA=@1 AND NON_UNIQUE=1;", schema);
        }

        public IQueryBuilder RenameTable(string tabName, string newName) {
            return new QueryBuilder().Append($"RENAME TABLE {tabName} TO {newName};");
        }

        public IQueryBuilder UpdateColumn(string table, string column, object value, IQueryBuilder conditions) {
            return new QueryBuilder().Append($"UPDATE {table} SET {column}=@value WHERE ").Append(conditions);
        }
        public IQueryBuilder RenameColumn(string table, string column, string newDefinition) {
            return new QueryBuilder().Append($"ALTER TABLE {table} RENAME {column} TO {newDefinition};");
        }
        public IQueryBuilder AlterColumnDataType(string table, MemberInfo member, FieldAttribute fieldAttribute) {
            return new QueryBuilder().Append($"ALTER TABLE {table} CHANGE COLUMN {member.Name} {GetColumnDefinition(member, fieldAttribute)};");
        }
        public IQueryBuilder AlterColumnNullability(string table, MemberInfo member, FieldAttribute fieldAttribute) {
            return new QueryBuilder().Append($"ALTER TABLE {table} CHANGE COLUMN {member.Name} {GetColumnDefinition(member, fieldAttribute)};");
        }

        public IQueryBuilder DropForeignKey(string target, string constraint) {
            return new QueryBuilder().Append($"ALTER TABLE {target} DROP FOREIGN KEY {constraint};");
        }
        public IQueryBuilder DropColumn(string table, string column) {
            return new QueryBuilder().Append($"ALTER TABLE {table} DROP COLUMN {column};");
        }
        public IQueryBuilder DropUnique(string target, string constraint) {
            return new QueryBuilder().Append($"ALTER TABLE {target} DROP KEY {constraint};");
        }
        public IQueryBuilder DropIndex(string target, string constraint) {
            return new QueryBuilder().Append($"ALTER TABLE {target} DROP INDEX {constraint};");
        }
        public IQueryBuilder DropPrimary(string target, string constraint) {
            return new QueryBuilder().Append($"ALTER TABLE {target} DROP PRIMARY KEY;");
        }

        public IQueryBuilder AddColumn(string table, string columnDefinition) {
            return new QueryBuilder().Append($"ALTER TABLE {table} ADD COLUMN {columnDefinition};");
        }

        public IQueryBuilder AddIndex(string table, string column, string constraintName) {
            return new QueryBuilder().Append($"ALTER TABLE {table} ADD INDEX {constraintName} ({column});");
        }
        public IQueryBuilder AddForeignKey(string table, string column, string refTable, string refColumn, string constraintName) {
            return new QueryBuilder().Append($"ALTER TABLE {table} ADD CONSTRAINT {constraintName} FOREIGN KEY ({column}) REFERENCES {refTable}({refColumn})");
        }

        public IQueryBuilder AddIndexForUniqueKey(string table, string column, string constraintName) {
            table = table.ToLower();
            column = column.ToLower();
            return new QueryBuilder().Append($"alter table {table} ADD INDEX {constraintName} ({column});");
        }
        public IQueryBuilder AddUniqueKey(string table, string column, string constraintName) {
            table = table.ToLower();
            column = column.ToLower();
            return new QueryBuilder().Append($"alter table {table} ADD UNIQUE KEY {constraintName}({column});");
        }
        public IQueryBuilder AddPrimaryKey(string table, string column, string constraintName) {
            table = table.ToLower();
            column = column.ToLower();
            return new QueryBuilder().Append($"ALTER TABLE {table} ADD CONSTRAINT PRIMARY KEY({column})");
        }

        public IQueryBuilder Purge(string table, string column, string refTable, string refColumn, bool isNullable) {
            if (!isNullable) {
                return new QueryBuilder().Append($"UPDATE {table} SET {column}=NULL WHERE {column} NOT IN (SELECT {refColumn} FROM {refTable})");
            } else {
                return new QueryBuilder().Append($"DELETE FROM {table} WHERE {column} IS NOT NULL AND {column} NOT IN (SELECT {refColumn} FROM {refTable})");
            }
        }

        private List<MemberInfo> GetMembers(Type t) {
            return ReflectionTool.FieldsWithAttribute<FieldAttribute>(t);
            //List<MemberInfo> lifi = new List<MemberInfo>();
            //var members = ReflectionTool.FieldsAndPropertiesOf(t);
            //foreach (var fi in members
            //    .Where((a) => a.GetCustomAttribute(typeof(FieldAttribute)) != null)
            //    .ToArray()) {
            //    foreach (var at in fi.CustomAttributes) {
            //        if (at.AttributeType == typeof(FieldAttribute)) {
            //            lifi.Add(fi);
            //        }
            //    }
            //}
            //return lifi;
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
            if(!rs.Any()) {
                return Qb.Fmt("SELECT 1 as Id, 'no-rid' as RID WHERE FALSE");
            }
            var t = rs.First().GetType();
            var id = FiTechBDadosExtensions.IdColumnOf[t];
            var rid = FiTechBDadosExtensions.RidColumnOf[t];

            var retv = Qb.Fmt($"SELECT {id}, {rid} FROM {t.Name} WHERE") + Qb.In(rid, rs, i => i.RID);

            //if(rs.Any(i=> i.Id>0)) {
            //    var existingIds = rs.Where(i => i.Id > 0).ToList();
            //    if (existingIds.Count > 0) {
            //        retv += Qb.Or() + Qb.In(id, existingIds, i => i.Id);
            //    }
            //}
            return retv;
        }

        public IQueryBuilder DisableForeignKeys() {
            return Qb.Fmt("SET FOREIGN_KEY_CHECKS=0;");
        }
        public IQueryBuilder EnableForeignKeys() {
            return Qb.Fmt("SET FOREIGN_KEY_CHECKS=0;");
        }
    }
}
