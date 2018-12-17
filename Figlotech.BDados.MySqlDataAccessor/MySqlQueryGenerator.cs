
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

        public IQueryBuilder GetCreationCommand(Type t) {
            String objectName = t.Name;

            var members = ReflectionTool.FieldsAndPropertiesOf(t);
            members.RemoveAll(
                (m) => m?.GetCustomAttribute<FieldAttribute>() == null);
            if (objectName == null || objectName.Length == 0)
                return null;
            QueryBuilder CreateTable = new QbFmt($"CREATE TABLE IF NOT EXISTS {objectName} (\n");
            for (int i = 0; i < members.Length; i++) {
                var info = members[i].GetCustomAttribute<FieldAttribute>();
                CreateTable.Append(Fi.Tech.GetColumnDefinition(members[i], info));
                CreateTable.Append(" ");
                CreateTable.Append(info.Options ?? "");
                if (i != members.Length - 1) {
                    CreateTable.Append(", \n");
                }
            }
            CreateTable.Append(" );");
            return CreateTable;
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

        static Dictionary<Type, QueryBuilder> AutoSelectCache = new Dictionary<Type, QueryBuilder>();
        public IQueryBuilder GenerateSelect<T>(IQueryBuilder condicoes = null, int? skip = null, int? limit = null, MemberInfo orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new() {
            
            var alias = "tba";
            QueryBuilder Query = new QueryBuilder();
            if (!AutoSelectCache.ContainsKey(typeof(T))) {
                Fi.Tech.WriteLine($"Generating SELECT {condicoes} {skip} {limit} {orderingMember?.Name} {ordering}");
                QueryBuilder baseSelect = new QbFmt("SELECT ");
                baseSelect.Append(GenerateFieldsString(typeof(T), false));
                baseSelect.Append(String.Format($"FROM {typeof(T).Name} AS { alias }"));
                AutoSelectCache[typeof(T)] = baseSelect;
            }

            Query.Append(AutoSelectCache[typeof(T)]);

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

        static string sid = IntEx.GenerateShortRid();
        static int gid = 0;

        public IQueryBuilder GenerateMultiUpdate<T>(IList<T> inputRecordset) where T : IDataObject {
            // -- 
            IList<T> workingSet = new List<T>();

            var rid = FiTechBDadosExtensions.RidColumnOf[typeof(T)];

            workingSet.AddRange(inputRecordset.Where((record) => record.IsPersisted));
            if (workingSet.Count < 1) {
                return null;
            }
            QueryBuilder Query = new QueryBuilder();
            Query.Append($"UPDATE {typeof(T).Name} ");
            Query.Append("SET \r\n");

            // -- 
            var members = GetMembers(typeof(T));
            members.RemoveAll(
                m => 
                    m.GetCustomAttribute<PrimaryKeyAttribute>() != null ||
                    m.GetCustomAttribute<ReliableIdAttribute>() != null
                    );
            int x = 0;
            int ggid = ++gid;
            for (var i = 0; i < members.Count; i++) {
                var memberType = ReflectionTool.GetTypeOf(members[i]);
                Query.Append($"\t{members[i].Name}=(CASE ");
                foreach (var a in inputRecordset) {
                    Query.Append($"WHEN {rid}=@{sid}_{ggid}_{++x} THEN @{sid}_{ggid}_{++x}", a.RID, ReflectionTool.GetMemberValue(members[i], a));
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

        public IQueryBuilder GenerateMultiInsert<T>(IList<T> inputRecordset, bool OmmitPk = false) where T : IDataObject {
            IList<T> workingSet = new List<T>();
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
            return new QueryBuilder().Append("SELECT * FROM information_schema.key_column_usage WHERE CONSTRAINT_SCHEMA=@1;", schema);
        }
        public IQueryBuilder InformationSchemaIndexes(string schema) {
            return new QueryBuilder().Append("SELECT * FROM information_schema.statistics WHERE INDEX_SCHEMA=@1;", schema);
        }

        public IQueryBuilder RenameTable(string tabName, string newName) {
            return new QueryBuilder().Append($"RENAME TABLE {tabName} TO {newName};");
        }

        public IQueryBuilder RenameColumn(string table, string column, string newDefinition) {
            return new QueryBuilder().Append($"ALTER TABLE {table} CHANGE COLUMN {column} {newDefinition};");
        }

        public IQueryBuilder DropForeignKey(string target, string constraint) {
            return new QueryBuilder().Append($"ALTER TABLE {target} DROP FOREIGN KEY {constraint};");
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

        public IQueryBuilder QueryIds<T>(IList<T> rs) where T : IDataObject {
            if(!rs.Any()) {
                return Qb.Fmt("SELECT 1 as Id, 'no-rid' as RID WHERE FALSE");
            }
            var t = rs.First().GetType();
            var id = ReflectionTool.FieldsAndPropertiesOf(t).FirstOrDefault(f => f.GetCustomAttribute<PrimaryKeyAttribute>() != null);
            var rid = ReflectionTool.FieldsAndPropertiesOf(t).FirstOrDefault(f => f.GetCustomAttribute<ReliableIdAttribute>() != null);

            var retv = Qb.Fmt($"SELECT {id.Name}, {rid.Name} FROM {t.Name} WHERE") + Qb.In(rid.Name, rs, i => i.RID);

            if(rs.Any(i=> i.Id>0)) {
                var existingIds = rs.Where(i => i.Id > 0).ToList();
                if (existingIds.Count > 0) {
                    retv += Qb.Or() + Qb.In(id.Name, existingIds, i => i.Id);
                }
            }
            return retv;
        }
    }
}
