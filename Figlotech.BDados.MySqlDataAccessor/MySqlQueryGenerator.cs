
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

namespace Figlotech.BDados.MySqlDataAccessor {
    public class MySqlQueryGenerator : IQueryGenerator
    {


        public IQueryBuilder GenerateInsertQuery(IDataObject inputObject) {
            QueryBuilder query = new QueryBuilder($"INSERT INTO {inputObject.GetType().Name}");
            query.Append("(");
            query.Append(GenerateFieldsString(inputObject.GetType(), true));
            query.Append(")");
            query.Append("VALUES (");
            var members = GetMembers(inputObject.GetType());
            members.RemoveAll(m => m.GetCustomAttribute<PrimaryKeyAttribute>() != null);
            for (int i = 0; i < members.Count; i++) {
                query.Append($"@{i + 1}", ReflectionTool.GetMemberValue(members[i], inputObject));
                if (i < members.Count - 1) {
                    query.Append(",");
                }
            }
            query.Append(") ON DUPLICATE KEY UPDATE ");
            var Fields = GetMembers(inputObject.GetType());
            for (int i = 0; i < Fields.Count; ++i) {
                if (Fields[i].GetCustomAttribute(typeof(PrimaryKeyAttribute)) != null)
                    continue;
                query.Append(String.Format("{0} = VALUES({0})", Fields[i].Name));
                if (i < Fields.Count - 1) {
                    query.Append(",");
                }
            }
            return query;
        }

        public IQueryBuilder GetCreationCommand(Type t) {
            String objectName = t.Name.ToLower();

            var members = ReflectionTool.FieldsAndPropertiesOf(t);
            members.RemoveAll(
                (m) => m?.GetCustomAttribute<FieldAttribute>() == null);
            if (objectName == null || objectName.Length == 0)
                return null;
            QueryBuilder CreateTable = new QueryBuilder($"CREATE TABLE IF NOT EXISTS {objectName} (\n");
            for (int i = 0; i < members.Count; i++) {
                var info = members[i].GetCustomAttribute<FieldAttribute>();
                CreateTable.Append(Fi.Tech.GetColumnDefinition(members[i], info));
                CreateTable.Append(" ");
                CreateTable.Append(info.Options ?? "");
                if (i != members.Count - 1) {
                    CreateTable.Append(", \n");
                }
            }
            CreateTable.Append(" );");
            return CreateTable;
        }

        public IQueryBuilder GenerateCallProcedure(string name, object[] args) {
            QueryBuilder query = new QueryBuilder("CALL {name}");
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

        public IQueryBuilder GenerateJoinQuery(JoinDefinition juncaoInput, IQueryBuilder conditions, int? p = 1, int? limit = 100, IQueryBuilder condicoesRoot = null) {
            if (condicoesRoot == null)
                condicoesRoot = new QueryBuilder("true");
            if (juncaoInput.Joins.Count < 1)
                throw new BDadosException("This join needs 1 or more tables.");

            List<Type> Tabelas = (from a in juncaoInput.Joins select a.ValueObject).ToList();
            List<String> tableNames = (from a in juncaoInput.Joins select a.ValueObject.Name.ToLower()).ToList();
            List<String> aliases = (from a in juncaoInput.Joins select a.Prefix).ToList();
            List<String> Aliases = (from a in juncaoInput.Joins select a.Alias).ToList();
            List<String> ArgsJuncoes = (from a in juncaoInput.Joins select a.Args).ToList();
            List<List<String>> Exclusoes = (from a in juncaoInput.Joins select a.Excludes).ToList();
            List<JoinType> joinTypes = (from a in juncaoInput.Joins select a.Type).ToList();
            List<int> capturedInSub = new List<int>();
            for (int i = 0; i < Tabelas.Count; i++) {
                //if (i > 0) {
                //    if (TiposJuncao[i] == JoinType.RIGHT || !RelatesWithMain(ArgsJuncoes[i])) {
                //        continue;
                //    }
                //}
                capturedInSub.Add(i);
            }

            QueryBuilder Query = new QueryBuilder("SELECT \n");
            for (int i = 1; i < Tabelas.Count; i++) {
                if (capturedInSub.Contains(i)) {
                    continue;
                }
                Query.Append($"\t-- Table {tableNames[i]}\n");
                var fields = ReflectionTool.FieldsAndPropertiesOf(
                    Tabelas[i])
                    .Where((a) => a.GetCustomAttribute(typeof(FieldAttribute)) != null)
                    .ToArray();
                var nonexcl = fields.Where((c) => !Exclusoes[i].Contains(c.Name)).ToArray();
                for (int j = 0; j < nonexcl.Length; j++) {
                    Query.Append($"\t{aliases[i]}.{nonexcl[j].Name} AS {aliases[i]}_{nonexcl[j].Name}");
                    if (true || j < nonexcl.Length - 1 || i < Tabelas.Count - 1) {
                        Query.Append(",");
                    }
                    Query.Append("\n");
                }
                Query.Append("\n");
            }
            Query.Append($"\tsub.*\n\t FROM (SELECT\n");
            for (int i = 0; i < Tabelas.Count; i++) {
                if (!capturedInSub.Contains(i)) {
                    continue;
                }
                Query.Append($"\t\t-- Table {tableNames[i]}\n");
                var fields = ReflectionTool.FieldsAndPropertiesOf(
                    Tabelas[i])
                    .Where((a) => a.GetCustomAttribute(typeof(FieldAttribute)) != null)
                    .ToArray();
                Exclusoes[i].Remove("RID");
                var nonexcl = fields.Where((c) => !Exclusoes[i].Contains(c.Name)).ToArray();
                for (int j = 0; j < nonexcl.Length; j++) {
                    Query.Append($"\t\t{aliases[i]}.{nonexcl[j].Name} AS {aliases[i]}_{nonexcl[j].Name}");
                    if (true || j < nonexcl.Length - 1 || i < Tabelas.Count - 1) {
                        Query.Append(",");
                    }
                    Query.Append("\n");
                }
                Query.Append("\n");
            }
            Query.Append($"\t\t1 FROM (SELECT * FROM {tableNames[0]}");
            //if (!condicoesRoot.IsEmpty) {
            //    Query.Append("WHERE ");
            //    Query.Append(condicoesRoot);
            //}
            //if (limit != null) {
            //    Query.Append($"LIMIT");
            //    if ((p ?? 0) > 0)
            //        Query.Append($"{(p - 1) * limit}, ");
            //    Query.Append($"{limit}");
            //} else {
            //    Query.Append("LIMIT 99999999999");
            //}
            Query.Append($") AS {aliases[0]}\n");
            for (int i = 1; i < Tabelas.Count; i++) {
                if (!capturedInSub.Contains(i)) {
                    continue;
                }
                //Query.Append($"\t\t{TiposJuncao[i].ToString().Replace('_', ' ').ToUpper()} JOIN {NomesTabelas[i]} AS {Prefixos[i]} ON {ArgsJuncoes[i]}\n");
                Query.Append($"\t\t{"LEFT"} JOIN {tableNames[i]} AS {aliases[i]} ON {ArgsJuncoes[i]}\n");
            }
            //if(limit != null) {
            //    if (condicoes == null) {
            //        condicoes = new QueryBuilder();
            //    }
            //    var q = new QueryBuilder($"{Prefixos[0]}.Id IN (SELECT Id FROM {NomesTabelas[0]}");
            //    q.Append("LIMIT ");
            //    if ((p ?? 0) > 0)
            //        Query.Append($"{(p - 1) * limit}, ");
            //    q.Append($"{limit}");
            //    q.Append(")");
            //    if(condicoes!=null && !condicoes.IsEmpty) {
            //        q.Append("AND");
            //        q.Append("(");
            //        q.Append(condicoes);
            //        q.Append(")");

            //        condicoes = q;
            //    }
            //}
            if (conditions != null && !conditions.IsEmpty) {
                Query.Append("\tWHERE");
                //if (condicoes.GetCommandText().ToLower().Contains("order by")) {
                //    Query.Append(condicoes.GetCommandText().Substring(0, condicoes.GetCommandText().ToUpper().IndexOf("ORDER BY")), condicoes.GetParameters().Select((a) => a.Value).ToArray());
                //} else {
                Query.Append(conditions);
                //}
            }
            if (limit != null) {
                Query.Append($"LIMIT");
                if ((p ?? 0) > 0)
                    Query.Append($"{(p - 1) * limit}, ");
                Query.Append($"{limit}");
            }
            Query.Append(") AS sub\n");
            var capPrefixes = aliases.Where((a, b) => capturedInSub.Contains(b)).ToList();
            for (int i = 1; i < Tabelas.Count; i++) {
                if (capturedInSub.Contains(i)) {
                    continue;
                }
                var onClause = ArgsJuncoes[i];
                onClause = onClause.Replace($"{aliases[i]}.", "##**##");
                var m = Regex.Match(onClause, "(\\w+)\\.").Groups[0].Value;
                if (capPrefixes.Contains(m.Replace(".", ""))) {
                    onClause = Regex.Replace(onClause, "(\\w+)\\.", "sub.$1_");
                }
                if (capPrefixes.Contains(aliases[i])) {
                    onClause = onClause.Replace("##**##", $"sub.{aliases[i]}_");
                }
                else {
                    onClause = onClause.Replace("##**##", $"{aliases[i]}.");
                }
                Query.Append($"\t{joinTypes[i].ToString().Replace('_', ' ').ToUpper()} JOIN {tableNames[i]} AS {aliases[i]} ON {onClause}\n");
            }
            return Query;
        }

        public IQueryBuilder GenerateSaveQuery(IDataObject tabelaInput) {
            return new QueryBuilder();
        }

        public IQueryBuilder GenerateSelectAll<T>() where T : IDataObject, new() {
            var type = typeof(T);
            QueryBuilder Query = new QueryBuilder("SELECT ");
            Query.Append(GenerateFieldsString(type, false));
            Query.Append($"FROM {type.Name.ToLower()} AS a;");
            return Query;
        }

        public IQueryBuilder GenerateSelect<T>(IQueryBuilder condicoes = null) where T : IDataObject, new() {
            var type = typeof(T);
            QueryBuilder Query = new QueryBuilder("SELECT ");
            Query.Append(GenerateFieldsString(type, false));
            Query.Append(String.Format("FROM {0} AS a", type.Name.ToLower()));
            if (condicoes != null && !condicoes.IsEmpty) {
                Query.Append("WHERE");
                Query.Append(condicoes);
            }
            else {
                Query.Append(";");
            }
            return Query;
        }

        public IQueryBuilder GenerateUpdateQuery(IDataObject tabelaInput) {
            var id = Fi.Tech.GetIdColumn(tabelaInput.GetType());
            QueryBuilder Query = new QueryBuilder(String.Format("UPDATE {0} ", tabelaInput.GetType().Name));
            Query.Append("SET");
            Query.Append(GerarParamsValoresUpdate(tabelaInput));
            Query.Append($" WHERE {id} = @id;", tabelaInput.Id);
            return Query;
        }

        internal QueryBuilder GerarParamsValoresUpdate(IDataObject tabelaInput) {
            QueryBuilder Query = new QueryBuilder();
            var lifi = GetMembers(tabelaInput.GetType());
            int k = 0;
            for (int i = 0; i < lifi.Count; i++) {
                if (lifi[i].GetCustomAttribute(typeof(PrimaryKeyAttribute)) != null)
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

        public IQueryBuilder GenerateValuesString(IDataObject tabelaInput) {
            var cod = IntEx.GenerateShortRid();
            QueryBuilder Query = new QueryBuilder();
            var fields = GetMembers(tabelaInput.GetType());
            fields.RemoveAll(m => m.GetCustomAttribute<PrimaryKeyAttribute>() != null);
            for (int i = 0; i < fields.Count; i++) {
                Object val = ReflectionTool.GetMemberValue(fields[i], tabelaInput);
                if (!Query.IsEmpty)
                    Query.Append(", ");
                Query.Append($"@{cod}{i + 1}", val);
            }
            return Query;

        }


        public IQueryBuilder GenerateMultiUpdate<T>(RecordSet<T> inputRecordset) where T : IDataObject, new() {
            // -- 
            RecordSet<T> workingSet = new RecordSet<T>(inputRecordset.DataAccessor);

            var id = Fi.Tech.GetIdColumn<T>();

            workingSet.AddRange(inputRecordset.Where((record) => record.IsPersisted));
            if (workingSet.Count < 1) {
                return null;
            }
            QueryBuilder Query = new QueryBuilder();
            Query.Append($"UPDATE {typeof(T).Name.ToLower()} ");
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
                    Query.Append($"WHEN {id}=@{sid}{x++} THEN @{sid}{x++}", a.Id, ReflectionTool.GetMemberValue(members[i], a));
                }
                Query.Append($"ELSE {members[i].Name} END)");
                if (i < members.Count - 1) {
                    Query.Append(",");
                }
            }

            Query.Append($"WHERE {id} IN (")
                .Append(Fi.Tech.ListIds(workingSet))
                .Append(");");
            // --
            return Query;
        }

        public IQueryBuilder GenerateMultiInsert<T>(RecordSet<T> inputRecordset) where T : IDataObject, new() {
            RecordSet<T> workingSet = new RecordSet<T>();
            workingSet.AddRange(inputRecordset.Where((r) => !r.IsPersisted));
            if (workingSet.Count < 1) return null;
            // -- 
            QueryBuilder Query = new QueryBuilder();
            Query.Append($"INSERT INTO {typeof(T).Name.ToLower()} (");
            Query.Append(GenerateFieldsString(typeof(T), true));
            Query.Append(") VALUES");
            // -- 
            for (int i = 0; i < workingSet.Count; i++) {
                Query.Append("(");
                Query.Append(GenerateValuesString(workingSet[i]));
                Query.Append(")");
                if (i < workingSet.Count - 1)
                    Query.Append(",");
            }
            // -- 
            Query.Append("ON DUPLICATE KEY UPDATE ");
            var Fields = GetMembers(typeof(T));
            for (int i = 0; i < Fields.Count; ++i) {
                if (Fields[i].GetCustomAttribute(typeof(PrimaryKeyAttribute)) != null)
                    continue;
                Query.Append(String.Format("{0} = VALUES({0})", Fields[i].Name));
                if (i < Fields.Count - 1) {
                    Query.Append(",");
                }
            }
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
            return new QueryBuilder("SELECT * FROM information_schema.tables WHERE TABLE_SCHEMA=@1;", schema);
        }
        public IQueryBuilder InformationSchemaQueryColumns(String schema) {
            return new QueryBuilder("SELECT * FROM information_schema.columns WHERE TABLE_SCHEMA=@1;", schema);
        }
        public IQueryBuilder InformationSchemaQueryKeys(string schema) {
            return new QueryBuilder("SELECT * FROM information_schema.key_column_usage WHERE CONSTRAINT_SCHEMA=@1;", schema);
        }

        public IQueryBuilder RenameTable(string tabName, string newName) {
            return new QueryBuilder($"RENAME TABLE {tabName} TO {newName};");
        }

        public IQueryBuilder RenameColumn(string table, string column, string newDefinition) {
            return new QueryBuilder($"ALTER TABLE {table} CHANGE COLUMN {column} {newDefinition};");
        }

        public IQueryBuilder DropForeignKey(string target, string constraint) {
            return new QueryBuilder($"ALTER TABLE {target} DROP FOREIGN KEY {constraint};");
        }

        public IQueryBuilder AddColumn(string table, string columnDefinition) {
            return new QueryBuilder($"ALTER TABLE {table.ToLower()} ADD COLUMN {columnDefinition};");
        }

        public IQueryBuilder AddForeignKey(string table, string column, string refTable, string refColumn) {
            return new QueryBuilder($"ALTER TABLE {table.ToLower()} ADD CONSTRAINT fk_{table.ToLower()}_{column.ToLower()} FOREIGN KEY ({column}) REFERENCES {refTable.ToLower()}({refColumn})");
        }

        public IQueryBuilder Purge(string table, string column, string refTable, string refColumn) {
            return new QueryBuilder($"DELETE FROM {table.ToLower()} WHERE {column} NOT IN (SELECT {refColumn} FROM {refTable.ToLower()})");
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
            return new QueryBuilder("SELECT last_insert_id()");
        }

        public IQueryBuilder GetIdFromRid<T>(object Rid) where T : IDataObject, new() {
            var id = ReflectionTool.FieldsAndPropertiesOf(typeof(T)).FirstOrDefault(f => f.GetCustomAttribute<PrimaryKeyAttribute>() != null);
            var rid = ReflectionTool.FieldsAndPropertiesOf(typeof(T)).FirstOrDefault(f => f.GetCustomAttribute<ReliableIdAttribute>() != null);
            return new QueryBuilder($"SELECT {id.Name} FROM {typeof(T).Name} WHERE {rid.Name}=@???", Rid);
        }

        public IQueryBuilder GetCreationCommand(ForeignKeyAttribute fkd) {
            var cname = $"fk_{fkd.Column}_{fkd.RefTable}_{fkd.RefColumn}";
            String creationCommand = $"ALTER TABLE {fkd.Table} ADD CONSTRAINT {cname} FOREIGN KEY ({fkd.Column}) REFERENCES {fkd.RefTable} ({fkd.RefColumn});";
            return new QueryBuilder(creationCommand);
        }
    }
}
