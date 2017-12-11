using Figlotech.BDados.Builders;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.TableNameTransformDefaults;
using Figlotech.Core;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace Figlotech.BDados.CustomForms {
    public class CustomFormsApi
    {
        public static String Prefix = "ibcf";
        public static void CheckStructure(IRdbmsDataAccessor da, CustomForm form) {

            var tableName = GetPrefixedTableName(ref form);

            // 
            DataTable schemaInfo = da.Query(
                new Qb($"SELECT * FROM information_schema.tables WHERE table_schema='{da.SchemaName}' AND table_name='{tableName}'"));
            // Se a tabela não existir, criar
            if (schemaInfo.Rows.Count < 1) {
                var creationQuery = BuildCreationQuery(form);
                da.Execute(creationQuery);
            } else {
                DataTable columnsInfo = da.Query(
                    new Qb($"SELECT * FROM information_schema.columns WHERE table_schema='{da.SchemaName}' AND table_name='{tableName}'"));

                for(var i = 0; i < form.Fields.Count; i++) {
                    bool exists = false;
                    int cIndex = -1;
                    for(var c = 0; c < columnsInfo.Rows.Count; c++) { // C++!!!
                        if(columnsInfo.Rows[c]["COLUMN_NAME"] as String == form.Fields[i].Name) {
                            exists = true;
                            cIndex = c;
                            break;
                        }
                    }
                    if(!exists) {
                        da.Execute(new Qb($"ALTER TABLE {tableName} ADD COLUMN {SqlType(form.Fields[i])} DEFAULT NULL;"));
                    } else {
                        ulong? sz = (ulong?) columnsInfo.Rows[cIndex]["CHARACTER_MAXIMUM_LENGTH"];
                        String dbtype = (String) columnsInfo.Rows[cIndex]["DATA_TYPE"];
                        if (dbtype.ToLower() == "varchar" && sz != (ulong) form.Fields[i].Size) {
                            da.Execute(new Qb($"ALTER TABLE {tableName} CHANGE COLUMN {form.Fields[i].Name} {form.Fields[i].Name} {SqlType(form.Fields[i])} DEFAULT NULL"));
                        }
                    }
                }
            }
            
        }

        public static String GetPrefixedTableName(ref CustomForm cf) {
            // to use a prefixed table name
            var tableName = cf.Name;
            if (Prefix.Length > 0)
                tableName = $"{Prefix}_{cf.Name}";

            if (cf.Name == null || cf.Name.Length < 1 || cf.Fields.Count < 1) {
                throw new Exception("The specified Custom Form is invalid and cannot be structured into the database.");
            }
            return tableName;
        }

        public static void Save(IRdbmsDataAccessor rda, CustomForm cf, CustomObject value) {
            var tableName = GetPrefixedTableName(ref cf);
            Object getId = value.Get("Id");
            if(getId is long && (long) getId > 0) {
                rda.Execute(BuildUpdateQuery(cf, value));
            } else {
                rda.Execute(BuildInsertQuery(cf, value));
            }
        }

        public static Object LoadByRid(IRdbmsDataAccessor rda, CustomForm cf, String rid) {

            DataTable results = rda.Query(BuildSelectByRidQuery(cf, rid));
            var retv = new List<CustomObject>();
            for (var i = 0; i < results.Rows.Count; i++) {
                CustomObject co = new CustomObject();
                co.Set("Id", results.Rows[i]["Id"]);
                for (var a = 0; a < cf.Fields.Count; a++) {
                    co.Set(cf.Fields[a].Name, results.Rows[i][cf.Fields[a].Name]);
                }
                co.Set("RID", results.Rows[i]["RID"]);
                if (co.Get("RID") == null) {
                    co.Set("RID", IntEx.GenerateUniqueRID());
                }

                return co.Refine();
            }

            return null;
        }

        public static List<Object> Load(IRdbmsDataAccessor rda, CustomForm cf, IDictionary<String, Object> conditions = null, int? p = 1, int? l = 200) {
            return LoadRaw(rda, cf, conditions, p, l).Select((co) => co.Refine()).ToList();
        }

        public static List<CustomObject> LoadRaw(IRdbmsDataAccessor rda, CustomForm cf, IDictionary<String, Object> conditions = null, int? p = 1, int? l = 200) {
            var tableName = GetPrefixedTableName(ref cf);

            DataTable results = rda.Query(BuildSelectQuery(cf, conditions, p, l));
            var retv = new List<CustomObject>();
            for (var i = 0; i < results.Rows.Count; i++) {
                CustomObject co = new CustomObject();
                co.Set("Id", results.Rows[i]["Id"]);
                for (var a = 0; a < cf.Fields.Count; a++) {
                    co.Set(cf.Fields[a].Name, results.Rows[i][cf.Fields[a].Name]);
                }
                co.Set("RID", results.Rows[i]["RID"]);
                if (co.Get("RID") == null) {
                    co.Set("RID", IntEx.GenerateUniqueRID());
                }

                retv.Add(co);
            }

            return retv;
        }

        public static IQueryBuilder BuildSelectQuery(CustomForm cf, IDictionary<String, Object> conditions  = null, int? p = 1, int? l = 200) {
            var tableName = GetPrefixedTableName(ref cf);

            QueryBuilder retv = new QueryBuilder();

            retv.Append("SELECT Id, ");
            for(int i = 0; i < cf.Fields.Count; i++) {
                retv.Append($"{cf.Fields[i].Name},");
            }
            retv.Append($"RID FROM {tableName}");

            if(conditions != null && conditions.Count > 0) {
                var validConditions = new Dictionary<String, Object>();
                foreach(var a in conditions) {
                    if(cf.Fields.Any((f)=>f.Name == a.Key)) {
                        validConditions.Add(a.Key, a.Value);
                    }
                }
                if(validConditions.Count > 0) {
                    retv.Append("WHERE");
                    for (var a = 0; a < validConditions.Count; a++) {
                        if(validConditions.ElementAt(a).Value is String) {
                            retv.Append($"{validConditions.ElementAt(a).Key} LIKE CONCAT('%', @{IntEx.GenerateShortRid()}, '%')", validConditions.ElementAt(a).Value);
                        }
                        else {
                            retv.Append($"{validConditions.ElementAt(a).Key}=@{IntEx.GenerateShortRid()}", validConditions.ElementAt(a).Value);
                        }
                    }
                }
            }

            if(l != null) {
                retv.Append("LIMIT");
                if(p != null) {
                    retv.Append($"{((p-1) * l)}, {l}");
                } else {
                    retv.Append($"{l}");
                }
            }

            retv.Append(";");

            return retv;
        }


        public static IQueryBuilder BuildSelectByRidQuery(CustomForm cf, String rid) {
            var tableName = GetPrefixedTableName(ref cf);

            QueryBuilder retv = new QueryBuilder();

            retv.Append("SELECT Id, ");
            for (int i = 0; i < cf.Fields.Count; i++) {
                retv.Append($"{cf.Fields[i].Name},");
            }
            retv.Append($"RID FROM {tableName}");

            retv.Append("WHERE RID=@1", rid);

            retv.Append(";");

            return retv;
        }

        public static string TypeTag(CustomFormField field) {
            var pre = SqlType(field);
            var retv = Regex.Match(pre, "^(\\d+)").Groups[0].Value.ToLower();

            return retv;
        }

        public static IQueryBuilder BuildInsertQuery(CustomForm form, CustomObject o) {
            var tableName = GetPrefixedTableName(ref form);
            QueryBuilder retv = new QueryBuilder();
            retv.Append($"INSERT INTO {tableName} (");
            for(var i = 0; i < form.Fields.Count; i++) {
                retv.Append($"{form.Fields[i].Name},");
            }
            retv.Append("RID");
            retv.Append(") VALUES (");

            for (var i = 0; i < form.Fields.Count; i++) {
                retv.Append($"@{IntEx.GenerateShortRid()},", o.Get(form.Fields[i].Name));
            }
            retv.Append($"@{IntEx.GenerateShortRid()})", IntEx.GenerateUniqueRID());
            return retv;
        }

        public static IQueryBuilder BuildUpdateQuery(CustomForm form, CustomObject o) {
            var tableName = GetPrefixedTableName(ref form);
            if(!(o.Get("Id") is long) || ((o.Get("Id") as long?)??0) < 1) {
                throw new Exception("Can't build an update query for an object that has no Id");
            }
            QueryBuilder retv = new QueryBuilder();
            retv.Append($"UPDATE {tableName} SET ");
            for (var i = 0; i < form.Fields.Count; i++) {
                retv.Append($"{form.Fields[i].Name}=@{IntEx.GenerateShortRid()}", o.Get(form.Fields[i].Name));
            }
            /* LMAO GET RID */
            retv.If(o.Get("RID") != null).Then()
                    .Append($"RID=@{IntEx.GenerateShortRid()}", o.Get("RID"))
                .Else()
                    .Append($"RID=@{IntEx.GenerateShortRid()}", IntEx.GenerateUniqueRID())
                .EndIf();

            retv.Append($"WHERE Id=@{IntEx.GenerateShortRid()};", o.Get("Id"));

            return retv;
        }

        public static IQueryBuilder BuildCreationQuery(CustomForm form) {
            var tableName = GetPrefixedTableName(ref form);

            QueryBuilder retv = new QueryBuilder();
            retv.Append($"CREATE TABLE {tableName} (");
            retv.Append("Id BIGINT(20) NOT NULL PRIMARY KEY AUTO_INCREMENT,");
            for(var i = 0; i < form.Fields.Count; i++) {

                retv.Append($"{form.Fields[i].Name} {SqlType(form.Fields[i])} DEFAULT NULL");
                //if (i < form.Campos.Count-1) {
                    retv.Append($",");
                //}
            }
            retv.Append("RID VARCHAR(64) NOT NULL UNIQUE");
            retv.Append(")");
            return retv;
        }
        public static String SqlType(CustomFormField campo) {
            switch (campo.Type.ToLower()) {
                case "int":
                case "int32":
                case "enum": return $"INT";
                case "uint":
                case "uint32": return $"INT UNSIGNED";
                case "long": return $"BIGINT";
                case "ulong": return $"BIGINT UNSIGNED";
                case "int16": return $"SMALLINT";
                case "uint16": return $"SMALLINT UNSIGNED";
                case "bool":
                case "boolean": return $"TINYINT(1)";
                case "date":
                case "datetime":
                case "time":
                case "timestamp": return $"DATETIME";
                case "rid": return $"VARCHAR(64)";
                case "string": return $"VARCHAR({campo.Size})";
                case "text": return "TEXT";
            }

            return $"VARCHAR({campo.Size})";
        }

        public static Type GetRuntimeType(CustomFormField field) {
            switch (field.Type) {
                case "int":
                case "int32":
                case "enum": return typeof(int?);
                case "uint":
                case "uint32": return typeof(uint?);
                case "long":
                case "int64": return typeof(long?);
                case "ulong":
                case "uint64": return typeof(ulong?);
                case "short":
                case "int16": return typeof(short?);
                case "ushort":
                case "uint16": return typeof(ushort?);
                case "bool":
                case "boolean": return typeof(bool?);
                case "date":
                case "datetime":
                case "time":
                case "timestamp": return typeof(DateTime?);
                case "rid":
                case "string":
                case "text": return typeof(String);
            }

            return typeof(String);
        }

    }
}
