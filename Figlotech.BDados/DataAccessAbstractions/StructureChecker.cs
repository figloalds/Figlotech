using Figlotech.BDados.DataAccessAbstractions.Attributes;
using Figlotech.BDados.Helpers;
using Figlotech.Core;
using Figlotech.Core.Helpers;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.DataAccessAbstractions {
    public class StructureChecker {
        IEnumerable<Type> workingTypes;
        IRdbmsDataAccessor DataAccessor;

        // "Mirror, mirror on the wall, who's code is the shittiest of them all?"

        public StructureChecker(IRdbmsDataAccessor dataAccessor) {
            DataAccessor = dataAccessor;
        }

        Benchmarker Benchmarker;

        public void CheckStructure(IEnumerable<Type> types) {
            Benchmarker = new Benchmarker("Check Structure");
            DataAccessor.Access(() => {

                Benchmarker.WriteToStdout = true;
                workingTypes = types
                    .Where((t) =>
                        t.GetInterfaces().Contains(typeof(IDataObject)) &&
                        t.GetCustomAttribute<ViewOnlyAttribute>() == null
                    );

                var dbName = DataAccessor.SchemaName;
                DataTable tables = DataAccessor.Query(
                    DataAccessor.QueryGenerator.InformationSchemaQueryTables(dbName));

                DataTable keys = DataAccessor.Query(
                    DataAccessor.QueryGenerator.InformationSchemaQueryKeys(dbName));
                Benchmarker.Mark("Clear Keys");
                ClearKeys(keys);

                Benchmarker.Mark("Work Tables");
                keys = DataAccessor.Query(
                    DataAccessor.QueryGenerator.InformationSchemaQueryKeys(dbName));
                WorkOnTables(tables, keys);

                Benchmarker.Mark("Work Columns");
                DataTable columns = DataAccessor.Query(
                    DataAccessor.QueryGenerator.InformationSchemaQueryColumns(dbName));
                try {
                    WorkOnColumns(columns, keys);
                } catch (Exception x) {
                    Fi.Tech.Write(x.Message);
                    Fi.Tech.Write(x.StackTrace);
                }

                // Re read keys here because work on tables and columns
                // probably changed this too much
                Benchmarker.Mark("Re Keys");
                keys = DataAccessor.Query(
                    DataAccessor.QueryGenerator.InformationSchemaQueryKeys(dbName));

                ReKeys(keys);

            }, (x) => {
                Fi.Tech.WriteLine(Fi.Tech.GetStrings().ERROR_IN_STRUCTURE_CHECK);
                Fi.Tech.WriteLine(x.Message);
                Fi.Tech.WriteLine(x.StackTrace);
            });
            Benchmarker.TotalMark();
        }

        private void Exec(IQueryBuilder query) {
            try {
                DataAccessor.Execute(query);
            } catch (Exception) { }
        }

        private void ClearKeys(DataTable keys) {
            for (int i = 0; i < keys.Rows.Count; i++) {
                bool found = false;
                var colName = keys.Rows[i]["COLUMN_NAME"] as String;
                var refColName = keys.Rows[i]["REFERENCED_COLUMN_NAME"] as String;
                var refTablName = keys.Rows[i]["REFERENCED_TABLE_NAME"] as String;
                if (refColName == null) continue;
                foreach (var type in workingTypes) {
                    var fields = ReflectionTool.FieldsAndPropertiesOf(type)
                        .Where((f) => f.GetCustomAttribute<FieldAttribute>() != null);
                    foreach (var field in fields) {
                        var fkDef = field.GetCustomAttribute<ForeignKeyAttribute>();
                        if (fkDef == null)
                            continue;
                        if (
                            fkDef.referencedColumn == refColName &&
                            fkDef.referencedType.Name.ToLower() == refTablName.ToLower() &&
                            field.Name == colName) {
                            found = true;
                            break;
                        }
                    }
                }
                if (!found) {
                    var target      = keys.Rows[i]["TABLE_NAME"] as String;
                    var constraint  = keys.Rows[i]["CONSTRAINT_NAME"] as String;
                    Exec(DataAccessor.QueryGenerator.DropForeignKey(target, constraint));
                }
            }
        }
        private void ReKeys(DataTable keys) {
            foreach (var type in workingTypes) {
                var fields = ReflectionTool.FieldsAndPropertiesOf(type)
                    .Where((f) => f.GetCustomAttribute<FieldAttribute>() != null);
                foreach (var field in fields) {
                    var fkDef = field.GetCustomAttribute<ForeignKeyAttribute>();
                    if (fkDef == null)
                        continue;
                    bool found = false;
                    for (int i = 0; i < keys.Rows.Count; i++) {
                        var tableName   = keys.Rows[i]["TABLE_NAME"] as String;
                        var colName     = keys.Rows[i]["COLUMN_NAME"] as String;
                        var refTablName = keys.Rows[i]["REFERENCED_TABLE_NAME"] as String;
                        var refColName  = keys.Rows[i]["REFERENCED_COLUMN_NAME"] as String;
                        if (
                            type.Name.ToLower() == tableName.ToLower() &&
                            field.Name == colName &&
                            fkDef.referencedColumn == refColName &&
                            fkDef.referencedType.Name.ToLower() == refTablName.ToLower()) {
                            found = true;
                            break;
                        } else {

                        }
                    }

                    if (!found) {
                        Benchmarker.Mark($"Purge for CONSTRAINT FK {type.Name.ToLower()}/{field.Name} references {fkDef.referencedType.Name.ToLower()}/{fkDef.referencedColumn}");
                        Exec(
                            DataAccessor.QueryGenerator.Purge(type.Name.ToLower(), field.Name, fkDef.referencedType.Name.ToLower(), fkDef.referencedColumn));

                        Benchmarker.Mark($"Create Constraint FK {type.Name.ToLower()}/{field.Name} references {fkDef.referencedType.Name.ToLower()}/{fkDef.referencedColumn}");
                        Exec(
                            DataAccessor.QueryGenerator.AddForeignKey(type.Name.ToLower(), field.Name, fkDef.referencedType.Name.ToLower(), fkDef.referencedColumn));
                    }
                }
            }
        }

        private void DekeyTable(String tableName, DataTable keys) {
            for (int i = 0; i < keys.Rows.Count; i++) {
                var refTableName = keys.Rows[i]["REFERENCED_TABLE_NAME"] as String;
                if (refTableName == tableName) {
                    var target = keys.Rows[i]["TABLE_NAME"] as String;
                    var constraint = keys.Rows[i]["CONSTRAINT_NAME"] as String;
                    Benchmarker.Mark($"Dekey Table {target} from {constraint} references {refTableName}");
                    Exec(
                            DataAccessor.QueryGenerator.DropForeignKey(target, constraint)
                            );
                }
            }
        }

        private void DekeyColumn(String tableName, String columnName, DataTable keys) {
            for (int i = 0; i < keys.Rows.Count; i++) {
                var refColName = keys.Rows[i]["REFERENCED_COLUMN_NAME"] as String;
                var refTablName = keys.Rows[i]["REFERENCED_TABLE_NAME"] as String;
                if (refTablName == tableName && refColName == columnName) {
                    var target = keys.Rows[i]["TABLE_NAME"] as String;
                    var constraint = keys.Rows[i]["CONSTRAINT_NAME"] as String;
                    Benchmarker.Mark($"Dekey Table {target} from {constraint} references {refTablName}/{refColName}");
                    Exec(
                            DataAccessor.QueryGenerator.DropForeignKey(target, constraint)
                            );
                }
            }
        }

        private bool WorkOnTables(DataTable tables, DataTable keys) {
            Dictionary<string, string> oldNames = new Dictionary<string, string>();
            foreach (DataRow a in tables.Rows) {
                var tabName = a["TABLE_NAME"] as String;
                foreach (var type in workingTypes) {
                    var oldNameAtt = type.GetCustomAttribute<OldNameAttribute>();
                    if (oldNameAtt != null) {
                        if (tabName.ToLower() == oldNameAtt.Name.ToLower()) {
                            oldNames.Add(type.Name.ToLower(), oldNameAtt.Name.ToLower());
                        }
                    }
                }
            }
            foreach (var type in workingTypes) {
                var found = false;
                foreach (DataRow a in tables.Rows) {
                    var tabName = a["TABLE_NAME"] as String;
                    if (tabName.ToLower() == type.Name.ToLower()) {
                        found = true;
                    }
                }
                if (!found) {
                    bool renamed = false;
                    foreach (var old in oldNames) {
                        if (old.Key == type.Name.ToLower()) {
                            DekeyTable(old.Value, keys);
                            Exec(DataAccessor.QueryGenerator.RenameTable(old.Value, old.Key));
                            renamed = true;
                            break;
                        }
                    }
                    if (!renamed) {
                        Exec(DataAccessor.QueryGenerator.GetCreationCommand(type));
                    }
                }
            }
            return true;
        }

        private bool WorkOnColumns(DataTable columns, DataTable keys) {
            Benchmarker.Mark("Find renamed columns");
            foreach (DataRow a in columns.Rows) {
                var tablName = a["TABLE_NAME"] as String;
                var colName = a["COLUMN_NAME"] as String;
                foreach (var type in workingTypes) {
                    var fields = ReflectionTool.FieldsAndPropertiesOf(type)
                    .Where((f) => f.GetCustomAttribute<FieldAttribute>() != null);
                    if (tablName.ToLower() != type.Name.ToLower()) continue;

                    var oldNames = new Dictionary<string, string>();
                    foreach (var field in fields) {
                        var oldNameAtt = field.GetCustomAttribute<OldNameAttribute>();
                        if (oldNameAtt != null) {
                            oldNames.Add(field.Name, oldNameAtt.Name);
                        }
                    }

                    foreach (var field in fields) {
                        var found = false;
                        for (int i = 0; i < columns.Rows.Count; i++) {
                            var colName2 = columns.Rows[i]["COLUMN_NAME"] as String;
                            if (colName2 == field.Name) {
                                found = true;
                                break;
                            }
                        }
                        // Not found columns, create or rename.
                        if (!found) {
                            bool renamed = false;
                            foreach (var old in oldNames) {
                                if (old.Key == field.Name) {
                                    Benchmarker.Mark($"ACTION: Rename {old.Value} to {field.Name}");
                                    DekeyColumn(type.Name.ToLower(), old.Value, keys);
                                    Exec(DataAccessor.QueryGenerator.RenameColumn(type.Name.ToLower(), old.Value, GetColumnDefinition(field)));
                                    renamed = true;
                                    break;
                                }
                                if (renamed) break;
                            }

                            if (!renamed) {
                                Benchmarker.Mark($"ACTION: Create column {field.Name}");
                                Exec(DataAccessor.QueryGenerator.AddColumn(type.Name.ToLower(), GetColumnDefinition(field)));
                            }
                        }
                    }

                }
            }

            Benchmarker.Mark("Fix column definitions");
            columns = DataAccessor.Query(DataAccessor.QueryGenerator.InformationSchemaQueryColumns(DataAccessor.SchemaName));
            for (int i = 0; i < columns.Rows.Count; i++) {
                foreach (var type in workingTypes) {
                    var tableName = columns.Rows[i]["TABLE_NAME"] as String;
                    if (tableName.ToLower() != type.Name.ToLower()) continue;
                    var fields = ReflectionTool.FieldsAndPropertiesOf(type)
                        .Where((f) => f.GetCustomAttribute<FieldAttribute>() != null);
                    foreach (var field in fields) {
                        var columnName = columns.Rows[i]["COLUMN_NAME"] as String;
                        if (field.Name != columnName) continue;
                        // Found columns, check definitions
                        var columnIsNullable = (columns.Rows[i]["IS_NULLABLE"] as String)?.ToUpper() == "YES";
                        var length = columns.Rows[i]["CHARACTER_MAXIMUM_LENGTH"];
                        var datatype = (columns.Rows[i]["DATA_TYPE"] as String)?.ToUpper();
                        var fieldAtt = field.GetCustomAttribute<FieldAttribute>();
                        if (fieldAtt == null) continue;
                        var dbdef = GetDatabaseType(field, fieldAtt);
                        var dbDefinition = dbdef.Substring(0, dbdef.IndexOf('(') > -1 ? dbdef.IndexOf('(') : dbdef.Length);
                        if (
                            columnIsNullable != fieldAtt.AllowNull ||
                            (int)(length ?? 0) != fieldAtt.Size ||
                            datatype != dbDefinition
                            ) {
                            Benchmarker.Mark($"ACTION: Alter Column {type.Name.ToLower()}/{field.Name}");
                            Exec(DataAccessor.QueryGenerator.RenameColumn(type.Name.ToLower(), field.Name, GetColumnDefinition(field)));
                        } else {
                        }
                    }
                }
            }

            return true;
        }

        // |...................|
        // |..... WARNING......|
        // |...................|
        // |......DRAGONS......|
        // |...................|
        //         |...|
        //         |...|
        //         |...|
        // Use safety gear when going down there.
        private static String GetDatabaseType(MemberInfo field, FieldAttribute info = null, bool size = true) {
            if (info == null)
                foreach (var att in field.GetCustomAttributes())
                    if (att is FieldAttribute) {
                        info = (FieldAttribute)att; break;
                    }
            if (info == null)
                return "VARCHAR(100)";

            string dataType;
            var fieldType = ReflectionTool.GetTypeOf(field);
            if (Nullable.GetUnderlyingType(fieldType) != null)
                dataType = Nullable.GetUnderlyingType(fieldType).Name;
            else
                dataType = fieldType.Name;
            if (fieldType.IsEnum) {
                return "INT";
            }
            String type = "VARCHAR(20)";
            if (info.Type != null && info.Type.Length > 0) {
                type = info.Type;
            } else {
                switch (dataType.ToLower()) {
                    case "rid":
                        type = $"VARCHAR(64)";
                        break;
                    case "byte[]":
                    case "byte":
                        type = $"BINARY({info.Size})";
                        break;
                    case "string":
                        type = $"VARCHAR({info.Size})";
                        break;
                    case "int":
                    case "int32":
                        type = $"INT";
                        break;
                    case "uint":
                    case "uint32":
                        type = $"INT UNSIGNED";
                        break;
                    case "short":
                    case "int16":
                        type = $"SMALLINT";
                        break;
                    case "ushort":
                    case "uint16":
                        type = $"SMALLINT UNSIGNED";
                        break;
                    case "long":
                    case "int64":
                        type = $"BIGINT";
                        break;
                    case "ulong":
                    case "uint64":
                        type = $"BIGINT UNSIGNED";
                        break;
                    case "bool":
                    case "boolean":
                        type = $"TINYINT(1)";
                        break;
                    case "float":
                    case "double":
                    case "single":
                        type = $"FLOAT(16,3)";
                        break;
                    case "datetime":
                        type = $"DATETIME";
                        break;
                }
            }
            return type;
        }

        private static String GetColumnDefinition(MemberInfo field, FieldAttribute info = null) {
            if (info == null)
                info = field.GetCustomAttribute<FieldAttribute>();
            if (info == null)
                return "VARCHAR(128)";
            var nome = field.Name;
            var fieldType = ReflectionTool.GetTypeOf(field);
            String tipo = GetDatabaseType(field, info);
            if (info.Type != null && info.Type.Length > 0)
                tipo = info.Type;
            var options = "";
            if (info.Options != null && info.Options.Length > 0) {
                options = info.Options;
            } else {
                if (!info.AllowNull) {
                    options += " NOT NULL";
                } else if (Nullable.GetUnderlyingType(field.GetType()) == null && fieldType.IsValueType && !info.AllowNull) {
                    options += " NOT NULL";
                }
                //if (info.Unique)
                //    options += " UNIQUE";
                if ((info.AllowNull && info.DefaultValue == null) || info.DefaultValue != null)
                    options += $" DEFAULT {CheapSanitize(info.DefaultValue)}";
                foreach (var att in field.GetCustomAttributes())
                    if (att is PrimaryKeyAttribute)
                        options += " AUTO_INCREMENT PRIMARY KEY";
            }

            return $"{nome} {tipo} {options}";
        }

        private static String CheapSanitize(Object value) {
            String valOutput;
            if (value == null)
                return "NULL";
            if (value.GetType().IsEnum) {
                return $"{(int)Convert.ChangeType(value, Enum.GetUnderlyingType(value.GetType()))}";
            }
            // We know for sure that value is not null at this point
            // But it may still be nullable.
            var checkingType = value.GetType();
            switch (value.GetType().Name.ToLower()) {
                case "string":
                    if (value.ToString() == "CURRENT_TIMESTAMP")
                        return "CURRENT_TIMESTAMP";
                    valOutput = ((String)value);
                    valOutput = valOutput.Replace("\\", "\\\\");
                    valOutput = valOutput.Replace("\'", "\\\'");
                    valOutput = valOutput.Replace("\"", "\\\"");
                    return $"'{valOutput}'";
                case "float":
                case "double":
                case "decimal":
                    valOutput = Convert.ToString(value).Replace(",", ".");
                    return $"{valOutput}";
                case "short":
                case "int":
                case "long":
                case "int16":
                case "int32":
                case "int64":
                    return Convert.ToString(value);
                case "datetime":
                    return $"'{((DateTime)value).ToString("yyyy-MM-dd HH:mm:ss")}'";
                default:
                    return $"'{Convert.ToString(value)}'";
            }

        }
    }
}
