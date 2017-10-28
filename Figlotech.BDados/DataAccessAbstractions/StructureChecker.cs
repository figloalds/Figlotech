

using Figlotech.BDados.DataAccessAbstractions.Attributes;
using Figlotech.Core;
using Figlotech.Core.Helpers;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;

namespace Figlotech.BDados.DataAccessAbstractions {
    public enum NecessaryActionType {
        CreateTable,
        CreateColumn,
        RenameTable,
        RenameColumn,
        CreateForeignKey,
        DropForeignKey,
        AlterColumnDefinition,
    }
    public class StructureCheckNecessaryAction {
        public NecessaryActionType ActionType { get; set; }
        public Type TableType { get; set; }
        public MemberInfo ColumnMember { get; set; }
        public String Table { get; set; }
        public String Column { get; set; }
        public String RefTable { get; set; }
        public String RefColumn { get; set; }
        public String NewName { get; set; }
        public String NewDefinition { get; set; }
        public String DeleteConstraint { get; set; }
    }

    public class StructureChecker {
        List<Type> workingTypes;
        IRdbmsDataAccessor DataAccessor;

        // "Mirror, mirror on the wall, who's code is the shittiest of them all?"

        public StructureChecker(IRdbmsDataAccessor dataAccessor, IEnumerable<Type> types) {
            DataAccessor = dataAccessor;
            workingTypes = workingTypes = types
                    .Where((t) =>
                        IsDataObject(t) &&
                        t.GetCustomAttribute<ViewOnlyAttribute>() == null &&
                        !t.IsAbstract
                    ).ToList();
        }

        Benchmarker Benchmarker;

        public bool IsDataObject(Type t) {
            if (t == null) return false;
            if (t == typeof(Object)) return false;
            var ifaces = t.GetTypeInfo().GetInterfaces();
            var isDo = ifaces.Any(a => a == typeof(IDataObject));
            return
                isDo
                || IsDataObject(t.BaseType);
        }

        public IEnumerable<StructureCheckNecessaryAction> EvaluateLegacyKeys(List<ForeignKeyAttribute> keys) {
            for (int i = 0; i < keys.Count; i++) {
                if (keys[i].RefColumn == null) continue;
                bool found = false;
                var tablName = keys[i].Table;
                var colName = keys[i].Column;
                var refColName = keys[i].RefColumn;
                var refTablName = keys[i].RefTable;
                if (refColName == null) continue;
                foreach (var type in workingTypes) {
                    if (type.Name.ToLower() != tablName.ToLower()) continue;
                    var fields = ReflectionTool.FieldsAndPropertiesOf(type)
                        .Where((f) => f.GetCustomAttribute<FieldAttribute>() != null);
                    foreach (var field in fields) {
                        var fkDef = field.GetCustomAttribute<ForeignKeyAttribute>();
                        if (fkDef == null)
                            continue;
                        if (
                            fkDef.RefColumn == refColName &&
                            fkDef.RefTable.ToLower() == refTablName.ToLower() &&
                            field.Name == colName) {
                            found = true;
                            break;
                        }
                    }
                }
                if (!found) {
                    yield return new StructureCheckNecessaryAction {
                        ActionType = NecessaryActionType.DropForeignKey,
                        Table = tablName,
                        Column = colName,
                        RefTable = refTablName,
                        RefColumn = refColName,
                    };
                }
            }
            yield break;
        }

        private IEnumerable<StructureCheckNecessaryAction> EvaluateForFullTableDekeyal(String target, List<String> tables, List<ForeignKeyAttribute> keys) {
            for (int i = 0; i < keys.Count; i++) {
                var refTableName = keys[i].RefTable;
                if (refTableName == target) {
                    var tableName = keys[i].Table;
                    var columnName = keys[i].Column;
                    var refColumnName = keys[i].RefColumn;
                    var constraint = keys[i].ConstraintName;
                    Benchmarker.Mark($"Dekey Table {tableName} from {constraint} references {refTableName}");
                    yield return new StructureCheckNecessaryAction {
                        ActionType = NecessaryActionType.DropForeignKey,
                        Table = tableName,
                        Column = columnName,
                        RefTable = refTableName,
                        RefColumn = refColumnName,
                    };
                }
            }
        }

        private IEnumerable<StructureCheckNecessaryAction> EvaluateTableChanges(List<String> tables, List<ForeignKeyAttribute> keys) {
            Dictionary<string, string> oldNames = new Dictionary<string, string>();
            foreach (String tableName in tables) {
                foreach (var type in workingTypes) {
                    var oldNameAtt = type.GetCustomAttribute<OldNameAttribute>();
                    if (oldNameAtt != null) {
                        if (tableName.ToLower() == oldNameAtt.Name.ToLower()) {
                            oldNames.Add(type.Name.ToLower(), oldNameAtt.Name.ToLower());
                        }
                    }
                }
            }

            foreach (var type in workingTypes) {
                var found = false;
                foreach (String tableName in tables) {
                    if (tableName.ToLower() == type.Name.ToLower()) {
                        found = true;
                    }
                }
                if (!found) {
                    bool renamed = false;
                    foreach (var old in oldNames) {
                        if (old.Key == type.Name.ToLower()) {
                            foreach (var a in EvaluateForFullTableDekeyal(old.Value, tables, keys)) {
                                yield return a;
                            }
                            yield return new StructureCheckNecessaryAction {
                                ActionType = NecessaryActionType.RenameTable,
                                Table = old.Value,
                                NewName = old.Key,
                            };
                            renamed = true;
                            break;
                        }
                    }
                    if (!renamed) {
                        yield return new StructureCheckNecessaryAction {
                            ActionType = NecessaryActionType.CreateTable,
                            TableType = type,
                        };
                    }
                }
            }

            yield break;
        }

        private IEnumerable<StructureCheckNecessaryAction> EvaluateColumnChanges(List<FieldAttribute> columns, List<ForeignKeyAttribute> keys) {

            foreach (var type in workingTypes) {
                var fields = ReflectionTool.FieldsAndPropertiesOf(type);
                foreach (var field in fields) {

                    var fieldExists = false;
                    foreach (var col in columns) {
                        var colName = col.Name;
                        if (field.Name == colName) {
                            fieldExists = true;
                        }
                    }

                    if (!fieldExists) {

                        var oldExists = false;
                        foreach (var col in columns) {
                            var colName = col.Name;
                            var ona = field.GetCustomAttribute<OldNameAttribute>();
                            if (ona != null) {
                                if (ona.Name == colName) {
                                    yield return new StructureCheckNecessaryAction {
                                        ActionType = NecessaryActionType.RenameColumn,
                                        Table = type.Name,
                                        Column = ona.Name,
                                        ColumnMember = field,
                                        NewName = field.Name,
                                    };
                                    oldExists = true;
                                }
                            }
                        }

                        if (!oldExists) {
                            yield return new StructureCheckNecessaryAction {
                                ActionType = NecessaryActionType.CreateColumn,
                                TableType = type,
                                ColumnMember = field,
                            };
                        }

                    } else {

                        foreach (var col in columns) {
                            var tableName = col.Table;
                            if (tableName.ToLower() != type.Name.ToLower()) continue;
                            var columnName = col.Name;
                            if (field.Name != columnName) continue;
                            // Found columns, check definitions
                            var columnIsNullable = col.AllowNull;
                            var length = col.Size;
                            var datatype = col.Type;
                            var fieldAtt = field.GetCustomAttribute<FieldAttribute>();
                            if (fieldAtt == null) continue;
                            var dbdef = GetDatabaseType(field, fieldAtt);
                            var dbDefinition = dbdef.Substring(0, dbdef.IndexOf('(') > -1 ? dbdef.IndexOf('(') : dbdef.Length);
                            if (
                                columnIsNullable != fieldAtt.AllowNull ||
                                length != fieldAtt.Size ||
                                datatype != dbDefinition
                                ) {
                                yield return new StructureCheckNecessaryAction {
                                    ActionType = NecessaryActionType.AlterColumnDefinition,
                                    Table = type.Name,
                                    Column = field.Name,
                                    ColumnMember = field,
                                    NewDefinition = GetColumnDefinition(field)
                                };
                            } else {

                            }
                        }

                    }

                }
            }


            yield break;
        }

        public int ExecuteNecessaryActions(IEnumerable<StructureCheckNecessaryAction> actions, Action<StructureCheckNecessaryAction, int> onActionExecuted) {
            var enny = actions.GetEnumerator();
            int retv = 0;
            int went = 0;
            WorkQueuer wq = new WorkQueuer("Strucheck Queuer");
            var cliQ = new WorkQueuer("cli_q", 1);
            while(enny.MoveNext()) {
                var thisAction = enny.Current;
                wq.Enqueue(() => {
                    retv += Execute(thisAction);
                    lock(wq) {
                        int myWent = went++;
                        cliQ.Enqueue(() => {
                            onActionExecuted?.Invoke(thisAction, myWent);
                        });
                    }
                });
            }
            wq.Stop();
            cliQ.Stop();
            return retv;
        }

        public IEnumerable<StructureCheckNecessaryAction> EvaluateNecessaryActions() {
            var keys = GetInfoSchemaKeys();
            var tables = GetInfoSchemaTables();
            var columns = GetInfoSchemaColumns();
            foreach (var a in EvaluateLegacyKeys(keys))
                yield return a;
            foreach (var a in EvaluateTableChanges(tables, keys))
                yield return a;
            foreach (var a in EvaluateColumnChanges(columns, keys))
                yield return a;
        }

        private List<ForeignKeyAttribute> GetInfoSchemaKeys() {
            var dbName = DataAccessor.SchemaName;
            return DataAccessor.Query(
                DataAccessor
                    .QueryGenerator
                    .InformationSchemaQueryKeys(dbName)
            )
            .Map<ForeignKeyAttribute>(new Dictionary<string, string> {
                { "TABLE_NAME", nameof(ForeignKeyAttribute.Table) },
                { "COLUMN_NAME", nameof(ForeignKeyAttribute.Column) },
                { "REFERENCED_COLUMN_NAME", nameof(ForeignKeyAttribute.RefColumn) },
                { "REFERENCED_TABLE_NAME", nameof(ForeignKeyAttribute.RefTable) },
            }).ToList();
        }
        private List<String> GetInfoSchemaTables() {
            var dbName = DataAccessor.SchemaName;
            return
                DataAccessor.Query(
                    DataAccessor.QueryGenerator.InformationSchemaQueryTables(dbName)
                )
                .Columns["TABLE_NAME"]
                .ToEnumerable<String>()
                .ToList();
        }
        private List<FieldAttribute> GetInfoSchemaColumns() {
            var dbName = DataAccessor.SchemaName;
            return DataAccessor.Query(
                    DataAccessor.QueryGenerator.InformationSchemaQueryTables(dbName)
            )
            .Map<FieldAttribute>(new Dictionary<string, string> {
                { "TABLE_NAME", nameof(FieldAttribute.Table) },
                { "COLUMN_NAME", nameof(FieldAttribute.Name) },
                { "COLUMN_DEFAULT", nameof(FieldAttribute.DefaultValue) },
                { "IS_NULLABLE", nameof(FieldAttribute.AllowNull) },
                { "DATA_TYPE", nameof(FieldAttribute.Type) },
                { "CHARACTER_MAXIMUM_LENGTH", nameof(FieldAttribute.Size) },
                { "NUMERIC_PRECISION", nameof(FieldAttribute.Precision) },
                { "CHARACTER_SET_NAME", nameof(FieldAttribute.Charset) },
                { "COLLATION_NAME", nameof(FieldAttribute.Collation) },
                { "COLUMN_COMMENT", nameof(FieldAttribute.Comment) },
                { "GENERATION_EXPRESSION", nameof(FieldAttribute.GenerationExpression) },
            }).ToList();
        }

        public int Execute(StructureCheckNecessaryAction scna) {
            switch(scna.ActionType) {
                case NecessaryActionType.DropForeignKey:
                    return Exec(DataAccessor.QueryGenerator.DropForeignKey(
                        scna.Table, scna.DeleteConstraint));
                case NecessaryActionType.CreateForeignKey:
                    return Exec(DataAccessor.QueryGenerator.AddForeignKey(
                        scna.Table.ToLower(), scna.Column, scna.RefTable.ToLower(), scna.RefColumn));
                case NecessaryActionType.CreateTable:
                    return Exec(DataAccessor.QueryGenerator.GetCreationCommand(
                        scna.TableType));
                case NecessaryActionType.CreateColumn:
                    return Exec(DataAccessor.QueryGenerator.AddColumn(scna.Table.ToLower(), GetColumnDefinition(scna.ColumnMember)));
                case NecessaryActionType.RenameTable:
                    return Exec(DataAccessor.QueryGenerator.RenameTable(
                        scna.Table, scna.NewName));
                case NecessaryActionType.RenameColumn:
                    return Exec(DataAccessor.QueryGenerator.RenameColumn(
                        scna.Table.ToLower(), scna.Column, GetColumnDefinition(scna.ColumnMember)));
                case NecessaryActionType.AlterColumnDefinition:
                    return Exec(DataAccessor.QueryGenerator.RenameColumn(
                        scna.Table.ToLower(), scna.Column, GetColumnDefinition(scna.ColumnMember)));
            }

            return 0;
        }

        public void CheckStructure() {
            Benchmarker = new Benchmarker("Check Structure");
            DataAccessor.Access(() => {

                Benchmarker.WriteToStdout = true;


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

                WorkOnColumns(columns, keys);

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

        private int Exec(IQueryBuilder query) {
            try {
                return DataAccessor.Execute(query);
            } catch (Exception) {

            }
            return 0;
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
                            fkDef.RefColumn == refColName &&
                            fkDef.RefTable.ToLower() == refTablName.ToLower() &&
                            field.Name == colName) {
                            found = true;
                            break;
                        }
                    }
                }
                if (!found) {
                    var target = keys.Rows[i]["TABLE_NAME"] as String;
                    var constraint = keys.Rows[i]["CONSTRAINT_NAME"] as String;
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
                        var tableName = keys.Rows[i]["TABLE_NAME"] as String;
                        var colName = keys.Rows[i]["COLUMN_NAME"] as String;
                        var refTablName = keys.Rows[i]["REFERENCED_TABLE_NAME"] as String;
                        var refColName = keys.Rows[i]["REFERENCED_COLUMN_NAME"] as String;
                        if (
                            type.Name.ToLower() == tableName.ToLower() &&
                            field.Name == colName &&
                            fkDef.RefColumn == refColName &&
                            fkDef.RefTable.ToLower() == refTablName.ToLower()) {
                            found = true;
                            break;
                        } else {

                        }
                    }

                    if (!found) {
                        try {
                            Benchmarker.Mark($"Purge for CONSTRAINT FK {type.Name.ToLower()}/{field.Name} references {fkDef.RefTable.ToLower()}/{fkDef.RefColumn}");
                            Exec(
                                DataAccessor.QueryGenerator.Purge(type.Name.ToLower(), field.Name, fkDef.RefTable.ToLower(), fkDef.RefColumn));

                            Benchmarker.Mark($"Create Constraint FK {type.Name.ToLower()}/{field.Name} references {fkDef.RefTable.ToLower()}/{fkDef.RefColumn}");
                            Exec(
                                DataAccessor.QueryGenerator.AddForeignKey(type.Name.ToLower(), field.Name, fkDef.RefTable.ToLower(), fkDef.RefColumn));
                        } catch (Exception x) {
                            Fi.Tech.WriteLine(x.Message);
                        }
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
                            var tablName2 = columns.Rows[i]["TABLE_NAME"] as String;

                            if (tablName2.ToLower() != type.Name.ToLower()) continue;

                            if (colName2 == field.Name
                                && type.Name.ToLower() == tablName2.ToLower()
                                ) {
                                found = true;
                                break;
                            }
                        }
                        // Not found columns, create or rename.
                        if (!found) {
                            bool renamed = false;
                            foreach (var old in oldNames) {
                                if (old.Key == field.Name
                                    && type.Name.ToLower() == tablName.ToLower()
                                    ) {
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
                        var length = columns.Rows[i]["CHARACTER_MAXIMUM_LENGTH"] as int?;
                        var datatype = (columns.Rows[i]["DATA_TYPE"] as String)?.ToUpper();
                        var fieldAtt = field.GetCustomAttribute<FieldAttribute>();
                        if (fieldAtt == null) continue;
                        var dbdef = GetDatabaseType(field, fieldAtt);
                        var dbDefinition = dbdef.Substring(0, dbdef.IndexOf('(') > -1 ? dbdef.IndexOf('(') : dbdef.Length);
                        if (
                            columnIsNullable != fieldAtt.AllowNull ||
                            (length ?? 0) != fieldAtt.Size ||
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
