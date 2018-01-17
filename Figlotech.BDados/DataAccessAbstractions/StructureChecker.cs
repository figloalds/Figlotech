
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using Figlotech.Core;
using Figlotech.Core.Helpers;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace Figlotech.BDados.DataAccessAbstractions {

    public class DropKeyScAction : AbstractIStructureCheckNecessaryAction {
        String _table;
        String _constraint;

        public DropKeyScAction(
            IRdbmsDataAccessor dataAccessor,
            string table, string constraint) : base(dataAccessor) {
            _table = table;
            _constraint = constraint;
        }

        public override int Execute() {
            return Exec(DataAccessor.QueryGenerator.DropForeignKey(
                    _table, _constraint));
        }

        public override string ToString() {
            return $"Drop key {_table}.{_constraint}";
        }
    }

    public class CreateForeignKeyScAction : AbstractIStructureCheckNecessaryAction {
        String _table;
        String _column;
        String _refTable;
        String _refColumn;

        public CreateForeignKeyScAction(
            IRdbmsDataAccessor dataAccessor,
            string table, string column, string refTable, string refColumn, MemberInfo columnMember) : base(dataAccessor) {
            _table = table;
            _column = column;
            _refTable = refTable;
            _refColumn = refColumn;
        }

        public override int Execute() {
            return Exec(DataAccessor.QueryGenerator.AddForeignKey(
                    _table, _column, _refTable, _refColumn));
        }

        public override string ToString() {
            return $"Create foreign key {_table}.fk_{_column}_{_refTable}_{_refColumn}";
        }
    }

    public class ExecutePugeForFkScAction : AbstractIStructureCheckNecessaryAction {
        String _table;
        String _column;
        String _refTable;
        String _refColumn;

        public ExecutePugeForFkScAction(
            IRdbmsDataAccessor dataAccessor,
            string table, string column, string refTable, string refColumn, MemberInfo columnMember) : base(dataAccessor) {
            _table = table;
            _column = column;
            _refTable = refTable;
            _refColumn = refColumn;
        } 

        public override int Execute() {
            return Exec(DataAccessor.QueryGenerator.Purge(
                _table, _column, _refTable, _refColumn));
        }

        public override string ToString() {
            return $"Data Purge for key {_table}.fk_{_column}_{_refTable}_{_refColumn}";
        }
    }

    public class CreateColumnScAction : AbstractIStructureCheckNecessaryAction {
        String _table;
        String _column;
        MemberInfo _columnMember;

        public CreateColumnScAction(
            IRdbmsDataAccessor dataAccessor,
            string table, string column, MemberInfo columnMember) : base(dataAccessor) {
            _table = table;
            _column = column;
            _columnMember = columnMember;
        }

        public override int Execute() {
            return Exec(DataAccessor.QueryGenerator.AddColumn(
                _table, StructureChecker.GetColumnDefinition(_columnMember)));
        }

        public override string ToString() {
            return $"Create column {_columnMember.Name}";
        }
    }

    public class RenameTableScAction : AbstractIStructureCheckNecessaryAction {
        String _table;
        String _newName;

        public RenameTableScAction(
            IRdbmsDataAccessor dataAccessor,
            string table, string newName) : base(dataAccessor) {
            _table = table;
            _newName = newName;
        }

        public override int Execute() {
            return Exec(DataAccessor.QueryGenerator.RenameTable(
                _table, _newName));
        }

        public override string ToString() {
            return $"Rename {_table} to {_newName}";
        }
    }

    public class RenameColumnScAction : AbstractIStructureCheckNecessaryAction {
        String _table;
        String _column;
        MemberInfo _columnMember;

        public RenameColumnScAction(
            IRdbmsDataAccessor dataAccessor,
            string table, string column, MemberInfo columnMember) : base(dataAccessor) {
            _table = table;
            _column = column;
            _columnMember = columnMember;
        }

        public override int Execute() {
            return Exec(DataAccessor.QueryGenerator.RenameColumn(
                _table, _column, StructureChecker.GetColumnDefinition(_columnMember)));
        }

        public override string ToString() {
            return $"Rename {_table}.{_column} to {_columnMember.Name}";
        }
    }

    public class AlterColumnDefinitionScAction : AbstractIStructureCheckNecessaryAction {
        String _table;
        String _column;
        MemberInfo _columnMember;

        public AlterColumnDefinitionScAction(
            IRdbmsDataAccessor dataAccessor,
            string table, string column, MemberInfo columnMember) : base(dataAccessor) {
            _table = table;
            _column = column;
            _columnMember = columnMember;
        }

        public override int Execute() {
            return Exec(DataAccessor.QueryGenerator.RenameColumn(
                _table, _column, StructureChecker.GetColumnDefinition(_columnMember)));
        }

        public override string ToString() {
            return $"Change column definition of {_columnMember.Name}";
        }
    }

    public class CreateTableScAction : AbstractIStructureCheckNecessaryAction {
        private Type _tableType;
        public CreateTableScAction(IRdbmsDataAccessor dataAccessor, Type tableType) : base(dataAccessor) {
            _tableType = tableType;
        }

        public override int Execute() {
            return Exec(DataAccessor.QueryGenerator.GetCreationCommand(_tableType));
        }

        public override string ToString() {
            return $"Create table {_tableType.Name}";
        }
    }

    public abstract class AbstractIStructureCheckNecessaryAction : IStructureCheckNecessaryAction {
        protected IRdbmsDataAccessor DataAccessor;
        public AbstractIStructureCheckNecessaryAction(IRdbmsDataAccessor dataAccessor) {
            DataAccessor = dataAccessor;
        }

        public abstract int Execute();

        protected int Exec(IQueryBuilder query) {
            try {
                return DataAccessor.Execute(query);
            } catch (Exception x) {
                throw new BDadosException($"Error executing [{this.ToString()}]: [{x.Message}]", x);
            }
        }
    }
    public interface IStructureCheckNecessaryAction {
        int Execute();
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

        public IEnumerable<IStructureCheckNecessaryAction> EvaluateLegacyKeys(List<ForeignKeyAttribute> keys) {
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
                            fkDef.RefColumn.ToLower() == refColName.ToLower() &&
                            fkDef.RefTable.ToLower() == refTablName.ToLower() &&
                            field.Name.ToLower() == colName.ToLower()) {
                            found = true;
                            break;
                        }
                    }
                }
                if (!found) {
                    yield return new DropKeyScAction(DataAccessor, tablName, keys[i].ConstraintName);
                }
            }
            yield break;
        }

        private IEnumerable<IStructureCheckNecessaryAction> EvaluateForFullTableDekeyal(String target, List<String> tables, List<ForeignKeyAttribute> keys) {
            for (int i = 0; i < keys.Count; i++) {
                var refTableName = keys[i].RefTable;
                if (refTableName == target) {
                    var tableName = keys[i].Table;
                    var columnName = keys[i].Column;
                    var refColumnName = keys[i].RefColumn;
                    var constraint = keys[i].ConstraintName;
                    Benchmarker.Mark($"Dekey Table {tableName} from {constraint} references {refTableName}");
                    yield return new DropKeyScAction(DataAccessor, tableName, keys[i].ConstraintName);
                }
            }
        }

        private IEnumerable<IStructureCheckNecessaryAction> EvaluateTableChanges(List<String> tables, List<ForeignKeyAttribute> keys) {
            Dictionary<string, string> oldNames = new Dictionary<string, string>();
            foreach (String tableName in tables) {
                foreach (var type in workingTypes) {
                    var oldNameAtt = type.GetCustomAttribute<OldNameAttribute>();
                    if (oldNameAtt != null) {
                        if (tableName == oldNameAtt.Name) {
                            oldNames.Add(type.Name, oldNameAtt.Name);
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
                        if (old.Key.ToLower() == type.Name.ToLower()) {
                            foreach (var a in EvaluateForFullTableDekeyal(old.Value, tables, keys)) {
                                yield return a;
                            }
                            yield return new RenameTableScAction(DataAccessor, old.Value, old.Key);
                            renamed = true;
                            break;
                        }
                    }
                    if (!renamed) {
                        TablesToCreate.Add(type.Name.ToLower());
                        yield return new CreateTableScAction(DataAccessor, type);
                    }
                }
            }

            yield break;
        }

        private List<string> TablesToCreate = new List<string>();

        private IEnumerable<IStructureCheckNecessaryAction> EvaluateColumnChanges(List<FieldAttribute> columns, List<ForeignKeyAttribute> keys) {

            foreach (var type in workingTypes) {
                var fields = ReflectionTool.FieldsAndPropertiesOf(type)
                    .Where(f => f.GetCustomAttribute<FieldAttribute>() != null);
                foreach (var field in fields) {

                    var fieldExists = false;
                    foreach (var col in columns) {
                        if (type.Name.ToLower() != col.Table.ToLower())
                            continue;
                        var colName = col.Name;
                        if (field.Name == colName) {
                            fieldExists = true;
                        }
                    }

                    if (!fieldExists) {

                        var oldExists = false;
                        foreach (var col in columns) {
                            if (col.Table != type.Name) continue;
                            var colName = col.Name;
                            var ona = field.GetCustomAttribute<OldNameAttribute>();
                            if (ona != null) {
                                if (ona.Name == colName && ona.Name != col.Name) {
                                    oldExists = true;
                                    yield return new RenameColumnScAction(DataAccessor, type.Name, ona.Name, field);
                                }
                            }
                        }

                        if (!oldExists && !TablesToCreate.Contains(type.Name.ToLower())) {
                            yield return new CreateColumnScAction(DataAccessor, type.Name, field.Name, field);
                        }

                    } else {

                        foreach (var col in columns) {
                            var tableName = col.Table;
                            if (tableName != type.Name) continue;
                            var columnName = col.Name;
                            if (field.Name != columnName) continue;
                            // Found columns, check definitions
                            var columnIsNullable = col.AllowNull;
                            var length = col.Size;
                            var datatype = col.Type;
                            var fieldAtt = field.GetCustomAttribute<FieldAttribute>();
                            if (fieldAtt == null) continue;
                            var typesToCheckSize = new string[] {
                                "VARCHAR", "VARBINARY"
                            };
                            var sizesMatch = !typesToCheckSize.Contains(datatype) || length == fieldAtt.Size;
                            var dbdef = GetDatabaseType(field, fieldAtt);
                            var dbDefinition = dbdef.Substring(0, dbdef.IndexOf('(') > -1 ? dbdef.IndexOf('(') : dbdef.Length);
                            if (
                                columnIsNullable != fieldAtt.AllowNull ||
                                !sizesMatch ||
                                datatype.ToUpper() != dbDefinition.ToUpper()
                                ) {
                                yield return new AlterColumnDefinitionScAction(DataAccessor, type.Name, field.Name, field);
                            } else {

                            }
                        }

                    }

                }
            }


            yield break;
        }

        public int ExecuteNecessaryActions(IEnumerable<IStructureCheckNecessaryAction> actions, Action<IStructureCheckNecessaryAction, int> onActionExecuted, Action<Exception> handleException = null) {
            var enny = actions.GetEnumerator();
            int retv = 0;
            int went = 0;
            WorkQueuer wq = new WorkQueuer("Strucheck Queuer");
            var cliQ = new WorkQueuer("cli_q", 1);
            List<Exception> exces = new List<Exception>();
            while (enny.MoveNext()) {
                var thisAction = enny.Current;
                wq.Enqueue(() => {
                    retv += thisAction.Execute();
                    //lock(wq) {
                    int myWent = went++;
                    cliQ.Enqueue(() => {
                        onActionExecuted?.Invoke(thisAction, myWent);
                    });
                    //}
                }, (x) => {
                    if (handleException == null)
                        lock (exces)
                            exces.Add(x);
                    else
                        handleException.Invoke(x);
                });
            }
            wq.Stop();
            cliQ.Stop();
            if (exces.Any()) {
                throw new AggregateException("There were errors executing actions", exces);
            }
            return retv;
        }

        public IEnumerable<IStructureCheckNecessaryAction> EvaluateNecessaryActions() {
            var keys = GetInfoSchemaKeys();
            var tables = GetInfoSchemaTables();
            var columns = GetInfoSchemaColumns();
            TablesToCreate.Clear();
            foreach (var a in EvaluateLegacyKeys(keys))
                yield return a;
            foreach (var a in EvaluateTableChanges(tables, keys))
                yield return a;
            foreach (var a in EvaluateColumnChanges(columns, keys))
                yield return a;
            foreach (var a in EvaluateMissingKeysCreation(columns, keys))
                yield return a;
        }

        private IEnumerable<ForeignKeyAttribute> GetNecessaryForeignKeys() {
            foreach(var t in workingTypes) {
                foreach(var f in ReflectionTool.FieldsAndPropertiesOf(t)) {
                    var fk = f.GetCustomAttribute<ForeignKeyAttribute>();
                    if(fk != null) {
                        fk.Table = t.Name;
                        fk.Column = f.Name;
                        yield return fk;
                        continue;
                    }
                    fk = new ForeignKeyAttribute();
                    fk.Table = t.Name;
                    fk.Column = f.Name;

                    var agf = f.GetCustomAttribute<AggregateFieldAttribute>();
                    if(agf != null) {
                        var classField = ReflectionTool.FieldsWithAttribute<FieldAttribute>(agf.RemoteObjectType)
                            .FirstOrDefault(a => a.Name == agf.ObjectKey);
                        if(classField != null) {
                            fk.Table = agf.RemoteObjectType.Name;
                            fk.Column = agf.ObjectKey;
                            fk.RefTable = t.Name;
                            fk.RefColumn = Fi.Tech.GetRidColumn(t);
                        } else {
                            fk.Table = t.Name;
                            fk.Column = agf.ObjectKey;
                            fk.RefTable = agf.RemoteObjectType.Name;
                            fk.RefColumn = Fi.Tech.GetRidColumn(agf.RemoteObjectType);
                        }
                        yield return fk;
                        continue;
                    }

                    var agff = f.GetCustomAttribute<AggregateFarFieldAttribute>();
                    if (agff != null) {
                        var cf = ReflectionTool.FieldsWithAttribute<FieldAttribute>(agff.ImediateType)
                            .FirstOrDefault(a => a.Name == agff.ImediateKey);
                        if (cf != null) {
                            fk.Table = agff.ImediateType.Name;
                            fk.Column = agff.ImediateKey;
                            fk.RefTable = t.Name;
                            fk.RefColumn = Fi.Tech.GetRidColumn(t);
                        } else {
                            fk.Table = t.Name;
                            fk.Column = agff.ImediateKey;
                            fk.RefTable = agff.ImediateType.Name;
                            fk.RefColumn = Fi.Tech.GetRidColumn(agff.ImediateType);
                        }
                        yield return fk;
                        fk = new ForeignKeyAttribute();
                        var cf2 = ReflectionTool.FieldsWithAttribute<FieldAttribute>(agff.FarType)
                            .FirstOrDefault(a => a.Name == agff.FarKey);
                        if (cf2 != null) {
                            fk.Table = agff.FarType.Name;
                            fk.Column = agff.FarKey;
                            fk.RefTable = t.Name;
                            fk.RefColumn = Fi.Tech.GetRidColumn(t);
                        } else {
                            fk.Table = t.Name;
                            fk.Column = agff.FarKey;
                            fk.RefTable = agff.FarType.Name;
                            fk.RefColumn = Fi.Tech.GetRidColumn(agff.FarType);
                        }
                        yield return fk;
                        continue;
                    }

                    var ago = f.GetCustomAttribute<AggregateObjectAttribute>();
                    if (ago != null) {
                        var type = ReflectionTool.GetTypeOf(f);
                        var classField = ReflectionTool.FieldsWithAttribute<FieldAttribute>(type)
                            .FirstOrDefault(a => a.Name == ago.ObjectKey);
                        if (classField != null) {
                            fk.Table = type.Name;
                            fk.Column = ago.ObjectKey;
                            fk.RefTable = t.Name;
                            fk.RefColumn = Fi.Tech.GetRidColumn(t);
                        } else {
                            fk.Table = t.Name;
                            fk.Column = ago.ObjectKey;
                            fk.RefTable = type.Name;
                            fk.RefColumn = Fi.Tech.GetRidColumn(type);
                        }
                        yield return fk;
                        continue;
                    }
                    var agl = f.GetCustomAttribute<AggregateListAttribute>();
                    if (agl != null) {
                        var classField = ReflectionTool.FieldsWithAttribute<FieldAttribute>(agl.RemoteObjectType)
                            .FirstOrDefault(a => a.Name == agl.RemoteField);
                        if (classField != null) {
                            fk.Table = agl.RemoteObjectType.Name;
                            fk.Column = agl.RemoteField;
                            fk.RefTable = t.Name;
                            fk.RefColumn = Fi.Tech.GetRidColumn(t);
                        } else {
                            fk.Table = t.Name;
                            fk.Column = agl.RemoteField;
                            fk.RefTable = agl.RemoteObjectType.Name;
                            fk.RefColumn = Fi.Tech.GetRidColumn(agl.RemoteObjectType);
                        }
                        yield return fk;
                        continue;
                    }
                }
            }
        }

        public IEnumerable<ExecutePugeForFkScAction> EnumeratePurgesFor(ForeignKeyAttribute fk, List<ForeignKeyAttribute> fkli) {
            if (_purgedKeys.Contains(fk.ToString())) {
                yield break;
            }

            _purgedKeys.Add(fk.ToString());
            foreach (var a in fkli.Where(b=> b.RefTable.ToLower() == fk.Table.ToLower())) {
                foreach(var x in EnumeratePurgesFor(a, fkli)) {
                    yield return x;
                }
            }

            yield return new ExecutePugeForFkScAction(DataAccessor, fk.Table, fk.Column, fk.RefTable, fk.RefColumn, null);
        }

        private List<string> _purgedKeys = new List<string>();
        public IEnumerable<IStructureCheckNecessaryAction> EvaluateMissingKeysCreation(List<FieldAttribute> columns, List<ForeignKeyAttribute> keys) {
            _purgedKeys.Clear();
            var needFK = GetNecessaryForeignKeys().ToList();
            var needFKDict = new Dictionary<string, ForeignKeyAttribute>();
            foreach(var a in needFK) {
                needFKDict[a.ToString()] = a;
            }
            needFK = needFKDict.Values.ToList();

            foreach(var fk in needFK) {
                if (!keys.Any(a => a.ToString() == fk.ToString())) {
                    foreach (var x in EnumeratePurgesFor(fk, needFK)) {
                        yield return x;
                    }
                    yield return new CreateForeignKeyScAction(DataAccessor, fk.Table, fk.Column, fk.RefTable, fk.RefColumn, null);
                }
            }
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
                { "CONSTRAINT_NAME", nameof(ForeignKeyAttribute.ConstraintName) },
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
                    DataAccessor.QueryGenerator.InformationSchemaQueryColumns(dbName)
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

        public async Task CheckStructureAsync(
            Func<IStructureCheckNecessaryAction, Task> onActionProcessed = null,
            Func<IStructureCheckNecessaryAction, Exception, Task> onError = null,
            Func<IStructureCheckNecessaryAction, Task<bool>> preAuthorizeAction = null,
            Func<int, Task> onReportTotalTasks = null) {
            var neededActions = EvaluateNecessaryActions().ToList();
            var ortt = onReportTotalTasks?.Invoke(neededActions.Count);

            var enumerator = neededActions.GetEnumerator();
            while (enumerator.MoveNext()) {
                var action = enumerator.Current;
                if (preAuthorizeAction != null)
                    if (!await preAuthorizeAction(action))
                        continue;

                try {
                    action.Execute();
                    await onActionProcessed(action);
                } catch (Exception x) {
                    if (onError != null)
                        await onError(action, x);
                }
            }

            if (ortt != null) await ortt;
        }

        // This code is here so that I can
        // finish the new SC method, it doesn't rekey yet.
        // And it also needs some anti-repetition method.
        //
        //public void LegacyCheckStructure() {
        //    Benchmarker = new Benchmarker("Check Structure");
        //    DataAccessor.Access(() => {

        //        Benchmarker.WriteToStdout = true;


        //        var dbName = DataAccessor.SchemaName;
        //        DataTable tables = DataAccessor.Query(
        //            DataAccessor.QueryGenerator.InformationSchemaQueryTables(dbName));

        //        DataTable keys = DataAccessor.Query(
        //            DataAccessor.QueryGenerator.InformationSchemaQueryKeys(dbName));

        //        Benchmarker.Mark("Clear Keys");
        //        ClearKeys(keys);

        //        Benchmarker.Mark("Work Tables");
        //        keys = DataAccessor.Query(
        //            DataAccessor.QueryGenerator.InformationSchemaQueryKeys(dbName));
        //        WorkOnTables(tables, keys);

        //        Benchmarker.Mark("Work Columns");
        //        DataTable columns = DataAccessor.Query(
        //            DataAccessor.QueryGenerator.InformationSchemaQueryColumns(dbName));

        //        WorkOnColumns(columns, keys);

        //        // Re read keys here because work on tables and columns
        //        // probably changed this too much
        //        Benchmarker.Mark("Re Keys");
        //        keys = DataAccessor.Query(
        //            DataAccessor.QueryGenerator.InformationSchemaQueryKeys(dbName));

        //        ReKeys(keys);

        //    }, (x) => {
        //        Fi.Tech.WriteLine(Fi.Tech.GetStrings().ERROR_IN_STRUCTURE_CHECK);
        //        Fi.Tech.WriteLine(x.Message);
        //        Fi.Tech.WriteLine(x.StackTrace);
        //    });
        //    Benchmarker.FinalMark();
        //}

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
                            fkDef.RefTable == refTablName &&
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
                            type.Name == tableName &&
                            field.Name == colName &&
                            fkDef.RefColumn == refColName &&
                            fkDef.RefTable == refTablName) {
                            found = true;
                            break;
                        } else {

                        }
                    }

                    if (!found) {
                        try {
                            Benchmarker.Mark($"Purge for CONSTRAINT FK {type.Name}/{field.Name} references {fkDef.RefTable}/{fkDef.RefColumn}");
                            Exec(
                                DataAccessor.QueryGenerator.Purge(type.Name, field.Name, fkDef.RefTable, fkDef.RefColumn));

                            Benchmarker.Mark($"Create Constraint FK {type.Name}/{field.Name} references {fkDef.RefTable}/{fkDef.RefColumn}");
                            Exec(
                                DataAccessor.QueryGenerator.AddForeignKey(type.Name, field.Name, fkDef.RefTable, fkDef.RefColumn));
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
                        if (tabName == oldNameAtt.Name) {
                            oldNames.Add(type.Name, oldNameAtt.Name);
                        }
                    }
                }
            }

            foreach (var type in workingTypes) {

                var found = false;
                foreach (DataRow a in tables.Rows) {
                    var tabName = a["TABLE_NAME"] as String;
                    if (tabName == type.Name) {
                        found = true;
                    }
                }
                if (!found) {
                    bool renamed = false;
                    foreach (var old in oldNames) {
                        if (old.Key == type.Name && old.Value != type.Name) {
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
                    if (tablName != type.Name) continue;
                    var fields = ReflectionTool.FieldsAndPropertiesOf(type)
                    .Where((f) => f.GetCustomAttribute<FieldAttribute>() != null);

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

                            if (tablName2 != type.Name) continue;

                            if (colName2 == field.Name
                                && type.Name == tablName2
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
                                    && type.Name == tablName
                                    ) {
                                    Benchmarker.Mark($"ACTION: Rename {old.Value} to {field.Name}");
                                    DekeyColumn(type.Name, old.Value, keys);
                                    Exec(DataAccessor.QueryGenerator.RenameColumn(type.Name, old.Value, GetColumnDefinition(field)));
                                    renamed = true;
                                    break;
                                }
                                if (renamed) break;
                            }

                            if (!renamed) {
                                Benchmarker.Mark($"ACTION: Create column {field.Name}");
                                Exec(DataAccessor.QueryGenerator.AddColumn(type.Name, GetColumnDefinition(field)));
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
                    if (tableName != type.Name) continue;
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
                            Benchmarker.Mark($"ACTION: Alter Column {type.Name}/{field.Name}");
                            Exec(DataAccessor.QueryGenerator.RenameColumn(type.Name, field.Name, GetColumnDefinition(field)));
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
        // Actually, these dragons should be refactored into IRdbmsAccessorPlugin
        // Soon
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
                switch (dataType) {
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

        internal static String GetColumnDefinition(MemberInfo field, FieldAttribute info = null) {
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
            switch (value.GetType().Name) {
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
