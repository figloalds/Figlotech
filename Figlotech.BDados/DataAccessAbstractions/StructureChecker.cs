
using Figlotech.BDados.Builders;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using Figlotech.Core;
using Figlotech.Core.Helpers;
using Figlotech.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace Figlotech.BDados.DataAccessAbstractions {
    public enum ScStructuralKeyType {
        Index,
        PrimaryKey,
        ForeignKey
    }

    public class ScStructuralLink {
        private String _linkName;

        public string TABLE_NAME { get => Table; set => Table = value; }
        public string COLUMN_NAME { get => Column; set => Column = value; } 
        public string NON_UNIQUE { get => IsUnique ? "NO": "YES"; set => IsUnique = value?.ToLower() == "no"; }
        public string INDEX_NAME { get => KeyName; set => KeyName = value; }

        public string REFERENCED_COLUMN_NAME { get => RefColumn; set => RefColumn = value; }
        public string REFERENCED_TABLE_NAME { get => RefTable; set => RefTable = value; }
        public string CONSTRAINT_NAME { get => KeyName; set => KeyName = value; }

        public String Table { get; set; }
        public String Column { get; set; }
        public String RefTable { get; set; }
        public String RefColumn { get; set; }
        public String KeyName { get => _linkName ?? this.FiTechKeyName; set => _linkName = value; }
        public bool IsUnique { get; set; } = false;

        public String FiTechKeyName {
            get {
                switch (this.Type) {
                    case ScStructuralKeyType.ForeignKey:
                        return $"fk_{Table}_{Column}".ToLower();
                    case ScStructuralKeyType.Index:
                        return $"{(IsUnique ? "uk_" : "idx_")}{Table}_{Column}".ToLower();
                    case ScStructuralKeyType.PrimaryKey:
                        return "PRIMARY";
                }
                return "";
            }
        }

        public static ScStructuralLink FromFkAttribute(ForeignKeyAttribute FkAtt) {
            var retv = new ScStructuralLink();
            retv.CopyFrom(FkAtt);
            retv.Type = ScStructuralKeyType.ForeignKey;
            return retv;
        }

        public ScStructuralKeyType Type { get; set; } = ScStructuralKeyType.Index;

        public override string ToString() {
            return $"{Table}_{Column}_{RefTable}_{RefColumn}".ToLower();
        }
    }

    public class DropFkScAction : AbstractIStructureCheckNecessaryAction {
        ScStructuralLink keyInfo;

        public DropFkScAction(
            IRdbmsDataAccessor dataAccessor,
            ScStructuralLink keyInfo, string reason) : base(dataAccessor, reason) {
            this.keyInfo = keyInfo;
        }

        public override int Execute(IRdbmsDataAccessor DataAccessor) {
            return Exec(DataAccessor, DataAccessor.QueryGenerator.DropForeignKey(
                    keyInfo.Table, keyInfo.KeyName));
        }

        public override string ToString() {
            return $"Drop foreign key {keyInfo.Table}.{keyInfo.KeyName}";
        }
    }

    public class DropPkScAction : AbstractIStructureCheckNecessaryAction {
        String _table;
        String _constraint;

        public DropPkScAction(
            IRdbmsDataAccessor dataAccessor,
            string table, string constraint, string reason) : base(dataAccessor, reason) {
            _table = table;
            _constraint = constraint;
        }

        public override int Execute(IRdbmsDataAccessor DataAccessor) {
            return Exec(DataAccessor, DataAccessor.QueryGenerator.DropPrimary(
                    _table, _constraint));
        }

        public override string ToString() {
            return $"Drop primary {_table}.{_constraint}";
        }
    }
    public class DropIdxScAction : AbstractIStructureCheckNecessaryAction {
        String _table;
        String _constraint;

        public DropIdxScAction(
            IRdbmsDataAccessor dataAccessor,
            string table, string constraint, string reason) : base(dataAccessor, reason) {
            _table = table;
            _constraint = constraint;
        }

        public override int Execute(IRdbmsDataAccessor DataAccessor) {
            return Exec(DataAccessor, DataAccessor.QueryGenerator.DropIndex(
                    _table, _constraint));
        }

        public override string ToString() {
            return $"Drop unique {_table}.{_constraint}";
        }

    }
    public class DropUkScAction : AbstractIStructureCheckNecessaryAction {
        String _table;
        String _constraint;

        public DropUkScAction(
            IRdbmsDataAccessor dataAccessor,
            string table, string constraint, string reason) : base(dataAccessor, reason) {
            _table = table;
            _constraint = constraint;
        }

        public override int Execute(IRdbmsDataAccessor DataAccessor) {
            return Exec(DataAccessor, DataAccessor.QueryGenerator.DropUnique(
                    _table, _constraint));
        }

        public override string ToString() {
            return $"Drop unique {_table}.{_constraint}";
        }

    }

    public class CreateIndexScAction : AbstractIStructureCheckNecessaryAction {
        ScStructuralLink keyInfo;

        bool _unique;


        public CreateIndexScAction(
            IRdbmsDataAccessor dataAccessor,
            ScStructuralLink keyInfo, string reason) : base(dataAccessor, reason) {
            this.keyInfo = keyInfo;
        }

        public override int Execute(IRdbmsDataAccessor DataAccessor) {

            int v = 0;

            if(keyInfo.IsUnique) {
                return Exec(DataAccessor, DataAccessor.QueryGenerator.AddUniqueKey(
                    keyInfo.Table, keyInfo.Column, keyInfo.KeyName));
            } else {
                return Exec(DataAccessor, DataAccessor.QueryGenerator.AddIndex(
                    keyInfo.Table, keyInfo.Column, keyInfo.KeyName));
            }
        }

        public override string ToString() {
            return $"Create unique key {keyInfo.Table}.{keyInfo.KeyName}";
        }
    }

    public class CreatePrimaryKeyScAction : AbstractIStructureCheckNecessaryAction {
        ScStructuralLink keyInfo;

        int _length;


        public CreatePrimaryKeyScAction(
            IRdbmsDataAccessor dataAccessor, ScStructuralLink keyInfo, string reason) : base(dataAccessor, reason) {
            this.keyInfo = keyInfo;
        }

        public override int Execute(IRdbmsDataAccessor DataAccessor) {
            return Exec(DataAccessor, DataAccessor.QueryGenerator.AddPrimaryKey(
                    keyInfo.Table, keyInfo.Column, keyInfo.KeyName));
        }

        public override string ToString() {
            return $"Create primary key {keyInfo.Table}.{keyInfo.KeyName}";
        }
    }

    public class CreateForeignKeyScAction : AbstractIStructureCheckNecessaryAction {
        ScStructuralLink keyInfo;

        public CreateForeignKeyScAction(
            IRdbmsDataAccessor dataAccessor,
             ScStructuralLink keyInfo, string reason) : base(dataAccessor, reason) {
            this.keyInfo = keyInfo;
        }

        public override int Execute(IRdbmsDataAccessor DataAccessor) {
            int v = 0;
            //try {
            //    v += Exec(DataAccessor.QueryGenerator.AddIndex(
            //        keyInfo.Table, keyInfo.Column, keyInfo.KeyName));
            //} catch (Exception x) { }
            v += Exec(DataAccessor, DataAccessor.QueryGenerator.AddForeignKey(
                    keyInfo.Table, keyInfo.Column, keyInfo.RefTable, keyInfo.RefColumn, keyInfo.KeyName));
            return v;
        }

        public override string ToString() {
            return $"Create foreign key {keyInfo.Table}.{keyInfo.KeyName}";
        }
    }

    public class ExecutePugeForFkScAction : AbstractIStructureCheckNecessaryAction {
        String _table;
        String _column;
        String _refTable;
        String _refColumn;
        bool _isNullable;

        public ExecutePugeForFkScAction(
            IRdbmsDataAccessor dataAccessor,
            string table, string column, string refTable, string refColumn, MemberInfo columnMember, string reason) : base(dataAccessor, reason) {
            _table = table;
            _column = column;
            _refTable = refTable;
            _refColumn = refColumn;
            _isNullable = columnMember.GetCustomAttribute<FieldAttribute>()?.AllowNull ?? true;
        }

        public override int Execute(IRdbmsDataAccessor DataAccessor) {
            return Exec(DataAccessor, DataAccessor.QueryGenerator.Purge(
                _table, _column, _refTable, _refColumn, _isNullable));

            return 0;

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
            string table, string column, MemberInfo columnMember, string reason) : base(dataAccessor, reason) {
            _table = table;
            _column = column;
            _columnMember = columnMember;
        }

        public override int Execute(IRdbmsDataAccessor DataAccessor) {
            return Exec(DataAccessor, DataAccessor.QueryGenerator.AddColumn(
                _table, DataAccessor.QueryGenerator.GetColumnDefinition(_columnMember)));
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
            string table, string newName, string reason) : base(dataAccessor, reason) {
            _table = table;
            _newName = newName;
        }

        public override int Execute(IRdbmsDataAccessor DataAccessor) {
            return Exec(DataAccessor, DataAccessor.QueryGenerator.RenameTable(
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
            string table, string column, MemberInfo columnMember, string reason) : base(dataAccessor, reason) {
            _table = table;
            _column = column;
            _columnMember = columnMember;
        }

        public override int Execute(IRdbmsDataAccessor DataAccessor) {
            return Exec(DataAccessor, DataAccessor.QueryGenerator.RenameColumn(
                _table, _column, DataAccessor.QueryGenerator.GetColumnDefinition(_columnMember)));
        }

        public override string ToString() {
            return $"Rename {_table}.{_column} to {_columnMember.Name}";
        }
    }

    public class UpdateExNullableColumnScAction : AbstractIStructureCheckNecessaryAction {
        String _table;
        String _column;
        object _defaultValue;

        public UpdateExNullableColumnScAction(
            IRdbmsDataAccessor dataAccessor,
            string table, string column, object defaultValue, string reason) : base(dataAccessor, reason) {
            _table = table;
            _column = column;
            _defaultValue = defaultValue;
        }

        public override int Execute(IRdbmsDataAccessor DataAccessor) {
            return Exec(DataAccessor, DataAccessor.QueryGenerator.UpdateColumn(_table, _column, _defaultValue, Qb.Fmt($"{_column} IS NULL")));
        }

        public override string ToString() {
            return $"Fill default value onto newly non-nullable column {_table}.{_column}";
        }
    }

    public class DropColumnScAction : AbstractIStructureCheckNecessaryAction {
        String _table;
        FieldAttribute _column;
        Type _type;

        public DropColumnScAction(
            IRdbmsDataAccessor dataAccessor,
            string table, FieldAttribute column, Type type, string reason) : base(dataAccessor, reason) {
            _table = table;
            _column = column;
            _type = type;
        }

        public override int Execute(IRdbmsDataAccessor DataAccessor) {
            _column.AllowNull = true;
            return Exec(DataAccessor,
                Qb.Fmt($@"
                    ALTER TABLE {_table} CHANGE COLUMN {_column.Name} {_column.Name} {_column.Type} DEFAULT NULL
                ")
            );
            //Exec(DataAccessor, Qb.Fmt(
            //    $@"
            //        SET @a=1;
            //        INSERT INTO _ResidualScData (RID, Type, Field, Value, ReferenceRID, CreatedBy, AlteredBy)
            //        SELECT CONCAT({FiTechBDadosExtensions.RidColumnOf[_type]}, '-', @a:=@a+1), '{_table}', '{_column}', CAST({_column} as BINARY), {FiTechBDadosExtensions.RidColumnOf[_type]}, '{RID.MachineRID.AsULong}','{RID.MachineRID.AsULong}' FROM {_table} WHERE {_column} IS NOT NULL;
            //    "
            //));
            //return Exec(DataAccessor, DataAccessor.QueryGenerator.DropColumn(
            //    _table, _column.Name));
        }

        public override string ToString() {
            return $"Drop column {_table}.{_column.Name}";
        }
    }

    public class AlterColumnDefinitionScAction : AbstractIStructureCheckNecessaryAction {
        String _table;
        String _column;
        MemberInfo _columnMember;

        public AlterColumnDefinitionScAction(
            IRdbmsDataAccessor dataAccessor,
            string table, string column, MemberInfo columnMember, string reason) : base(dataAccessor, reason) {
            _table = table;
            _column = column;
            _columnMember = columnMember;
        }

        public override int Execute(IRdbmsDataAccessor DataAccessor) {
            return Exec(DataAccessor, DataAccessor.QueryGenerator.RenameColumn(
                _table, _column, DataAccessor.QueryGenerator.GetColumnDefinition(_columnMember)));
        }

        public override string ToString() {
            return $"Change column definition of {_columnMember.Name}";
        }
    }

    public class CreateTableScAction : AbstractIStructureCheckNecessaryAction {
        private Type _tableType;
        public CreateTableScAction(IRdbmsDataAccessor dataAccessor, Type tableType, string reason) : base(dataAccessor, reason) {
            _tableType = tableType;
        }

        public override int Execute(IRdbmsDataAccessor DataAccessor) {
            return Exec(DataAccessor, DataAccessor.QueryGenerator.GetCreationCommand(_tableType));
        }

        public override string ToString() {
            return $"Create table {_tableType.Name}";
        }
    }

    public abstract class AbstractIStructureCheckNecessaryAction : IStructureCheckNecessaryAction {

        public string ActionType => this.GetType().Name;
        public string Description => this.ToString();
        public string Reason { get; set; }
        public AbstractIStructureCheckNecessaryAction(IRdbmsDataAccessor dataAccessor, String reason) {
            this.Reason = reason;
        }

        public abstract int Execute(IRdbmsDataAccessor DataAccessor);

        protected int Exec(IRdbmsDataAccessor DataAccessor, IQueryBuilder query) {
            try {
                return DataAccessor.Execute(query);
            } catch (Exception x) {
                throw new BDadosException($"Error executing [{this.ToString()}]: [{x.Message}]", x);
            }
        }
    }

    public interface IStructureCheckNecessaryAction {
        string Description { get;  }
        string Reason { get;  }
        int Execute(IRdbmsDataAccessor DataAccessor);
    }

    public class StructureChecker {
        List<Type> workingTypes;
        IRdbmsDataAccessor DataAccessor;

        // "Mirror, mirror on the wall, who's code is the shittiest of them all?"

        public StructureChecker(IRdbmsDataAccessor dataAccessor, Assembly assembly) {
            Init(dataAccessor, ReflectionTool.GetLoadableTypesFrom(assembly));
        }
        public StructureChecker(IRdbmsDataAccessor dataAccessor, IEnumerable<Type> types) {
            Init(dataAccessor, types);
        }

        private void Init(IRdbmsDataAccessor dataAccessor, IEnumerable<Type> types) {
            DataAccessor = dataAccessor;
            workingTypes = new List<Type>();
            workingTypes.Add(typeof(_ResidualScData));
            workingTypes.AddRange(
                types
                    .Where((t) =>
                        IsDataObject(t) &&
                        t.GetCustomAttribute<ViewOnlyAttribute>() == null &&
                        !t.IsAbstract
                    )
            );
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

        private bool CheckMatch(ScStructuralLink a, ScStructuralLink n) {
            if(a.Table.ToLower() != n.Table.ToLower()) {
                return false;
            }
            if (a.Type != n.Type)
                return false;
            switch(a.Type) {
                case ScStructuralKeyType.Index:
                    return
                        (a.Table.ToLower() == n.Table.ToLower() && a.KeyName.ToLower() == n.KeyName.ToLower()) || (
                            a.Table.ToLower() == n.Table.ToLower() &&
                            a.Column.ToLower() == n.Column.ToLower()
                        );
                case ScStructuralKeyType.ForeignKey:
                    return
                        (a.Table.ToLower() == n.Table.ToLower() && a.KeyName == n.KeyName) || (
                            a.Table.ToLower() == n.Table.ToLower() &&
                            a.Column.ToLower() == n.Column.ToLower() &&
                            a.RefTable.ToLower() == n.RefTable.ToLower() &&
                            a.RefColumn.ToLower() == n.RefColumn.ToLower()
                        );
                case ScStructuralKeyType.PrimaryKey:
                    return
                        (a.Table.ToLower() == n.Table.ToLower() && a.KeyName == n.KeyName) || (
                            a.Table.ToLower() == n.Table.ToLower() &&
                            a.Column.ToLower() == n.Column.ToLower()
                        );
            }
            throw new Exception("Something supposedly impossible happened within StructureChecker internal logic.");
        }

        public IEnumerable<IStructureCheckNecessaryAction> EvaluateLegacyKeys(List<ScStructuralLink> keys) {

            var needFk = GetNecessaryLinks().ToList();
            foreach (var a in keys) {
                if (!needFk.Any(n=> CheckMatch(a,n) )) {
                    switch(a.Type) {
                        case ScStructuralKeyType.ForeignKey:
                            yield return new DropFkScAction(DataAccessor, a, $"Foreign Key {a.KeyName} is not in the Model");
                            break;
                        case ScStructuralKeyType.Index:
                            if (a.IsUnique)
                                yield return new DropUkScAction(DataAccessor, a.Table, a.KeyName, $"Unike Index {a.KeyName} is not in the Model");
                            else
                                yield return new DropIdxScAction(DataAccessor, a.Table, a.KeyName, $"Index {a.KeyName} is not in the Model"); 
                            break;
                        case ScStructuralKeyType.PrimaryKey:
                            yield return new DropPkScAction(DataAccessor, a.Table, a.KeyName, $"Key {a.KeyName} is not in the Model");
                            break;
                    }
                }
            }

            yield break;
        }

        private IEnumerable<IStructureCheckNecessaryAction> EvaluateForFullTableDekeyal(String target, List<String> tables, List<ScStructuralLink> keys) {
            for (int i = 0; i < keys.Count; i++) {
                var refTableName = keys[i].RefTable;
                if (refTableName == target) {
                    var tableName = keys[i].Table;
                    var columnName = keys[i].Column;
                    var refColumnName = keys[i].RefColumn;
                    var constraint = keys[i].KeyName;
                    Benchmarker.Mark($"Dekey Table {tableName} from {constraint} references {refTableName}");
                    yield return new DropFkScAction(DataAccessor, keys[i], $"Key {tableName}::{keys[i].KeyName} needs to be removed to alter table {target}");
                }
            }
        }

        private IEnumerable<IStructureCheckNecessaryAction> EvaluateForColumnDekeyal(string tableName, string columnName, List<ScStructuralLink> keys) {
            for (int i = 0; i < keys.Count; i++) {
                var refTableName = keys[i].RefTable;
                if (refTableName == tableName.ToLower() && columnName.ToLower() == keys[i].RefColumn.ToLower()) {
                    yield return new DropFkScAction(DataAccessor, keys[i], $"Key {keys[i].KeyName} needs to be removed to alter column {tableName}::{columnName}");
                }
            }
        }

        private IEnumerable<IStructureCheckNecessaryAction> EvaluateTableChanges(List<String> tables, List<ScStructuralLink> keys) {
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
                        if (old.Key.ToLower() == type.Name.ToLower()) {
                            foreach (var a in EvaluateForFullTableDekeyal(old.Value, tables, keys)) {
                                yield return a;
                            }
                            yield return new RenameTableScAction(DataAccessor, old.Value, old.Key, $"Table OldName attribute and structure state matches for {old.Value} -> {old.Key}");
                            renamed = true;
                            break;
                        }
                    }
                    if (!renamed) {
                        TablesToCreate.Add(type.Name.ToLower());
                        keys.Add(new ScStructuralLink {
                            Column = Fi.Tech.GetIdColumn(type),
                            Type = ScStructuralKeyType.PrimaryKey,
                            KeyName = "PRIMARY",
                            IsUnique = true,
                            Table = type.Name
                        });
                        yield return new CreateTableScAction(DataAccessor, type, $"Table {type.Name} does not exit in the structure state.");
                    }
                }
            }

            yield break;
        }

        private List<string> TablesToCreate = new List<string>();

        private IEnumerable<IStructureCheckNecessaryAction> EvaluateColumnChanges(List<FieldAttribute> columns, List<ScStructuralLink> keys) {

            foreach (var type in workingTypes) {
                var fields = ReflectionTool.FieldsAndPropertiesOf(type)
                    .Where(f => f.GetCustomAttribute<FieldAttribute>() != null);
                foreach (var field in fields) {

                    var fieldExists = false;
                    foreach (var col in columns) {
                        if (type.Name.ToLower() != col.Table.ToLower())
                            continue;
                        var colName = col.Name;
                        if (field.Name.ToLower() == colName.ToLower()) {
                            fieldExists = true;
                            continue;
                        }
                    }

                    if (!fieldExists) {

                        var oldExists = false;
                        foreach (var col in columns) {
                            if (col.Table.ToLower() != type.Name.ToLower()) continue;
                            var colName = col.Name;
                            var ona = field.GetCustomAttribute<OldNameAttribute>();
                            if (ona != null) {
                                if (ona.Name.ToLower() == colName.ToLower()) {
                                    oldExists = true;
                                    foreach (var a in EvaluateForColumnDekeyal(type.Name, ona.Name, keys)) {
                                        yield return a;
                                    }
                                    yield return new RenameColumnScAction(DataAccessor, type.Name, ona.Name, field, $"OldName attribute matches with structure state {type.Name}::{ona.Name} -> {field}");
                                }
                            }
                        }

                        if (!oldExists && !TablesToCreate.Contains(type.Name.ToLower())) {
                            yield return new CreateColumnScAction(DataAccessor, type.Name, field.Name, field, $"Column {field} does not exist in structure state");
                        }

                    } else {

                        foreach (var col in columns) {
                            var tableName = col.Table.ToLower();
                            if (tableName != type.Name.ToLower()) continue;
                            var columnName = col.Name.ToLower();
                            if (columnName != field.Name.ToLower()) continue;
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
                            var dbDefinition = DataAccessor.QueryGenerator.GetDatabaseType(field, fieldAtt);
                            //var dbDefinition = dbdef.Substring(0, dbdef.IndexOf('(') > -1 ? dbdef.IndexOf('(') : dbdef.Length);
                            if (
                                columnIsNullable != fieldAtt.AllowNull ||
                                !sizesMatch ||
                                datatype.ToUpper() != dbDefinition.ToUpper()
                                ) {
                                foreach(var a in EvaluateForColumnDekeyal(type.Name, field.Name, keys)) {
                                    yield return a;
                                }
                                if(columnIsNullable && !fieldAtt.AllowNull && fieldAtt.DefaultValue != null) {
                                    yield return new UpdateExNullableColumnScAction(DataAccessor, type.Name, field.Name, fieldAtt.DefaultValue, $"Need to update formerly nullable column {type.Name}.{field.Name} with the new default value");
                                }
                                yield return new AlterColumnDefinitionScAction(DataAccessor, type.Name, field.Name, field, $"Definition mismatch: {datatype.ToUpper()}->{dbDefinition.ToUpper()}; {length}->{fieldAtt.Size}; {columnIsNullable}->{fieldAtt.AllowNull};  ");
                            } else {

                            }
                        }

                    }

                }
            }


            yield break;
        }

        private IEnumerable<IStructureCheckNecessaryAction> EvaluateRemovedColumns(List<FieldAttribute> columns, List<ScStructuralLink> keys) {

            foreach(var c in columns) {
                var type = workingTypes.FirstOrDefault(t => t.Name.ToLower() == c.Table.ToLower());
                if(type == null) {
                    continue;
                }
                var fields = ReflectionTool.FieldsAndPropertiesOf(type)
                    .Where(f => f.GetCustomAttribute<FieldAttribute>() != null);
                if(!
                    fields.Any(f=> 
                        f.Name.ToLower() == c.Name.ToLower() ||
                        f.GetCustomAttribute<OldNameAttribute>()?.Name == c.Name
                    )
                ) {
                    foreach(var action in EvaluateForColumnDekeyal(c.Table, c.Name, keys)) {
                        yield return action;
                    }
                    yield return new DropColumnScAction(DataAccessor, c.Table, c, type, "Column does not exist in structure");
                }
            }

            yield break;
        }

        public async Task<int> ExecuteNecessaryActions(IEnumerable<IStructureCheckNecessaryAction> actions, Action<IStructureCheckNecessaryAction, int> onActionExecuted, Action<Exception> handleException = null) {

            var enny = actions.GetEnumerator();
            int retv = 0;
            int went = 0;
            WorkQueuer wq = new WorkQueuer("Strucheck Queuer");
            var cliQ = new WorkQueuer("cli_q", 1);
            List<Exception> exces = new List<Exception>();
            while (enny.MoveNext()) {
                var thisAction = enny.Current;
                var t = wq.Enqueue(async () => {
                    await Task.Yield();
                    retv += thisAction.Execute(DataAccessor);
                    //lock(wq) {
                    int myWent = went++;
                    onActionExecuted?.Invoke(thisAction, myWent);
                    //}
                }, async (x) => {
                    await Task.Yield();
                    if (handleException == null)
                        lock (exces)
                            exces.Add(x);
                    else
                        handleException.Invoke(x);
                });
            }
            await wq.Stop(true);
            await cliQ.Stop();
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
            foreach (var a in EvaluateRemovedColumns(columns, keys))
                yield return a;
        }

        private ScStructuralLink GetIndexForFk(ScStructuralLink fk) {
            return new ScStructuralLink {
                Table = fk.Table,
                Column = fk.Column,
                KeyName = $"idx_{fk.Table.ToLower()}_{fk.Column.ToLower()}",
                Type = ScStructuralKeyType.Index
            };
        }

        private IEnumerable<ScStructuralLink> GetNecessaryLinks() {
            foreach (var t in workingTypes) {
                foreach (var f in ReflectionTool.FieldsAndPropertiesOf(t)) {
                    var uk = f.GetCustomAttribute<FieldAttribute>();
                    if (uk == null) {
                        continue;
                    }

                    var constraint = new ScStructuralLink();

                    if (uk != null && uk.Unique) {
                        yield return new ScStructuralLink {
                            Table = t.Name,
                            Column = f.Name,
                            IsUnique = true,
                            Type = ScStructuralKeyType.Index,
                        };
                    }

                    var pri = f.GetCustomAttribute<PrimaryKeyAttribute>();
                    if (pri != null) {
                        yield return new ScStructuralLink {
                            Table = t.Name,
                            Column = f.Name,
                            Type = ScStructuralKeyType.PrimaryKey,
                        };
                    }


                    var fk = f.GetCustomAttribute<ForeignKeyAttribute>();
                    if (fk != null) {
                        constraint = ScStructuralLink.FromFkAttribute(fk);
                        constraint.Table = t.Name;
                        constraint.Column = f.Name;
                        yield return GetIndexForFk(constraint);
                        yield return constraint;
                        continue;
                    }

                    constraint = new ScStructuralLink();
                    constraint.Table = t.Name;
                    constraint.Column = f.Name;

                    var agf = f.GetCustomAttribute<AggregateFieldAttribute>();
                    if (agf != null) {
                        var classField = ReflectionTool.FieldsWithAttribute<FieldAttribute>(agf.RemoteObjectType)
                            .FirstOrDefault(a => a.Name == agf.ObjectKey);
                        if (classField != null) {
                            constraint.Table = agf.RemoteObjectType.Name;
                            constraint.Column = agf.ObjectKey;
                            constraint.RefTable = t.Name;
                            constraint.RefColumn = FiTechBDadosExtensions.RidColumnOf[t];
                            constraint.Type = ScStructuralKeyType.ForeignKey;
                        } else {
                            constraint.Table = t.Name;
                            constraint.Column = agf.ObjectKey;
                            constraint.RefTable = agf.RemoteObjectType.Name;
                            constraint.RefColumn = FiTechBDadosExtensions.RidColumnOf[agf.RemoteObjectType];
                            constraint.Type = ScStructuralKeyType.ForeignKey;
                        }
                        yield return GetIndexForFk(constraint);
                        yield return constraint;
                        continue;
                    }

                    var agff = f.GetCustomAttribute<AggregateFarFieldAttribute>();
                    if (agff != null) {
                        var cf = ReflectionTool.FieldsWithAttribute<FieldAttribute>(agff.ImediateType)
                            .FirstOrDefault(a => a.Name == agff.ImediateKey);
                        if (cf != null) {
                            constraint.Table = agff.ImediateType.Name;
                            constraint.Column = agff.ImediateKey;
                            constraint.RefTable = t.Name;
                            constraint.RefColumn = FiTechBDadosExtensions.RidColumnOf[t];
                            constraint.Type = ScStructuralKeyType.ForeignKey;
                        } else {
                            constraint.Table = t.Name;
                            constraint.Column = agff.ImediateKey;
                            constraint.RefTable = agff.ImediateType.Name;
                            constraint.RefColumn = FiTechBDadosExtensions.RidColumnOf[agff.ImediateType];
                            constraint.Type = ScStructuralKeyType.ForeignKey;
                        }
                        yield return GetIndexForFk(constraint);
                        yield return constraint;
                        constraint = new ScStructuralLink();

                        var cf2 = ReflectionTool.FieldsWithAttribute<FieldAttribute>(agff.FarType)
                            .FirstOrDefault(a => a.Name == agff.FarKey);
                        if (cf2 != null) {
                            constraint.Table = agff.FarType.Name;
                            constraint.Column = agff.FarKey;
                            constraint.RefTable = t.Name;
                            constraint.RefColumn = FiTechBDadosExtensions.RidColumnOf[t];
                            constraint.Type = ScStructuralKeyType.ForeignKey;
                        } else {
                            constraint.Table = t.Name;
                            constraint.Column = agff.FarKey;
                            constraint.RefTable = agff.FarType.Name;
                            constraint.RefColumn = FiTechBDadosExtensions.RidColumnOf[agff.FarType];
                            constraint.Type = ScStructuralKeyType.ForeignKey;
                        }
                        yield return GetIndexForFk(constraint);
                        yield return constraint;
                        continue;
                    }

                    var ago = f.GetCustomAttribute<AggregateObjectAttribute>();
                    if (ago != null) {
                        var type = ReflectionTool.GetTypeOf(f);
                        var classField = ReflectionTool.FieldsWithAttribute<FieldAttribute>(type)
                            .FirstOrDefault(a => a.Name == ago.ObjectKey);
                        if (classField != null) {
                            constraint.Table = type.Name;
                            constraint.Column = ago.ObjectKey;
                            constraint.RefTable = t.Name;
                            constraint.RefColumn = FiTechBDadosExtensions.RidColumnOf[t];
                            constraint.Type = ScStructuralKeyType.ForeignKey;
                        } else {
                            constraint.Table = t.Name;
                            constraint.Column = ago.ObjectKey;
                            constraint.RefTable = type.Name;
                            constraint.RefColumn = FiTechBDadosExtensions.RidColumnOf[type];
                            constraint.Type = ScStructuralKeyType.ForeignKey;
                        }
                        yield return GetIndexForFk(constraint);
                        yield return constraint;
                        continue;
                    }
                    var agl = f.GetCustomAttribute<AggregateListAttribute>();
                    if (agl != null) {
                        var classField = ReflectionTool.FieldsWithAttribute<FieldAttribute>(agl.RemoteObjectType)
                            .FirstOrDefault(a => a.Name == agl.RemoteField);
                        if (classField != null) {
                            constraint.Table = agl.RemoteObjectType.Name;
                            constraint.Column = agl.RemoteField;
                            constraint.RefTable = t.Name;
                            constraint.RefColumn = FiTechBDadosExtensions.RidColumnOf[t];
                            constraint.Type = ScStructuralKeyType.ForeignKey;
                        } else {
                            constraint.Table = t.Name;
                            constraint.Column = agl.RemoteField;
                            constraint.RefTable = agl.RemoteObjectType.Name;
                            constraint.RefColumn = FiTechBDadosExtensions.RidColumnOf[agl.RemoteObjectType];
                            constraint.Type = ScStructuralKeyType.ForeignKey;
                        }
                        yield return GetIndexForFk(constraint);
                        yield return constraint;
                        continue;
                    }
                }
            }
        }

        public IEnumerable<DropFkScAction> DekeyItAll() {
            var keys = GetInfoSchemaKeys();
            foreach (var a in keys) {
                yield return new DropFkScAction(DataAccessor, a, "Action generated from DEKEYITALL function");
            }
        }

        public IEnumerable<ExecutePugeForFkScAction> EnumeratePurgesFor(ScStructuralLink fk, List<ScStructuralLink> fkli) {
            if (_purgedKeys.Contains(fk.ToString())) {
                yield break;
            }
            if (String.IsNullOrEmpty(fk.RefTable)) {
                yield break;
            }

            _purgedKeys.Add(fk.ToString());
            foreach (var a in fkli.Where(b => b.RefTable?.ToLower() == fk.Table.ToLower())) {
                foreach (var x in EnumeratePurgesFor(a, fkli)) {
                    yield return x;
                }
            }
            var mem = ReflectionTool.FieldsWithAttribute<FieldAttribute>(workingTypes.FirstOrDefault(t => t.Name == fk.Table)).FirstOrDefault();
            if (mem != null)
                yield return new ExecutePugeForFkScAction(DataAccessor, fk.Table, fk.Column, fk.RefTable, fk.RefColumn, mem, $"Needs to purge inconsistent data from {fk.Table} to apply constraint {fk.KeyName}.");
        }



        private List<string> _purgedKeys = new List<string>();
        public IEnumerable<IStructureCheckNecessaryAction> EvaluateMissingKeysCreation(List<FieldAttribute> columns, List<ScStructuralLink> keys) {

            _purgedKeys.Clear();
            var needFK = GetNecessaryLinks().ToList();

            needFK.RemoveAll(a => keys.Any(b => CheckMatch(a, b)));

            foreach (var fk in needFK) {
                if (!keys.Any(n => CheckMatch(fk, n))) {
                    switch(fk.Type) {
                        case ScStructuralKeyType.ForeignKey:
                            foreach (var x in EnumeratePurgesFor(fk, needFK)) {
                                yield return x;
                            }
                            var foreignIndex = new ScStructuralLink {
                                Type = ScStructuralKeyType.Index,
                                Table = fk.RefTable,
                                Column = fk.RefColumn,
                            };
                            if(!keys.Any(n=> CheckMatch(foreignIndex, n))) {
                                yield return new CreateIndexScAction(DataAccessor, foreignIndex, $"Need to create an index for {fk.RefTable}.{fk.RefColumn} for {fk.KeyName}");
                            }
                            yield return new CreateForeignKeyScAction(DataAccessor, fk, $"Key absent in structure state {fk.KeyName}");
                            break;
                        case ScStructuralKeyType.Index:
                            yield return new CreateIndexScAction(DataAccessor, fk, $"Key absent in structure state {fk.KeyName}");
                            break; 
                        case ScStructuralKeyType.PrimaryKey:
                            yield return new CreatePrimaryKeyScAction(DataAccessor, fk, $"Key absent in structure state {fk.KeyName}");
                            break;
                    }
                }
            }
        }

        private List<ScStructuralLink> GetInfoSchemaKeys() {
            var dbName = DataAccessor.SchemaName;
            var retv = new List<ScStructuralLink>();
            var fk = DataAccessor.Query<ScStructuralLink>(
                DataAccessor
                    .QueryGenerator
                    .InformationSchemaQueryKeys(dbName)
            );
            fk.RemoveAll(f => String.IsNullOrEmpty(f.RefColumn));
            fk.ForEach(a => a.Type = ScStructuralKeyType.ForeignKey);
            retv.AddRange(fk);
            var idx = DataAccessor.Query<ScStructuralLink>(
                 DataAccessor
                     .QueryGenerator
                     .InformationSchemaIndexes(dbName)
             );
            idx.ForEach(a => {
                a.Type = ScStructuralKeyType.Index;
                a.IsUnique = !a.IsUnique;
                if (a.KeyName == "PRIMARY") {
                    a.Type = ScStructuralKeyType.PrimaryKey;
                }
            });
            retv.AddRange(idx);
            var wtNames = workingTypes.Select(wt => wt.Name.ToLower());
            retv.RemoveAll(r => !wtNames.Contains(r.Table.ToLower()));
            return retv;
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
            return DataAccessor.Query<FieldAttribute>(
                    DataAccessor.QueryGenerator.InformationSchemaQueryColumns(dbName)
            );
        }

        public async Task CheckStructureAsync(
            Func<IStructureCheckNecessaryAction, Task> onActionProcessed = null,
            Func<IStructureCheckNecessaryAction, Exception, Task> onError = null,
            Func<IStructureCheckNecessaryAction, Task<bool>> preAuthorizeAction = null,
            Func<int, Task> onReportTotalTasks = null
        ) {
            var neededActions = EvaluateNecessaryActions().ToList();
            var ortt = onReportTotalTasks?.Invoke(neededActions.Count);
            try {
                DataAccessor.EnsureDatabaseExists();
            } catch(Exception x) {
                // DRAGONS

            }
            DataAccessor.Execute(DataAccessor.QueryGenerator.DisableForeignKeys());
            //DataAccessor.BeginTransaction();
            try {

                var enumerator = neededActions.GetEnumerator();
                while (enumerator.MoveNext()) {
                    var action = enumerator.Current;
                    if (preAuthorizeAction != null)
                        if (!await preAuthorizeAction(action))
                            continue;

                    try {
                        action.Execute(DataAccessor);
                        var t = onActionProcessed?.Invoke(action);
                        if(t != null && t is Task tk) {
                            await tk;
                        }
                    } catch (Exception x) {
                        if (onError != null)
                            await onError?.Invoke(action, x);
                    }
                }

            } catch (Exception) {

            } finally {
                DataAccessor.Execute(DataAccessor.QueryGenerator.EnableForeignKeys());
                //DataAccessor.EndTransaction();
            }
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
