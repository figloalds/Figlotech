
using Figlotech.BDados.Builders;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using Figlotech.Core;
using Figlotech.Core.Helpers;
using Figlotech.Core.Interfaces;
using Figlotech.Data;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace Figlotech.BDados.DataAccessAbstractions {
    public enum ScStructuralKeyType {
        Index,
        PrimaryKey,
        ForeignKey
    }

    public sealed class ScStructuralLink {
        private String _linkName;

        public string TABLE_NAME { get => Table; set => Table = value; }
        public string COLUMN_NAME { get => Column; set => Column = value; } 
        public string NON_UNIQUE { get => IsUnique ? "NO": "YES"; set => IsUnique = value?.ToLower() == "no"; }
        public string INDEX_NAME { get => KeyName; set => KeyName = value; }

        public string REFERENCED_COLUMN_NAME { get => RefColumn; set => RefColumn = value; }
        public string REFERENCED_TABLE_NAME { get => RefTable; set => RefTable = value; }
        public string CONSTRAINT_NAME { get => KeyName; set => KeyName = value; }
        public string CONSTRAINT_TYPE { get; set; }

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
                        return $"pk_{Table.ToLower()}";
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

    public sealed class DropFkScAction : AbstractIStructureCheckNecessaryAction {
        ScStructuralLink keyInfo;

        public DropFkScAction(
            IRdbmsDataAccessor dataAccessor,
            ScStructuralLink keyInfo, string reason) : base(dataAccessor, reason) {
            this.keyInfo = keyInfo;
        }

        public override int Execute(BDadosTransaction tsn, IRdbmsDataAccessor DataAccessor) {
            return Exec(tsn, DataAccessor, DataAccessor.QueryGenerator.DropForeignKey(
                    keyInfo.Table, keyInfo.KeyName));
        }

        public override string ToString() {
            return $"Drop foreign key {keyInfo.Table}.{keyInfo.KeyName}";
        }
    }

    public sealed class DropPkScAction : AbstractIStructureCheckNecessaryAction {
        String _table;
        String _constraint;

        public DropPkScAction(
            IRdbmsDataAccessor dataAccessor,
            string table, string constraint, string reason) : base(dataAccessor, reason) {
            _table = table;
            _constraint = constraint;
        }

        public override int Execute(BDadosTransaction tsn, IRdbmsDataAccessor DataAccessor) {
            return Exec(tsn, DataAccessor, DataAccessor.QueryGenerator.DropPrimary(
                    _table, _constraint));
        }

        public override string ToString() {
            return $"Drop primary {_table}.{_constraint}";
        }
    }
    public sealed class DropIdxScAction : AbstractIStructureCheckNecessaryAction {
        String _table;
        String _constraint;

        public DropIdxScAction(
            IRdbmsDataAccessor dataAccessor,
            string table, string constraint, string reason) : base(dataAccessor, reason) {
            _table = table;
            _constraint = constraint;
        }

        public override int Execute(BDadosTransaction tsn, IRdbmsDataAccessor DataAccessor) {
            return Exec(tsn, DataAccessor, DataAccessor.QueryGenerator.DropIndex(
                    _table, _constraint));
        }

        public override string ToString() {
            return $"Drop index {_table}.{_constraint}";
        }

    }
    public sealed class DropUkScAction : AbstractIStructureCheckNecessaryAction {
        String _table;
        String _constraint;

        public DropUkScAction(
            IRdbmsDataAccessor dataAccessor,
            string table, string constraint, string reason) : base(dataAccessor, reason) {
            _table = table;
            _constraint = constraint;
        }

        public override int Execute(BDadosTransaction tsn, IRdbmsDataAccessor DataAccessor) {
            return Exec(tsn, DataAccessor, DataAccessor.QueryGenerator.DropUnique(
                    _table, _constraint));
        }

        public override string ToString() {
            return $"Drop unique {_table}.{_constraint}";
        }

    }

    public sealed class CreateIndexScAction : AbstractIStructureCheckNecessaryAction {
        ScStructuralLink keyInfo;

        bool _unique;


        public CreateIndexScAction(
            IRdbmsDataAccessor dataAccessor,
            ScStructuralLink keyInfo, string reason) : base(dataAccessor, reason) {
            this.keyInfo = keyInfo;
        }

        public override int Execute(BDadosTransaction tsn, IRdbmsDataAccessor DataAccessor) {

            int v = 0;

            if(keyInfo.IsUnique) {
                return Exec(tsn, DataAccessor, DataAccessor.QueryGenerator.AddUniqueKey(
                    keyInfo.Table, keyInfo.Column, keyInfo.KeyName));
            } else {
                return Exec(tsn, DataAccessor, DataAccessor.QueryGenerator.AddIndex(
                    keyInfo.Table, keyInfo.Column, keyInfo.KeyName));
            }
        }

        public override string ToString() {
            return $"Create {(keyInfo.IsUnique ? "UNIQUE KEY" : "INDEX" )} {keyInfo.Table}.{keyInfo.KeyName}";
        }
    }

    public sealed class CreatePrimaryKeyScAction : AbstractIStructureCheckNecessaryAction {
        ScStructuralLink keyInfo;

        int _length;


        public CreatePrimaryKeyScAction(
            IRdbmsDataAccessor dataAccessor, ScStructuralLink keyInfo, string reason) : base(dataAccessor, reason) {
            this.keyInfo = keyInfo;
        }

        public override int Execute(BDadosTransaction tsn, IRdbmsDataAccessor DataAccessor) {
            return Exec(tsn, DataAccessor, DataAccessor.QueryGenerator.AddPrimaryKey(
                    keyInfo.Table, keyInfo.Column, keyInfo.KeyName));
        }

        public override string ToString() {
            return $"Create primary key {keyInfo.Table}.{keyInfo.KeyName}";
        }
    }

    public sealed class CreateForeignKeyScAction : AbstractIStructureCheckNecessaryAction {
        ScStructuralLink keyInfo;

        public CreateForeignKeyScAction(
            IRdbmsDataAccessor dataAccessor,
             ScStructuralLink keyInfo, string reason) : base(dataAccessor, reason) {
            this.keyInfo = keyInfo;
        }

        public override int Execute(BDadosTransaction tsn, IRdbmsDataAccessor DataAccessor) {
            int v = 0;
            //try {
            //    v += Exec(tsn, DataAccessor.QueryGenerator.AddIndex(
            //        keyInfo.Table, keyInfo.Column, keyInfo.KeyName));
            //} catch (Exception x) { }
            v += Exec(tsn, DataAccessor, DataAccessor.QueryGenerator.AddForeignKey(
                    keyInfo.Table, keyInfo.Column, keyInfo.RefTable, keyInfo.RefColumn, keyInfo.KeyName));
            return v;
        }

        public override string ToString() {
            return $"Create foreign key {keyInfo.Table}.{keyInfo.KeyName}";
        }
    }

    public sealed class ExecutePugeForFkScAction : AbstractIStructureCheckNecessaryAction {
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

        public override int Execute(BDadosTransaction tsn, IRdbmsDataAccessor DataAccessor) {
            return Exec(tsn, DataAccessor, DataAccessor.QueryGenerator.Purge(
                _table, _column, _refTable, _refColumn, _isNullable));

            return 0;

        }

        public override string ToString() {
            return $"Data Purge for key {_table}.fk_{_table}_{_column} References ({_refTable}.{_refColumn})";
        }
    }

    public sealed class CreateColumnScAction : AbstractIStructureCheckNecessaryAction {
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

        public override int Execute(BDadosTransaction tsn, IRdbmsDataAccessor DataAccessor) {
            return Exec(tsn, DataAccessor, DataAccessor.QueryGenerator.AddColumn(
                _table, DataAccessor.QueryGenerator.GetColumnDefinition(_columnMember)));
        }

        public override string ToString() {
            return $"Create column {_table}.{_columnMember.Name}";
        }
    }

    public sealed class RenameTableScAction : AbstractIStructureCheckNecessaryAction {
        String _table;
        String _newName;

        public RenameTableScAction(
            IRdbmsDataAccessor dataAccessor,
            string table, string newName, string reason) : base(dataAccessor, reason) {
            _table = table;
            _newName = newName;
        }

        public override int Execute(BDadosTransaction tsn, IRdbmsDataAccessor DataAccessor) {
            return Exec(tsn, DataAccessor, DataAccessor.QueryGenerator.RenameTable(
                _table, _newName));
        }

        public override string ToString() {
            return $"Rename {_table} to {_newName}";
        }
    }

    public sealed class RenameColumnScAction : AbstractIStructureCheckNecessaryAction {
        String _table;
        String _column;
        MemberInfo _columnMember;
        FieldAttribute _info;

        public RenameColumnScAction(
            IRdbmsDataAccessor dataAccessor,
            string table, string column, MemberInfo columnMember, FieldAttribute info, string reason) : base(dataAccessor, reason) {
            _table = table;
            _column = column;
            _columnMember = columnMember;
            _info = info;
        }

        public override int Execute(BDadosTransaction tsn, IRdbmsDataAccessor DataAccessor) {
            return Exec(tsn, DataAccessor, DataAccessor.QueryGenerator.RenameColumn(
                _table, _column, _columnMember, _info));
        }

        public override string ToString() {
            return $"Rename {_table}.{_column} to {_columnMember.Name}";
        }
    }

    public sealed class UpdateExNullableColumnScAction : AbstractIStructureCheckNecessaryAction {
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

        public override int Execute(BDadosTransaction tsn, IRdbmsDataAccessor DataAccessor) {
            return Exec(tsn, DataAccessor, DataAccessor.QueryGenerator.UpdateColumn(_table, _column, _defaultValue, Qb.Fmt($"{_column} IS NULL")));
        }

        public override string ToString() {
            return $"Fill default value onto newly non-nullable column {_table}.{_column}";
        }
    }

    public sealed class DropColumnScAction : AbstractIStructureCheckNecessaryAction {
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

        public override int Execute(BDadosTransaction tsn, IRdbmsDataAccessor DataAccessor) {
            _column.AllowNull = true;
            return Exec(tsn, DataAccessor,
                Qb.Fmt($@"
                    ALTER TABLE {_table} DROP COLUMN {_column.Name}
                ")
            );
            //Exec(tsn, DataAccessor, Qb.Fmt(
            //    $@"
            //        SET @a=1;
            //        INSERT INTO _ResidualScData (RID, Type, Field, Value, ReferenceRID, CreatedBy, AlteredBy)
            //        SELECT CONCAT({FiTechBDadosExtensions.RidColumnOf[_type]}, '-', @a:=@a+1), '{_table}', '{_column}', CAST({_column} as BINARY), {FiTechBDadosExtensions.RidColumnOf[_type]}, '{RID.MachineRID.AsULong}','{RID.MachineRID.AsULong}' FROM {_table} WHERE {_column} IS NOT NULL;
            //    "
            //));
            //return Exec(tsn, DataAccessor, DataAccessor.QueryGenerator.DropColumn(
            //    _table, _column.Name));
        }

        public override string ToString() {
            return $"Drop column {_table}.{_column.Name}";
        }
    }

    public sealed class AlterColumnDataTypeScAction : AbstractIStructureCheckNecessaryAction
    {
        String _table;
        MemberInfo _member;
        FieldAttribute _fieldAttribute;

        public AlterColumnDataTypeScAction(
            IRdbmsDataAccessor dataAccessor,
            string table, MemberInfo field, FieldAttribute fieldAttribute, string reason) : base(dataAccessor, reason) {
            _table = table;
            _member = field;
            _fieldAttribute = fieldAttribute;
        }

        public override int Execute(BDadosTransaction tsn, IRdbmsDataAccessor DataAccessor) {
            return Exec(tsn, DataAccessor, DataAccessor.QueryGenerator.AlterColumnDataType(_table, _member, _fieldAttribute));
        }

        public override string ToString() {
            return $"Change column type of {_table}.{_member.Name}";
        }
    }

    public sealed class AlterColumnNullabilityScAction : AbstractIStructureCheckNecessaryAction
    {
        String _table;
        MemberInfo _member;
        FieldAttribute _fieldAttribute;

        public AlterColumnNullabilityScAction(
            IRdbmsDataAccessor dataAccessor,
            string table, MemberInfo field, FieldAttribute fieldAttribute, string reason) : base(dataAccessor, reason) {
            _table = table;
            _member = field;
            _fieldAttribute = fieldAttribute;
        }

        public override int Execute(BDadosTransaction tsn, IRdbmsDataAccessor DataAccessor) {
            return Exec(tsn, DataAccessor, DataAccessor.QueryGenerator.AlterColumnNullability(_table, _member, _fieldAttribute));
        }

        public override string ToString() {
            return $"Change column nulability of {_table}.{_member.Name} to {(_fieldAttribute.AllowNull ? "ALLOW NULL" : "NOT NULL")}";
        }
    }

    public sealed class CreateTableScAction : AbstractIStructureCheckNecessaryAction {
        private Type _tableType;
        public CreateTableScAction(IRdbmsDataAccessor dataAccessor, Type tableType, string reason) : base(dataAccessor, reason) {
            _tableType = tableType;
        }

        public override int Execute(BDadosTransaction tsn, IRdbmsDataAccessor DataAccessor) {
            return Exec(tsn, DataAccessor, DataAccessor.QueryGenerator.GetCreationCommand(_tableType));
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

        public abstract int Execute(BDadosTransaction tsn, IRdbmsDataAccessor DataAccessor);

        protected int Exec(BDadosTransaction tsn, IRdbmsDataAccessor DataAccessor, IQueryBuilder query) {
            try {
                return DataAccessor.Execute(tsn, query);
            } catch (Exception x) {
                throw new BDadosException($"Error executing [{this.ToString()}]: [{x.Message}]", x);
            }
        }
    }

    public interface IStructureCheckNecessaryAction {
        string Description { get;  }
        string Reason { get;  }
        int Execute(BDadosTransaction tsn, IRdbmsDataAccessor DataAccessor);
    }

    public sealed class StructureChecker {
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
            if(a.Type == ScStructuralKeyType.Index && a.Column == null) {
                a.Column = a.CONSTRAINT_NAME.Substring(a.CONSTRAINT_NAME.LastIndexOf("_") + 1);
            }
            try {
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
            } catch(Exception x) {
                Debugger.Break();
                throw new Exception($"Error comparing Key Definitions {a.ToString()} | {n.ToString()}", x);
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
                                yield return new DropUkScAction(DataAccessor, a.Table, a.KeyName, $"Unique Index {a.KeyName} is not in the Model");
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
                            yield return new RenameTableScAction(
                                DataAccessor, 
                                old.Value, 
                                old.Key, 
                                $"Table OldName attribute and structure state matches for {old.Value} -> {old.Key}"
                            );
                            renamed = true;
                            break;
                        }
                    }
                    if (!renamed) {
                        TablesToCreate.Add(type.Name.ToLower());
                        keys.Add(new ScStructuralLink {
                            Column = Fi.Tech.GetIdColumn(type),
                            Type = ScStructuralKeyType.PrimaryKey,
                            KeyName = $"pk_{type.Name.ToLower()}",
                            IsUnique = true,
                            Table = type.Name
                        });
                        yield return new CreateTableScAction(
                            DataAccessor, 
                            type, 
                            $"Table {type.Name} does not exit in the structure state."
                        );
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
                                var info = field.GetCustomAttribute<FieldAttribute>();
                                if (ona.Name.ToLower() == colName.ToLower()) {
                                    oldExists = true;
                                    foreach (var a in EvaluateForColumnDekeyal(type.Name, ona.Name, keys)) {
                                        yield return a;
                                    }
                                    yield return new RenameColumnScAction(
                                        DataAccessor, 
                                        type.Name, 
                                        ona.Name, 
                                        field,
                                        info,
                                        $"OldName attribute matches with structure state {type.Name}::{ona.Name} -> {field}"
                                    );
                                }
                            }
                        }

                        if (!oldExists && !TablesToCreate.Contains(type.Name.ToLower())) {
                            yield return new CreateColumnScAction(
                                DataAccessor, 
                                type.Name, 
                                field.Name, 
                                field, 
                                $"Column {field} does not exist in structure state"
                            );
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
                            var dbDefinition = DataAccessor.QueryGenerator.GetDatabaseType(field, fieldAtt);
                            var sizesMatch = !typesToCheckSize.Contains(dbDefinition.ToUpper()) || length == fieldAtt.Size;
                            //var dbDefinition = dbdef.Substring(0, dbdef.IndexOf('(') > -1 ? dbdef.IndexOf('(') : dbdef.Length);
                            if (columnIsNullable != fieldAtt.AllowNull) {
                                yield return new AlterColumnNullabilityScAction(DataAccessor, type.Name, field, fieldAtt, $"Column Nullability mismatch: {datatype.ToUpper()}->{dbDefinition.ToUpper()}; {length}->{fieldAtt.Size}; {columnIsNullable}->{fieldAtt.AllowNull};  ");
                            }
                            if (!sizesMatch || datatype.ToUpper() != dbDefinition.ToUpper()) {
                                foreach(var a in EvaluateForColumnDekeyal(type.Name, field.Name, keys)) {
                                    yield return a;
                                }
                                if(columnIsNullable && !fieldAtt.AllowNull && fieldAtt.DefaultValue != null) {
                                    yield return new UpdateExNullableColumnScAction(
                                        DataAccessor, 
                                        type.Name, 
                                        field.Name, 
                                        fieldAtt.DefaultValue, 
                                        $"Need to update formerly nullable column {type.Name}.{field.Name} with the new default value"
                                    );
                                }
                                yield return new AlterColumnDataTypeScAction(
                                    DataAccessor,
                                    type.Name, 
                                    field, 
                                    fieldAtt,
                                    $"Definition mismatch: {datatype.ToUpper()}->{dbDefinition.ToUpper()}; {length}->{fieldAtt.Size}; {columnIsNullable}->{fieldAtt.AllowNull};  "
                                );
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
                    if(!c.AllowNull || c.DefaultValue != null) {
                        yield return new DropColumnScAction(DataAccessor, c.Table, c, type, "Column does not exist in structure");
                    }
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
                wq.Enqueue(async () => {
                    await Task.Yield();
                    DataAccessor.Access(tsn => {
                        retv += thisAction.Execute(tsn, DataAccessor);
                    });
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
            var columns = DataAccessor.GetInfoSchemaColumns();
            Console.WriteLine("Evaluating Necessary Actions:");
            Console.WriteLine($"{tables.Count} tables, {columns.Count} columns, {keys.Count} keys");

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

                    if (uk != null && (uk.Unique || uk.Index)) {
                        yield return new ScStructuralLink {
                            Table = t.Name,
                            Column = f.Name,
                            IsUnique = uk.Unique,
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
                        if (fk.RefColumn == null) {
                            fk.RefColumn = FiTechBDadosExtensions.RidColumnOf[fk.RefType];
                        }
                        if(fk.RefColumn == null) {
                            throw new Exception($"Trying to create relation {fk.ToString()} but the target type {fk.RefType.Name} does not have a [ReliableId]");
                        }
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
                        var classField = ReflectionTool.GetAttributedMemberValues<FieldAttribute>(agf.RemoteObjectType)
                            .FirstOrDefault(a => a.Member.Name == agf.ObjectKey);
                        if (classField.Attribute != null) {
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
                        var cf = ReflectionTool.GetAttributedMemberValues<FieldAttribute>(agff.ImediateType)
                            .FirstOrDefault(a => a.Member.Name == agff.ImediateKey);
                        if (cf.Attribute != null) {
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

                        var cf2 = ReflectionTool.GetAttributedMemberValues<FieldAttribute>(agff.FarType)
                            .FirstOrDefault(a => a.Member.Name == agff.FarKey);
                        if (cf2.Attribute != null) {
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
                        var classField = ReflectionTool.GetAttributedMemberValues<FieldAttribute>(type)
                            .FirstOrDefault(a => a.Member.Name == ago.ObjectKey);
                        if (classField.Attribute != null) {
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
                        var classField = ReflectionTool.GetAttributedMemberValues<FieldAttribute>(agl.RemoteObjectType)
                            .FirstOrDefault(a => a.Member.Name == agl.RemoteField);
                        if (classField.Attribute != null) {
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
            var mem = ReflectionTool.GetAttributedMemberValues<FieldAttribute>(workingTypes.FirstOrDefault(t => t.Name == fk.Table)).FirstOrDefault();
            if (mem.Attribute != null)
                yield return new ExecutePugeForFkScAction(DataAccessor, fk.Table, fk.Column, fk.RefTable, fk.RefColumn, mem.Member, $"Needs to purge inconsistent data from {fk.Table} to apply constraint {fk.KeyName}.");
        }



        private List<string> _purgedKeys = new List<string>();
        public IEnumerable<IStructureCheckNecessaryAction> EvaluateMissingKeysCreation(List<FieldAttribute> columns, List<ScStructuralLink> keys) {

            _purgedKeys.Clear();
            var needFK = GetNecessaryLinks().ToList();

            needFK.RemoveAll(a => keys.Any(b => CheckMatch(a, b)));
            needFK.Sort((a, b) => {
                if (a.Type == ScStructuralKeyType.PrimaryKey && b.Type != ScStructuralKeyType.PrimaryKey) {
                    return -1;
                }
                if (a.Type != ScStructuralKeyType.PrimaryKey && b.Type == ScStructuralKeyType.PrimaryKey) {
                    return 1;
                }

                return 0;
            });

            foreach (var fk in needFK) {
                if (!keys.Any(n => CheckMatch(fk, n))) {
                    switch(fk.Type) {
                        case ScStructuralKeyType.ForeignKey:
                            foreach (var x in EnumeratePurgesFor(fk, needFK)) {
                                yield return x;
                            }
                            var foreignIndex = new ScStructuralLink {
                                Type = ScStructuralKeyType.Index,
                                Table = fk.Table,
                                Column = fk.Column,
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
            //fk.RemoveAll(f => String.IsNullOrEmpty(f.RefColumn));
            //fk.ForEach(a => a.Type = ScStructuralKeyType.ForeignKey);
            retv.AddRange(fk);
            var idx = DataAccessor.Query<ScStructuralLink>(
                 DataAccessor
                     .QueryGenerator
                     .InformationSchemaIndexes(dbName)
             );
            retv.AddRange(idx.Where(x=> !retv.Any(b=> b.CONSTRAINT_NAME == x.INDEX_NAME)));
            retv.ForEach(a => {
                switch (a.CONSTRAINT_TYPE) {
                    case "PRIMARY KEY":
                        a.Type = ScStructuralKeyType.PrimaryKey;
                        break;
                    case "FOREIGN KEY":
                        a.Type = ScStructuralKeyType.ForeignKey;
                        break;
                    case "UNIQUE":
                        a.Type = ScStructuralKeyType.Index;
                        a.IsUnique = true;
                        break;
                    default:
                        a.Type = ScStructuralKeyType.Index;
                        break;
                }
            });
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
                        DataAccessor.Access(tsn => {
                            action.Execute(tsn, DataAccessor);
                        });
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
