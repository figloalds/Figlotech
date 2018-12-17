﻿
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
                _table, StructureChecker.GetColumnDefinition(_columnMember)));
        }

        public override string ToString() {
            return $"Create column {_columnMember.Name} {StructureChecker.GetColumnDefinition(_columnMember)}";
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
            string table, string column, MemberInfo columnMember, string reason) : base(dataAccessor, reason) {
            _table = table;
            _column = column;
            _columnMember = columnMember;
        }

        public override int Execute(IRdbmsDataAccessor DataAccessor) {
            return Exec(DataAccessor, DataAccessor.QueryGenerator.RenameColumn(
                _table, _column, StructureChecker.GetColumnDefinition(_columnMember)));
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

        private bool CheckMatch(ScStructuralLink a, ScStructuralLink n) {
            if (a.Type != n.Type) return false;
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
                            var dbDefinition = GetDatabaseType(field, fieldAtt);
                            //var dbDefinition = dbdef.Substring(0, dbdef.IndexOf('(') > -1 ? dbdef.IndexOf('(') : dbdef.Length);
                            if (
                                columnIsNullable != fieldAtt.AllowNull ||
                                !sizesMatch ||
                                datatype.ToUpper() != dbDefinition.ToUpper()
                                ) {
                                foreach(var a in EvaluateForColumnDekeyal(type.Name, field.Name, keys)) {
                                    yield return a;
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

        public async Task<int> ExecuteNecessaryActions(IEnumerable<IStructureCheckNecessaryAction> actions, Action<IStructureCheckNecessaryAction, int> onActionExecuted, Action<Exception> handleException = null) {
            var enny = actions.GetEnumerator();
            int retv = 0;
            int went = 0;
            WorkQueuer wq = new WorkQueuer("Strucheck Queuer");
            var cliQ = new WorkQueuer("cli_q", 1);
            List<Exception> exces = new List<Exception>();
            while (enny.MoveNext()) {
                var thisAction = enny.Current;
                wq.Enqueue(() => {
                    retv += thisAction.Execute(DataAccessor);
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
            var fk = DataAccessor.Query(
                DataAccessor
                    .QueryGenerator
                    .InformationSchemaQueryKeys(dbName)
            )
            .Map<ScStructuralLink>(new Dictionary<string, string> {
                { "TABLE_NAME", nameof(ScStructuralLink.Table) },
                { "COLUMN_NAME", nameof(ScStructuralLink.Column) },
                { "REFERENCED_COLUMN_NAME", nameof(ScStructuralLink.RefColumn) },
                { "REFERENCED_TABLE_NAME", nameof(ScStructuralLink.RefTable) },
                { "CONSTRAINT_NAME", nameof(ScStructuralLink.KeyName) },
            }).ToList();
            fk.RemoveAll(f => String.IsNullOrEmpty(f.RefColumn));
            fk.ForEach(a => a.Type = ScStructuralKeyType.ForeignKey);
            retv.AddRange(fk);
            var idx = DataAccessor.Query(
                 DataAccessor
                     .QueryGenerator
                     .InformationSchemaIndexes(dbName)
             )
             .Map<ScStructuralLink>(new Dictionary<string, string> {
                { "TABLE_NAME", nameof(ScStructuralLink.Table) },
                { "COLUMN_NAME", nameof(ScStructuralLink.Column) },
                { "NON_UNIQUE", nameof(ScStructuralLink.IsUnique) },
                { "INDEX_NAME", nameof(ScStructuralLink.KeyName) },
             }).ToList();
            idx.ForEach(a => {
                a.Type = ScStructuralKeyType.Index;
                a.IsUnique = !a.IsUnique;
                if(a.KeyName == "PRIMARY") {
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
            return DataAccessor.Query(
                    DataAccessor.QueryGenerator.InformationSchemaQueryColumns(dbName)
            )
            .Map<FieldAttribute>(new Dictionary<string, string> {
                { "TABLE_NAME", nameof(FieldAttribute.Table) },
                { "COLUMN_NAME", nameof(FieldAttribute.Name) },
                { "COLUMN_DEFAULT", nameof(FieldAttribute.DefaultValue) },
                { "IS_NULLABLE", nameof(FieldAttribute.AllowNull) },
                { "COLUMN_TYPE", nameof(FieldAttribute.Type) },
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

            DataAccessor.Execute("SET FOREIGN_KEY_CHECKS=0;");
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
                        await onActionProcessed?.Invoke(action);
                    } catch (Exception x) {
                        if (onError != null)
                            await onError?.Invoke(action, x);
                    }
                }

            } catch (Exception) {

            } finally {
                DataAccessor.Execute("SET FOREIGN_KEY_CHECKS=1;");
                //DataAccessor.EndTransaction();
            }
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
                return "INT(11)";
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
                        if (info.Size != 0 && info.Size <= 255)
                            type = $"BINARY({info.Size})";
                        else
                            type = "BLOB";
                        break;
                    case "string":
                        type = $"VARCHAR({info.Size})";
                        break;
                    case "int":
                    case "int32":
                        type = $"INT(11)";
                        break;
                    case "uint":
                    case "uint32":
                        type = $"INT(11) UNSIGNED";
                        break;
                    case "short":
                    case "int16":
                        type = $"SMALLINT(3)";
                        break;
                    case "ushort":
                    case "uint16":
                        type = $"SMALLINT(3) UNSIGNED";
                        break;
                    case "long":
                    case "int64":
                        type = $"BIGINT(20)";
                        break;
                    case "ulong":
                    case "uint64":
                        type = $"BIGINT(20) UNSIGNED";
                        break;
                    case "bool":
                    case "boolean":
                        type = $"TINYINT(1)";
                        break;
                    case "float":
                    case "single":
                        type = $"FLOAT(16,3)";
                        break;
                    case "decimal":
                        type = $"DECIMAL(20,3)";
                        break;
                    case "double":
                        type = $"DOUBLE(20,3)";
                        break;
                    case "datetime":
                        type = $"DATETIME";
                        break;
                    case "FnVal`1":
                        var underlying = field.GetType().GetProperty("Value");
                        type = GetDatabaseType(underlying, info, size);
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
