using Figlotech.BDados.Attributes;
using Figlotech.BDados.Helpers;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.DataAccessAbstractions
{
    public static class RdbmsDataAccessorExtensions
    {


        //public void CheckStructureDropKeysPass(IEnumerable<Type> types) {
        //    Logger?.WriteLog("-- ");
        //    Logger?.WriteLog("-- CHECK STRUCTURE");
        //    Logger?.WriteLog("-- DROP KEYS PASS");
        //    Logger?.WriteLog("-- Dropping keys so that we can work with the table structures");
        //    Logger?.WriteLog("-- ");

        //    int errs = 0;
        //    var dt = this.Query("SELECT * FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA=@1", Configuration.Database);
        //    for (int i = 0; i < dt.Rows.Count; i++) {
        //        var table_name = dt.Rows[i].Field<String>("TABLE_NAME");
        //        var constraint_name = dt.Rows[i].Field<String>("CONSTRAINT_NAME");
        //        var referenced_table_name = dt.Rows[i].Field<String>("REFERENCED_TABLE_NAME");
        //        if (!types.Any(t => t.Name.ToLower() == table_name.ToLower())) {
        //            // Not fucking with tables that don't concern us.
        //            continue;
        //        }

        //        try {
        //            var keyType = "KEY";
        //            if (referenced_table_name != null) {
        //                keyType = "FOREIGN KEY";
        //            }
        //            //if (constraint_name != "PRIMARY")
        //            int rst = 0;
        //            int p = 0;
        //            int lim = 100000;
        //            this.Execute($"CREATE TABLE IF NOT EXISTS {table_name}_dekey SELECT * FROM {table_name} LIMIT 0");
        //            this.Execute($"DELETE FROM {table_name}_dekey");
        //            do {
        //                rst = this.Execute($"INSERT INTO {table_name}_dekey SELECT * FROM {table_name} LIMIT {p++ * lim}, {lim}");
        //            } while (rst > 0);
        //            this.Execute($"DROP TABLE IF EXISTS {table_name}");
        //            this.Execute($"ALTER TABLE {table_name}_dekey RENAME TO {table_name}");
        //        } catch (Exception) {
        //            errs++;
        //        }
        //    }
        //}

        //public void CheckStructureSpecialDropKeysPass(IEnumerable<Type> types) {
        //    Logger?.WriteLog("-- ");
        //    Logger?.WriteLog("-- CHECK STRUCTURE");
        //    Logger?.WriteLog("-- DROP FOREIGN KEYS PASS");
        //    Logger?.WriteLog("-- Dropping foreign keys so that we can work with the table structures");
        //    Logger?.WriteLog("-- ");
        //    Benchmarker bench = new Benchmarker("");
        //    bench.WriteToOutput = showPerformanceLogs;
        //    int errs = 0;
        //    var dt = this.Query("SELECT * FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA=@1", Configuration.Database);
        //    for (int i = 0; i < dt.Rows.Count; i++) {
        //        var table_name = dt.Rows[i].Field<String>("TABLE_NAME");
        //        var constraint_name = dt.Rows[i].Field<String>("CONSTRAINT_NAME");
        //        var referenced_table_name = dt.Rows[i].Field<String>("REFERENCED_TABLE_NAME");
        //        var type = types.FirstOrDefault(t => t.Name.ToLower() == table_name.ToLower());
        //        if (type == null) {
        //            // We're NOT fucking with tables beyond the scope of the program
        //            continue;
        //        }

        //        try {
        //            bool isThisKeyRelevantInStructure = false;

        //            var keyType = "KEY";
        //            if (referenced_table_name != null) {
        //                Execute($"ALTER TABLE {table_name} DROP FOREIGN KEY {constraint_name}");
        //            }
        //        } catch (Exception) {
        //            errs++;
        //        }
        //    }
        //    Logger?.WriteLog($"Special Dekeying took {bench.Mark("")}ms");
        //}

        //public void CheckStructureDicrepancyCheckPass(IEnumerable<Type> types) {
        //    Logger?.WriteLog("-- ");
        //    Logger?.WriteLog("-- CHECK STRUCTURE");
        //    Logger?.WriteLog("-- DISCREPANCY CHECK PASS");
        //    Logger?.WriteLog("-- Verifying if existing data complies with foreign key definitions");
        //    Logger?.WriteLog("-- ");
        //    // Get Keys straight.
        //    foreach (var t in types) {
        //        if (!t.GetInterfaces().Contains(typeof(IDataObject))) {
        //            continue;
        //        }
        //        var fields = ReflectionTool.FieldsAndPropertiesOf(t);
        //        for (var i = 0; i < fields.Count; i++) {
        //            var fkeyatt = fields[i].GetCustomAttribute<ForeignKeyAttribute>();
        //            var fieldatt = fields[i].GetCustomAttribute<FieldAttribute>();
        //            var pkeyatt = fields[i].GetCustomAttribute<PrimaryKeyAttribute>();
        //            if ((fieldatt?.Unique ?? false)) {
        //                int rst = 0;
        //                int p = 0;
        //                int lim = 50000;
        //                var pk = t.GetFields().Where((f) => f.GetCustomAttribute<PrimaryKeyAttribute>() != null).FirstOrDefault();
        //                if (pk == null)
        //                    continue;

        //                var selectDupes = $"{t.Name.ToLower()} WHERE {fields[i].Name} IS NOT NULL AND {pk.Name} NOT IN (SELECT {pk.Name} FROM (SELECT {pk.Name} FROM {t.Name.ToLower()} GROUP BY {fields[i].Name}) sub)";

        //                var dupesTableName = $"_{t.Name.ToLower()}_duplicate_{fields[i].Name.ToLower()}";

        //                this.Execute($"CREATE TABLE IF NOT EXISTS {dupesTableName} SELECT * FROM {t.Name.ToLower()} LIMIT 0;");
        //                this.Execute($"ALTER TABLE {dupesTableName} RENAME TO {dupesTableName}_1;");

        //                //do {
        //                rst = this.Execute($"CREATE TABLE {dupesTableName} SELECT * FROM {selectDupes} UNION SELECT * FROM {dupesTableName}_1");
        //                this.Execute($"DROP TABLE IF EXISTS {dupesTableName}_1;");
        //                //} while (rst > 0);

        //                var dupecount = this.Query($"SELECT COUNT(TRUE) FROM {dupesTableName}").Rows[0].Field<long>(0);
        //                if (dupecount <= 0) {
        //                    this.Execute($"DROP TABLE IF EXISTS {dupesTableName}");
        //                } else {
        //                    int rst2 = 0;
        //                    do {
        //                        rst2 = this.Execute($"DELETE FROM {t.Name.ToLower()} WHERE {pk.Name} IN  (SELECT {pk.Name} FROM {dupesTableName})");
        //                    } while (rst2 > 0);
        //                }
        //            }
        //        }
        //    }
        //    // Foreign keys pass.
        //    foreach (var t in types) {
        //        if (!t.GetInterfaces().Contains(typeof(IDataObject))) {
        //            continue;
        //        }
        //        var fields = ReflectionTool.FieldsAndPropertiesOf(t);
        //        var pk = t.GetFields().Where((f) => f.GetCustomAttribute<PrimaryKeyAttribute>() != null).FirstOrDefault();
        //        if (pk == null)
        //            continue;
        //        for (var i = 0; i < fields.Count; i++) {
        //            var fkeyatt = fields[i].GetCustomAttribute<ForeignKeyAttribute>();
        //            var fieldatt = fields[i].GetCustomAttribute<FieldAttribute>();
        //            if (fkeyatt != null) {

        //                int p = 0;
        //                int lim = 100000;
        //                var subq = t.Name.ToLower() != fkeyatt.referencedType.Name.ToLower() ?
        //                    $"(SELECT {fkeyatt.referencedColumn} FROM {fkeyatt.referencedType.Name.ToLower()})" :
        //                    $"(SELECT {fkeyatt.referencedColumn} FROM (SELECT {fkeyatt.referencedColumn} FROM {fkeyatt.referencedType.Name.ToLower()}) sub)";
        //                var selectDupes = $"{t.Name.ToLower()} WHERE {fields[i].Name} IS NOT NULL AND {fields[i].Name} NOT IN " + subq;

        //                int rst = 0;
        //                var invalidsTableName = $"_{t.Name.ToLower()}_invalid_{fields[i].Name.ToLower()}";

        //                this.Execute($"CREATE TABLE IF NOT EXISTS {invalidsTableName} SELECT * FROM {t.Name.ToLower()} LIMIT 0;");
        //                this.Execute($"ALTER TABLE {invalidsTableName} RENAME TO {invalidsTableName}_1;");

        //                //do {
        //                rst = this.Execute($"CREATE TABLE {invalidsTableName} SELECT * FROM {selectDupes} UNION SELECT * FROM {invalidsTableName}_1");
        //                this.Execute($"DROP TABLE IF EXISTS {invalidsTableName}_1;");
        //                //} while (rst > 0);

        //                var invalicount = this.Query($"SELECT COUNT(TRUE) FROM {invalidsTableName}").Rows[0].Field<long>(0);
        //                if (invalicount <= 0) {
        //                    this.Execute($"DROP TABLE {invalidsTableName}");
        //                } else {
        //                    int rst2 = 0;
        //                    do {
        //                        rst2 = this.Execute($"DELETE FROM {t.Name.ToLower()} WHERE {pk.Name} IN (SELECT {pk.Name} FROM {invalidsTableName})");
        //                    } while (rst2 > 0);
        //                }
        //            }
        //        }
        //    }

        //    //foreach (var t in types) {
        //    //    db.Execute($"ALTER TABLE {t.Name.ToLower()} RENAME TO {t.Name.ToLower()}_valid");
        //    //}
        //    //foreach (var t in types) {
        //    //    db.Execute($"CREATE TABLE IF NOT EXISTS {t.Name.ToLower()} SELECT * FROM {t.Name.ToLower()}_valid");
        //    //    db.Execute($"DROP TABLE {t.Name.ToLower()}_valid");
        //    //}
        //}

        //private static bool CheckStructureRenameColumnsPass(IEnumerable<Type> types) {
        //    Logger?.WriteLog("-- ");
        //    Logger?.WriteLog("-- CHECK STRUCTURE");
        //    Logger?.WriteLog("-- RENAME COLUMNS PASS");
        //    Logger?.WriteLog("-- Checking for renamed Columns");
        //    Logger?.WriteLog("-- ");
        //    DataTable columns = Query(
        //                 "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, COLUMN_TYPE, IS_NULLABLE FROM information_schema.columns WHERE TABLE_SCHEMA=@", Configuration.Database);

        //    foreach (DataRow a in columns.Rows) {
        //        var colName = a.Field<String>("COLUMN_NAME");
        //        foreach (var type in types) {
        //            var members = ReflectionTool.FieldsAndPropertiesOf(type);
        //            if (type.Name == a.Field<String>("TABLE_NAME")) {
        //                foreach (var member in members) {
        //                    var oldNameAtt = member.GetCustomAttribute<OldNameAttribute>();
        //                    if (oldNameAtt != null) {
        //                        if (colName == oldNameAtt.Name) {
        //                            Execute($"ALTER TABLE {type.Name.ToLower()} CHANGE COLUMN {colName} {GetColumnDefinition((FieldInfo)member, member.GetCustomAttribute<FieldAttribute>())}");
        //                        }
        //                    }
        //                }
        //            }
        //        }
        //    }

        //    return true;
        //}

        public static bool CheckStructure(this IRdbmsDataAccessor dataAccessor, Assembly ass, bool resetKeys = true) {
            var assembly = ass;
            IEnumerable<Type> types = assembly.GetTypes()
                .Where(t=> 
                    t.GetInterfaces().Contains(typeof(IDataObject)) &&
                    t.GetCustomAttribute<ViewOnlyAttribute>() != null
                );
            return CheckStructure(dataAccessor, types, resetKeys);
        }
        public static bool CheckStructure(this IRdbmsDataAccessor dataAccessor, IEnumerable<Type> types, bool resetKeys) {
            var checker = new StructureChecker(dataAccessor);
            checker.CheckStructure(types);
            return true;
        }

    }
}
