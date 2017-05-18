using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Figlotech.BDados.Entity;
using System.IO;
using Figlotech.BDados.Interfaces;
using Figlotech.BDados.Builders;
using System.Reflection;
using System.Collections;
using System.Dynamic;
using System.Linq;
using System.Runtime.Serialization;
using System.Linq.Expressions;
using System.Management;
using Figlotech.BDados.Helpers;
using Figlotech.BDados.Requirements;
using MySql.Data.MySqlClient;
using Figlotech.BDados.Attributes;
using Figlotech.Core;
using System.Runtime.Serialization.Formatters.Binary;

namespace Figlotech.BDados {
    public class MySqlDataAccessor : IRdbmsDataAccessor, IRequiresLogger {

        ILogger _logger;
        public ILogger Logger {
            get {
                return _logger ?? (_logger = new Logger(new FileAccessor(FTH.DefaultLogRepository)));
            }
            set {
                _logger = value;
            }
        }
        public Type[] _workingTypes = new Type[0];
        public Type[] WorkingTypes {
            get { return _workingTypes; }
            set {
                _workingTypes = value.Where(t => t.GetInterfaces().Contains(typeof(IDataObject))).ToArray();
            }
        }

        #region **** Global Declaration ****
        internal static Dictionary<String, List<MySqlConnection>> SqlConnections = new Dictionary<String, List<MySqlConnection>>();
        private MySqlConnection SqlConnection;
        public bool RethrowExceptions = true;

        public static DataAccessorConfiguration GlobalConfiguration;
        private DataAccessorConfiguration Configuration;
        private int _simmultaneousConnections;
        private bool _accessSwitch = false;
        public String SchemaName { get { return Configuration.Database; } }

        private static int counter = 0;
        private int myId = ++counter;
        private String _readLock = $"readLock{counter + 1}";

        public static String Version {
            get {
                return Assembly.GetExecutingAssembly().GetName().Version.ToString();
            }
        }

        #endregion **************************
        //
        #region **** General Functions ****
        public MySqlDataAccessor() {
            if (GlobalConfiguration == null)
                throw new BDadosException("No RDBMS configuration provided, no default connection configured. Crashing. On purpose.");
            Configuration = GlobalConfiguration;
        }
        public MySqlDataAccessor(DataAccessorConfiguration Config) {
            Configuration = Config;
        }

        public static void SetGlobalConfiguration(DataAccessorConfiguration config) {
            GlobalConfiguration = config;
        }

        public T ForceExist<T>(Func<T> Default, String query, params object[] args) where T : IDataObject, new() {
            return ForceExist<T>(Default, new QueryBuilder(query, args));
        }
        public T ForceExist<T>(Func<T> Default, IQueryBuilder qb) where T : IDataObject, new() {
            var f = LoadAll<T>(qb.Append("LIMIT 1"));
            if (f.Any()) {
                return f.First();
            }
            else {
                T quickSave = Default();
                quickSave.RID = new T().RID;
                quickSave.DataAccessor = this;
                quickSave.Save();
                return quickSave;
            }
        }

        public string GetCreateTable(String table) {
            return ScalarQuery<String>($"SHOW CREATE TABLE {table}");
        }

        public bool showPerformanceLogs = false;

        public void Backup(Stream s) {
            Access((bd) => {
                var conn = (bd as MySqlDataAccessor).SqlConnection;
                var cmd = conn.CreateCommand();
                MySqlBackup backie = new MySqlBackup(cmd);
                backie.ExportToTextWriter(new StreamWriter(s));
            });
        }

        public T ForceExist<T>(Func<T> Default, Conditions<T> qb) where T : IDataObject, new() {
            var f = LoadAll<T>(qb, 1, 1);
            if (f.Any()) {
                return f.First();
            }
            else {
                T quickSave = Default();
                quickSave.DataAccessor = this;
                quickSave.Save();
                return quickSave;
            }
        }

        public bool Test() {
            bool result = false;
            Access((bd) => {
                Query("SELECT 1");
                result = true;
            });
            return result;
        }
        #endregion ***************************

        public T Instantiate<T>() where T : IDataObject, new() {
            var retv = Activator.CreateInstance<T>();
            retv.DataAccessor = this;
            return retv;
        }

        public T LoadFirstOrDefault<T>(Expression<Func<T, bool>> condicoes, int? page = default(int?), int? limit = 200) where T : IDataObject, new() {
            return LoadAll<T>(condicoes, page, limit).FirstOrDefault();
        }

        public IQueryBuilder GetPreferredQueryBuilder() {
            return new QueryBuilder();
        }

        private static String GetDatabaseType(FieldInfo field, FieldAttribute info = null) {
            if (info == null)
                foreach (var att in field.GetCustomAttributes())
                    if (att is FieldAttribute) {
                        info = (FieldAttribute)att; break;
                    }
            if (info == null)
                return "VARCHAR(100)";

            string tipoDados;
            if (Nullable.GetUnderlyingType(field.FieldType) != null)
                tipoDados = Nullable.GetUnderlyingType(field.FieldType).Name;
            else
                tipoDados = field.FieldType.Name;
            if (field.FieldType.IsEnum) {
                return "INT";
            }
            String type = "VARCHAR(20)";
            if (info.Type != null && info.Type.Length > 0) {
                type = info.Type;
            }
            else {
                switch (tipoDados.ToLower()) {
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

        private static String GetColumnDefinition(FieldInfo field, FieldAttribute info = null) {
            if (info == null)
                info = field.GetCustomAttribute<FieldAttribute>();
            if (info == null)
                return "VARCHAR(128)";
            var nome = field.Name;
            String tipo = GetDatabaseType(field, info);
            if (info.Type != null && info.Type.Length > 0)
                tipo = info.Type;
            var options = "";
            if (info.Options != null && info.Options.Length > 0) {
                options = info.Options;
            }
            else {
                if (!info.AllowNull) {
                    options += " NOT NULL";
                }
                else if (Nullable.GetUnderlyingType(field.GetType()) == null && field.FieldType.IsValueType && !info.AllowNull) {
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

        internal static String CheapSanitize(Object value) {
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
                    valOutput = ((String) value);
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

        public bool CheckStructure(Assembly ass, bool resetKeys = true) {
            var assembly = ass;
            IEnumerable<Type> types = new Type[0];
            try {
                types = assembly.GetTypes();
            }
            catch (ReflectionTypeLoadException e) {
                types = e.Types.Where(t => t != null);
            }
            return CheckStructure(types, resetKeys);
        }

        public bool CheckStructureRenameColumnsPass(IEnumerable<Type> types) {
            Logger.WriteLog("-- ");
            Logger.WriteLog("-- CHECK STRUCTURE");
            Logger.WriteLog("-- RENAME COLUMNS PASS");
            Logger.WriteLog("-- Checking for renamed Columns");
            Logger.WriteLog("-- ");
            DataTable columns = Query(
                         "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, COLUMN_TYPE, IS_NULLABLE FROM information_schema.columns WHERE TABLE_SCHEMA=@", Configuration.Database);

            foreach (DataRow a in columns.Rows) {
                var colName = a.Field<String>("COLUMN_NAME");
                foreach (var type in types) {
                    var members = ReflectionTool.FieldsAndPropertiesOf(type);
                    if (type.Name == a.Field<String>("TABLE_NAME")) {
                        foreach (var member in members) {
                            var oldNameAtt = member.GetCustomAttribute<OldNameAttribute>();
                            if (oldNameAtt != null) {
                                if (colName == oldNameAtt.Name) {
                                    Execute($"ALTER TABLE {type.Name.ToLower()} CHANGE COLUMN {colName} {GetColumnDefinition((FieldInfo)member, member.GetCustomAttribute<FieldAttribute>())}");
                                }
                            }
                        }
                    }
                }
            }

            return true;
        }


        public bool CheckStructureRenameTablesPass(IEnumerable<Type> types) {
            Logger.WriteLog("-- ");
            Logger.WriteLog("-- CHECK STRUCTURE");
            Logger.WriteLog("-- RENAME TABLES PASS");
            Logger.WriteLog("-- Changing table names from their [OldName]s");
            Logger.WriteLog("-- ");
            DataTable tables = Query(
                "SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA=@1", Configuration.Database);

            foreach (DataRow a in tables.Rows) {
                var tabName = a.Field<String>("TABLE_NAME");
                foreach (var type in types) {
                    var oldNameAtt = type.GetCustomAttribute<OldNameAttribute>();
                    if (oldNameAtt != null) {
                        if (tabName == oldNameAtt.Name) {
                            Execute($"RENAME TABLE {tabName} TO {type.Name.ToLower()}");
                        }
                    }
                }
            }

            return true;
        }

        public void CheckStructureDropKeysPass(IEnumerable<Type> types) {
            Logger.WriteLog("-- ");
            Logger.WriteLog("-- CHECK STRUCTURE");
            Logger.WriteLog("-- DROP KEYS PASS");
            Logger.WriteLog("-- Dropping keys so that we can work with the table structures");
            Logger.WriteLog("-- ");

            int errs = 0;
            var dt = this.Query("SELECT * FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA=@1", Configuration.Database);
            for (int i = 0; i < dt.Rows.Count; i++) {
                var table_name = dt.Rows[i].Field<String>("TABLE_NAME");
                var constraint_name = dt.Rows[i].Field<String>("CONSTRAINT_NAME");
                var referenced_table_name = dt.Rows[i].Field<String>("REFERENCED_TABLE_NAME");
                if (!types.Any(t => t.Name.ToLower() == table_name.ToLower())) {
                    // Not fucking with tables that don't concern us.
                    continue;
                }

                try {
                    var keyType = "KEY";
                    if (referenced_table_name != null) {
                        keyType = "FOREIGN KEY";
                    }
                    //if (constraint_name != "PRIMARY")
                    int rst = 0;
                    int p = 0;
                    int lim = 100000;
                    this.Execute($"CREATE TABLE IF NOT EXISTS {table_name}_dekey SELECT * FROM {table_name} LIMIT 0");
                    this.Execute($"DELETE FROM {table_name}_dekey");
                    do {
                        rst = this.Execute($"INSERT INTO {table_name}_dekey SELECT * FROM {table_name} LIMIT {p++ * lim}, {lim}");
                    } while (rst > 0);
                    this.Execute($"DROP TABLE IF EXISTS {table_name}");
                    this.Execute($"ALTER TABLE {table_name}_dekey RENAME TO {table_name}");
                }
                catch (Exception) {
                    errs++;
                }
            }
        }

        public void CheckStructureSpecialDropKeysPass(IEnumerable<Type> types) {
            Logger.WriteLog("-- ");
            Logger.WriteLog("-- CHECK STRUCTURE");
            Logger.WriteLog("-- DROP FOREIGN KEYS PASS");
            Logger.WriteLog("-- Dropping foreign keys so that we can work with the table structures");
            Logger.WriteLog("-- ");
            Benchmarker bench = new Benchmarker("");
            bench.WriteToOutput = showPerformanceLogs;
            int errs = 0;
            var dt = this.Query("SELECT * FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA=@1", Configuration.Database);
            for (int i = 0; i < dt.Rows.Count; i++) {
                var table_name = dt.Rows[i].Field<String>("TABLE_NAME");
                var constraint_name = dt.Rows[i].Field<String>("CONSTRAINT_NAME");
                var referenced_table_name = dt.Rows[i].Field<String>("REFERENCED_TABLE_NAME");
                var type = types.FirstOrDefault(t => t.Name.ToLower() == table_name.ToLower());
                if (type == null) {
                    // We're NOT fucking with tables beyond the scope of the program
                    continue;
                }

                try {
                    bool isThisKeyRelevantInStructure = false;

                    var keyType = "KEY";
                    if (referenced_table_name != null) {
                        Execute($"ALTER TABLE {table_name} DROP FOREIGN KEY {constraint_name}");
                    }
                }
                catch (Exception) {
                    errs++;
                }
            }
            Logger.WriteLog($"Special Dekeying took {bench.Mark("")}ms");
        }

        public void CheckStructureDicrepancyCheckPass(IEnumerable<Type> types) {
            Logger.WriteLog("-- ");
            Logger.WriteLog("-- CHECK STRUCTURE");
            Logger.WriteLog("-- DISCREPANCY CHECK PASS");
            Logger.WriteLog("-- Verifying if existing data complies with foreign key definitions");
            Logger.WriteLog("-- ");
            // Get Keys straight.
            foreach (var t in types) {
                if (!t.GetInterfaces().Contains(typeof(IDataObject))) {
                    continue;
                }
                var fields = ReflectionTool.FieldsAndPropertiesOf(t);
                for (var i = 0; i < fields.Count; i++) {
                    var fkeyatt = fields[i].GetCustomAttribute<ForeignKeyAttribute>();
                    var fieldatt = fields[i].GetCustomAttribute<FieldAttribute>();
                    var pkeyatt = fields[i].GetCustomAttribute<PrimaryKeyAttribute>();
                    if ((fieldatt?.Unique ?? false)) {
                        int rst = 0;
                        int p = 0;
                        int lim = 50000;
                        var pk = t.GetFields().Where((f) => f.GetCustomAttribute<PrimaryKeyAttribute>() != null).FirstOrDefault();
                        if (pk == null)
                            continue;

                        var selectDupes = $"{t.Name.ToLower()} WHERE {fields[i].Name} IS NOT NULL AND {pk.Name} NOT IN (SELECT {pk.Name} FROM (SELECT {pk.Name} FROM {t.Name.ToLower()} GROUP BY {fields[i].Name}) sub)";

                        var dupesTableName = $"_{t.Name.ToLower()}_duplicate_{fields[i].Name.ToLower()}";

                        this.Execute($"CREATE TABLE IF NOT EXISTS {dupesTableName} SELECT * FROM {t.Name.ToLower()} LIMIT 0;");
                        this.Execute($"ALTER TABLE {dupesTableName} RENAME TO {dupesTableName}_1;");

                        //do {
                        rst = this.Execute($"CREATE TABLE {dupesTableName} SELECT * FROM {selectDupes} UNION SELECT * FROM {dupesTableName}_1");
                        this.Execute($"DROP TABLE IF EXISTS {dupesTableName}_1;");
                        //} while (rst > 0);

                        var dupecount = this.Query($"SELECT COUNT(TRUE) FROM {dupesTableName}").Rows[0].Field<long>(0);
                        if (dupecount <= 0) {
                            this.Execute($"DROP TABLE IF EXISTS {dupesTableName}");
                        }
                        else {
                            int rst2 = 0;
                            do {
                                rst2 = this.Execute($"DELETE FROM {t.Name.ToLower()} WHERE {pk.Name} IN  (SELECT {pk.Name} FROM {dupesTableName})");
                            } while (rst2 > 0);
                        }
                    }
                }
            }
            // Foreign keys pass.
            foreach (var t in types) {
                if (!t.GetInterfaces().Contains(typeof(IDataObject))) {
                    continue;
                }
                var fields = ReflectionTool.FieldsAndPropertiesOf(t);
                var pk = t.GetFields().Where((f) => f.GetCustomAttribute<PrimaryKeyAttribute>() != null).FirstOrDefault();
                if (pk == null)
                    continue;
                for (var i = 0; i < fields.Count; i++) {
                    var fkeyatt = fields[i].GetCustomAttribute<ForeignKeyAttribute>();
                    var fieldatt = fields[i].GetCustomAttribute<FieldAttribute>();
                    if (fkeyatt != null) {

                        int p = 0;
                        int lim = 100000;
                        var subq = t.Name.ToLower() != fkeyatt.referencedType.Name.ToLower() ?
                            $"(SELECT {fkeyatt.referencedColumn} FROM {fkeyatt.referencedType.Name.ToLower()})" :
                            $"(SELECT {fkeyatt.referencedColumn} FROM (SELECT {fkeyatt.referencedColumn} FROM {fkeyatt.referencedType.Name.ToLower()}) sub)";
                        var selectDupes = $"{t.Name.ToLower()} WHERE {fields[i].Name} IS NOT NULL AND {fields[i].Name} NOT IN " + subq;

                        int rst = 0;
                        var invalidsTableName = $"_{t.Name.ToLower()}_invalid_{fields[i].Name.ToLower()}";

                        this.Execute($"CREATE TABLE IF NOT EXISTS {invalidsTableName} SELECT * FROM {t.Name.ToLower()} LIMIT 0;");
                        this.Execute($"ALTER TABLE {invalidsTableName} RENAME TO {invalidsTableName}_1;");

                        //do {
                        rst = this.Execute($"CREATE TABLE {invalidsTableName} SELECT * FROM {selectDupes} UNION SELECT * FROM {invalidsTableName}_1");
                        this.Execute($"DROP TABLE IF EXISTS {invalidsTableName}_1;");
                        //} while (rst > 0);

                        var invalicount = this.Query($"SELECT COUNT(TRUE) FROM {invalidsTableName}").Rows[0].Field<long>(0);
                        if (invalicount <= 0) {
                            this.Execute($"DROP TABLE {invalidsTableName}");
                        }
                        else {
                            int rst2 = 0;
                            do {
                                rst2 = this.Execute($"DELETE FROM {t.Name.ToLower()} WHERE {pk.Name} IN (SELECT {pk.Name} FROM {invalidsTableName})");
                            } while (rst2 > 0);
                        }
                    }
                }
            }

            //foreach (var t in types) {
            //    db.Execute($"ALTER TABLE {t.Name.ToLower()} RENAME TO {t.Name.ToLower()}_valid");
            //}
            //foreach (var t in types) {
            //    db.Execute($"CREATE TABLE IF NOT EXISTS {t.Name.ToLower()} SELECT * FROM {t.Name.ToLower()}_valid");
            //    db.Execute($"DROP TABLE {t.Name.ToLower()}_valid");
            //}
        }

        public void CheckStructureRebuildKeysPass(IEnumerable<Type> types) {
            //CheckStructureDicrepancyCheckPass(types);
            Access((db) => {
                // Get Keys straight.
                foreach (var t in types) {
                    if (!t.GetInterfaces().Contains(typeof(IDataObject))) {
                        continue;
                    }
                    var fields = ReflectionTool.FieldsAndPropertiesOf(t);
                    for (var i = 0; i < fields.Count; i++) {
                        var fkeyatt = fields[i].GetCustomAttribute<ForeignKeyAttribute>();
                        var fieldatt = fields[i].GetCustomAttribute<FieldAttribute>();
                        var pkeyatt = fields[i].GetCustomAttribute<PrimaryKeyAttribute>();
                        if (pkeyatt != null) {
                            try {
                                db.Execute($"ALTER TABLE {t.Name.ToLower()} ADD PRIMARY KEY ({fields[i].Name})");
                            }
                            catch (Exception) { }
                            try {
                                db.Execute($"ALTER TABLE {t.Name.ToLower()} CHANGE COLUMN {fields[i].Name} {fields[i].Name} {GetDatabaseType((FieldInfo)fields[i])} AUTO_INCREMENT");
                            }
                            catch (Exception) { }
                        }
                        if (fkeyatt != null && !(fieldatt?.Unique ?? false)) {
                            try {
                                db.Execute($"ALTER TABLE {t.Name.ToLower()} ADD INDEX idx_{fields[i].Name} ({fields[i].Name})");
                            }
                            catch (Exception) { }
                        }
                        if ((fieldatt?.Unique ?? false)) {
                            try {
                                db.Execute($"ALTER TABLE {t.Name.ToLower()} ADD CONSTRAINT uk_{fields[i].Name} UNIQUE ({fields[i].Name})");
                            }
                            catch (Exception) { }
                        }
                    }
                }
                // Foreign keys pass.

                //foreach (var t in types) {
                //    if (!t.GetInterfaces().Contains(typeof(IDataObject))) {
                //        continue;
                //    }
                //    var fields = ReflectionTool.FieldsAndPropertiesOf(t);
                //    for (var i = 0; i < fields.Count; i++) {
                //        var fkeyatt = fields[i].GetCustomAttribute<ForeignKeyAttribute>();
                //        if (fkeyatt != null) {
                //            db.Execute($"ALTER TABLE {t.Name.ToLower()} ADD CONSTRAINT fk_{t.Name.ToLower()}_{fields[i].Name.ToLower()} FOREIGN KEY ({fields[i].Name}) REFERENCES {fkeyatt.referencedType.Name.ToLower()}({fkeyatt.referencedColumn})");
                //        }
                //    }
                //}

            });
        }

        public void CheckStructureSpecialAddForeignKeys(IEnumerable<Type> types) {
            Logger.WriteLog("-- ");
            Logger.WriteLog("-- CHECK STRUCTURE");
            Logger.WriteLog("-- SPECIAL FOREIGN KEYING PASS");
            Logger.WriteLog("-- Adding required foreign keys to structure.");
            Logger.WriteLog("-- ");
            Benchmarker bench = new Benchmarker("");
            bench.WriteToOutput = showPerformanceLogs;
            // Gather all required keys and dictionary them into a 
            // Map that will show us which keys need creating.
            Dictionary<ForeignKeyDefinition, bool> fkMap = new Dictionary<ForeignKeyDefinition, bool>();
            foreach (var type in types) {
                if (!type.GetInterfaces().Contains(typeof(IDataObject)))
                    continue;
                var fields = type.GetFields();
                foreach (var field in fields) {
                    var fkinfo = field.GetCustomAttribute<ForeignKeyAttribute>();
                    if (fkinfo != null) {
                        // Add all fkinfos as "true", "does need to be created"
                        var fk = new ForeignKeyDefinition(
                            type.Name,
                            field.Name,
                            fkinfo.referencedType.Name,
                            fkinfo.referencedColumn
                        );
                        fkMap.Add(fk, true);
                    }
                }
            }
            Access((db) => {
                // Here we'll check which keys already exist and 
                // update the map so that we can skip them.
                var dt = db.Query("SELECT * FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE TABLE_SCHEMA=@1", Configuration.Database);
                for (int i = 0; i < dt.Rows.Count; i++) {
                    var table_name = dt.Rows[i].Field<String>("TABLE_NAME");
                    var column_name = dt.Rows[i].Field<String>("COLUMN_NAME");
                    var referenced_table_name = dt.Rows[i].Field<String>("REFERENCED_TABLE_NAME");
                    var referenced_column_name = dt.Rows[i].Field<String>("REFERENCED_COLUMN_NAME");
                    var constraint_name = dt.Rows[i].Field<String>("CONSTRAINT_NAME");
                    var type = types.FirstOrDefault(t => t.Name.ToLower() == table_name.ToLower());
                    if (type == null) {
                        // We're NOT fucking with tables beyond the scope of the program
                        continue;
                    }
                    // Get map wired
                    for (int k = 0; k < fkMap.Count; k++) {
                        if (
                            fkMap.ElementAt(k).Key.TableName.ToUpper() == table_name.ToUpper() &&
                            fkMap.ElementAt(k).Key.ColumnName.ToUpper() == column_name.ToUpper() &&
                            fkMap.ElementAt(k).Key.ReferencedColumn.ToUpper() == referenced_column_name.ToUpper() &&
                            fkMap.ElementAt(k).Key.ReferencedTable.ToUpper() == referenced_table_name.ToUpper()) {
                            fkMap[fkMap.ElementAt(k).Key] = false;
                        }
                    }

                }

                // Use the map to smart add keys.
                for (int k = 0; k < fkMap.Count; k++) {
                    if (fkMap.ElementAt(k).Value) {
                        try {
                            var fk = fkMap.ElementAt(k).Key;
                            db.Execute($"ALTER TABLE {fk.TableName.ToLower()} ADD CONSTRAINT fk_{fk.TableName.ToLower()}_{fk.ColumnName.ToLower()} FOREIGN KEY ({fk.ColumnName}) REFERENCES {fk.ReferencedTable.ToLower()}({fk.ReferencedColumn})");
                        }
                        catch (Exception) {
                        }
                    }
                }
                // Foreign keys pass.
                foreach (var t in types) {
                    if (!t.GetInterfaces().Contains(typeof(IDataObject))) {
                        continue;
                    }
                    var fields = ReflectionTool.FieldsAndPropertiesOf(t);
                    for (var i = 0; i < fields.Count; i++) {
                        try {
                            var fkeyatt = fields[i].GetCustomAttribute<ForeignKeyAttribute>();
                            if (fkeyatt != null) {
                                db.Execute($"ALTER TABLE {t.Name.ToLower()} ADD CONSTRAINT fk_{t.Name.ToLower()}_{fields[i].Name.ToLower()} FOREIGN KEY ({fields[i].Name}) REFERENCES {fkeyatt.referencedType.Name.ToLower()}({fkeyatt.referencedColumn})");
                            }
                        }
                        catch (Exception x) {

                        }
                    }
                }

            });
            Logger.WriteLog($"Special Foreign Keying took {bench.Mark("")}ms");
        }

        public void CheckStructureCollationsPass(IEnumerable<Type> types) {
            Access((db) => {
                var dt = db.Query("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=@1", Configuration.Database);
                foreach (var a in types) {
                    for (int i = 0; i < dt.Rows.Count; i++) {
                        var table_name = dt.Rows[i].Field<String>("TABLE_NAME");
                        var rowformat = dt.Rows[i].Field<String>("ROW_FORMAT");
                        if (table_name.ToLower() != a.Name.ToLower())
                            continue;

                        db.Execute($"CREATE TABLE IF NOT EXISTS {table_name}_1 SELECT * FROM {table_name}");
                        db.Execute($"DELETE FROM {table_name}");
                        db.Execute($"ALTER TABLE {table_name}_1 CONVERT TO CHARACTER SET utf8 COLLATE utf8_general_ci");
                        break;
                    }
                }
                foreach (var a in types) {
                    for (int i = 0; i < dt.Rows.Count; i++) {
                        var table_name = dt.Rows[i].Field<String>("TABLE_NAME");
                        var rowformat = dt.Rows[i].Field<String>("ROW_FORMAT");
                        if (table_name.ToLower() != a.Name.ToLower())
                            continue;

                        int count = 0;
                        int p = 0;
                        int lim = 50000;
                        if (rowformat.ToUpper() != "COMPACT")
                            db.Execute($"ALTER TABLE {table_name}_1 ROW_FORMAT=COMPACT");
                        do {
                            count = db.Execute($"INSERT INTO {table_name}_1 SELECT * FROM {table_name} LIMIT {p++ * lim}, {lim}");
                        } while (count > 0);
                        db.Execute($"DROP TABLE {table_name}");
                        break;
                    }
                }
                foreach (var a in types) {
                    for (int i = 0; i < dt.Rows.Count; i++) {
                        var table_name = dt.Rows[i].Field<String>("TABLE_NAME");
                        var rowformat = dt.Rows[i].Field<String>("ROW_FORMAT");
                        if (table_name.ToLower() != a.Name.ToLower())
                            continue;

                        db.Execute($"ALTER TABLE {table_name}_1 RENAME TO {table_name}");
                        break;
                    }
                }
                db.Execute($"ALTER DATABASE {Configuration.Database} CHARACTER SET utf8 COLLATE utf8_general_ci;");

            });
        }

        public bool CheckStructure(IEnumerable<Type> types, bool resetKeys = true) {
            lock ($"BDADOS_CHECK_STRUCTURE_{this.GetType().Name}_{this.myId}") {
                List<Type> instances = new List<Type>();

                instances.AddRange(
                    types.Where(t =>
                        t.GetInterfaces().Contains(typeof(IDataObject)) &&
                        t.GetCustomAttribute<ViewOnlyAttribute>() == null
                    ));


                Dictionary<Type, int> orderingStuff = new Dictionary<Type, int>();
                foreach (var a in instances) {
                    orderingStuff.Add(a, 1);
                }
                int pass = 0;
                do {
                    pass = 0;
                    bool circref = false;
                    for (int i = 0; i < orderingStuff.Count; i++) {
                        for (int j = 0; j < orderingStuff.Count; j++) {
                            var fks = ReflectionTool.FieldsAndPropertiesOf(orderingStuff.ElementAt(i).Key)
                                .Where(f => f.GetCustomAttribute<ForeignKeyAttribute>() != null)
                                .Select(f => f.GetCustomAttribute<ForeignKeyAttribute>());
                            foreach (var a in fks) {
                                if (a.referencedType == orderingStuff.ElementAt(j).Key) {
                                    if (orderingStuff.ElementAt(i).Value <= orderingStuff.ElementAt(j).Value) {
                                        orderingStuff[orderingStuff.ElementAt(i).Key] += orderingStuff.ElementAt(j).Value;
                                        pass++;
                                    }
                                }
                            }
                        }
                        if (orderingStuff.ElementAt(i).Value > instances.Count * 3) {
                            circref = true;
                        }
                    }
                    if (circref) {
                        Logger.WriteLog("-- Circular references detected in structure              --");
                        Logger.WriteLog("-- We will bypass FK checks while restructuring           --");
                        break;
                    }
                } while (pass > 0);

                instances = instances.OrderBy(i => orderingStuff[i]).ToList();

                //for (int i = 0; i < types.Count(); i++) {
                //    try {
                //        var t = types.ElementAt(i);
                //        var lv = t;
                //        while (lv.BaseType != null) {
                //            lv = lv.BaseType;
                //            if (lv == typeof(BaseDataObject)) {
                //                BaseDataObject newinst = (BaseDataObject)Activator.CreateInstance(t);
                //                ((BaseDataObject)newinst).GenerateMetadata();
                //                instances.Add((BaseDataObject)newinst);
                //            }
                //        }
                //    } catch (Exception x) {
                //        while (x != null && x.InnerException != x) {
                //            Logger.WriteLog($"Err strut {x.Message}");
                //            x = x.InnerException;
                //        }
                //    }
                //}
                Logger.WriteLog($"Verifying Database Structure | {instances.Count} value objects.");

                bool retv = false;
                //Access((bd) => {
                //    try {
                //      ADD BACKUP LOGIC HERE
                //      I'll do it sometime, but don't hold your breath.
                //        var backupcmd = SqlConnection.CreateCommand();
                //        using (MySqlBackup backie = new MySqlBackup(backupcmd)) {
                //            if (!Directory.Exists(BDados.DefaultBackupStore))
                //                Directory.CreateDirectory(BDados.DefaultBackupStore);
                //            try {
                //                backie.ExportToFile(Path.Combine(BDados.DefaultBackupStore, $"{BDadosInst.Configuration.Database}_" + DateTime.Now.ToString("dd-MM-yyyy HH-mm") + ".sql"));
                //            } catch (Exception x) {
                //                Logger.WriteLog("Erro ao fazer backup para reparar estrutura.");
                //                while (x != null) {
                //                    Logger.WriteLog(x.Message);
                //                    Logger.WriteLog(x.StackTrace);
                //                    Logger.WriteLog("--");
                //                    x = x.InnerException;
                //                }
                //                //return;
                //            }
                //        }
                //    } catch (Exception x) {

                //    }
                //});
                Access((db) => {
                    db.Execute("SET LOCAL sql_mode=''");
                    db.Execute("SET LOCAL FOREIGN_KEY_CHECKS=0;");
                    //CheckStructureSpecialDropKeysPass(instances);
                    CheckStructureRenameColumnsPass(instances);
                    CheckStructureRenameTablesPass(instances);

                    //if (resetKeys)
                    //    CheckStructureCollationsPass(instances);
                    DataTable tables = Query(
                        "SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA=@1", Configuration.Database);
                    for (int i = 0; i < instances.Count; i++) {

                        // Criar tabelas inexistentes.
                        Type t = instances[i];
                        var found = false;
                        for (int row = 0; row < tables.Rows.Count; row++) {
                            if (tables.Rows[row].Field<String>("TABLE_NAME").ToLower() == t.Name.ToLower()) {
                                //if (tables.Rows[row].Field<String>("TABLE_NAME") != t.ValueObjectName) {
                                //    try {
                                //        if (bd.Execute($"CREATE TABLE {t.ValueObjectName} SELECT * FROM {tables.Rows[row].Field<String>("TABLE_NAME")};") > -1) {
                                //            bd.Execute($"DROP TABLE {tables.Rows[row].Field<String>("TABLE_NAME")};");
                                //        }
                                //    } catch (Exception x) { }
                                //}
                                found = true;
                                break;
                            }
                        }

                        var fields = t.GetFields().Where((a) => a.GetCustomAttribute<FieldAttribute>() != null).ToArray();
                        if (!found) {
                            //Logger.WriteLog($"{instances[i].ValueObjectName} doesn't exist");
                            StringBuilder createTable = new StringBuilder();
                            createTable.Append($"CREATE TABLE {t.Name.ToLower()} (");
                            for (int x = 0; x < fields.Length; x++) {
                                FieldInfo f = fields[x];
                                foreach (Attribute a in f.GetCustomAttributes()) {
                                    if (a is FieldAttribute) {
                                        var defn = GetColumnDefinition(f, (FieldAttribute)a);
                                        createTable.Append(defn);
                                        if (x < fields.Length - 1) {
                                            createTable.Append(",");
                                        }
                                    }
                                }
                            }
                            createTable.Append($") ENGINE=INNODB DEFAULT CHARSET=UTF8 COLLATE=UTF8_GENERAL_CI ROW_FORMAT=COMPACT;");
                            try {
                                Execute(createTable.ToString());
                                continue;
                            }
                            catch (Exception x) {
                                Logger.WriteLog("Error creating table.");
                                Logger.WriteLog(x.Message);
                                Logger.WriteLog(x.StackTrace);
                            }
                        }
                        else {
                            //Logger.WriteLog($"{instances[i].ValueObjectName} exists");
                        }

                        DataTable columns = Query(
                        "SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, COLUMN_TYPE, IS_NULLABLE FROM information_schema.columns WHERE TABLE_SCHEMA=@1 AND LOWER(TABLE_NAME)=LOWER(@2)", Configuration.Database, t.Name.ToLower());
                        // Criar Campos inexistentes
                        foreach (var c in fields) {
                            found = false;
                            for (int row = 0; row < columns.Rows.Count; row++) {
                                if (columns.Rows[row].Field<String>("TABLE_NAME").ToLower() == t.Name.ToLower() &&
                                    columns.Rows[row].Field<String>("COLUMN_NAME") == c.Name) {
                                    found = true;
                                    FieldAttribute info = null;
                                    try {
                                        foreach (Attribute att in c.GetCustomAttributes())
                                            if (att is FieldAttribute) {
                                                info = (FieldAttribute)att; break;
                                            }
                                    }
                                    catch (Exception x) {
                                        Logger.WriteLog("Erro ao capturar o ColunaAttribute INFO");
                                    }
                                    String dt = columns.Rows[row].Field<String>("DATA_TYPE");
                                    ulong? dtLength2 = columns.Rows[row].Field<ulong?>("CHARACTER_MAXIMUM_LENGTH");
                                    var cdef = GetColumnDefinition(c);
                                    int index = Math.Min(cdef.IndexOf(' '), cdef.IndexOf('(') > 0 ? cdef.IndexOf('(') : cdef.Length);
                                    String tipo = GetDatabaseType(c);
                                    if (tipo.Contains('('))
                                        tipo = tipo.Substring(0, tipo.IndexOf('(')).Trim();
                                    bool dbIsNullable = columns.Rows[row].Field<String>("IS_NULLABLE") == "YES";
                                    if (dt.ToLower() != tipo?.ToLower() ||
                                        ((dt.ToLower() == "varchar") && ((long?)dtLength2 != info?.Size)) ||
                                        (dbIsNullable != info?.AllowNull)) {
                                        Logger.WriteLog($"{t.Name.ToLower()}: {dt.ToLower()}->{tipo?.ToLower()} | {dtLength2}->{info?.Size} | {dbIsNullable}->{info?.AllowNull}");
                                        try {
                                            Execute($"ALTER TABLE {t.Name.ToLower()} {(info == null ? "ADD" : "CHANGE")} COLUMN {c.Name} {cdef}");
                                        }
                                        catch (Exception x) {
                                            Logger.WriteLog("Error renaming column.");
                                            Logger.WriteLog(x.Message);
                                            Logger.WriteLog(x.StackTrace);
                                        }
                                    }
                                    break;
                                }
                            }

                            if (!found) {
                                //Logger.WriteLog($"{c.Name} doesn't exist");
                                var catt = c.GetCustomAttribute<FieldAttribute>();
                                if (catt == null) continue;
                                String query = $"ALTER TABLE {t.Name.ToLower()} ADD COLUMN {GetColumnDefinition(c, catt)};";
                                try {
                                    Execute(
                                        query);
                                }
                                catch (Exception x) {
                                    Logger.WriteLog(query);
                                    Logger.WriteLog("Error adding column.");
                                    Logger.WriteLog(x.Message);
                                    Logger.WriteLog(x.StackTrace);
                                    Logger.WriteLog("");
                                }
                            }
                            else {
                                //Logger.WriteLog($"{c.Name} exists");
                            }

                        }
                        // remover NOT NULL de colunas inexistentes na estrutura.
                        for (int y = 0; y < columns.Rows.Count; y++) {
                            DataRow col = columns.Rows[y];
                            //Logger.WriteLog(col.Field<String>("TABLE_NAME"));
                            if (col.Field<String>("TABLE_NAME").ToLower() != t.Name.ToLower())
                                continue;
                            String nomeCol = col.Field<String>("COLUMN_NAME");
                            //if (nomeCol.StartsWith("old_"))
                            //    continue;
                            bool foundCol = false;
                            ReflectionTool.ForFields(t, (f) => {
                                if (col.Field<String>("COLUMN_NAME") == f.Name) {
                                    foundCol = true;
                                }
                            });
                            if (!foundCol) {
                                int att = 0;
                                //try {
                                //    Execute($"ALTER TABLE {t.NomeTabela} CHANGE COLUMN {nomeCol} old_{nomeCol} {col.Field<String>("COLUMN_TYPE")} DEFAULT NULL;");
                                //} catch (Exception) {
                                //    Execute($"ALTER TABLE {t.NomeTabela} DROP COLUMN {nomeCol};");
                                //}
                                Execute($"ALTER TABLE {t.Name.ToLower()} DROP COLUMN {nomeCol};");
                            }
                        }


                        retv = true;
                    }

                    CheckStructureRebuildKeysPass(instances);
                    CheckStructureSpecialAddForeignKeys(types);

                    // Clear constraints
                    var constraintCommands = Query($"SELECT CONCAT('ALTER TABLE ', table_name, ' DROP INDEX ', constraint_name,';') FROM information_schema.key_column_usage WHERE table_schema='{Configuration.Database}' AND constraint_name REGEXP '([^\\_]{{1,1}})\\_([0-9]*)$'");
                    if (constraintCommands.Rows.Count > 0) {
                        StringBuilder theVeryStatement = new StringBuilder();
                        foreach (DataRow a in constraintCommands.Rows) {
                            theVeryStatement.Append(a.Field<String>(0));
                        }
                        foreach (var killConstraint in theVeryStatement.ToString().Split(';')) {
                            if (killConstraint.Trim().Length > 0) {
                                try {
                                    Execute(killConstraint);
                                }
                                catch (Exception) { }
                            }
                        }
                        //Execute(theVeryStatement.ToString());
                    }
                    //CheckStructureCollationsPass(instances);
                }, (ex) => {
                    if (Debugger.IsAttached)
                        Debugger.Break();
                });
                return retv;
            }
        }
        
        public bool SaveItem(IDataObject input, Action fn = null) {
            bool retv = false;
            if (SqlConnection?.State != ConnectionState.Open) {
                Access(bd => {
                    retv = bd.SaveItem(input, fn);
                });
                return retv;
            }
            int rs = -33;

            var id = GetIdColumn(input.GetType());
            var rid = GetRidColumn(input.GetType());

            input.UpdatedTime = DateTime.UtcNow;
            if (input.IsPersisted()) {
                rs = Execute(GetQueryGenerator().GenerateUpdateQuery(input));
                retv = true;
                if (fn != null)
                    fn.Invoke();
                return retv;
            }

            rs = Execute(GetQueryGenerator().GenerateInsertQuery(input));
            if (rs == 0) {
                Logger.WriteLog("** Something went SERIOUSLY NUTS in SaveItem<T> **");
            }
            retv = rs > 0;
            if (retv && !input.IsPersisted()) {
                long retvId = 0;
                if (input.GetType().GetFields().Where((f) => f.GetCustomAttribute<ReliableIdAttribute>() != null).Any()) {
                    DataTable dt = Query($"SELECT {id} FROM " + input.GetType().Name + $" WHERE {rid}=@1", input.RID);
                    retvId = dt.Rows[0].Field<long>(id);
                }
                else {
                    retvId = (long) ScalarQuery($"SELECT last_insert_id();");
                }
                if (retvId > 0) {
                    input.ForceId(retvId);
                }
                if (fn != null)
                    fn.Invoke();
                retv = true;
            }
            return retv;
        }

        //public List<T> Load<T>(string Consulta, params object[] args) {
        //    List<T> retv = new List<T>();
        //    Access((bd) => {

        //        return Query(Consulta, args);
        //    });retv;
        //}

        public MySqlConnection GetConnection() {
            return SqlConnection;
        }

        public List<T> Query<T>(IQueryBuilder query) {
            if (query == null) {
                return new List<T>();
            }
            DataTable resultado = Query(query);
            var fields = ReflectionTool.FieldsAndPropertiesOf(typeof(T));
            String[] columnNames = new string[resultado.Columns.Count];
            for (int c = 0; c < resultado.Columns.Count; c++) {
                columnNames[c] = resultado.Columns[c].ColumnName;
            }
            var retv = new T[resultado.Rows.Count];
            var objBuilder = new ObjectReflector();
            for (int i = 0; i < resultado.Rows.Count; i++) {
                objBuilder.Slot(retv[i]);
                Parallel.ForEach(fields, (col) => {
                    bool exists = false;
                    var typeofCol = ReflectionTool.GetTypeOf(col);
                    for (int c = 0; c < columnNames.Length; c++) {
                        if (resultado.Columns[c].ColumnName == col.Name) {
                            exists = true;
                            break;
                        }
                    }
                    if (!exists) return;

                    Object o = resultado.Rows[i].Field<Object>(col.Name);
                    if (Nullable.GetUnderlyingType(typeofCol) != null && o == null)
                        objBuilder[col] = null;
                    else if (Nullable.GetUnderlyingType(typeofCol) != null && o != null)
                        objBuilder[col] = Convert.ChangeType(o, Nullable.GetUnderlyingType(typeofCol));
                    else if (Nullable.GetUnderlyingType(typeofCol) == null && o != null) {
                        try {
                            if (typeofCol.IsEnum) {
                                objBuilder[col] = Enum.ToObject(typeofCol, o);
                            }
                            else {
                                objBuilder[col] = Convert.ChangeType(o, typeofCol);
                            }
                        }
                        catch (Exception) {
                            objBuilder[col] = Activator.CreateInstance(typeofCol);
                        }
                    }
                    else if (Nullable.GetUnderlyingType(typeofCol) == null && o != null) {
                        objBuilder[col] = Activator.CreateInstance(typeofCol);
                    }
                });
            }
            return retv.ToList();
        }

        public List<T> Query<T>(string queryString, params object[] args) {
            return Query<T>(new QueryBuilder(queryString, args));
        }

        public static String GetIdColumn<T>() where T : IDataObject, new() { return GetIdColumn(typeof(T)); }
        public static String GetIdColumn(Type type) {
            var fields = new List<FieldInfo>();
            do {
                fields.AddRange(type.GetFields());
                type = type.BaseType;
            } while (type != null);

            var retv = fields
                .Where((f) => f.GetCustomAttribute<PrimaryKeyAttribute>() != null)
                .FirstOrDefault()
                ?.Name
                ?? "Id";
            return retv;
        }

        public static String GetRidColumn<T>() where T : IDataObject, new() { return GetRidColumn(typeof(T)); }
        public static String GetRidColumn(Type type) {
            var fields = new List<FieldInfo>();
            do {
                fields.AddRange(type.GetFields());
                type = type.BaseType;
            } while (type != null);

            var retv = fields
                .Where((f) => f.GetCustomAttribute<ReliableIdAttribute>() != null)
                .FirstOrDefault()
                ?.Name
                ?? "RID";
            return retv;
        }

        public T LoadById<T>(long Id) where T : IDataObject, new() {
            T retv = default(T);
            if (SqlConnection?.State != ConnectionState.Open) {
                Access((bd) => {
                    retv = bd.LoadById<T>(Id);
                });
                return retv;
            }
            var name = GetIdColumn<T>();

            // Cria uma instancia Dummy só pra poder pegar o reflector da classe usada como T.
            T dummy = (T)Activator.CreateInstance(typeof(T));
            BaseDataObject dummy2 = (BaseDataObject)(object)dummy;

            // Usando o dummy2 eu consigo puxar uma query de select baseada nos campos da classe filha
            DataTable dt = Query(GetQueryGenerator().GenerateSelect<T>(new ConditionParametrizer($"{name}=@1", Id)));
            int i = 0;
            if (dt.Rows.Count > 0) {
                T add = (T)Activator.CreateInstance(typeof(T));
                var members = ReflectionTool.FieldsAndPropertiesOf(typeof(T))
                    .Where(m=> m.GetCustomAttribute<FieldAttribute>() != null);
                var objBuilder = new ObjectReflector(add);
                foreach (var col in members) {
                    try {
                        var typeofCol = ReflectionTool.GetTypeOf(col);
                        Type t = typeofCol;
                        Object o = dt.Rows[i].Field<Object>(col.Name);
                        objBuilder[col] = o;
                    }
                    catch (Exception) { }
                }
                retv = add;
            }

            return retv;
        }

        public T LoadByRid<T>(RID RID) where T : IDataObject, new() {
            T retv = default(T);
            if (SqlConnection?.State != ConnectionState.Open) {
                Access((bd) => {
                    retv = bd.LoadByRid<T>(RID);
                });
                return retv;
            }
            var rid = GetRidColumn<T>();

            // Cria uma instancia Dummy só pra poder pegar o reflector da classe usada como T.
            T dummy = (T)Activator.CreateInstance(typeof(T));
            BaseDataObject dummy2 = (BaseDataObject)(object)dummy;
            Access((bd) => {
                // Usando o dummy2 eu consigo puxar uma query de select baseada nos campos da classe filha
                DataTable dt = Query(GetQueryGenerator().GenerateSelect<T>(new ConditionParametrizer($"{rid}=@1", RID)));
                int i = 0;
                if (dt.Rows.Count > 0) {
                    T add = (T)Activator.CreateInstance(typeof(T));
                    var members = ReflectionTool.FieldsAndPropertiesOf(typeof(T))
                        .Where(m => m.GetCustomAttribute<FieldAttribute>() != null);
                    var objBuilder = new ObjectReflector(add);
                    foreach (var col in members) {
                        try {
                            var typeofCol = ReflectionTool.GetTypeOf(col);
                            Type t = typeofCol;
                            Object o = dt.Rows[i].Field<Object>(col.Name);
                            objBuilder[col] = o;
                        }
                        catch (Exception) { }
                    }
                    retv = add;
                }
            });
            return retv;
        }

        public RecordSet<T> LoadAll<T>(String where = "TRUE", params object[] args) where T : IDataObject, new()  {
            return LoadAll<T>(new QueryBuilder(where, args));
        }

        public RecordSet<T> LoadAll<T>(Expression<Func<T, bool>> conditions = null, int? page = null, int? limit = 200) where T : IDataObject, new()  {
            var query = new ConditionParser().ParseExpression(conditions);
            if (page != null && limit != null)
                query.Append($"LIMIT {(page - 1) * limit}, {limit}");
            else if (limit != null)
                query.Append($"LIMIT {limit}");
            return LoadAll<T>(query);
        }

        Benchmarker Bench = null;

        public RecordSet<T> LoadAll<T>(IQueryBuilder condicoes) where T : IDataObject, new()  {
            RecordSet<T> retv = new RecordSet<T>(this);
            if (SqlConnection?.State != ConnectionState.Open) {
                Access(bd => {
                    retv = bd.LoadAll<T>(condicoes);
                });
                retv.DataAccessor = this;
                return retv;
            }
            Bench.Mark("--");

            if (condicoes == null) {
                condicoes = new QueryBuilder("TRUE");
            }
            // Cria uma instancia Dummy só pra poder pegar o reflector da classe usada como T.
            T dummy = (T)Activator.CreateInstance(typeof(T));
            BaseDataObject dummy2 = (BaseDataObject)(object)dummy;
            // Usando o dummy2 eu consigo puxar uma query de select baseada nos campos da classe filha
            DataTable dt = null;
            Access((bd) => {
                Bench.Mark("Data Load ---");
                var selectQuery = GetQueryGenerator().GenerateSelect<T>(condicoes);
                Bench.Mark("Generate SELECT");
                dt = Query(selectQuery);
                Bench.Mark("Execute SELECT");
                if (dt == null)
                    return;
                if (dt.Rows.Count < 1) {
                    Logger.WriteLog("Query returned no results.");
                    return;
                }
                var fields = ReflectionTool.FieldsAndPropertiesOf(typeof(T))
                    .Where((a) => a.GetCustomAttribute<FieldAttribute>() != null)
                    .ToList();

                var objBuilder = new ObjectReflector();
                for (int i = 0; i < dt.Rows.Count; i++) {
                    T add = new T();
                    objBuilder.Slot(add);
                    Parallel.ForEach(fields, col => {
                        try {
                            var typeofCol = ReflectionTool.GetTypeOf(col);
                            Object o = dt.Rows[i].Field<Object>(col.Name);
                            if (o == null) {
                                if (Nullable.GetUnderlyingType(typeofCol) != null
                                || !typeofCol.IsValueType)
                                    objBuilder[col] = null;
                                return;
                            }
                            if (typeofCol.IsAssignableFrom(o.GetType())
                                || (Nullable.GetUnderlyingType(typeofCol)?.IsAssignableFrom(o.GetType()) ?? false)) {
                                objBuilder[col] = o;
                            }
                            else if (typeofCol.IsEnum || (Nullable.GetUnderlyingType(typeofCol)?.IsEnum ?? false)) {
                                objBuilder[col] = Enum.ToObject(typeofCol, o);
                            }
                            else
                                objBuilder[col] = Convert.ChangeType(o, typeofCol);
                        }
                        catch (Exception x) {
                            Logger?.WriteLog($"{x.Message}");
                        }
                    });
                    add.DataAccessor = this;
                    retv.Add(add);
                }
                Bench.Mark("Build RecordSet");
            }, (x) => {
                Logger.WriteLog(x.Message);
                Logger.WriteLog(x.StackTrace);
                Bench.Mark("Build RecordSet");
            });
            return retv;
        }

        public bool Delete(IDataObject obj) {
            bool retv = false;
            if (SqlConnection?.State != ConnectionState.Open) {
                Access((bd) => {
                    retv = bd.Delete(obj);
                });
                return retv;
            }
            //var id = GetIdColumn(obj.GetType());
            var rid = obj.RID;
            var ridcol = (from a in ReflectionTool.FieldsAndPropertiesOf(obj.GetType())
                          where ReflectionTool.GetAttributeFrom<ReliableIdAttribute>(a) != null
                          select a).FirstOrDefault();
            string ridname = "RID";
            if (ridcol != null) {
                ridname = ridcol.Name;
            }

            var query = new QueryBuilder($"DELETE FROM {obj.GetType().Name} WHERE {ridname}=@rid", obj.RID);
            retv = Execute(query) > 0;
            return retv;
        }

        public bool Delete<T>(Expression<Func<T, bool>> conditions) where T : IDataObject, new() {
            bool retv = false;
            if (SqlConnection?.State != ConnectionState.Open) {
                Access((bd) => {
                    retv = bd.Delete<T>(conditions);
                });
                return retv;
            }
            T tabela = (T)Activator.CreateInstance(typeof(T));
            var id = GetIdColumn<T>();
            var p = new PrefixMaker();
            var join = MakeJoin(
                    (q) => {
                        // Starting with T itself
                        q.AggregateRoot<T>(p.GetAliasFor("root", typeof(T).Name)).As(p.GetAliasFor("root", typeof(T).Name));
                        MakeQueryAggregations(ref q, typeof(T), "root", typeof(T).Name, p, false);
                    });
            var query = new QueryBuilder($"DELETE FROM {typeof(T).Name.ToLower()} WHERE ");
            query.Append($"{id} IN (SELECT a_{id} as {id} FROM (");
            query.Append(join.GenerateQuery(DefaultQueryGenerator, new ConditionParser(p).ParseExpression<T>(conditions)));
            query.Append(") sub)");
            retv = Execute(query) > 0;
            return retv;
        }

        #region **** BDados API ****
        int fail = 0;
        private List<Task<Object>> Workers = new List<Task<Object>>();
        private static int accessCount = 0;

        public bool Open() {
            try {
                Logger.WriteLog($"[{accessId}] BDados Open Connection --");
                if (SqlConnection == null)
                    SqlConnection = new MySqlConnection(Configuration.GetConnectionString());

                if (SqlConnection.State != ConnectionState.Open)
                    SqlConnection.Open();
                _simmultaneousConnections++;
            }
            catch (MySqlException x) {
                Logger.WriteLog($"[{accessId}] BDados Open: {x.Message}");
                Logger.WriteLog(x.Message);
                Logger.WriteLog(x.StackTrace);
                Logger.WriteLog($"BDados Open: {x.Message}");
                if (++fail < 50) {
                    System.Threading.Thread.Sleep(25);
                    return Open();
                }
                else {
                    throw new Exception("Failed to Open Mysql Connection.");
                }
            }
            catch (Exception x) {
                Logger.WriteLog($"[{accessId}] BDados Open: {x.Message}");
            }
            fail = 0;
            return SqlConnection.State == ConnectionState.Open;
        }

        //public delegate void FuncoesDados(BDados banco);
        //public delegate void TrataExceptions(Exception x);

        public Object ScalarQuery(String query, params Object[] args) {
            Object retv = null;
            if (SqlConnection?.State != ConnectionState.Open) {
                Access((bd) => {
                    retv = (bd as MySqlDataAccessor)?.ScalarQuery(query, args);
                });
                return retv;
            }
            try {
                retv = Query(query, args).Rows[0].Field<Object>(0);
            }
            catch (Exception) {
            }
            return retv;
        }

        public T ScalarQuery<T>(String query, params Object[] args) {
            T retv = default(T);
            if (SqlConnection?.State != ConnectionState.Open) {
                Access((bd) => {
                    if (bd is MySqlDataAccessor)
                        retv = (bd as MySqlDataAccessor).ScalarQuery<T>(query, args);
                });
                return retv;
            }
            try {
                retv = Query(query, args).Rows[0].Field<T>(0);
            }
            catch (Exception) {
            }
            return retv;
        }

        public IJoinBuilder MakeJoin(Action<Entity.JoinDefinition> fn) {
            var retv = new JoinObjectBuilder(this, fn);
            return retv;
        }

        public IQueryGenerator DefaultQueryGenerator {
            get {
                return GetQueryGenerator();
            }
        }
        public IQueryGenerator GetQueryGenerator() {
            switch (Configuration.Provider) {
                case DataProvider.MySql:
                    return new MySqlQueryGenerator();

                default:
                    throw new BDadosException("No valid data provider informed, impossible to get a query generator!");
            }
        }

        public void GenerateValueObjectDefinitions(String defaultNamespace, String baseDir) {
            Access((bd) => {
                DataTable t = Query("SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA=@1;", this.Configuration.Database);
                DataTable cols = Query("SELECT * FROM information_schema.columns WHERE TABLE_SCHEMA=@1", this.Configuration.Database);
                for (int i = 0; i < t.Rows.Count; i++) {
                    String Tabela = t.Rows[i].Field<String>("TABLE_NAME");
                    List<String> lines = new List<String>();
                    lines.Add($"// --------------------------------------------------");
                    lines.Add($"// BDados v{FTH.Version}");
                    lines.Add($"// Arquivo gerado automaticamente.");
                    lines.Add($"// --------------------------------------------------");
                    lines.Add("using System;");
                    lines.Add("using Figlotech.BDados.Attributes;");
                    lines.Add("using Figlotech.BDados.Interfaces;");
                    lines.Add("using Figlotech.BDados.Entity;");
                    lines.Add("");
                    lines.Add($"// ------------------------------------------");
                    lines.Add($"// Tabela {Tabela} ");
                    lines.Add($"// ------------------------------------------");
                    lines.Add($"namespace {defaultNamespace} {{");
                    lines.Add($"\t public partial class {Tabela} : BaseDataObject " + "{");
                    for (int c = 0; c < cols.Rows.Count; c++) {
                        var thisCol = cols.Rows[c];
                        if (thisCol.Field<String>("TABLE_NAME") != Tabela)
                            continue;
                        if (thisCol.Field<String>("COLUMN_NAME") == "Id" ||
                            thisCol.Field<String>("COLUMN_NAME") == "RID")
                            continue;
                        StringBuilder attLineOptions = new StringBuilder();
                        List<String> lineOptions = new List<String>();
                        //for(int x = 0; x < cols.Columns.Count; x++) {
                        //    Console.Write(thisCol.Field<Object>(x));
                        //    Console.Write("|");
                        //}
                        //Logger.WriteLog();
                        lines.Add("");
                        if (thisCol.Field<String>("COLUMN_KEY") == "PRI") {
                            lineOptions.Add("PrimaryKey=true");
                            lines.Add("\t\t[PrimaryKey]");
                        }
                        ulong? l = thisCol.Field<ulong?>("CHARACTER_MAXIMUM_LENGTH");
                        if (l != null && l > 0)
                            lineOptions.Add($"Size={l}");
                        if ("YES" == (thisCol.Field<String>("IS_NULLABLE")))
                            lineOptions.Add("AllowNull=true");
                        if (thisCol.Field<String>("COLUMN_KEY") == "UNI")
                            lineOptions.Add("Unique=true");
                        if (thisCol.Field<Object>("COLUMN_DEFAULT") != null) {
                            Object defVal = thisCol.Field<Object>("COLUMN_DEFAULT");
                            if (defVal is String)
                                defVal = "\"" + defVal + "\"";
                            else
                                defVal = defVal.ToString().ToLower();
                            lineOptions.Add($"DefaultValue={defVal}");
                        }
                        for (int a = 0; a < lineOptions.Count; a++) {
                            attLineOptions.Append(lineOptions[a]);
                            if (a < lineOptions.Count - 1)
                                attLineOptions.Append(", ");
                        }
                        lines.Add($"\t\t[Field({attLineOptions.ToString()})]");

                        String tipo = "String";
                        bool usgn = thisCol.Field<String>("COLUMN_TYPE").ToLower().Contains("unsigned");
                        switch (thisCol.Field<String>("DATA_TYPE").ToUpper()) {
                            case "VARCHAR":
                            case "CHAR":
                                tipo = "String"; break;
                            case "BIT":
                            case "TINYINT":
                                tipo = "bool"; break;
                            case "INT":
                                tipo = (usgn ? "u" : "") + "int"; break;
                            case "BIGINT":
                                tipo = (usgn ? "u" : "") + "long"; break;
                            case "SMALLINT":
                                tipo = (usgn ? "u" : "") + "short"; break;
                            case "FLOAT":
                            case "SINGLE":
                                tipo = "float"; break;
                            case "DOUBLE":
                                tipo = "double"; break;
                            case "DATETIME":
                            case "TIMESTAMP":
                            case "DATE":
                            case "TIME":
                                tipo = "DateTime"; break;
                        }
                        var nable = tipo != "String" && "YES" == (thisCol.Field<String>("IS_NULLABLE")) ? "?" : "";
                        lines.Add($"\t\tpublic {tipo}{nable} {thisCol.Field<String>("COLUMN_NAME")};");
                    }
                    lines.Add("\t}");
                    lines.Add("}");
                    File.WriteAllLines(Path.Combine(baseDir, Tabela + ".cs"), lines);
                }
            });
        }

        public async Task AsyncAccess(Action<IDataAccessor> function, Action onFinish = null, Action<Exception> handler = null) {
            Task<Object> t = Task.Run<Object>(() => Access(function, handler));
            Workers.Add(t);
            Workers.RemoveAll(s => s.Status == TaskStatus.RanToCompletion);
            await t;
            onFinish.Invoke();
        }

        public IDataAccessor MakeNew() {
            return new MySqlDataAccessor(Configuration);
        }

        int accessId = 0;

        public Object Access(Action<IRdbmsDataAccessor> functions, Action<Exception> handler = null) {
            int aid = accessId;
            try {
                if (SqlConnection?.State == ConnectionState.Open) {
                    lock (SqlConnection) {
                        functions.Invoke(this);
                    }
                    return null;
                }
                else {
                    aid = ++accessId;
                    if (Bench == null) {
                        Bench = new Benchmarker($"---- Access [{++aid}]");
                        Bench.WriteToOutput = showPerformanceLogs;
                    }
                    Open();
                    lock (SqlConnection) {
                        functions.Invoke(this);
                    }
                }
                var total = Bench?.TotalMark();
                Logger.WriteLog(String.Format("---- Access [{0}] returned OK: [{1} ms]", aid, total));
                return null;
            }
            catch (Exception x) {
                var total = Bench?.TotalMark();
                Logger.WriteLog(String.Format("---- Access [{0}] returned WITH ERRORS: [{1} ms]", aid, total));
                var ex = x;
                Logger.WriteLog("Detalhes dessa exception:");
                while (ex != null && ex.InnerException != ex) {
                    Logger.WriteLog(String.Format("{0} - {1}", ex.Message, ex.StackTrace));
                    ex = ex.InnerException;
                }
                //if (WorkingTypes.Length > 0)
                //    //FTH.AsyncOp(() => {
                //    Access((bd) => {
                //        bd.CheckStructure(WorkingTypes, false);
                //    });
                ////});
                if (this.RethrowExceptions) {
                    throw x;
                }

                if (handler != null)
                    handler.Invoke(x);
                return null;
            }
            finally {
                if (!Configuration.ContinuousConnection) {
                    Close();
                }
                //var total = Bench?.TotalMark();
                //Logger.WriteLog(String.Format("(Total: {0,0} milis)", total));
            }

        }

        public DataTable Query(IQueryBuilder query) {
            if (query == null || query.GetCommandText() == null) {
                return new DataTable();
            }
            DateTime Inicio = DateTime.Now;
            if (SqlConnection?.State != ConnectionState.Open) {
                DataTable retv = new DataTable();
                Access((bd) => { retv = bd.Query(query); });
                return retv;
            }

            String QueryText = query.GetCommandText();

            lock (SqlConnection) {
                using (MySqlCommand Comando = SqlConnection.CreateCommand()) {
                    Comando.CommandText = QueryText;
                    Comando.CommandTimeout = Configuration.Timeout;
                    int i = 0;
                    Logger.WriteLog($"[{accessId}] -- Query: {QueryText}");
                    // Adiciona os parametros
                    foreach (var param in query.GetParameters()) {
                        Comando.Parameters.AddWithValue(param.Key, param.Value);
                        Logger.WriteLog($"[{accessId}] @{param.Key} = {param.Value?.ToString() ?? "null"}");
                    }
                    // --
                    DataTable retv = new DataTable();
                    using (MySqlDataAdapter Adaptador = new MySqlDataAdapter(Comando)) {
                        DataSet ds = new DataSet();
                        try {
                            Adaptador.Fill(ds);
                            int resultados = 0;
                            if (ds.Tables.Count < 1) {
                                throw new BDadosException("Database did not return any table.");
                            }
                            resultados = ds.Tables[0].Rows.Count;
                            Logger.WriteLog($"[{accessId}] -------- Queried [OK] ({resultados} results) [{DateTime.Now.Subtract(Inicio).TotalMilliseconds} ms]");
                            retv = ds.Tables[0];
                        }
                        catch (Exception x) {
                            Logger.WriteLog($"[{accessId}] -------- Error: {x.Message} ([{DateTime.Now.Subtract(Inicio).TotalMilliseconds} ms]");
                            Logger.WriteLog(x.Message);
                            Logger.WriteLog(x.StackTrace);
                            throw x;
                        }
                        finally {
                            Adaptador.Dispose();
                            Comando.Dispose();
                            Logger.WriteLog("------------------------------------");
                        }
                    }
                    return retv;
                }
            }
        }

        public DataTable Query(String Query, params Object[] args) {
            return this.Query(new QueryBuilder(Query, args));
        }

        public int Execute(IQueryBuilder query) {
            if (query == null)
                return 0;

            if (SqlConnection?.State != ConnectionState.Open) {
                int retv = -1;
                Access((bd) => { retv = bd.Execute(query); });
                return retv;
            }
            Bench.Mark("--");

            Logger.WriteLog($"[{accessId}] -- Execute: {query.GetCommandText()} [{Configuration.Timeout}s timeout]");
            foreach (var param in query.GetParameters()) {
                Logger.WriteLog($"[{accessId}] @{param.Key} = {param.Value?.ToString() ?? "null"}");
            }
            using (MySqlCommand scom = SqlConnection.CreateCommand()) {
                try {
                    scom.CommandText = query.GetCommandText();
                    foreach (var param in query.GetParameters()) {
                        scom.Parameters.AddWithValue(param.Key, param.Value);
                    }
                    scom.CommandTimeout = Configuration.Timeout;
                    Logger.WriteLog(scom.CommandText);
                    Bench.Mark("Prepared Statement");
                    int result = scom.ExecuteNonQuery();
                    var elaps = Bench.Mark("Executed Statement");
                    Logger.WriteLog($"[{accessId}] --------- Executed [OK] ({result} lines affected) [{elaps} ms]");
                    return result;
                }
                catch (Exception x) {
                    Logger.WriteLog($"[{accessId}] -------- Error: {x.Message} ([{Bench.Mark("Error")} ms]");
                    Logger.WriteLog(x.Message);
                    Logger.WriteLog(x.StackTrace);
                    Logger.WriteLog($"BDados Execute: {x.Message}");
                    throw x;
                }
                finally {
                    Logger.WriteLog("------------------------------------");
                    scom.Dispose();
                }
            }
        }
        public int Execute(String Query, params Object[] args) {
            return Execute(new QueryBuilder(Query, args));
        }

        public void Close() {
            try {
                SqlConnection.Close();
            }
            catch (Exception x) {
                Logger.WriteLog($"[{accessId}] BDados Close: {x.Message}");
            }
        }

        public List<T> ExecuteProcedure<T>(params object[] args) where T : ProcedureResult {
            DateTime inicio = DateTime.Now;
            List<T> retv = new List<T>();
            Logger.WriteLog($"[{accessId}] Executar procedure -- ");
            using (SqlConnection = new MySqlConnection(Configuration.GetConnectionString())) {
                Open();
                try {
                    Logger.WriteLog($"[{accessId}] Abriu conexão em [{DateTime.Now.Subtract(inicio).TotalMilliseconds} ms] -- ");
                    QueryBuilder query = (QueryBuilder)GetQueryGenerator().GenerateCallProcedure(typeof(T).Name, args);
                    DataTable dt = Query(query);
                    foreach (DataRow r in dt.Rows) {
                        T newval = Activator.CreateInstance<T>();
                        foreach (FieldInfo f in newval.GetType().GetFields()) {
                            try {
                                Object v = r.Field<Object>(f.Name);
                                if (v == null) {
                                    f.SetValue(newval, null);
                                }
                                else {
                                    f.SetValue(newval, r.Field<Object>(f.Name));
                                }
                            }
                            catch (Exception x) {
                                throw x;
                            }
                        }
                        foreach (PropertyInfo p in newval.GetType().GetProperties()) {
                            try {
                                Object v = r.Field<Object>(p.Name);
                                if (v == null) {
                                    p.SetValue(newval, null);
                                }
                                else if (Nullable.GetUnderlyingType(p.PropertyType) != null) {
                                    p.SetValue(newval, r.Field<Object>(p.Name));
                                }
                                else {
                                    p.SetValue(newval, Convert.ChangeType(r.Field<Object>(p.Name), p.PropertyType));
                                }
                            }
                            catch (Exception x) {
                                throw x;
                            }
                        }
                        retv.Add(newval);
                    }
                }
                finally {
                    Close();
                }
            }
            Logger.WriteLog($"[{accessId}] Total Procedure [{DateTime.Now.Subtract(inicio).TotalMilliseconds} ms] -- ");
            return retv;
        }


        //private PrefixMaker prefixer = new PrefixMaker();
        /*
         * HERE BE DRAGONS
         * jk.
         * It works and it is actually really good
         * But the logic behind this is crazy,
         * it took a lot of coffee to achieve.
         */
        private void MakeQueryAggregations(ref JoinDefinition query, Type theType, String parentAlias, String nameofThis, PrefixMaker prefixer, bool Linear = false) {
            var membersOfT = new List<MemberInfo>();
            membersOfT.AddRange(theType.GetFields());
            membersOfT.AddRange(theType.GetProperties());
            var reflectedJoinMethod = query.GetType().GetMethod("Join");

            String thisAlias = prefixer.GetAliasFor(parentAlias, nameofThis);

            // Iterating through AggregateFields and AggregateObjects
            foreach (var field in membersOfT.Where(
                    (f) =>
                        f.GetCustomAttribute<AggregateFieldAttribute>() != null ||
                        f.GetCustomAttribute<AggregateObjectAttribute>() != null)) {
                var memberType = ReflectionTool.GetTypeOf(field);
                var type =
                    field.GetCustomAttribute<AggregateFieldAttribute>()?.RemoteObjectType ?? ReflectionTool.GetTypeOf(field);
                var key =
                    field.GetCustomAttribute<AggregateFieldAttribute>()?.ObjectKey ??
                    field.GetCustomAttribute<AggregateObjectAttribute>()?.ObjectKey;
                var infoField = field.GetCustomAttribute<AggregateFieldAttribute>();
                var infoObj = field.GetCustomAttribute<AggregateObjectAttribute>();
                String childAlias;

                // This inversion principle might be fucktastic.
                if (infoField != null) {
                    childAlias = prefixer.GetAliasFor(thisAlias, infoField?.ObjectKey ?? field.Name);
                    var qjoins = query.Joins.Where((a) => a.Alias == childAlias);
                    if (qjoins.Any()) {
                        qjoins.First().Excludes.Remove(infoField?.RemoteField);
                        continue;
                    }
                }
                else {
                    childAlias = prefixer.GetAliasFor(thisAlias, field.Name);
                }

                String OnClause = $"{thisAlias}.{key}={childAlias}.RID";

                if (!ReflectionTool.TypeContains(theType, key)) {
                    OnClause = $"{thisAlias}.RID={childAlias}.{key}";
                }
                var joh = reflectedJoinMethod.MakeGenericMethod(type).Invoke(
                    query,
                    // Alias a bit confusing I bet, but ok.
                    new Object[] { childAlias, OnClause, JoinType.LEFT }
                // ON CLAUSE
                );
                // Parent Alias is typeof(T).Name
                // Child Alias is field.Name
                // The ultra supreme gimmick mode reigns supreme here.
                joh.GetType().GetMethod("As").Invoke(joh, new object[] { childAlias });
                if (field.GetCustomAttribute<AggregateFieldAttribute>() != null) {
                    joh.GetType().GetMethod("OnlyFields").Invoke(joh, new object[] { new string[] { field.GetCustomAttribute<AggregateFieldAttribute>().RemoteField } });
                }

                if (!Linear && field.GetCustomAttribute<AggregateObjectAttribute>() != null) {
                    MakeQueryAggregations(ref query, ReflectionTool.GetTypeOf(field), thisAlias, field.Name, prefixer);
                }
            }
            // Iterating through AggregateFarFields
            foreach (var field in membersOfT.Where(
                    (f) =>
                        f.GetCustomAttribute<AggregateFarFieldAttribute>() != null)) {
                var memberType = ReflectionTool.GetTypeOf(field);
                var info = field.GetCustomAttribute<AggregateFarFieldAttribute>();
                String childAlias = prefixer.GetAliasFor(thisAlias, info.ImediateKey ?? field.Name);
                String farAlias = prefixer.GetAliasFor(childAlias, info.FarKey ?? info.FarField);

                var qimediate = query.Joins.Where((j) => j.Alias == childAlias);
                if (!qimediate.Any()) {

                    string OnClause = $"{thisAlias}.{info.ImediateKey}={childAlias}.RID";
                    // This inversion principle will be fucktastic.
                    // But has to be this way for now.
                    if (!ReflectionTool.TypeContains(theType, info.ImediateKey)) {
                        OnClause = $"{thisAlias}.RID={childAlias}.{info.ImediateKey}";
                    }
                    if (query.Joins.Where((a) => a.Alias == childAlias).Any())
                        continue;
                    var joh1 = reflectedJoinMethod.MakeGenericMethod(info.ImediateType).Invoke(
                        query,
                        // Alias a bit confusing I bet, but ok.
                        new Object[] { childAlias, OnClause, JoinType.LEFT }
                    // ON CLAUSE
                    );
                    // Parent Alias is typeof(T).Name
                    // Child Alias is field.Name
                    // The ultra supreme gimmick mode reigns supreme here.
                    joh1.GetType().GetMethod("As").Invoke(joh1, new object[] { childAlias });
                    joh1.GetType().GetMethod("OnlyFields").Invoke(joh1, new object[] { new string[] { info.FarKey } });

                }

                var qfar = query.Joins.Where((j) => j.Alias == farAlias);
                if (qfar.Any()) {
                    qfar.First().Excludes.Remove(info.FarField);
                    continue;
                }
                else {
                    String OnClause2 = $"{childAlias}.{info.FarKey}={farAlias}.RID";
                    // This inversion principle will be fucktastic.
                    // But has to be this way for now.
                    if (!ReflectionTool.TypeContains(info.ImediateType, info.FarKey)) {
                        OnClause2 = $"{childAlias}.RID={farAlias}.{info.FarKey}";
                    }

                    var joh2 = reflectedJoinMethod.MakeGenericMethod(info.FarType).Invoke(
                        query,
                        // Alias a bit confusing I bet, but ok.
                        new Object[] { farAlias, OnClause2, JoinType.LEFT }
                    // ON CLAUSE
                    );
                    // Parent Alias is typeof(T).Name
                    // Child Alias is field.Name
                    // The ultra supreme gimmick mode reigns supreme here.
                    joh2.GetType().GetMethod("As").Invoke(joh2, new object[] { farAlias });
                    joh2.GetType().GetMethod("OnlyFields").Invoke(joh2, new object[] { new string[] { info.FarField } });
                }
            }
            // We want to skip aggregate lists 
            // When doing linear aggregate loads
            // The linear option is just to provide faster
            // and shallower information.
            if (Linear)
                return;
            // Iterating through AggregateLists
            foreach (var field in membersOfT.Where(
                    (f) =>
                        f.GetCustomAttribute<AggregateListAttribute>() != null)) {
                var memberType = ReflectionTool.GetTypeOf(field);
                var info = field.GetCustomAttribute<AggregateListAttribute>();
                String childAlias = prefixer.GetAliasFor(thisAlias, field.Name);

                String OnClause = $"{childAlias}.{info.RemoteField}={thisAlias}.RID";
                // Yuck
                if (!ReflectionTool.TypeContains(info.RemoteObjectType, info.RemoteField)) {
                    OnClause = $"{childAlias}.RID={thisAlias}.{info.RemoteField}";
                }
                var joh = reflectedJoinMethod.MakeGenericMethod(info.RemoteObjectType).Invoke(
                    query,
                    // Alias
                    new Object[] { childAlias, OnClause, JoinType.RIGHT }
                // ON CLAUSE
                );
                // The ultra supreme gimmick mode reigns supreme here.
                joh.GetType().GetMethod("As").Invoke(joh, new object[] { childAlias });
                if (!Linear)
                    MakeQueryAggregations(ref query, info.RemoteObjectType, thisAlias, field.Name, prefixer);
            }
        }


        private void MakeBuildAggregations(ref BuildParametersHelper build, Type theType, String parentAlias, String nameofThis, PrefixMaker prefixer, bool Linear = false) {
            // Don't try this at home kids.
            var membersOfT = new List<MemberInfo>();
            membersOfT.AddRange(theType.GetFields());
            membersOfT.AddRange(theType.GetProperties());

            String thisAlias = prefixer.GetAliasFor(parentAlias, nameofThis);
            // Iterating through AggregateFields
            foreach (var field in membersOfT.Where((f) => f.GetCustomAttribute<AggregateFieldAttribute>() != null)) {
                var memberType = ReflectionTool.GetTypeOf(field);
                var info = field.GetCustomAttribute<AggregateFieldAttribute>();
                String childAlias = prefixer.GetAliasFor(thisAlias, info?.ObjectKey ?? field.Name);
                build.AggregateField(thisAlias, childAlias, info.RemoteField, field.Name);
            }
            // Iterating through AggregateFarFields
            foreach (var field in membersOfT.Where(
                    (f) =>
                        f.GetCustomAttribute<AggregateFarFieldAttribute>() != null)) {
                var memberType = ReflectionTool.GetTypeOf(field);
                var info = field.GetCustomAttribute<AggregateFarFieldAttribute>();
                String childAlias = prefixer.GetAliasFor(thisAlias, info?.ImediateKey ?? field.Name);
                String farAlias = prefixer.GetAliasFor(childAlias, info?.FarKey ?? info.FarField);
                build.AggregateField(thisAlias, farAlias, info.FarField, field.Name);
            }
            // Iterating through AggregateObjects
            foreach (var field in membersOfT.Where((f) => f.GetCustomAttribute<AggregateObjectAttribute>() != null)) {
                var memberType = ReflectionTool.GetTypeOf(field);
                var info = field.GetCustomAttribute<AggregateObjectAttribute>();
                String childAlias = prefixer.GetAliasFor(thisAlias, field.Name);
                build.AggregateObject(thisAlias, childAlias, field.Name);
                if (!Linear)
                    MakeBuildAggregations(ref build, ReflectionTool.GetTypeOf(field), thisAlias, field.Name, prefixer);
            }
            // Iterating through ComputeFields
            foreach (var field in membersOfT.Where((f) => ReflectionTool.GetTypeOf(f) == typeof(ComputeField))) {
                var memberType = ReflectionTool.GetTypeOf(field);
                String childAlias = prefixer.GetAliasFor(thisAlias, field.Name);
                if (field is FieldInfo) {
                    build.ComputeField(thisAlias, field.Name.Replace("Compute", ""), (ComputeField)((FieldInfo)field).GetValue(null));
                }
                if (field is PropertyInfo) {
                    build.ComputeField(thisAlias, field.Name.Replace("Compute", ""), (ComputeField)((PropertyInfo)field).GetValue(null));
                }
            }
            // We want to skip aggregate lists 
            // When doing linear aggregate loads
            // To avoid LIMIT ORDER BY MySQL dead-lock
            if (Linear)
                return;
            // Iterating through AggregateLists
            foreach (var field in membersOfT.Where((f) => f.GetCustomAttribute<AggregateListAttribute>() != null)) {
                var memberType = ReflectionTool.GetTypeOf(field);
                var info = field.GetCustomAttribute<AggregateListAttribute>();
                String childAlias = prefixer.GetAliasFor(thisAlias, field.Name);
                build.AggregateList(thisAlias, childAlias, field.Name);
                if (!Linear)
                    MakeBuildAggregations(ref build, info.RemoteObjectType, thisAlias, field.Name, prefixer);
            }
        }

        public RecordSet<T> AggregateLoad<T>(Expression<Func<T, bool>> cnd = null, int? limit = null, int? page = null, int PageSize = 200, MemberInfo OrderingType = null, OrderingType Ordering = OrderingType.Asc, bool Linear = false) where T : IDataObject, new() {
            RecordSet<T> retv = new RecordSet<T>();

            var prefixer = new PrefixMaker();
            var Members = ReflectionTool.FieldsAndPropertiesOf(typeof(T));
            bool proofproof = false;
            foreach (var a in Members) {
                proofproof =
                    a.GetCustomAttribute<AggregateFieldAttribute>() != null ||
                    a.GetCustomAttribute<AggregateFarFieldAttribute>() != null ||
                    a.GetCustomAttribute<AggregateObjectAttribute>() != null ||
                    a.GetCustomAttribute<AggregateListAttribute>() != null;
                if (proofproof)
                    break;
            }

            Logger.WriteLog($"Running Aggregate Load All for {typeof(T).Name.ToLower()}? {proofproof}.");
            // CLUMSY
            if (proofproof) {
                var membersOfT = new List<MemberInfo>();
                membersOfT.AddRange(typeof(T).GetFields());
                membersOfT.AddRange(typeof(T).GetProperties());

                var join = MakeJoin(
                        (query) => {
                            // Starting with T itself
                            query.AggregateRoot<T>(prefixer.GetAliasFor("root", typeof(T).Name)).As(prefixer.GetAliasFor("root", typeof(T).Name));
                            MakeQueryAggregations(ref query, typeof(T), "root", typeof(T).Name, prefixer, Linear);
                        });
                var builtConditions = (cnd == null ? new QueryBuilder("TRUE") : new ConditionParser(prefixer).ParseExpression(cnd));
                var builtConditionsRoot = (cnd == null ? new QueryBuilder("TRUE") : new ConditionParser(prefixer).ParseExpression(cnd, false));
                builtConditions.If(OrderingType != null).Then()
                                    .Append($"ORDER BY a.{OrderingType?.Name} {Ordering.ToString().ToUpper()}")
                                .EndIf();

                builtConditions.EndIf();
                var dynamicJoinJumble = join.BuildObject<T>(
                        (build) => {
                            MakeBuildAggregations(ref build, typeof(T), "root", typeof(T).Name, prefixer, Linear);
                        }, builtConditions, page, limit, builtConditionsRoot.Append($"{(OrderingType != null ? $"ORDER BY {OrderingType?.Name} {Ordering.ToString().ToUpper()}" : "")}"));

                // Yay.
                // Confusing but effective. Okay das.
                //List<T> list = dynamicJoinJumble.Qualify<T>();
                //for (int i = 0; i < list.Count; i++) {
                //    list[i].DataAccessor = this;
                //    list[i].SelfCompute(i > 0 ? list[i - 1] : default(T));
                //    retv.Add(list[i]);
                //}
            }
            else {
                Logger.WriteLog(cnd?.ToString());

                retv.AddRange(LoadAll<T>(new ConditionParser().ParseExpression<T>(cnd)
                    .If(OrderingType != null).Then()
                        .Append($"ORDER BY {OrderingType?.Name} {Ordering.ToString().ToUpper()}")
                    .EndIf()
                    .If(limit != null).Then()
                        .Append($"LIMIT {(page != null && page > 0 ? $"{(page - 1) * limit}," : "")}{limit}"))
                );
            }
            return retv;
        }

        public bool DeleteWhereRidNotIn<T>(Expression<Func<T, bool>> cnd, RecordSet<T> list) where T : IDataObject, new() {
            int retv = 0;
            if (list == null)
                return true;

            var id = GetIdColumn<T>();
            var rid = GetRidColumn<T>();
            Access(
                (bd) => {
                    var query = new QueryBuilder($"DELETE FROM {typeof(T).Name.ToLower()} WHERE ");
                    if (cnd != null) {
                        query.Append($"{rid} IN (SELECT {rid} FROM (SELECT {rid} FROM {typeof(T).Name.ToLower()} AS a WHERE ");
                        query.Append(new ConditionParser().ParseExpression<T>(cnd));
                        query.Append(") sub)");
                    }
                    if (list.Count > 0) {
                        query.Append($"AND {rid} NOT IN (");
                        for (var i = 0; i < list.Count; i++) {
                            query.Append($"@{IntEx.GerarShortRID()}", list[i].RID);
                            if (i < list.Count - 1)
                                query.Append(",");
                        }
                        query.Append(")");
                    }
                    retv = Execute(query);
                }
            );
            return retv > 0;
        }

        public bool SaveRecordSet<T>(RecordSet<T> rs) where T : IDataObject, new() {
            bool retv = true;
            if (rs.Count == 0)
                return true;
            for (int it = 0; it < rs.Count; it++) {
                if (rs[it].RID == null) {
                    rs[it].RID = FTH.GenerateRID();
                }
            }
            var members = new List<MemberInfo>();
            members.AddRange(typeof(T).GetFields());
            members.AddRange(typeof(T).GetProperties());
            int i = 0;
            int cnt = 0;
            int cut = 100;
            RecordSet<T> paleative = new RecordSet<T>();
            int rst = 0;
            while (i * cut < rs.Count) {
                var sub = new RecordSet<T>();
                sub.AddRange(rs.Skip(i * cut).Take(Math.Min(rs.Count, cut)));
                //rs.RemoveRange(0, Math.Min(rs.Count, cut));
                rst += Execute(GetQueryGenerator().GenerateMultiInsert(sub));
                rst += Execute(GetQueryGenerator().GenerateMultiUpdate(sub));
                sub.Clear();
                i++;
            }
            return retv;
        }
        #endregion *****************
        //
    }
}