using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Reflection;
using System.Linq;
using System.Linq.Expressions;
using MySql.Data.MySqlClient;
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.Interfaces;
using Figlotech.BDados.Builders;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using Figlotech.BDados.Helpers;
using Figlotech.Core;

namespace Figlotech.BDados {
    public class MySqlDataAccessor : IRdbmsDataAccessor {

        public ILogger Logger { get; set; }
        public Type[] _workingTypes = new Type[0];
        public Type[] WorkingTypes {
            get { return _workingTypes; }
            set {
                _workingTypes = value.Where(t => t.GetInterfaces().Contains(typeof(IDataObject))).ToArray();
            }
        }

        public IQueryGenerator QueryGenerator { get; set; } = new MySqlQueryGenerator();

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
            } else {
                T quickSave = Default();
                quickSave.RID = new T().RID;
                SaveItem(quickSave);
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
            } else {
                T quickSave = Default();
                SaveItem(quickSave);
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
            } else {
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
            } else {
                if (!info.AllowNull) {
                    options += " NOT NULL";
                } else if (Nullable.GetUnderlyingType(field.GetType()) == null && field.FieldType.IsValueType && !info.AllowNull) {
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

            input.CreatedTime = DateTime.UtcNow;
            if (input.IsPersisted) {
                rs = Execute(GetQueryGenerator().GenerateUpdateQuery(input));
                retv = true;
                if (fn != null)
                    fn.Invoke();
                return retv;
            }

            rs = Execute(GetQueryGenerator().GenerateInsertQuery(input));
            if (rs == 0) {
                Logger?.WriteLog("** Something went SERIOUSLY NUTS in SaveItem<T> **");
            }
            retv = rs > 0;
            if (retv && !input.IsPersisted) {
                long retvId = 0;
                var ridAtt = ReflectionTool.FieldsAndPropertiesOf(input.GetType()).Where((f) => f.GetCustomAttribute<ReliableIdAttribute>() != null);
                if (ridAtt.Any()) {
                    DataTable dt = Query($"SELECT {id} FROM " + input.GetType().Name + $" WHERE {rid}=@1", input.RID);
                    retvId = dt.Rows[0].Field<long>(id);
                } else {
                    retvId = (int)ScalarQuery($"SELECT last_insert_id();");
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

        public List<T> Query<T>(IQueryBuilder query) where T : new() {
            if (query == null) {
                return new List<T>();
            }
            var retv = new List<T>();
            DataTable resultado = Query(query);
            FTH.Map<T>(retv, resultado);
            return retv;
        }

        public List<T> Query<T>(string queryString, params object[] args) where T : new() {
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
                    .Where(m => m.GetCustomAttribute<FieldAttribute>() != null);
                var objBuilder = new ObjectReflector(add);
                foreach (var col in members) {
                    try {
                        if (!dt.Columns.Contains(col.Name)) continue;
                        var typeofCol = ReflectionTool.GetTypeOf(col);
                        Type t = typeofCol;
                        Object o = dt.Rows[i].Field<Object>(col.Name);
                        objBuilder[col] = o;
                    } catch (Exception) { }
                }
                retv = add;
            }

            return retv;
        }

        public T LoadByRid<T>(String RID) where T : IDataObject, new() {
            T retv = new T();
            if (SqlConnection?.State != ConnectionState.Open) {
                Access((bd) => {
                    retv = bd.LoadByRid<T>(RID);
                }, x => {
                    throw x;
                });
                return retv;
            }
            var rid = GetRidColumn<T>();

            DataTable dt = Query(GetQueryGenerator().GenerateSelect<T>(new ConditionParametrizer($"{rid}=@1", RID)));
            int i = 0;
            if (dt.Rows.Count > 0) {
                var members = ReflectionTool.FieldsAndPropertiesOf(typeof(T))
                    .Where(m => m.GetCustomAttribute<FieldAttribute>() != null)
                    .ToList();
                var objBuilder = new ObjectReflector(retv);
                foreach (var col in members) {
                    if (!dt.Columns.Contains(col.Name)) continue;
                    var typeofCol = ReflectionTool.GetTypeOf(col);

                    Object o = dt.Rows[i].Field<Object>(col.Name);
                    objBuilder[col] = o;
                }
            } else {
                retv = default(T);
            }
            return retv;
        }

        public RecordSet<T> LoadAll<T>(String where = "TRUE", params object[] args) where T : IDataObject, new() {
            return LoadAll<T>(new QueryBuilder(where, args));
        }

        public RecordSet<T> LoadAll<T>(Expression<Func<T, bool>> conditions = null, int? page = null, int? limit = 200) where T : IDataObject, new() {
            var query = new ConditionParser().ParseExpression(conditions);
            if (page != null && limit != null)
                query.Append($"LIMIT {(page - 1) * limit}, {limit}");
            else if (limit != null)
                query.Append($"LIMIT {limit}");
            return LoadAll<T>(query);
        }

        Benchmarker Bench = null;

        public RecordSet<T> LoadAll<T>(IQueryBuilder condicoes) where T : IDataObject, new() {
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
                    Logger?.WriteLog("Query returned no results.");
                    return;
                }
                var fields = ReflectionTool.FieldsAndPropertiesOf(typeof(T))
                    .Where((a) => a.GetCustomAttribute<FieldAttribute>() != null)
                    .ToList();

                var objBuilder = new ObjectReflector();
                for (int i = 0; i < dt.Rows.Count; i++) {
                    T add = FTH.Map<T>(dt.Rows[i], dt.Columns);
                    retv.Add(add);
                }
                Bench.Mark("Build RecordSet");
            }, (x) => {
                Logger?.WriteLog(x.Message);
                Logger?.WriteLog(x.StackTrace);
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
                Logger?.WriteLog($"[{accessId}] BDados Open Connection --");
                if (SqlConnection == null)
                    SqlConnection = new MySqlConnection(Configuration.GetConnectionString());

                if (SqlConnection.State != ConnectionState.Open)
                    SqlConnection.Open();
                _simmultaneousConnections++;
            } catch (MySqlException x) {
                Logger?.WriteLog($"[{accessId}] BDados Open: {x.Message}");
                Logger?.WriteLog(x.Message);
                Logger?.WriteLog(x.StackTrace);
                Logger?.WriteLog($"BDados Open: {x.Message}");
                if (++fail < 50) {
                    System.Threading.Thread.Sleep(25);
                    return Open();
                } else {
                    throw new Exception("Failed to Open Mysql Connection.");
                }
            } catch (Exception x) {
                Logger?.WriteLog($"[{accessId}] BDados Open: {x.Message}");
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
            } catch (Exception) {
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
            } catch (Exception) {
            }
            return retv;
        }

        public IJoinBuilder MakeJoin(Action<JoinDefinition> fn) {
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
                        //    FTH.Write(thisCol.Field<Object>(x));
                        //    FTH.Write("|");
                        //}
                        //Logger?.WriteLog();
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
            var retv = new MySqlDataAccessor(Configuration);
            retv.Logger = this.Logger;
            return retv;
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
                } else {
                    aid = ++accessId;
                    if (Bench == null) {
                        Bench = new Benchmarker($"---- Access [{++aid}]");
                        Bench.WriteToStdout = showPerformanceLogs;
                    }
                    Open();
                    lock (SqlConnection) {
                        functions.Invoke(this);
                    }
                }
                var total = Bench?.TotalMark();
                Logger?.WriteLog(String.Format("---- Access [{0}] returned OK: [{1} ms]", aid, total));
                return null;
            } catch (Exception x) {
                var total = Bench?.TotalMark();
                Logger?.WriteLog(String.Format("---- Access [{0}] returned WITH ERRORS: [{1} ms]", aid, total));
                var ex = x;
                Logger?.WriteLog("Detalhes dessa exception:");
                while (ex != null && ex.InnerException != ex) {
                    Logger?.WriteLog(String.Format("{0} - {1}", ex.Message, ex.StackTrace));
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
            } finally {
                if (!Configuration.ContinuousConnection) {
                    Close();
                }
                //var total = Bench?.TotalMark();
                //Logger?.WriteLog(String.Format("(Total: {0,0} milis)", total));
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
                    Logger?.WriteLog($"[{accessId}] -- Query: {QueryText}");
                    // Adiciona os parametros
                    foreach (var param in query.GetParameters()) {
                        Comando.Parameters.AddWithValue(param.Key, param.Value);
                        var pval = $"'{param.Value?.ToString() ?? "null"}'";
                        if (param.Value is DateTime || param.Value is DateTime? && ((DateTime?)param.Value).HasValue) {
                            pval = ((DateTime)param.Value).ToString("yyyy-MM-dd HH:mm:ss");
                            pval = $"'{pval}'";
                        }
                        Logger?.WriteLog($"[{accessId}] SET @{param.Key} = {pval}");
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
                            Logger?.WriteLog($"[{accessId}] -------- Queried [OK] ({resultados} results) [{DateTime.Now.Subtract(Inicio).TotalMilliseconds} ms]");
                            retv = ds.Tables[0];
                        } catch (Exception x) {
                            Logger?.WriteLog($"[{accessId}] -------- Error: {x.Message} ([{DateTime.Now.Subtract(Inicio).TotalMilliseconds} ms]");
                            Logger?.WriteLog(x.Message);
                            Logger?.WriteLog(x.StackTrace);
                            throw x;
                        } finally {
                            Adaptador.Dispose();
                            Comando.Dispose();
                            Logger?.WriteLog("------------------------------------");
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

            Logger?.WriteLog($"[{accessId}] -- Execute: {query.GetCommandText()} [{Configuration.Timeout}s timeout]");
            foreach (var param in query.GetParameters()) {
                Logger?.WriteLog($"[{accessId}] @{param.Key} = {param.Value?.ToString() ?? "null"}");
            }
            using (MySqlCommand scom = SqlConnection.CreateCommand()) {
                try {
                    scom.CommandText = query.GetCommandText();
                    foreach (var param in query.GetParameters()) {
                        scom.Parameters.AddWithValue(param.Key, param.Value);
                    }
                    scom.CommandTimeout = Configuration.Timeout;
                    Logger?.WriteLog(scom.CommandText);
                    Bench.Mark("Prepared Statement");
                    int result = scom.ExecuteNonQuery();
                    var elaps = Bench.Mark("Executed Statement");
                    Logger?.WriteLog($"[{accessId}] --------- Executed [OK] ({result} lines affected) [{elaps} ms]");
                    return result;
                } catch (Exception x) {
                    Logger?.WriteLog($"[{accessId}] -------- Error: {x.Message} ([{Bench.Mark("Error")} ms]");
                    Logger?.WriteLog(x.Message);
                    Logger?.WriteLog(x.StackTrace);
                    Logger?.WriteLog($"BDados Execute: {x.Message}");
                    throw x;
                } finally {
                    Logger?.WriteLog("------------------------------------");
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
            } catch (Exception x) {
                Logger?.WriteLog($"[{accessId}] BDados Close: {x.Message}");
            }
        }

        public List<T> ExecuteProcedure<T>(params object[] args) where T : ProcedureResult {
            DateTime inicio = DateTime.Now;
            List<T> retv = new List<T>();
            Logger?.WriteLog($"[{accessId}] Executar procedure -- ");
            using (SqlConnection = new MySqlConnection(Configuration.GetConnectionString())) {
                Open();
                try {
                    Logger?.WriteLog($"[{accessId}] Abriu conexão em [{DateTime.Now.Subtract(inicio).TotalMilliseconds} ms] -- ");
                    QueryBuilder query = (QueryBuilder)GetQueryGenerator().GenerateCallProcedure(typeof(T).Name, args);
                    DataTable dt = Query(query);
                    foreach (DataRow r in dt.Rows) {
                        T newval = Activator.CreateInstance<T>();
                        foreach (FieldInfo f in newval.GetType().GetFields()) {
                            try {
                                Object v = r.Field<Object>(f.Name);
                                if (v == null) {
                                    f.SetValue(newval, null);
                                } else {
                                    f.SetValue(newval, r.Field<Object>(f.Name));
                                }
                            } catch (Exception x) {
                                throw x;
                            }
                        }
                        foreach (PropertyInfo p in newval.GetType().GetProperties()) {
                            try {
                                Object v = r.Field<Object>(p.Name);
                                if (v == null) {
                                    p.SetValue(newval, null);
                                } else if (Nullable.GetUnderlyingType(p.PropertyType) != null) {
                                    p.SetValue(newval, r.Field<Object>(p.Name));
                                } else {
                                    p.SetValue(newval, Convert.ChangeType(r.Field<Object>(p.Name), p.PropertyType));
                                }
                            } catch (Exception x) {
                                throw x;
                            }
                        }
                        retv.Add(newval);
                    }
                } finally {
                    Close();
                }
            }
            Logger?.WriteLog($"[{accessId}] Total Procedure [{DateTime.Now.Subtract(inicio).TotalMilliseconds} ms] -- ");
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
            var membersOfT = ReflectionTool.FieldsAndPropertiesOf(theType);
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
                } else {
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
                } else {
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
            var membersOfT = ReflectionTool.FieldsAndPropertiesOf(theType);

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
            //foreach (var field in membersOfT.Where((f) => ReflectionTool.GetTypeOf(f) == typeof(ComputeField))) {
            //    var memberType = ReflectionTool.GetTypeOf(field);
            //    String childAlias = prefixer.GetAliasFor(thisAlias, field.Name);
            //    if (field is FieldInfo) {
            //        build.ComputeField(thisAlias, field.Name.Replace("Compute", ""), (ComputeField)((FieldInfo)field).GetValue(null));
            //    }
            //    if (field is PropertyInfo) {
            //        build.ComputeField(thisAlias, field.Name.Replace("Compute", ""), (ComputeField)((PropertyInfo)field).GetValue(null));
            //    }
            //}
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

            Logger?.WriteLog($"Running Aggregate Load All for {typeof(T).Name.ToLower()}? {proofproof}.");
            // CLUMSY
            if (proofproof) {
                var membersOfT = ReflectionTool.FieldsAndPropertiesOf(typeof(T));

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
                retv = dynamicJoinJumble;
                // Yay.
                // Confusing but effective. Okay das.
                //List<T> list = dynamicJoinJumble.Qualify<T>();
                //for (int i = 0; i < list.Count; i++) {
                //    list[i].DataAccessor = this;
                //    list[i].SelfCompute(i > 0 ? list[i - 1] : default(T));
                //    retv.Add(list[i]);
                //}
            } else {
                Logger?.WriteLog(cnd?.ToString());

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
                            query.Append($"@{IntEx.GenerateShortRid()}", list[i].RID);
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
                    rs[it].RID = RID.GenerateRID();
                }
            }
            var members = ReflectionTool.FieldsAndPropertiesOf(typeof(T))
                .Where(t => t.GetCustomAttribute<FieldAttribute>() != null);
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