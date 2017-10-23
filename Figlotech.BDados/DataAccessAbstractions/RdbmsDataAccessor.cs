using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Reflection;
using System.Linq;
using System.Linq.Expressions;
using Figlotech.Core.Interfaces;
using Figlotech.BDados.Builders;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using Figlotech.BDados.Helpers;
using Figlotech.Core.Helpers;
using Figlotech.Core;

namespace Figlotech.BDados.DataAccessAbstractions {
    public class RdbmsDataAccessor<T> : RdbmsDataAccessor where T : IRdbmsPluginAdapter, new() {
        public RdbmsDataAccessor(DataAccessorConfiguration config) : base(new T()) {
            Plugin.Config = config;
        }
    }
    public class RdbmsDataAccessor : IRdbmsDataAccessor {

        public ILogger Logger { get; set; }
        public Type[] _workingTypes = new Type[0];
        public Type[] WorkingTypes {
            get { return _workingTypes; }
            set {
                _workingTypes = value.Where(t => t.GetInterfaces().Contains(typeof(IDataObject))).ToArray();
            }
        }

        #region **** Global Declaration ****
        protected IRdbmsPluginAdapter Plugin;
        public bool RethrowExceptions { get; set; } = true;

        //private DataAccessorPlugin.Config Plugin.Config;
        private int _simmultaneousConnections;
        private bool _accessSwitch = false;
        public String SchemaName { get { return Plugin.Config.Database; } }

        public IDbConnection ConnectionHandle;

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

        public RdbmsDataAccessor(IRdbmsPluginAdapter plugin) {
            Plugin = plugin;
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
            throw new NotImplementedException("It's... Not implemented yet");
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
            try {
                Query("SELECT 1");
                result = true;
            } catch (Exception) { }
            return result;
        }

        #endregion ***************************

        public T LoadFirstOrDefault<T>(Expression<Func<T, bool>> condicoes, int? page = default(int?), int? limit = 200) where T : IDataObject, new() {
            return LoadAll<T>(condicoes, page, limit).FirstOrDefault();
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

            int rs = -33;

            var id = GetIdColumn(input.GetType());
            var rid = GetRidColumn(input.GetType());

            input.CreatedTime = DateTime.UtcNow;
            if (input.IsPersisted) {
                rs = Execute(Plugin.QueryGenerator.GenerateUpdateQuery(input));
                retv = true;
                if (fn != null)
                    fn.Invoke();
                return retv;
            }

            rs = Execute(Plugin.QueryGenerator.GenerateInsertQuery(input));
            if (rs == 0) {
                this.WriteLog("** Something went SERIOUSLY NUTS in SaveItem<T> **");
            }
            retv = rs > 0;
            if (retv && !input.IsPersisted) {
                long retvId = 0;
                var ridAtt = ReflectionTool.FieldsAndPropertiesOf(input.GetType()).Where((f) => f.GetCustomAttribute<ReliableIdAttribute>() != null);
                //if (ridAtt.Any()) {
                //    var query = (IQueryBuilder)Plugin.QueryGenerator
                //        .GetType()
                //        .GetMethod(nameof(Plugin.QueryGenerator.GetIdFromRid))
                //        .MakeGenericMethod(input.GetType())
                //        .Invoke(Plugin.QueryGenerator, new Object[] { input.RID });
                //    DataTable dt = Query(query);
                //    retvId = (long)dt.Rows[0][id];
                //} else {


                if (ReflectionTool.FieldsAndPropertiesOf(input.GetType()).Any(a => a.GetCustomAttribute<ReliableIdAttribute>() != null)) {
                    try {
                        var query = (IQueryBuilder)Plugin.QueryGenerator
                            .GetType()
                            .GetMethod(nameof(Plugin.QueryGenerator.GetIdFromRid))
                            .MakeGenericMethod(input.GetType())
                            .Invoke(Plugin.QueryGenerator, new Object[] { input.RID });
                        var gid = ScalarQuery(query);
                        if (gid is long l)
                            retvId = l;
                        if (gid is string s) {
                            if(Int64.TryParse(s, out retvId)) {
                            }
                        }
                    } catch(Exception x) {
                        Fi.Tech.WriteLine(x.Message);
                    }
                }

                if (retvId <= 0) {
                    try {
                        var query = (IQueryBuilder)Plugin.QueryGenerator
                            .GetType()
                            .GetMethod(nameof(Plugin.QueryGenerator.GetLastInsertId))
                            .MakeGenericMethod(input.GetType())
                            .Invoke(Plugin.QueryGenerator, new Object[0]);
                        retvId = ((long?)ScalarQuery(query)) ?? 0;
                        var gid = ScalarQuery(query);
                        if (gid is long l)
                            retvId = l;
                        if (gid is string s) {
                            if (Int64.TryParse(s, out retvId)) {
                            }
                        }
                    } catch (Exception x) {

                    }
                }

                //}
                if (retvId > 0) {
                    input.ForceId(retvId);
                } else {
                }
                if (fn != null)
                    fn.Invoke();
                retv = true;
            }
            return retv;
        }

        //public List<T> Load<T>(string Consulta, params object[] args) {
        //    List<T> retv = new List<T>();
        //    Access(() => {

        //        return Query(Consulta, args);
        //    });retv;
        //}

        public IDbConnection GetConnection() {
            return ConnectionHandle;
        }

        public List<T> Query<T>(IQueryBuilder query) where T : new() {
            if (query == null) {
                return new List<T>();
            }
            var retv = new List<T>();
            DataTable resultado = Query(query);
            Fi.Tech.Map<T>(retv, resultado);
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

            var name = GetIdColumn<T>();


            DataTable dt = Query(Plugin.QueryGenerator.GenerateSelect<T>(new ConditionParametrizer($"{name}=@1", Id)));
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
                        Object o = dt.Rows[i][col.Name];
                        objBuilder[col] = o;
                    } catch (Exception) { }
                }
                retv = add;
            }

            return retv;
        }

        public T LoadByRid<T>(String RID) where T : IDataObject, new() {
            if (RID == null)
                return default(T);
            T retv = new T();

            var rid = GetRidColumn<T>();

            DataTable dt = Query(Plugin.QueryGenerator.GenerateSelect<T>(new ConditionParametrizer($"{rid}=@1", RID)));
            int i = 0;
            if (dt.Rows.Count > 0) {
                var members = ReflectionTool.FieldsAndPropertiesOf(typeof(T))
                    .Where(m => m.GetCustomAttribute<FieldAttribute>() != null)
                    .ToList();
                var objBuilder = new ObjectReflector(retv);
                foreach (var col in members) {
                    if (!dt.Columns.Contains(col.Name)) continue;
                    var typeofCol = ReflectionTool.GetTypeOf(col);

                    Object o = dt.Rows[i][col.Name];
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

            if (condicoes == null) {
                condicoes = new QueryBuilder("TRUE");
            }
            // Cria uma instancia Dummy só pra poder pegar o reflector da classe usada como T.
            // Usando o dummy2 eu consigo puxar uma query de select baseada nos campos da classe filha
            DataTable dt = null;
            Access(() => {

                Bench?.Mark("--");

                Bench?.Mark("Data Load ---");
                var selectQuery = Plugin.QueryGenerator.GenerateSelect<T>(condicoes);
                Bench?.Mark("Generate SELECT");
                dt = Query(selectQuery);
                Bench?.Mark("Execute SELECT");
                if (dt == null)
                    return;
                if (dt.Rows.Count < 1) {
                    this.WriteLog("Query returned no results.");
                    return;
                }
                var fields = ReflectionTool.FieldsAndPropertiesOf(typeof(T))
                    .Where((a) => a.GetCustomAttribute<FieldAttribute>() != null)
                    .ToList();


                Fi.Tech.Map(retv, dt);

                Bench?.Mark("Build RecordSet");
            }, (x) => {
                this.WriteLog(x.Message);
                this.WriteLog(x.StackTrace);
                if (RethrowExceptions) throw x;
                Bench?.Mark("Build RecordSet");
            });
            return retv;
        }

        public bool Delete(IDataObject obj) {
            bool retv = false;

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

            T dataObject = (T)Activator.CreateInstance(typeof(T));
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
            query.Append(join.GenerateQuery(Plugin.QueryGenerator, new ConditionParser(p).ParseExpression<T>(conditions)));
            query.Append(") sub)");
            retv = Execute(query) > 0;
            return retv;
        }

        #region **** BDados API ****
        int fail = 0;
        private List<Task<Object>> Workers = new List<Task<Object>>();
        private static int accessCount = 0;

        public void Open() {
            if (ConnectionHandle == null) {
                ConnectionHandle = Plugin.GetNewConnection();
            }
            if (ConnectionHandle.State != ConnectionState.Open) {
                ConnectionHandle.Open();
            }
        }

        public bool TryOpen() {
            try {
                Open();
            } catch (Exception x) {
                this.WriteLog(x.Message);
                throw new BDadosException(String.Format(Fi.Tech.GetStrings().RDBMS_CANNOT_CONNECT, x.Message));
            }
            return true;
        }

        //public delegate void FuncoesDados(BDados banco);
        //public delegate void TrataExceptions(Exception x);

        public Object ScalarQuery(IQueryBuilder qb) {
            Object retv = null;
            try {
                retv = Query(qb).Rows[0][0];
            } catch (Exception) {
            }
            return retv;
        }

        public T ScalarQuery<T>(String query, params Object[] args) {
            T retv = default(T);
            try {
                retv = (T)Query(query, args).Rows[0][0];
            } catch (Exception) {
            }
            return retv;
        }

        public IJoinBuilder MakeJoin(Action<JoinDefinition> fn) {
            var retv = new JoinObjectBuilder(this, fn);
            return retv;
        }

        public IQueryGenerator QueryGenerator => Plugin.QueryGenerator;

        //public void GenerateValueObjectDefinitions(String defaultNamespace, String baseDir) {
        //    Access(() => {
        //        DataTable t = Query("SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA=@1;", this.Plugin.Config.Database);
        //        DataTable cols = Query("SELECT * FROM information_schema.columns WHERE TABLE_SCHEMA=@1", this.Plugin.Config.Database);
        //        for (int i = 0; i < t.Rows.Count; i++) {
        //            String Tabela = (string)t.Rows[i]["TABLE_NAME"];
        //            List<String> lines = new List<String>();
        //            lines.Add($"// --------------------------------------------------");
        //            lines.Add($"// BDados v{Fi.Tech.GetVersion()}");
        //            lines.Add($"// Arquivo gerado automaticamente.");
        //            lines.Add($"// --------------------------------------------------");
        //            lines.Add("using System;");
        //            lines.Add("using Figlotech.BDados.Attributes;");
        //            lines.Add("using Figlotech.Core.Interfaces;");
        //            lines.Add("using Figlotech.BDados.Entity;");
        //            lines.Add("");
        //            lines.Add($"// ------------------------------------------");
        //            lines.Add($"// Tabela {Tabela} ");
        //            lines.Add($"// ------------------------------------------");
        //            lines.Add($"namespace {defaultNamespace} {{");
        //            lines.Add($"\t public partial class {Tabela} : BaseDataObject " + "{");
        //            for (int c = 0; c < cols.Rows.Count; c++) {
        //                var thisCol = cols.Rows[c];
        //                if (thisCol["TABLE_NAME"] != Tabela)
        //                    continue;
        //                if (thisCol["COLUMN_NAME"] == "Id" ||
        //                    thisCol["COLUMN_NAME"] == "RID")
        //                    continue;
        //                StringBuilder attLineOptions = new StringBuilder();
        //                List<String> lineOptions = new List<String>();
        //                //for(int x = 0; x < cols.Columns.Count; x++) {
        //                //    Fi.Tech.Write(thisCol[x]);
        //                //    Fi.Tech.Write("|");
        //                //}
        //                //this.WriteLog();
        //                lines.Add("");
        //                if (thisCol["COLUMN_KEY"] == "PRI") {
        //                    lineOptions.Add("PrimaryKey=true");
        //                    lines.Add("\t\t[PrimaryKey]");
        //                }
        //                ulong? l = (ulong?)thisCol["CHARACTER_MAXIMUM_LENGTH"];
        //                if (l != null && l > 0)
        //                    lineOptions.Add($"Size={l}");
        //                if ("YES" == (thisCol["IS_NULLABLE"]))
        //                    lineOptions.Add("AllowNull=true");
        //                if (thisCol["COLUMN_KEY"] == "UNI")
        //                    lineOptions.Add("Unique=true");
        //                if (thisCol["COLUMN_DEFAULT"] != null) {
        //                    Object defVal = thisCol["COLUMN_DEFAULT"];
        //                    if (defVal is String)
        //                        defVal = "\"" + defVal + "\"";
        //                    else
        //                        defVal = defVal.ToString().ToLower();
        //                    lineOptions.Add($"DefaultValue={defVal}");
        //                }
        //                for (int a = 0; a < lineOptions.Count; a++) {
        //                    attLineOptions.Append(lineOptions[a]);
        //                    if (a < lineOptions.Count - 1)
        //                        attLineOptions.Append(", ");
        //                }
        //                lines.Add($"\t\t[Field({attLineOptions.ToString()})]");

        //                String tipo = "String";
        //                bool usgn = ((string)thisCol["COLUMN_TYPE"]).ToLower().Contains("unsigned");
        //                switch (((string)thisCol["DATA_TYPE"]).ToUpper()) {
        //                    case "VARCHAR":
        //                    case "CHAR":
        //                        tipo = "String"; break;
        //                    case "BIT":
        //                    case "TINYINT":
        //                        tipo = "bool"; break;
        //                    case "INT":
        //                        tipo = (usgn ? "u" : "") + "int"; break;
        //                    case "BIGINT":
        //                        tipo = (usgn ? "u" : "") + "long"; break;
        //                    case "SMALLINT":
        //                        tipo = (usgn ? "u" : "") + "short"; break;
        //                    case "FLOAT":
        //                    case "SINGLE":
        //                        tipo = "float"; break;
        //                    case "DOUBLE":
        //                        tipo = "double"; break;
        //                    case "DATETIME":
        //                    case "TIMESTAMP":
        //                    case "DATE":
        //                    case "TIME":
        //                        tipo = "DateTime"; break;
        //                }
        //                var nable = tipo != "String" && "YES" == (thisCol["IS_NULLABLE"]) ? "?" : "";
        //                lines.Add($"\t\tpublic {tipo}{nable} {thisCol["COLUMN_NAME"]};");
        //            }
        //            lines.Add("\t}");
        //            lines.Add("}");
        //            File.WriteAllLines(Path.Combine(baseDir, Tabela + ".cs"), lines);
        //        }
        //    });
        //}

        int accessId = 0;

        public void Access(Action functions, Action<Exception> handler = null) {
            Access(() => {
                functions?.Invoke();
                return 0;
            }, handler);
        }

        public T Access<T>(Func<T> functions, Action<Exception> handler = null) {
            if (functions == null) return default(T);
            if (ConnectionHandle != null && ConnectionHandle.State == ConnectionState.Open) {
                return functions.Invoke();
            }
            int aid = accessId;
            try {
                aid = ++accessId;
                if (Bench == null) {
                    Bench = new Benchmarker($"---- Access [{++aid}]");
                    Bench.WriteToStdout = showPerformanceLogs;
                }
                return UseConnection(() => {
                    return functions.Invoke();
                });
                var total = Bench?.TotalMark();
                this.WriteLog(String.Format("---- Access [{0}] returned OK: [{1} ms]", aid, total));
                return default(T);
            } catch (Exception x) {
                var total = Bench?.TotalMark();
                this.WriteLog(String.Format("---- Access [{0}] returned WITH ERRORS: [{1} ms]", aid, total));
                var ex = x;
                this.WriteLog("Detalhes dessa exception:");
                while (ex != null && ex.InnerException != ex) {
                    this.WriteLog(String.Format("{0} - {1}", ex.Message, ex.StackTrace));
                    ex = ex.InnerException;
                }

                if (handler != null)
                    handler.Invoke(x);
                else if (this.RethrowExceptions) {
                    throw x;
                }
                return default(T);
            } finally {
                if (!Plugin.Config.ContinuousConnection) {
                    Close();
                }
            }

        }

        public DataTable Query(IQueryBuilder query) {
            if (query == null || query.GetCommandText() == null) {
                return new DataTable();
            }
            DateTime Inicio = DateTime.Now;
            String QueryText = query.GetCommandText();
            DataTable retv = new DataTable();
            return Access(() => {
                using (var command = ConnectionHandle.CreateCommand()) {
                    command.CommandText = QueryText;
                    command.CommandTimeout = Plugin.Config.Timeout;
                    int i = 0;
                    this.WriteLog($"[{accessId}] -- Query: {QueryText}");
                    // Adiciona os parametros
                    foreach (KeyValuePair<String, Object> param in query.GetParameters()) {
                        var cmdParam = command.CreateParameter();
                        cmdParam.ParameterName = param.Key;
                        cmdParam.Value = param.Value;
                        command.Parameters.Add(cmdParam);
                        var pval = $"'{param.Value?.ToString() ?? "null"}'";
                        if (param.Value is DateTime || param.Value is DateTime? && ((DateTime?)param.Value).HasValue) {
                            pval = ((DateTime)param.Value).ToString("yyyy-MM-dd HH:mm:ss");
                            pval = $"'{pval}'";
                        }
                        this.WriteLog($"[{accessId}] SET @{param.Key} = {pval}");
                    }
                    // --
                    DataSet ds = Plugin.GetDataSet(command);
                    try {
                        int resultados = 0;
                        if (ds.Tables.Count < 1) {
                            throw new BDadosException("Database did not return any table.");
                        }
                        resultados = ds.Tables[0].Rows.Count;
                        this.WriteLog($"[{accessId}] -------- Queried [OK] ({resultados} results) [{DateTime.Now.Subtract(Inicio).TotalMilliseconds} ms]");
                        return ds.Tables[0];
                    } catch (Exception x) {
                        this.WriteLog($"[{accessId}] -------- Error: {x.Message} ([{DateTime.Now.Subtract(Inicio).TotalMilliseconds} ms]");
                        this.WriteLog(x.Message);
                        this.WriteLog(x.StackTrace);
                        throw x;
                    } finally {
                        command.Dispose();
                        this.WriteLog("------------------------------------");
                    }
                }
            });
        }

        public DataTable Query(String Query, params Object[] args) {
            return this.Query(new QueryBuilder(Query, args));
        }

        public void WriteLog(String s) {
            Logger?.WriteLog(s);
            Fi.Tech.WriteLine(s);
        }

        public int Execute(IQueryBuilder query) {

            if (query == null)
                return 0;
            int result = -1;

            Access(() => {
                Bench?.Mark("--");
                this.WriteLog($"[{accessId}] -- Execute: {query.GetCommandText()} [{Plugin.Config.Timeout}s timeout]");
                foreach (var param in query.GetParameters()) {
                    this.WriteLog($"[{accessId}] @{param.Key} = {param.Value?.ToString() ?? "null"}");
                }
                using (var command = ConnectionHandle.CreateCommand()) {
                    try {
                        command.CommandText = query.GetCommandText();
                        foreach (var param in query.GetParameters()) {
                            var cmdParam = command.CreateParameter();
                            cmdParam.ParameterName = param.Key;
                            cmdParam.Value = param.Value;
                            command.Parameters.Add(cmdParam);
                        }
                        command.CommandTimeout = Plugin.Config.Timeout;
                        this.WriteLog(command.CommandText);
                        Bench?.Mark("Prepared Statement");
                        result = command.ExecuteNonQuery();
                        var elaps = Bench?.Mark("Executed Statement");
                        this.WriteLog($"[{accessId}] --------- Executed [OK] ({result} lines affected) [{elaps} ms]");
                    } catch (Exception x) {
                        this.WriteLog($"[{accessId}] -------- Error: {x.Message} ([{Bench?.Mark("Error")} ms]");
                        this.WriteLog(x.Message);
                        this.WriteLog(x.StackTrace);
                        this.WriteLog($"BDados Execute: {x.Message}");
                        throw x;
                    } finally {
                        this.WriteLog("------------------------------------");
                        command.Dispose();
                    }
                }
            });
            return result;
        }
        public int Execute(String Query, params Object[] args) {
            return Execute(new QueryBuilder(Query, args));
        }

        public void Close() {
            try {
                ConnectionHandle.Close();
            } catch (Exception x) {
                this.WriteLog($"[{accessId}] BDados Close: {x.Message}");
            }
        }

        private T UseConnection<T>(Func<T> func) {
            if (func == null) return default(T);
            TryOpen();
            lock (ConnectionHandle)
                using (ConnectionHandle)
                    return func.Invoke();
        }

        public List<T> ExecuteProcedure<T>(params object[] args) where T : ProcedureResult {
            DateTime inicio = DateTime.Now;
            List<T> retv = new List<T>();
            this.WriteLog($"[{accessId}] Exec procedure -- ");

            Access(() => {
                try {
                    QueryBuilder query = (QueryBuilder)Plugin.QueryGenerator.GenerateCallProcedure(typeof(T).Name, args);
                    DataTable dt = Query(query);
                    foreach (DataRow r in dt.Rows) {
                        T newval = Activator.CreateInstance<T>();
                        foreach (FieldInfo f in newval.GetType().GetFields()) {
                            try {
                                Object v = r[f.Name];
                                if (v == null) {
                                    f.SetValue(newval, null);
                                } else {
                                    f.SetValue(newval, r[f.Name]);
                                }
                            } catch (Exception x) {
                                throw x;
                            }
                        }
                        foreach (PropertyInfo p in newval.GetType().GetProperties()) {
                            try {
                                Object v = r[p.Name];
                                if (v == null) {
                                    p.SetValue(newval, null);
                                } else if (Nullable.GetUnderlyingType(p.PropertyType) != null) {
                                    p.SetValue(newval, r[p.Name]);
                                } else {
                                    p.SetValue(newval, Convert.ChangeType(r[p.Name], p.PropertyType));
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
            });
            this.WriteLog($"[{accessId}] Total Procedure [{DateTime.Now.Subtract(inicio).TotalMilliseconds} ms] -- ");
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
            bool hasAnyAggregations = false;
            foreach (var a in Members) {
                hasAnyAggregations =
                    a.GetCustomAttribute<AggregateFieldAttribute>() != null ||
                    a.GetCustomAttribute<AggregateFarFieldAttribute>() != null ||
                    a.GetCustomAttribute<AggregateObjectAttribute>() != null ||
                    a.GetCustomAttribute<AggregateListAttribute>() != null;
                if (hasAnyAggregations)
                    break;
            }

            this.WriteLog($"Running Aggregate Load All for {typeof(T).Name.ToLower()}? {hasAnyAggregations}.");
            // CLUMSY
            if (hasAnyAggregations) {
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
                if (limit != null) {
                    builtConditions.Append($"LIMIT {(page ?? 0) * limit}, {limit}");
                }
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
                this.WriteLog(cnd?.ToString());

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
            Access(() => {
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
                    rs[it].RID = new RID().ToString();
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
                rst += Execute(Plugin.QueryGenerator.GenerateMultiInsert(sub));
                rst += Execute(Plugin.QueryGenerator.GenerateMultiUpdate(sub));
                sub.Clear();
                i++;
            }
            return retv;
        }
        #endregion *****************
        //
    }
}