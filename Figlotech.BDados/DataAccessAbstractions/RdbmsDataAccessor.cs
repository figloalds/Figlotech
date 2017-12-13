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
using Figlotech.Core.BusinessModel;
using Figlotech.BDados.TableNameTransformDefaults;
using Figlotech.BDados.Extensions;

namespace Figlotech.BDados.DataAccessAbstractions {
    public class RdbmsDataAccessor<T> : RdbmsDataAccessor where T : IRdbmsPluginAdapter, new() {
        public RdbmsDataAccessor(IDictionary<String, object> Configuration) : base(new T()) {
            Plugin = new T();
            Plugin.SetConfiguration(Configuration);
        }
    }
    public class RdbmsDataAccessor : IRdbmsDataAccessor, IDisposable {

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

        //private DataAccessorPlugin.Config Plugin.Config;
        private int _simmultaneousConnections;
        private bool _accessSwitch = false;
        public String SchemaName { get { return Plugin.SchemaName; } }

        //public IDbConnection ConnectionHandle;

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
            return ForceExist<T>(Default, Qb.Fmt(query, args));
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
            return ScalarQuery<String>(Qb.Fmt($"SHOW CREATE TABLE {table}"));
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
                Query(Qb.Fmt("SELECT 1"));
                result = true;
            } catch (Exception) { }
            return result;
        }

        #endregion ***************************

        public T LoadFirstOrDefault<T>(Expression<Func<T, bool>> condicoes, int? page = default(int?), int? limit = 200, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new() {
            return LoadAll<T>(condicoes, page, limit, orderingMember, ordering).FirstOrDefault();
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

        private MemberInfo FindMember(Expression x) {
            if (x is UnaryExpression) {
                return FindMember((x as UnaryExpression).Operand);
            }
            if (x is MemberExpression) {
                return (x as MemberExpression).Member;
            }

            return null;
        }

        public MemberInfo GetOrderingMember<T>(Expression<Func<T, object>> fn) {
            if (fn == null) return null;
            try {
                var orderingExpression = fn.Compile();
                var OrderingMember = FindMember(fn.Body);
            } catch (Exception) { }

            return null;
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
            return Access((connection) => {
                return SaveItem(connection, input, fn);
            }, (x) => {
                this.WriteLog(x.Message);
                this.WriteLog(x.StackTrace);
                throw x;
            });
        }

        public bool SaveItem(IDbConnection connection, IDataObject input, Action fn = null) {
            bool retv = false;

            int rs = -33;

            var id = GetIdColumn(input.GetType());
            var rid = GetRidColumn(input.GetType());

            input.CreatedTime = DateTime.UtcNow;

            if (input.IsPersisted) {
                rs = Execute(connection, Plugin.QueryGenerator.GenerateUpdateQuery(input));
                retv = true;
                if (fn != null)
                    fn.Invoke();
                return retv;
            }

            rs = Execute(connection, Plugin.QueryGenerator.GenerateInsertQuery(input));
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
                        var gid = ScalarQuery(connection, query);
                        if (gid is long l)
                            retvId = l;
                        if (gid is string s) {
                            if (Int64.TryParse(s, out retvId)) {
                            }
                        }
                    } catch (Exception x) {
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
                        retvId = ((long?)ScalarQuery(connection, query)) ?? 0;
                        var gid = ScalarQuery(connection, query);
                        if (gid is long l)
                            retvId = l;
                        if (gid is string s) {
                            if (Int64.TryParse(s, out retvId)) {
                            }
                        }
                    } catch (Exception x) {
                        OnFailedSave?.Invoke(input?.GetType(), input, x);
                    }
                }

                //}
                if (retvId > 0) {
                    input.Id = retvId;
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

        //public IDbConnection GetConnection() {
        //    return ConnectionHandle;
        //}

        public IEnumerable<T> Query<T>(IQueryBuilder query) where T : new() {
            return Access((connection) => {
                return Query<T>(connection, query);
            });
        }
        public IEnumerable<T> Query<T>(IDbConnection connection, IQueryBuilder query) where T : new() {
            if (query == null) {
                return new List<T>();
            }
            DataTable resultado = Query(connection, query);
            return Fi.Tech.Map<T>(resultado);
        }

        public IEnumerable<T> Query<T>(string queryString, params object[] args) where T : new() {
            return Query<T>(Qb.Fmt(queryString, args));
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
            var id = GetIdColumn(typeof(T));
            return LoadAll<T>(new Qb().Append($"{id}=@id", Id)).FirstOrDefault();
        }

        public T LoadByRid<T>(String RID) where T : IDataObject, new() {
            var rid = GetRidColumn(typeof(T));
            return LoadAll<T>(new Qb().Append($"{rid}=@rid", RID)).FirstOrDefault();
        }

        public RecordSet<T> LoadAll<T>(String where = "TRUE", params object[] args) where T : IDataObject, new() {
            return Fetch<T>(Qb.Fmt(where, args)).ToRecordSet();
        }

        public RecordSet<T> LoadAll<T>(Expression<Func<T, bool>> conditions = null, int? page = null, int? limit = 200, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new() {
            return Fetch<T>(conditions, page, limit, orderingMember, ordering).ToRecordSet();
        }

        public RecordSet<T> LoadAll<T>(IQueryBuilder condicoes, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new() {
            return Access((connection) => {
                return LoadAll<T>(connection, condicoes, orderingMember, ordering).ToRecordSet();
            });
        }

        public RecordSet<T> LoadAll<T>(IDbConnection connection, IQueryBuilder condicoes, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new() {
            return Fetch<T>(connection, condicoes, orderingMember, ordering).ToRecordSet();
        }

        public IEnumerable<T> Fetch<T>(String where = "TRUE", params object[] args) where T : IDataObject, new() {
            return Fetch<T>(Qb.Fmt(where, args));
        }

        public IEnumerable<T> Fetch<T>(Expression<Func<T, bool>> conditions = null, int? page = null, int? limit = 200, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new() {
            var query = new ConditionParser().ParseExpression(conditions);
            if (page != null && limit != null)
                query.Append($"LIMIT {(page - 1) * limit}, {limit}");
            else if (limit != null)
                query.Append($"LIMIT {limit}");
            return Fetch<T>(query, orderingMember, ordering);
        }

        Benchmarker Bench = null;

        public IEnumerable<T> Fetch<T>(IQueryBuilder condicoes, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new() {
            return Access((connection) => {
                return Fetch<T>(connection, condicoes, orderingMember, ordering);
            }, (x) => {
                this.WriteLog(x.Message);
                this.WriteLog(x.StackTrace);
                throw x;
            });
        }


        public IEnumerable<T> Fetch<T>(IDbConnection connection, IQueryBuilder condicoes, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new() {

            if (condicoes == null) {
                condicoes = Qb.Fmt("TRUE");
            }
            var member = GetOrderingMember<T>(orderingMember);
            DataTable dt = null;

            Bench?.Mark("--");

            Bench?.Mark("Data Load ---");
            var selectQuery = Plugin.QueryGenerator.GenerateSelect<T>(condicoes, member, ordering);
            Bench?.Mark("Generate SELECT");
            dt = Query(connection, selectQuery);
            Bench?.Mark("Execute SELECT");

            if (dt == null)
                return new T[0];
            if (dt.Rows.Count < 1) {
                this.WriteLog("Query returned no results.");
                return new T[0];
            }

            var fields = ReflectionTool.FieldsAndPropertiesOf(typeof(T))
                .Where((a) => a.GetCustomAttribute<FieldAttribute>() != null)
                .ToList();

            var mapFn = Fi.Tech.Map<T>(dt);
            return RunAfterLoads(mapFn);

        }

        private IEnumerable<T> RunAfterLoads<T>(IEnumerable<T> target) {
            var enumerator = target.GetEnumerator();
            while (enumerator.MoveNext()) {
                if (enumerator.Current is IBusinessObject bo)
                    bo.OnAfterLoad();

                if (enumerator.Current != null)
                    yield return enumerator.Current;
            }
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

            var query = new Qb().Append($"DELETE FROM {obj.GetType().Name} WHERE {ridname}=@rid", obj.RID);
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
            var query = Qb.Fmt($"DELETE FROM {typeof(T).Name} WHERE ");
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

        //public void Open() {
        //    if (ConnectionHandle == null) {
        //        ConnectionHandle = Plugin.GetNewConnection();
        //    } else {
        //        ConnectionHandle.Close();
        //    }
        //    if (ConnectionHandle.State != ConnectionState.Open) {
        //        ConnectionHandle.Open();
        //    }
        //}

        //public bool TryOpen() {
        //    try {
        //        Open();
        //    } catch (Exception x) {
        //        this.WriteLog(x.Message);
        //        throw new BDadosException(String.Format(Fi.Tech.GetStrings().RDBMS_CANNOT_CONNECT, x.Message));
        //    }
        //    return true;
        //}

        //public delegate void FuncoesDados(BDados banco);
        //public delegate void TrataExceptions(Exception x);

        public Object ScalarQuery(IQueryBuilder qb) {
            return Access(connection => ScalarQuery(connection, qb));
        }

        public Object ScalarQuery(IDbConnection connection, IQueryBuilder qb) {
            Object retv = null;
            try {
                retv = Query(connection, qb).Rows[0][0];
            } catch (Exception) {
            }
            return retv;
        }

        public T ScalarQuery<T>(IQueryBuilder qb) {
            T retv = default(T);
            try {
                retv = (T)Query(qb).Rows[0][0];
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

        public event Action<Type, IDataObject> OnSuccessfulSave;
        public event Action<Type, IDataObject, Exception> OnFailedSave;

        public void Access(Action<IDbConnection> functions, Action<Exception> handler = null) {
            var i = Access<int>((connection) => {
                functions?.Invoke(connection);
                return 0;
            }, handler);
        }

        public T Access<T>(Func<IDbConnection, T> functions, Action<Exception> handler = null) {
            if (functions == null) return default(T);
            //if (ConnectionHandle != null && ConnectionHandle.State == ConnectionState.Open) {
            //    return functions.Invoke(connection);
            //}
            int aid = accessId;
            return UseConnection((connection) => {
                try {
                    aid = ++accessId;
                    if (Bench == null) {
                        Bench = new Benchmarker($"---- Access [{++aid}]");
                        Bench.WriteToStdout = showPerformanceLogs;
                    }
                    return functions.Invoke(connection);
                } catch (Exception x) {
                    var ex = x;
                    this.WriteLog("Exception Details:");
                    var depth = 1;
                    while (ex != null && ex.InnerException != ex) {
                        this.WriteLog($"[{aid}]{new String('-', depth)} {ex.Message}");
                        this.WriteLog($"[{aid}]{new String('-', depth)} {ex.StackTrace}");
                        this.WriteLog($"{new String('-', depth)}>");
                        depth++;
                        ex = ex.InnerException;
                    }

                    if (handler != null) {
                        handler.Invoke(x);
                    } else {
                        throw x;
                    }

                    return default(T);
                } finally {
                    var total = Bench?.FinalMark();
                    this.WriteLog(String.Format("---- Access [{0}] Finished in {1}ms", aid, total));
                    Close(connection);
                }
            });

        }

        public DataTable Query(IQueryBuilder query) {
            return Access((connection) => {
                return Query(connection, query);
            });
        }

        public DataTable Query(IDbConnection connection, IQueryBuilder query) {
            if (query == null || query.GetCommandText() == null) {
                return new DataTable();
            }
            DateTime Inicio = DateTime.Now;
            String QueryText = query.GetCommandText();
            DataTable retv = new DataTable();
            using (var command = connection.CreateCommand()) {
                command.CommandText = QueryText;
                command.CommandTimeout = Plugin.CommandTimeout;
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
        }

        public DataTable Query(String Query, params Object[] args) {
            return this.Query(Qb.Fmt(Query, args));
        }

        public void WriteLog(String s) {
            Logger?.WriteLog(s);
            Fi.Tech.WriteLine(s);
        }

        public int Execute(String str, params object[] args) {
            return Execute(Qb.Fmt(str, args));
        }

        public int Execute(IQueryBuilder query) {
            return Access((connection) => {
                return Execute(connection, query);
            });
        }

        public int Execute(IDbConnection connection, IQueryBuilder query) {
            if (query == null)
                return 0;
            int result = -1;

            Bench?.Mark("--");
            this.WriteLog($"[{accessId}] -- Execute: {query.GetCommandText()} [{Plugin.CommandTimeout}s timeout]");
            foreach (var param in query.GetParameters()) {
                this.WriteLog($"[{accessId}] @{param.Key} = {param.Value?.ToString() ?? "null"}");
            }
            using (var command = connection.CreateCommand()) {
                try {
                    command.CommandText = query.GetCommandText();
                    foreach (var param in query.GetParameters()) {
                        var cmdParam = command.CreateParameter();
                        cmdParam.ParameterName = param.Key;
                        cmdParam.Value = param.Value;
                        command.Parameters.Add(cmdParam);
                    }
                    command.CommandTimeout = Plugin.CommandTimeout;
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
                }
            }
            return result;
        }

        public void Close(IDbConnection connection) {
            try {
                connection.Close();
            } catch (Exception x) {
                this.WriteLog($"[{accessId}] BDados Close: {x.Message}");
            }
        }

        private T UseConnection<T>(Func<IDbConnection, T> func) {
            if (func == null) return default(T);

            using (var connection = Plugin.GetNewConnection()) {
                lock (connection) {
                    if (connection?.State != ConnectionState.Open) {
                        connection.Open();
                    }
                    var retv = func.Invoke(connection);
                    Close(connection);

                    return retv;
                }
            }
        }

        public List<T> ExecuteProcedure<T>(params object[] args) where T : ProcedureResult {
            DateTime inicio = DateTime.Now;
            List<T> retv = new List<T>();
            this.WriteLog($"[{accessId}] Exec procedure -- ");

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

        public IEnumerable<T> AggregateLoad<T>(Expression<Func<T, bool>> cnd = null, MemberInfo orderingMember = null, OrderingType otype = OrderingType.Asc, int? limit = null, int? page = null, int PageSize = 200, MemberInfo GroupingMember = null, MemberInfo OrderingType = null, OrderingType Ordering = OrderingType.Asc, bool Linear = false) where T : IDataObject, new() {

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

            this.WriteLog($"Running Aggregate Load All for {typeof(T).Name}? {hasAnyAggregations}. Linear? {Linear}");
            // CLUMSY
            if (hasAnyAggregations) {
                var membersOfT = ReflectionTool.FieldsAndPropertiesOf(typeof(T));

                var join = MakeJoin(
                        (query) => {
                            // Starting with T itself
                            query.AggregateRoot<T>(prefixer.GetAliasFor("root", typeof(T).Name)).As(prefixer.GetAliasFor("root", typeof(T).Name));
                            MakeQueryAggregations(ref query, typeof(T), "root", typeof(T).Name, prefixer, Linear);
                        });
                var builtConditions = (cnd == null ? Qb.Fmt("TRUE") : new ConditionParser(prefixer).ParseExpression(cnd));
                var builtConditionsRoot = (cnd == null ? Qb.Fmt("TRUE") : new ConditionParser(prefixer).ParseExpression(cnd, false));
                //builtConditions
                //    .If(GroupingMember != null).Then()
                //        .Append($"GROUP BY a.{GroupingMember?.Name}")
                //    .EndIf();
                //.If(OrderingType != null).Then()
                //    .Append($"ORDER BY a.{OrderingType?.Name} {Ordering.ToString().ToUpper()}")
                //.EndIf();
                //if (limit != null) {
                //    builtConditions.Append($"LIMIT {(page ?? 0) * limit}, {limit}");
                //}
                var dynamicJoinJumble = join.BuildObject<T>(
                        (build) => {
                            MakeBuildAggregations(ref build, typeof(T), "root", typeof(T).Name, prefixer, Linear);
                        }, builtConditions, orderingMember, otype, page, limit, builtConditionsRoot
                            //.If(GroupingMember != null).Then()
                            //    .Append($"GROUP BY a.{GroupingMember?.Name}")
                            //.EndIf()
                            //.Append($"{(OrderingType != null ? $"ORDER BY {OrderingType?.Name} {Ordering.ToString().ToUpper()}" : "")}")
                            );

                return dynamicJoinJumble;
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

                return Fetch<T>(new ConditionParser().ParseExpression<T>(cnd)
                    .If(GroupingMember != null).Then()
                        .Append($"GROUP BY a.{GroupingMember?.Name}")
                    .EndIf()
                    .If(OrderingType != null).Then()
                        .Append($"ORDER BY a.{OrderingType?.Name} {Ordering.ToString().ToUpper()}")
                    .EndIf()
                    .If(limit != null).Then()
                        .Append($"LIMIT {(page != null && page > 0 ? $"{(page - 1) * limit}," : "")}{limit}"));
            }
        }

        public bool DeleteWhereRidNotIn<T>(Expression<Func<T, bool>> cnd, RecordSet<T> list) where T : IDataObject, new() {
            int retv = 0;
            if (list == null)
                return true;

            var id = GetIdColumn<T>();
            var rid = GetRidColumn<T>();
            Access((connection) => {
                var query = Qb.Fmt($"DELETE FROM {typeof(T).Name} WHERE ");
                if (cnd != null) {
                    query.Append($"{rid} IN (SELECT {rid} FROM (SELECT {rid} FROM {typeof(T).Name} AS a WHERE ");
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
                retv = Execute(connection, query);
            }
            );
            return retv > 0;
        }

        public bool SaveRecordSet<T>(RecordSet<T> rs) where T : IDataObject, new() {
            return Access((connection) => {
                return SaveRecordSet<T>(connection, rs);
            });
        }

        public bool SaveRecordSet<T>(IDbConnection connection, RecordSet<T> rs) where T : IDataObject, new() {
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
            int cut = 500;
            int rst = 0;
            RecordSet<T> temp;
            if (rs.Count > cut) {
                temp = new RecordSet<T>();
                temp.AddRange(rs);
                temp.OrderBy(it => it.IsPersisted);
            } else {
                temp = rs;
            }
            while (i * cut < rs.Count) {
                var sub = new RecordSet<T>();
                sub.AddRange(temp.Skip(i * cut).Take(Math.Min(rs.Count, cut)));
                var inserts = sub.Where(it => !it.IsPersisted).ToRecordSet();
                var updates = sub.Where(it => it.IsPersisted).ToRecordSet();
                if (inserts.Count > 0) {
                    rst += Execute(Plugin.QueryGenerator.GenerateMultiInsert(inserts, false));
                }
                if (updates.Count > 0) {
                    rst += Execute(Plugin.QueryGenerator.GenerateMultiUpdate(updates));
                }
                sub.Clear();
                i++;
            }
            return retv;
        }

        public void Dispose() {
            //this.Close();
        }
        #endregion *****************
        //
    }
}