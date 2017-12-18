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
    public class ConnectionInfo {
        public IDbConnection Connection { get; set; }
        public IDbTransaction Transaction { get; set; }
        public Benchmarker Benchmarker { get; set; }
    }
    public class RdbmsDataAccessor<T> : RdbmsDataAccessor where T : IRdbmsPluginAdapter, new() {
        public RdbmsDataAccessor(IDictionary<String, object> Configuration) : base(new T()) {
            Plugin = new T();
            Plugin.SetConfiguration(Configuration);
        }
    }
    public class RdbmsDataAccessor : IRdbmsDataAccessor, IDisposable {
        public bool UseTransactions { get; set; } = false;
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
        private int _simmultaneoustransactions;
        private bool _accessSwitch = false;
        public String SchemaName { get { return Plugin.SchemaName; } }

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
            if (currentTransaction != null) {
                return ForceExist<T>(currentTransaction, Default, qb);
            }
            return Access((transaction) => ForceExist<T>(transaction, Default, qb));
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

        ConnectionInfo currentTransaction = null;
        public void BeginTransaction() {
            lock (this) {
                if (this.currentTransaction == null) {
                    WriteLog("Opening Transaction");
                    var connection = Plugin.GetNewConnection();
                    if (connection?.State != ConnectionState.Open) {
                        connection.Open();
                    }
                    this.currentTransaction = new ConnectionInfo();
                    this.currentTransaction.Connection = connection;
                    this.currentTransaction.Transaction = connection.BeginTransaction(IsolationLevel.RepeatableRead);
                    WriteLog("Transaction Open");
                }
            }
        }

        public void EndTransaction() {
            lock (this) {
                if (this.currentTransaction != null) {
                    WriteLog("Ending Transaction");
                    var conn = this.currentTransaction.Connection;
                    this.currentTransaction.Transaction?.Dispose();
                    conn?.Dispose();

                    this.currentTransaction = null;
                    WriteLog("Transaction ended");
                }
            }
        }

        public void Commit() {
            WriteLog("Committing Transaction");
            lock (this)
                this.currentTransaction?.Transaction?.Commit();
            WriteLog("Commit OK");
        }
        public void Rollback() {
            WriteLog("Rolling back Transaction");
            lock (this)
                this.currentTransaction?.Transaction?.Rollback();
            WriteLog("Rollback OK");
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
            if (x == null)
                return null;
            MemberInfo retv = null;

            if (x is LambdaExpression lex) {
                retv = FindMember(lex.Body);
            }

            if (x is UnaryExpression) {
                retv = FindMember((x as UnaryExpression).Operand);
            }
            if (x is MemberExpression) {
                retv = (x as MemberExpression).Member;
            }
            if (x is BinaryExpression bex) {
                retv = FindMember(bex.Left);
            }

            if (retv == null)
                throw new MissingMemberException($"Member not found: {x.ToString()}");

            return retv;
        }

        public MemberInfo GetOrderingMember<T>(Expression<Func<T, object>> fn) {
            var OrderingMember = FindMember(fn);
            return OrderingMember;
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

        public bool SaveItem(IDataObject input) {
            if (currentTransaction != null) {
                return SaveItem(currentTransaction, input);
            }
            return Access((transaction) => {
                return SaveItem(transaction, input);
            }, (x) => {
                this.WriteLog(x.Message);
                this.WriteLog(x.StackTrace);
                throw x;
            });
        }

        public List<T> Query<T>(IQueryBuilder query) where T : new() {
            return Access((transaction) => {
                return Query<T>(transaction, query);
            });
        }

        public List<T> Query<T>(string queryString, params object[] args) where T : new() {
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
            if (currentTransaction != null) {
                return LoadById<T>(currentTransaction, Id);
            }
            return Access((transaction) => LoadById<T>(transaction, Id));
        }

        public T LoadByRid<T>(String RID) where T : IDataObject, new() {
            if (currentTransaction != null) {
                return LoadByRid<T>(currentTransaction, RID);
            }
            return Access((transaction) => LoadByRid<T>(transaction, RID));
        }

        public RecordSet<T> LoadAll<T>(String where = "TRUE", params object[] args) where T : IDataObject, new() {
            return Fetch<T>(Qb.Fmt(where, args)).ToRecordSet();
        }


        public RecordSet<T> LoadAll<T>(Expression<Func<T, bool>> conditions = null, int? skip = null, int? limit = 200, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new() {
            return Fetch<T>(conditions, skip, limit, orderingMember, ordering).ToRecordSet();
        }

        public RecordSet<T> LoadAll<T>(IQueryBuilder condicoes, int? skip = null, int? limit = 200, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new() {
            if (currentTransaction != null) {
                return LoadAll<T>(currentTransaction, condicoes, skip, limit, orderingMember, ordering).ToRecordSet();
            }
            return Access((transaction) => {
                return LoadAll<T>(transaction, condicoes, skip, limit, orderingMember, ordering).ToRecordSet();
            });
        }

        public List<T> Fetch<T>(String where = "TRUE", params object[] args) where T : IDataObject, new() {
            return Fetch<T>(Qb.Fmt(where, args));
        }

        public List<T> Fetch<T>(Expression<Func<T, bool>> conditions = null, int? skip = null, int? limit = 200, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new() {
            if (currentTransaction != null) {
                return Fetch(currentTransaction, conditions, skip, limit, orderingMember, ordering);
            }
            return Access((transaction) => Fetch(transaction, conditions, skip, limit, orderingMember, ordering));
        }
        
        public List<T> Fetch<T>(IQueryBuilder condicoes, int? skip = null, int? limit = 200, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new() {
            if (currentTransaction != null) {
                return Fetch<T>(currentTransaction, condicoes, skip, limit, orderingMember, ordering);
            }
            return Access((transaction) => {
                return Fetch<T>(transaction, condicoes, skip, limit, orderingMember, ordering);
            }, (x) => {
                this.WriteLog(x.Message);
                this.WriteLog(x.StackTrace);
                throw x;
            });
        }

        private List<T> RunAfterLoads<T>(List<T> target) {
            foreach (var a in target) {
                if (target is IBusinessObject ibo) {
                    ibo.OnAfterLoad();
                }
            }
            return target;
        }
        public bool Delete(IDataObject obj) {
            if (currentTransaction != null) {
                return Delete(currentTransaction, obj);
            }
            return Access((transaction) => Delete(transaction, obj));
        }
        public bool Delete<T>(Expression<Func<T, bool>> conditions) where T : IDataObject, new() {
            if (currentTransaction != null) {
                return Delete(currentTransaction, conditions);
            }
            return Access((transaction) => Delete(transaction, conditions));
        }

        #region **** BDados API ****
        int fail = 0;
        private List<Task<Object>> Workers = new List<Task<Object>>();
        private static int accessCount = 0;

        public Object ScalarQuery(IQueryBuilder qb) {
            if (currentTransaction != null) {
                return ScalarQuery(currentTransaction, qb);
            }
            return Access(transaction => ScalarQuery(transaction, qb));
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

        int accessId = 0;

        public event Action<Type, IDataObject> OnSuccessfulSave;
        public event Action<Type, IDataObject, Exception> OnFailedSave;

        public void Access(Action<ConnectionInfo> functions, Action<Exception> handler = null) {
            var i = Access<int>((transaction) => {
                functions?.Invoke(transaction);
                return 0;
            }, handler);
        }

        public T Access<T>(Func<ConnectionInfo, T> functions, Action<Exception> handler = null) {
            if (functions == null) return default(T);
            //if (transactionHandle != null && transactionHandle.State == transactionState.Open) {
            //    return functions.Invoke(transaction);
            //}
            int aid = accessId;
            return UseTransaction((transaction) => {
                aid = ++accessId;
                if (transaction.Benchmarker == null) {
                    transaction.Benchmarker = new Benchmarker($"---- Access [{++aid}]");
                    transaction.Benchmarker.WriteToStdout = FiTechCoreExtensions.EnableStdoutLogs;
                }

                var retv = functions.Invoke(transaction);
                var total = transaction.Benchmarker?.FinalMark();
                this.WriteLog(String.Format("---- Access [{0}] Finished in {1}ms", aid, total));
                return retv;
            }, handler);
        }

        public DataTable Query(IQueryBuilder query) {
            if (currentTransaction != null) {
                return Query(currentTransaction, query);
            }
            return Access((transaction) => {
                return Query(transaction, query);
            });
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
            if (currentTransaction != null) {
                return Execute(currentTransaction, query);
            }
            return Access((transaction) => {
                return Execute(transaction, query);
            });
        }


        public void Close(ConnectionInfo transaction) {
            try {
                transaction?.Transaction.Commit();
            } catch (Exception x) {
                transaction?.Transaction.Rollback();
                this.WriteLog($"[{accessId}] BDados Close: {x.Message}");
            } finally {
                transaction.Connection.Dispose();
                transaction?.Transaction.Dispose();
            }
        }

        private T UseTransaction<T>(Func<ConnectionInfo, T> func, Action<Exception> handler = null) {
            if (func == null) return default(T);

            using (var connection = Plugin.GetNewConnection()) {
                lock (connection) {
                    if (connection?.State != ConnectionState.Open) {
                        connection.Open();
                    }
                    var connInfo = new ConnectionInfo();
                    connInfo.Connection = connection;
                    var b = new Benchmarker("TRANSACTION");
                    connInfo.Benchmarker = b;
                    b.WriteToStdout = FiTechCoreExtensions.EnableStdoutLogs;
                    var usetrans = UseTransactions;
                    b.Mark($"INIT UsingTransactions: ({usetrans})");

                    if (usetrans)
                        connInfo.Transaction = connection.BeginTransaction();
                    //lock (connInfo) {
                    try {
                        b.Mark("Run User Code");
                        var retv = func.Invoke(connInfo);
                        WriteLog($"Committing [{accessId}]");
                        b.Mark("Begin Commit");
                        connInfo?.Transaction?.Commit();
                        b.Mark("End Commit");
                        WriteLog($"Commited OK [{accessId}]");
                        return retv;
                    } catch (Exception x) {
                        var ex = x;
                        WriteLog($"Rolling back [{accessId}]: {x.Message} {x.StackTrace}");
                        b.Mark("Begin Rollback");
                        connInfo?.Transaction?.Rollback();
                        b.Mark("End Rollback");
                        WriteLog($"Transaction rolled back [{accessId}]");

                        this.WriteLog("Exception Details:");
                        var depth = 1;
                        while (ex != null && ex.InnerException != ex) {
                            this.WriteLog($"{new String('-', depth)} ERROR [{accessId}]{ex.Message}");
                            this.WriteLog($"{new String('-', depth)} STACKTRACE [{accessId}] {ex.StackTrace}");
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
                        b.Mark("Dispose objects");
                        var c = connection;
                        if (usetrans)
                            connInfo.Transaction.Dispose();
                        c.Dispose();

                        b.FinalMark();
                    }
                    //}
                }
            }
        }

        //public List<T> ExecuteProcedure<T>(params object[] args) where T : ProcedureResult {
        //    DateTime inicio = DateTime.Now;
        //    List<T> retv = new List<T>();
        //    this.WriteLog($"[{accessId}] Exec procedure -- ");

        //    QueryBuilder query = (QueryBuilder)Plugin.QueryGenerator.GenerateCallProcedure(typeof(T).Name, args);
        //    DataTable dt = Query(query);
        //    foreach (DataRow r in dt.Rows) {
        //        T newval = Activator.CreateInstance<T>();
        //        foreach (FieldInfo f in newval.GetType().GetFields()) {
        //            try {
        //                Object v = r[f.Name];
        //                if (v == null) {
        //                    f.SetValue(newval, null);
        //                } else {
        //                    f.SetValue(newval, r[f.Name]);
        //                }
        //            } catch (Exception x) {
        //                throw x;
        //            }
        //        }
        //        foreach (PropertyInfo p in newval.GetType().GetProperties()) {
        //            try {
        //                Object v = r[p.Name];
        //                if (v == null) {
        //                    p.SetValue(newval, null);
        //                } else if (Nullable.GetUnderlyingType(p.PropertyType) != null) {
        //                    p.SetValue(newval, r[p.Name]);
        //                } else {
        //                    p.SetValue(newval, Convert.ChangeType(r[p.Name], p.PropertyType));
        //                }
        //            } catch (Exception x) {
        //                throw x;
        //            }
        //        }
        //        retv.Add(newval);
        //    }

        //    this.WriteLog($"[{accessId}] Total Procedure [{DateTime.Now.Subtract(inicio).TotalMilliseconds} ms] -- ");
        //    return retv;
        //}


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
                if (!Linear) {
                    MakeQueryAggregations(ref query, info.RemoteObjectType, thisAlias, field.Name, prefixer);
                }
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
                if (!Linear) {
                    MakeBuildAggregations(ref build, ReflectionTool.GetTypeOf(field), thisAlias, field.Name, prefixer);
                }
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
                if (!Linear) {
                    MakeBuildAggregations(ref build, info.RemoteObjectType, thisAlias, field.Name, prefixer);
                }
            }
        }

        public List<T> AggregateLoad<T>(
            Expression<Func<T, bool>> cnd = null, int? skip = null, int? limit = null,
            Expression<Func<T, object>> orderingMember = null, OrderingType otype = OrderingType.Asc,
            MemberInfo GroupingMember = null, bool Linear = false) where T : IDataObject, new() {
            if (currentTransaction != null) {
                return AggregateLoad(
                    currentTransaction, cnd, skip, limit,
                    orderingMember, otype,
                    GroupingMember, Linear);
            }
            return Access((transaction) =>
                AggregateLoad(
                    transaction, cnd, skip, limit,
                    orderingMember, otype,
                    GroupingMember, Linear));
        }



        public bool DeleteWhereRidNotIn<T>(Expression<Func<T, bool>> cnd, RecordSet<T> list) where T : IDataObject, new() {
            if (currentTransaction != null) {
                return DeleteWhereRidNotIn(currentTransaction, cnd, list);
            }
            return Access((transaction) => DeleteWhereRidNotIn(transaction, cnd, list));
        }

        public void Dispose() {
            //this.Close();
        }
        #endregion *****************
        //
        #region Default Transaction Using Core Funcitons.
        public T ForceExist<T>(ConnectionInfo transaction, Func<T> Default, IQueryBuilder qb) where T : IDataObject, new() {
            var f = LoadAll<T>(transaction, qb.Append("LIMIT 1"));
            if (f.Any()) {
                return f.First();
            } else {
                T quickSave = Default();
                quickSave.RID = new T().RID;
                SaveItem(quickSave);
                return quickSave;
            }
        }

        public bool SaveRecordSet<T>(ConnectionInfo transaction, RecordSet<T> rs) where T : IDataObject, new() {
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

        public Object ScalarQuery(ConnectionInfo transaction, IQueryBuilder qb) {
            Object retv = null;
            try {
                retv = Query(transaction, qb).Rows[0][0];
            } catch (Exception) {
            }
            return retv;
        }

        public bool DeleteWhereRidNotIn<T>(ConnectionInfo transaction, Expression<Func<T, bool>> cnd, RecordSet<T> list) where T : IDataObject, new() {
            int retv = 0;
            if (list == null)
                return true;

            var id = GetIdColumn<T>();
            var rid = GetRidColumn<T>();
            var query = Qb.Fmt($"DELETE FROM {typeof(T).Name} WHERE ");
            if (cnd != null) {
                PrefixMaker pm = new PrefixMaker();
                query.Append($"{rid} IN (SELECT {rid} FROM (SELECT {rid} FROM {typeof(T).Name} AS {pm.GetAliasFor("root", typeof(T).Name)} WHERE ");
                query.Append(new ConditionParser(pm).ParseExpression<T>(cnd));
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
            retv = Execute(transaction, query);
            return retv > 0;
        }

        public bool SaveRecordSet<T>(RecordSet<T> rs) where T : IDataObject, new() {
            if (currentTransaction != null) {
                return SaveRecordSet<T>(currentTransaction, rs);
            }
            return Access((transaction) => {
                return SaveRecordSet<T>(transaction, rs);
            });
        }

        public bool Delete<T>(ConnectionInfo transaction, Expression<Func<T, bool>> conditions) where T : IDataObject, new() {
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
            retv = Execute(transaction, query) > 0;
            return retv;
        }

        public List<T> Query<T>(ConnectionInfo transaction, IQueryBuilder query) where T : new() {
            if (query == null) {
                return new List<T>();
            }
            DataTable resultado = Query(transaction, query);
            return Fi.Tech.Map<T>(resultado);
        }

        public T LoadById<T>(ConnectionInfo transaction, long Id) where T : IDataObject, new() {
            var id = GetIdColumn(typeof(T));
            return LoadAll<T>(transaction, new Qb().Append($"{id}=@id", Id)).FirstOrDefault();
        }

        public T LoadByRid<T>(ConnectionInfo transaction, String RID) where T : IDataObject, new() {
            var rid = GetRidColumn(typeof(T));
            return LoadAll<T>(transaction, new Qb().Append($"{rid}=@rid", RID)).FirstOrDefault();
        }

        public RecordSet<T> LoadAll<T>(ConnectionInfo transaction, Expression<Func<T, bool>> conditions = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new() {
            return Fetch<T>(transaction, conditions, skip, limit, orderingMember, ordering).ToRecordSet();
        }

        public RecordSet<T> LoadAll<T>(ConnectionInfo transaction, IQueryBuilder condicoes, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new() {
            return Fetch<T>(transaction, condicoes, skip, limit, orderingMember, ordering).ToRecordSet();
        }

        public bool Delete(ConnectionInfo transaction, IDataObject obj) {
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
            retv = Execute(transaction, query) > 0;
            return retv;
        }

        public bool SaveItem(ConnectionInfo transaction, IDataObject input) {
            bool retv = false;

            int rs = -33;

            var id = GetIdColumn(input.GetType());
            var rid = GetRidColumn(input.GetType());

            input.CreatedTime = DateTime.UtcNow;

            if (input.IsPersisted) {
                rs = Execute(transaction, Plugin.QueryGenerator.GenerateUpdateQuery(input));
                retv = true;
                return retv;
            }

            rs = Execute(transaction, Plugin.QueryGenerator.GenerateInsertQuery(input));
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
                        var gid = ScalarQuery(transaction, query);
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
                        retvId = ((long?)ScalarQuery(transaction, query)) ?? 0;
                        var gid = ScalarQuery(transaction, query);
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
                retv = true;
            }
            return retv;
        }

        public List<T> AggregateLoad<T>
            (ConnectionInfo transaction,
            Expression<Func<T, bool>> cnd = null, int? skip = null, int? limit = null,
            Expression<Func<T, object>> orderingMember = null, OrderingType otype = OrderingType.Asc,
            MemberInfo GroupingMember = null, bool Linear = false) where T : IDataObject, new() {
            transaction?.Benchmarker?.Mark("Begin AggregateLoad");
            var prefixer = new PrefixMaker();
            var Members = ReflectionTool.FieldsAndPropertiesOf(typeof(T));
            bool hasAnyAggregations = false;
            transaction?.Benchmarker?.Mark("Check if model is Aggregate");
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

                transaction?.Benchmarker?.Mark("Construct Join Definition");
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
                transaction?.Benchmarker?.Mark("Resolve ordering Member");
                var om = GetOrderingMember(orderingMember);
                transaction?.Benchmarker?.Mark("--");
                transaction?.Benchmarker?.Mark("Build final Result");
                var dynamicJoinJumble = join.BuildObject<T>(
                    transaction,
                    (build) => {
                        MakeBuildAggregations(ref build, typeof(T), "root", typeof(T).Name, prefixer, Linear);
                    }, builtConditions, skip, limit, om, otype, builtConditionsRoot
                    //.If(GroupingMember != null).Then()
                    //    .Append($"GROUP BY a.{GroupingMember?.Name}")
                    //.EndIf()
                    //.Append($"{(OrderingType != null ? $"ORDER BY {OrderingType?.Name} {Ordering.ToString().ToUpper()}" : "")}")
                    ).ToList();
                transaction?.Benchmarker?.Mark("--");

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
                var pm = new PrefixMaker();
                var cndb = new ConditionParser(pm).ParseExpression<T>(cnd);
                return Fetch<T>(transaction, cndb, skip, limit, orderingMember, otype).ToList();
            }
        }

        public T LoadFirstOrDefault<T>(ConnectionInfo transaction, Expression<Func<T, bool>> condicoes, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new() {
            return LoadAll<T>(transaction, condicoes, skip, limit, orderingMember, ordering).FirstOrDefault();
        }

        public List<T> Fetch<T>(ConnectionInfo transaction, Expression<Func<T, bool>> conditions = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new() {
            var query = new ConditionParser().ParseExpression(conditions);

            return Fetch<T>(transaction, query, skip, limit, orderingMember, ordering);
        }

        public List<T> Fetch<T>(ConnectionInfo transaction, IQueryBuilder condicoes, int? skip, int? limit, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new() {

            if (condicoes == null) {
                condicoes = Qb.Fmt("TRUE");
            }
            DataTable dt = null;

            transaction.Benchmarker?.Mark("--");

            transaction.Benchmarker?.Mark("Data Load ---");
            MemberInfo ordMember = GetOrderingMember<T>(orderingMember);
            var selectQuery = Plugin.QueryGenerator.GenerateSelect<T>(condicoes, skip, limit, ordMember, ordering);
            transaction.Benchmarker?.Mark("Generate SELECT");
            dt = Query(transaction, selectQuery);
            transaction.Benchmarker?.Mark("Execute SELECT");

            if (dt == null)
                return new List<T>();
            if (dt.Rows.Count < 1) {
                this.WriteLog("Query returned no results.");
                return new List<T>();
            }

            var fields = ReflectionTool.FieldsAndPropertiesOf(typeof(T))
                .Where((a) => a.GetCustomAttribute<FieldAttribute>() != null)
                .ToList();

            var mapFn = Fi.Tech.Map<T>(dt);
            return RunAfterLoads(mapFn).ToList();
        }

        public DataTable Query(ConnectionInfo transaction, IQueryBuilder query) {
            if (query == null || query.GetCommandText() == null) {
                return new DataTable();
            }
            DateTime Inicio = DateTime.Now;
            String QueryText = query.GetCommandText();
            DataTable retv = new DataTable();
            using (var command = transaction.Connection.CreateCommand()) {
                command.Transaction = transaction?.Transaction;
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
        public int Execute(ConnectionInfo transaction, IQueryBuilder query) {
            if (query == null)
                return 0;
            int result = -1;

            transaction.Benchmarker?.Mark("--");
            this.WriteLog($"[{accessId}] -- Execute: {query.GetCommandText()} [{Plugin.CommandTimeout}s timeout]");
            foreach (var param in query.GetParameters()) {
                this.WriteLog($"[{accessId}] @{param.Key} = {param.Value?.ToString() ?? "null"}");
            }
            using (var command = transaction.Connection.CreateCommand()) {
                command.Transaction = transaction?.Transaction;
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
                    transaction.Benchmarker?.Mark("Prepared Statement");
                    result = command.ExecuteNonQuery();
                    var elaps = transaction.Benchmarker?.Mark("Executed Statement");
                    this.WriteLog($"[{accessId}] --------- Executed [OK] ({result} lines affected) [{elaps} ms]");
                } catch (Exception x) {
                    this.WriteLog($"[{accessId}] -------- Error: {x.Message} ([{transaction.Benchmarker?.Mark("Error")} ms]");
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

        #endregion

    }
}