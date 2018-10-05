using Figlotech.BDados.Builders;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using Figlotech.BDados.Helpers;
using Figlotech.Core;
using Figlotech.Core.BusinessModel;
using Figlotech.Core.Helpers;
using Figlotech.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.BDados.DataAccessAbstractions {

    public class ConnectionInfo {
        private IDbConnection Connection { get; set; }
        private IDbTransaction Transaction { get; set; }
        public Benchmarker Benchmarker { get; set; }
        private RdbmsDataAccessor DataAccessor { get; set; }

        public bool IsUsingRdbmsTransaction => Transaction != null;

        public ConnectionInfo(RdbmsDataAccessor rda, IDbConnection connection) {
            DataAccessor = rda;
            Connection = connection;
        }

        private List<IDataObject> ObjectsToNotify { get; set; } = new List<IDataObject>();

        public void NotifyChange(IDataObject[] ido) {

            if (Transaction == null) {
                DataAccessor.RaiseForChangeIn(ido);
            } else {
                lock (ObjectsToNotify)
                    ObjectsToNotify.AddRange(ido);
            }
        }

        public IDbCommand CreateCommand() {
            var retv = Connection?.CreateCommand();
            retv.Transaction = Transaction;
            return retv;
        }

        public void BeginTransaction(bool useTransaction = false, IsolationLevel ilev = IsolationLevel.ReadUncommitted) {
            if (useTransaction) {
                Transaction = Connection?.BeginTransaction(ilev);
            }
        }

        public void Commit() {
            Transaction?.Commit();
            lock (ObjectsToNotify) {
                DataAccessor.RaiseForChangeIn(ObjectsToNotify.ToArray());
                ObjectsToNotify.Clear();
            }
        }

        public void Rollback() {
            Transaction?.Rollback();
            lock (ObjectsToNotify)
                ObjectsToNotify.Clear();
        }

        public void EndTransaction() {
            var conn = Connection;
            Transaction?.Dispose();
            conn?.Dispose();
        }
    }

    public class RdbmsDataAccessor<T> : RdbmsDataAccessor where T : IRdbmsPluginAdapter, new() {
        public RdbmsDataAccessor(IDictionary<String, object> Configuration) : base(new T()) {
            Plugin = new T();
            Plugin.SetConfiguration(Configuration);
        }
    }

    public partial class RdbmsDataAccessor : IRdbmsDataAccessor, IDisposable {
        public ILogger Logger { get; set; }

        public static int DefaultMaxOpenAttempts { get; set; } = 30;
        public static int DefaultOpenAttemptInterval { get; set; } = 100;

        public Type[] _workingTypes = new Type[0];
        public Type[] WorkingTypes {
            get { return _workingTypes; }
            set {
                _workingTypes = value.Where(t => t.GetInterfaces().Contains(typeof(IDataObject))).ToArray();
            }
        }

        internal void RaiseForChangeIn(IDataObject[] ido) {
            if (!ido.Any()) {
                return;
            }
            OnDataObjectAltered?.Invoke(ido.First().GetType(), ido);
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
            if (CurrentTransaction != null) {
                return ForceExist<T>(CurrentTransaction, Default, qb);
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
            var f = LoadAll<T>(qb, null, 1);
            if (f.Any()) {
                return f.First();
            } else {
                T quickSave = Default();
                SaveItem(quickSave);
                return quickSave;
            }
        }

        bool isOnline = true;

        public bool Test() {
            if (isOnline) {
                return true;
            }
            try {
                Execute("SELECT 1");
                return isOnline = true;
            } catch (Exception) {
                return isOnline = false;
            }
        }

        #endregion ***************************

        public T LoadFirstOrDefault<T>(Expression<Func<T, bool>> condicoes, int? page = default(int?), int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new() {
            return LoadAll<T>(condicoes, page, limit, orderingMember, ordering).FirstOrDefault();
        }

        public int ThreadId => Thread.CurrentThread.ManagedThreadId;

        Dictionary<int, ConnectionInfo> _currentTransaction = new Dictionary<int, ConnectionInfo>();
        ConnectionInfo CurrentTransaction {
            get {
                if (_currentTransaction.ContainsKey(ThreadId)) {
                    return _currentTransaction[ThreadId];
                }
                return null;
            }
            set {
                _currentTransaction[ThreadId] = value;
            }
        }

        public void BeginTransaction(bool useTransaction = false, IsolationLevel ilev = IsolationLevel.ReadUncommitted) {
            //lock (this) {
            if (CurrentTransaction == null) {
                WriteLog("Opening Transaction");
                //if (FiTechCoreExtensions.EnableDebug) {
                //    WriteLog(Environment.StackTrace);
                //}
                var connection = Plugin.GetNewConnection
                    ();
                OpenConnection(connection);
                CurrentTransaction = new ConnectionInfo(this, connection);
                CurrentTransaction?.BeginTransaction(useTransaction, ilev);
                CurrentTransaction.Benchmarker = new Benchmarker("Database Access");
                WriteLog("Transaction Open");
            }
            //}
        }

        public void EndTransaction() {
            //lock (this) {
            if (CurrentTransaction != null) {
                WriteLog("Ending Transaction");
                CurrentTransaction?.EndTransaction();

                CurrentTransaction?.Benchmarker.FinalMark();
                CurrentTransaction = null;
                WriteLog("Transaction ended");
            }
            //}
        }

        public void Commit() {
            if (CurrentTransaction != null && CurrentTransaction.IsUsingRdbmsTransaction) {
                WriteLog("Committing Transaction");
                lock ($"DATA_ACCESSOR_COMMIT_{myId}") {
                    CurrentTransaction?.Commit();
                }

                WriteLog("Commit OK");
            }
        }

        public void Rollback() {
            if (CurrentTransaction != null && CurrentTransaction.IsUsingRdbmsTransaction) {
                WriteLog("Rolling back Transaction");
                //lock (this)
                CurrentTransaction?.Rollback();
                WriteLog("Rollback OK");
            }
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
            if (CurrentTransaction != null) {
                return SaveItem(CurrentTransaction, input);
            }
            return Access((transaction) => {
                return SaveItem(transaction, input);
            }, (x) => {
                //this.WriteLog(x.Message);
                //this.WriteLog(x.StackTrace);
                throw x;
            });
        }

        public IList<T> Query<T>(IQueryBuilder query) where T : new() {
            if (CurrentTransaction != null) {
                return Query<T>(CurrentTransaction, query);
            }
            return Access((transaction) => Query<T>(transaction, query));
        }

        public IList<T> Query<T>(string queryString, params object[] args) where T : new() {
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
            return FiTechBDadosExtensions.RidColumnOf[type];
        }

        public T LoadById<T>(long Id) where T : IDataObject, new() {
            if (CurrentTransaction != null) {
                return LoadById<T>(CurrentTransaction, Id);
            }
            return Access((transaction) => LoadById<T>(transaction, Id));
        }

        public T LoadByRid<T>(String RID) where T : IDataObject, new() {
            if (CurrentTransaction != null) {
                return LoadByRid<T>(CurrentTransaction, RID);
            }
            return Access((transaction) => LoadByRid<T>(transaction, RID));
        }

        public IList<T> LoadAll<T>(String where = "TRUE", params object[] args) where T : IDataObject, new() {
            return Fetch<T>(Qb.Fmt(where, args)).ToList();
        }


        public IList<T> LoadAll<T>(Expression<Func<T, bool>> conditions = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new() {
            return Fetch<T>(conditions, skip, limit, orderingMember, ordering).ToList();
        }

        public IList<T> LoadAll<T>(IQueryBuilder condicoes, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new() {
            if (CurrentTransaction != null) {
                return LoadAll<T>(CurrentTransaction, condicoes, skip, limit, orderingMember, ordering).ToList();
            }
            return Access((transaction) => {
                return LoadAll<T>(transaction, condicoes, skip, limit, orderingMember, ordering).ToList();
            });
        }

        public IList<T> Fetch<T>(String where = "TRUE", params object[] args) where T : IDataObject, new() {
            return Fetch<T>(Qb.Fmt(where, args));
        }

        public IList<T> Fetch<T>(Expression<Func<T, bool>> conditions = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new() {
            if (CurrentTransaction != null) {
                return Fetch(CurrentTransaction, conditions, skip, limit, orderingMember, ordering);
            }
            return Access((transaction) => Fetch(transaction, conditions, skip, limit, orderingMember, ordering));
        }

        public IList<T> Fetch<T>(IQueryBuilder condicoes, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new() {
            if (CurrentTransaction != null) {
                return Fetch<T>(CurrentTransaction, condicoes, skip, limit, orderingMember, ordering);
            }
            return Access((transaction) => {
                return Fetch<T>(transaction, condicoes, skip, limit, orderingMember, ordering);
            }, (x) => {
                WriteLog(x.Message);
                WriteLog(x.StackTrace);
                throw x;
            });
        }

        private IList<T> RunAfterLoads<T>(IList<T> target, bool isAggregateLoad) {
            foreach (var a in target) {
                if (target is IBusinessObject ibo) {
                    ibo.OnAfterLoad(new DataLoadContext {
                        DataAccessor = this,
                        IsAggregateLoad = isAggregateLoad
                    });
                }
            }
            return target;
        }

        public bool Delete<T>(IEnumerable<T> obj) where T : IDataObject, new() {
            if (CurrentTransaction != null) {
                return Delete(CurrentTransaction, obj);
            }
            return Access((transaction) => Delete(transaction, obj));
        }

        public bool Delete(IDataObject obj) {
            if (CurrentTransaction != null) {
                return Delete(CurrentTransaction, obj);
            }
            return Access((transaction) => Delete(transaction, obj));
        }

        public bool Delete<T>(Expression<Func<T, bool>> conditions) where T : IDataObject, new() {
            if (CurrentTransaction != null) {
                return Delete(CurrentTransaction, conditions);
            }
            return Access((transaction) => Delete(transaction, conditions));
        }

        #region **** BDados API ****
        int fail = 0;
        private List<Task<Object>> Workers = new List<Task<Object>>();
        private static int accessCount = 0;

        public Object ScalarQuery(IQueryBuilder qb) {
            if (CurrentTransaction != null) {
                return ScalarQuery(CurrentTransaction, qb);
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

        public static IJoinBuilder MakeJoin(Action<JoinDefinition> fn) {
            var retv = new JoinObjectBuilder(fn);
            return retv;
        }

        public IQueryGenerator QueryGenerator => Plugin.QueryGenerator;

        int accessId = 0;

        public event Action<Type, IDataObject[]> OnSuccessfulSave;
        public event Action<Type, IDataObject[], Exception> OnFailedSave;
        public event Action<Type, IDataObject[]> OnDataObjectAltered;
        public event Action<Type, IDataObject[]> OnObjectsDeleted;

        public void Access(Action<ConnectionInfo> functions, Action<Exception> handler = null, bool useTransaction = false) {
            var i = Access<int>((transaction) => {
                functions?.Invoke(transaction);
                return 0;
            }, handler);
        }

        public T Access<T>(Func<ConnectionInfo, T> functions, Action<Exception> handler = null, bool useTransaction = false) {
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
                WriteLog(String.Format("---- Access [{0}] Finished in {1}ms", aid, total));
                return retv;
            }, handler, useTransaction);
        }

        public DataTable Query(IQueryBuilder query) {
            if (CurrentTransaction != null) {
                return Query(CurrentTransaction, query);
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
            Fi.Tech.WriteLine("FTH:RDB", s);
        }

        public int Execute(String str, params object[] args) {
            return Execute(Qb.Fmt(str, args));
        }

        public int Execute(IQueryBuilder query) {
            if (CurrentTransaction != null) {
                return Execute(CurrentTransaction, query);
            }
            return Access((transaction) => {
                return Execute(transaction, query);
            });
        }

        private void OpenConnection(IDbConnection connection) {
            int attempts = DefaultMaxOpenAttempts;
            Exception ex = null;
            while (connection?.State != ConnectionState.Open && attempts-- >= 0) {
                try {
                    connection.Open();
                    isOnline = true;
                    break;
                } catch (Exception x) {
                    isOnline = false;
                    ex = x;
                    if (x.Message.Contains("Unable to connect")) {
                        break;
                    }
                    Thread.Sleep(DefaultOpenAttemptInterval);
                }
            }
            if (connection?.State != ConnectionState.Open) {
                throw new BDadosException($"Cannot open connection to the RDBMS database service (Using {Plugin.GetType().Name}).", ex);
            }
        }

        private T UseTransaction<T>(Func<ConnectionInfo, T> func, Action<Exception> handler = null, bool useTransaction = false) {

            if (func == null) return default(T);

            if (CurrentTransaction != null) {
                try {
                    return func.Invoke(CurrentTransaction);
                } catch (Exception x) {
                    handler?.Invoke(x);
                }
                return default(T);
            }

            BeginTransaction(useTransaction);
            var b = CurrentTransaction.Benchmarker;
            if (FiTechCoreExtensions.EnableDebug) {
                try {
                    int maxFrames = 2;
                    var stack = new StackTrace();
                    foreach (var f in stack.GetFrames()) {
                        var m = f.GetMethod();
                        var mName = m.Name;
                        var t = m.DeclaringType;
                        var tName = t.Name;
                        if (f.GetMethod().DeclaringType.Assembly != GetType().Assembly) {
                            b.Mark($"Database Access from {tName}->{mName}");
                            if (maxFrames-- <= 0) {
                                break;
                            }
                        }
                    }
                } catch (Exception) {

                }
            }
            try {
                CurrentTransaction.Benchmarker = b;
                b.Mark("Run User Code");
                var retv = func.Invoke(CurrentTransaction);

                if (useTransaction) {
                    WriteLog($"[{accessId}] Committing");
                    b.Mark($"[{accessId}] Begin Commit");
                    Commit();
                    b.Mark($"[{accessId}] End Commit");
                    WriteLog($"[{accessId}] Commited OK ");
                }
                return retv;
            } catch (Exception x) {
                if (useTransaction) {
                    WriteLog($"[{accessId}] Begin Rollback : {x.Message} {x.StackTrace}");
                    b.Mark($"[{accessId}] Begin Rollback");
                    Rollback();
                    b.Mark($"[{accessId}] End Rollback");
                    WriteLog($"[{accessId}] Transaction rolled back ");
                }
                if (handler != null) {
                    handler?.Invoke(x);
                } else {
                    throw x;
                }
            } finally {
                EndTransaction();
                b.FinalMark();
            }

            return default(T);

            //using (var connection = Plugin.GetNewConnection()) {
            //    lock (connection) {
            //        OpenConnection(connection);
            //        var connInfo = new ConnectionInfo();
            //        CurrentTransaction = connInfo;
            //        connInfo.Connection = connection;
            //        var b = new Benchmarker("Database Access");
            //        connInfo.Benchmarker = b;
            //        b.WriteToStdout = FiTechCoreExtensions.EnableStdoutLogs;
            //        b.Mark($"INIT Use Transaction: ({useTransaction})");

            //        if (useTransaction)
            //            connInfo.Transaction = connection.BeginTransaction();
            //        //lock (connInfo) {
            //        try {
            //            b.Mark("Run User Code");
            //            var retv = func.Invoke(connInfo);
            //            if (useTransaction) {
            //                WriteLog($"[{accessId}] Committing");
            //                b.Mark($"[{accessId}] Begin Commit");
            //                connInfo?.Transaction?.Commit();
            //                b.Mark($"[{accessId}] End Commit");
            //                WriteLog($"[{accessId}] Commited OK ");
            //            }
            //            return retv;
            //        } catch (Exception x) {
            //            var ex = x;
            //            if (useTransaction) {
            //                WriteLog($"[{accessId}] Begin Rollback : {x.Message} {x.StackTrace}");
            //                b.Mark($"[{accessId}] Begin Rollback");
            //                connInfo?.Transaction?.Rollback();
            //                b.Mark($"[{accessId}] End Rollback");
            //                WriteLog($"[{accessId}] Transaction rolled back ");
            //            }

            //            this.WriteLog("Exception Details:");
            //            var depth = 1;
            //            while (ex != null && ex.InnerException != ex) {
            //                this.WriteLog($"{new String('-', depth)} ERROR [{accessId}]{ex.Message}");
            //                this.WriteLog($"{new String('-', depth)} STACKTRACE [{accessId}] {ex.StackTrace}");
            //                this.WriteLog($"{new String('-', depth)}>");
            //                depth++;
            //                ex = ex.InnerException;
            //            }


            //            if (handler != null) {
            //                handler.Invoke(x);
            //            } else {
            //                throw x;
            //            }

            //            return default(T);
            //        } finally {
            //            CurrentTransaction = null;
            //            b.Mark($"[{accessId}] Dispose objects");
            //            if (useTransaction)
            //                connInfo.Transaction.Dispose();

            //            b.FinalMark();
            //        }
            //        //}
            //    }
            //}
        }

        //public IList<T> ExecuteProcedure<T>(params object[] args) where T : ProcedureResult {
        //    DateTime inicio = DateTime.Now;
        //    IList<T> retv = new List<T>();
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
        private static void MakeQueryAggregations(ref JoinDefinition query, Type theType, String parentAlias, String nameofThis, String pKey, PrefixMaker prefixer, bool Linear = false) {
            var membersOfT = ReflectionTool.FieldsAndPropertiesOf(theType);
            //var reflectedJoinMethod = query.GetType().GetMethod("Join");

            String thisAlias = prefixer.GetAliasFor(parentAlias, nameofThis, pKey);

            // Iterating through AggregateFields
            foreach (var field in membersOfT.Where(
                    (f) =>
                        f.GetCustomAttribute<AggregateFieldAttribute>() != null)) {
                var infoField = field.GetCustomAttribute<AggregateFieldAttribute>();
                var type = infoField?.RemoteObjectType;
                var key = infoField?.ObjectKey;
                String childAlias;
                var tname = type.Name;
                var pkey = key;
                // This inversion principle might be fucktastic.
                childAlias = prefixer.GetAliasFor(thisAlias,
                    tname,
                    pkey);

                String OnClause = $"{thisAlias}.{key}={childAlias}.RID";

                if (!ReflectionTool.TypeContains(theType, key)) {
                    OnClause = $"{thisAlias}.RID={childAlias}.{key}";
                }
                var joh = query.Join(type, childAlias, OnClause, JoinType.LEFT);

                joh.As(childAlias);

                var qjoins = query.Joins.Where((a) => a.Alias == childAlias);
                if (qjoins.Any() && !qjoins.First().Columns.Contains(infoField?.RemoteField)) {
                    qjoins.First().Columns.Add(infoField?.RemoteField);
                    //continue;
                }

                if (field.GetCustomAttribute<AggregateFieldAttribute>() != null) {
                    joh.GetType().GetMethod("OnlyFields").Invoke(joh, new object[] { new string[] { field.GetCustomAttribute<AggregateFieldAttribute>().RemoteField } });
                }
            }

            // Iterating through AggregateFarFields
            foreach (var field in membersOfT.Where(
                    (f) =>
                        f.GetCustomAttribute<AggregateFarFieldAttribute>() != null)) {
                var memberType = ReflectionTool.GetTypeOf(field);
                var info = field.GetCustomAttribute<AggregateFarFieldAttribute>();
                String childAlias = prefixer.GetAliasFor(thisAlias, info.ImediateType.Name, info.ImediateKey);
                String farAlias = prefixer.GetAliasFor(childAlias, info.FarType.Name, info.FarKey);

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

                    var joh1 = query.Join(info.ImediateType, childAlias, OnClause, JoinType.LEFT);

                    // Parent Alias is typeof(T).Name
                    // Child Alias is field.Name
                    // The ultra supreme gimmick mode reigns supreme here.
                    joh1.As(childAlias);
                    joh1.OnlyFields(new string[] { info.FarKey });

                }

                var qfar = query.Joins.Where((j) => j.Alias == farAlias);
                if (qfar.Any() && !qfar.First().Columns.Contains(info.FarField)) {
                    qfar.First().Columns.Add(info.FarField);
                    continue;
                } else {
                    String OnClause2 = $"{childAlias}.{info.FarKey}={farAlias}.RID";
                    // This inversion principle will be fucktastic.
                    // But has to be this way for now.
                    if (!ReflectionTool.TypeContains(info.ImediateType, info.FarKey)) {
                        OnClause2 = $"{childAlias}.RID={farAlias}.{info.FarKey}";
                    }

                    var joh2 = query.Join(info.FarType, farAlias, OnClause2, JoinType.LEFT);
                    // Parent Alias is typeof(T).Name
                    // Child Alias is field.Name
                    // The ultra supreme gimmick mode reigns supreme here.
                    joh2.As(farAlias);
                    joh2.OnlyFields(new string[] { info.FarField });
                }
            }
            // We want to skip aggregate objects and lists 
            // When doing linear aggregate loads
            // The linear option is just to provide faster
            // and shallower information.
            if (Linear)
                return;
            // Iterating through AggregateObjects
            foreach (var field in membersOfT.Where(
                    (f) => f.GetCustomAttribute<AggregateObjectAttribute>() != null)) {
                var memberType = ReflectionTool.GetTypeOf(field);
                var type = ReflectionTool.GetTypeOf(field);
                var key = field.GetCustomAttribute<AggregateObjectAttribute>()?.ObjectKey;
                var infoObj = field.GetCustomAttribute<AggregateObjectAttribute>();
                String childAlias;
                var tname = type.Name;
                var pkey = key;
                // This inversion principle might be fucktastic.
                childAlias = prefixer.GetAliasFor(thisAlias,
                    tname,
                    pkey);

                String OnClause = $"{thisAlias}.{key}={childAlias}.RID";

                if (!ReflectionTool.TypeContains(theType, key)) {
                    OnClause = $"{thisAlias}.RID={childAlias}.{key}";
                }

                var joh = query.Join(type, childAlias, OnClause, JoinType.LEFT);

                joh.As(childAlias);

                var qjoins = query.Joins.Where((a) => a.Alias == childAlias);
                if (qjoins.Any()) {
                    qjoins.First().Columns.AddRange(
                        ReflectionTool.FieldsWithAttribute<FieldAttribute>(memberType)
                            .Select(m => m.Name)
                            .Where(i => !qjoins.First().Columns.Contains(i))
                    );
                    //continue;
                }

                var ago = field.GetCustomAttribute<AggregateObjectAttribute>();
                if (ago != null) {
                    MakeQueryAggregations(ref query, type, thisAlias, tname, pkey, prefixer);
                }
            }
            // Iterating through AggregateLists
            foreach (var field in membersOfT.Where(
                    (f) =>
                        f.GetCustomAttribute<AggregateListAttribute>() != null)) {
                var memberType = ReflectionTool.GetTypeOf(field);
                var info = field.GetCustomAttribute<AggregateListAttribute>();
                String childAlias = prefixer.GetAliasFor(thisAlias, info.RemoteObjectType.Name, info.RemoteField);

                String OnClause = $"{childAlias}.{info.RemoteField}={thisAlias}.RID";
                // Yuck
                if (!ReflectionTool.TypeContains(info.RemoteObjectType, info.RemoteField)) {
                    OnClause = $"{childAlias}.RID={thisAlias}.{info.RemoteField}";
                }
                var joh = query.Join(info.RemoteObjectType, childAlias, OnClause, JoinType.RIGHT);
                // The ultra supreme gimmick mode reigns supreme here.
                joh.GetType().GetMethod("As").Invoke(joh, new object[] { childAlias });

                var qjoins = query.Joins.Where((a) => a.Alias == childAlias);
                if (qjoins.Any()) {
                    qjoins.First().Columns.AddRange(
                        ReflectionTool.FieldsWithAttribute<FieldAttribute>(info.RemoteObjectType)
                            .Select(m => m.Name)
                            .Where(i => !qjoins.First().Columns.Contains(i))
                    );
                    //continue;
                }

                if (!Linear) {
                    MakeQueryAggregations(ref query, info.RemoteObjectType, thisAlias, info.RemoteObjectType.Name, info.RemoteField, prefixer);
                }
            }
        }


        private static void MakeBuildAggregations(BuildParametersHelper build, Type theType, String parentAlias, String nameofThis, String pKey, PrefixMaker prefixer, bool Linear = false) {
            // Don't try this at home kids.
            var membersOfT = ReflectionTool.FieldsAndPropertiesOf(theType);

            String thisAlias = prefixer.GetAliasFor(parentAlias, nameofThis, pKey);
            // Iterating through AggregateFields
            foreach (var field in membersOfT.Where((f) => f.GetCustomAttribute<AggregateFieldAttribute>() != null)) {
                var memberType = ReflectionTool.GetTypeOf(field);
                var info = field.GetCustomAttribute<AggregateFieldAttribute>();
                String childAlias = prefixer.GetAliasFor(thisAlias, info.RemoteObjectType.Name, info.ObjectKey);
                build.AggregateField(thisAlias, childAlias, info.RemoteField, field.Name);
            }
            // Iterating through AggregateFarFields
            foreach (var field in membersOfT.Where(
                    (f) =>
                        f.GetCustomAttribute<AggregateFarFieldAttribute>() != null)) {
                var memberType = ReflectionTool.GetTypeOf(field);
                var info = field.GetCustomAttribute<AggregateFarFieldAttribute>();
                String childAlias = prefixer.GetAliasFor(thisAlias, info.ImediateType.Name, info.ImediateKey);
                String farAlias = prefixer.GetAliasFor(childAlias, info.FarType.Name, info.FarKey);
                build.AggregateField(thisAlias, farAlias, info.FarField, field.Name);
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
            // Iterating through AggregateObjects
            foreach (var field in membersOfT.Where((f) => f.GetCustomAttribute<AggregateObjectAttribute>() != null)) {
                var memberType = ReflectionTool.GetTypeOf(field);
                var info = field.GetCustomAttribute<AggregateObjectAttribute>();
                String childAlias = prefixer.GetAliasFor(thisAlias, memberType.Name, info.ObjectKey);
                build.AggregateObject(thisAlias, childAlias, field.Name);
                if (!Linear) {
                    MakeBuildAggregations(build, ReflectionTool.GetTypeOf(field), thisAlias, memberType.Name, info.ObjectKey, prefixer);
                }
            }
            // Iterating through AggregateLists
            foreach (var field in membersOfT.Where((f) => f.GetCustomAttribute<AggregateListAttribute>() != null)) {
                var memberType = ReflectionTool.GetTypeOf(field);
                var info = field.GetCustomAttribute<AggregateListAttribute>();
                String childAlias = prefixer.GetAliasFor(thisAlias, info.RemoteObjectType.Name, info.RemoteField);
                build.AggregateList(thisAlias, childAlias, field.Name);
                if (!Linear) {
                    MakeBuildAggregations(build, info.RemoteObjectType, thisAlias, info.RemoteObjectType.Name, info.RemoteField, prefixer);
                }
            }
        }

        public IList<T> AggregateLoad<T>(
            Expression<Func<T, bool>> cnd = null, int? skip = null, int? limit = null,
            Expression<Func<T, object>> orderingMember = null, OrderingType otype = OrderingType.Asc,
            MemberInfo GroupingMember = null, bool Linear = false) where T : IDataObject, new() {

            if (CurrentTransaction != null) {
                return AggregateLoad(
                    CurrentTransaction, cnd, skip, limit,
                    orderingMember, otype,
                    GroupingMember, Linear);
            }
            return Access((transaction) =>
                AggregateLoad(
                    transaction, cnd, skip, limit,
                    orderingMember, otype,
                    GroupingMember, Linear));
        }



        public bool DeleteWhereRidNotIn<T>(Expression<Func<T, bool>> cnd, IList<T> list) where T : IDataObject, new() {
            if (CurrentTransaction != null) {
                return DeleteWhereRidNotIn(CurrentTransaction, cnd, list);
            }
            return Access((transaction) => DeleteWhereRidNotIn(transaction, cnd, list));
        }

        public void Dispose() {
            //this.Close();
        }
        #endregion *****************
        //
        #region Default Transaction Using Core Functions.
        public T ForceExist<T>(ConnectionInfo transaction, Func<T> Default, IQueryBuilder qb) where T : IDataObject, new() {
            var f = LoadAll<T>(transaction, qb, null, 1);
            if (f.Any()) {
                return f.First();
            } else {
                T quickSave = Default();
                SaveItem(quickSave);
                return quickSave;
            }
        }

        public bool SaveList<T>(ConnectionInfo transaction, IList<T> rs, bool recoverIds = false) where T : IDataObject {
            bool retv = true;
            if (rs.Count == 0)
                return true;
            if (rs.Count == 1) {
                return SaveItem(transaction, rs.First());
            }
            for (int it = 0; it < rs.Count; it++) {
                if (rs[it].RID == null) {
                    rs[it].RID = new RID().ToString();
                }
            }

            rs.ForEach(item => {
                item.IsPersisted = false;
                if(!item.IsReceivedFromSync) {
                    item.UpdatedTime = DateTime.UtcNow;
                }
            });
            List<T> conflicts = new List<T>();
            var persistedMap = Query(transaction, Plugin.QueryGenerator.QueryIds(rs));
            foreach (DataRow a in persistedMap.Rows) {
                var rowEquivalentObject = rs.FirstOrDefault(item => item.Id == ((long) Convert.ChangeType(a[0], typeof(long))) || item.RID == a[1] as String);
                if (rowEquivalentObject != null) {
                    if(rowEquivalentObject.RID != a[1] as String) {
                        rowEquivalentObject.RID = a[1] as String;
                    }
                    rowEquivalentObject.IsPersisted = true;
                }
            }

            var members = ReflectionTool.FieldsAndPropertiesOf(typeof(T))
                .Where(t => t.GetCustomAttribute<FieldAttribute>() != null);
            int i2 = 0;
            int cut = 500;
            int rst = 0;
            IList<T> temp;

            if (rs.Count > cut) {
                temp = new List<T>();
                temp.AddRange(rs);
                temp.OrderBy(it => it.IsPersisted);
            } else {
                temp = rs;
            }
            List<Exception> failedSaves = new List<Exception>();
            List<IDataObject> successfulSaves = new List<IDataObject>();
            List<IDataObject> failedObjects = new List<IDataObject>();
            transaction?.Benchmarker.Mark($"Begin SaveList process");
            WorkQueuer wq = rs.Count > cut ? new WorkQueuer("SaveList_Annonymous_Queuer", Environment.ProcessorCount, true) : null;
            while (i2 * cut < rs.Count) {
                int i = i2;
                Action WorkFn = () => {
                    List<T> sub;
                    lock (temp)
                        sub = temp.Skip(i * cut).Take(Math.Min(rs.Count - (i * cut), cut)).ToList();
                    var inserts = sub.Where(it => !it.IsPersisted).ToArray();
                    var updates = sub.Where(it => it.IsPersisted).ToArray();
                    if (inserts.Length > 0) {
                        try {
                            transaction?.Benchmarker.Mark($"Generate MultiInsert Query for {inserts.Length} items");
                            var query = Plugin.QueryGenerator.GenerateMultiInsert(inserts, false);
                            transaction?.Benchmarker.Mark($"Execute MultiInsert Query {inserts.Length}");
                            lock (transaction)
                                rst += Execute(transaction, query);
                            lock (successfulSaves)
                                successfulSaves.AddRange(inserts.Select(a => (IDataObject)a));
                        } catch (Exception x) {
                            Fi.Tech.RunAndForget(() => {
                                OnFailedSave?.Invoke(typeof(T), inserts.Select(a => (IDataObject)a).ToArray(), x);
                            });

                            lock (failedSaves)
                                failedSaves.Add(x);
                        }
                        var queryIds = Query(transaction, QueryGenerator.QueryIds(inserts));
                        foreach (DataRow dr in queryIds.Rows) {
                            var psave = inserts.FirstOrDefault(it => it.RID == dr[1] as String);
                            if (psave == null) {
                                psave.Id = Int64.Parse(dr[0] as String);
                            }
                        }
                    }

                    if (updates.Length > 0) {
                        try {
                            transaction?.Benchmarker.Mark($"Generate MultiUpdate Query for {updates.Length} items");
                            var query = Plugin.QueryGenerator.GenerateMultiUpdate(updates);
                            transaction?.Benchmarker.Mark($"Execute MultiUpdate Query for {updates.Length} items");
                            lock (transaction)
                                rst += Execute(transaction, query);
                            lock (successfulSaves)
                                successfulSaves.AddRange(updates.Select(a => (IDataObject)a));
                        } catch (Exception x) {
                            Fi.Tech.RunAndForget(() => {
                                OnFailedSave?.Invoke(typeof(T), updates.Select(a => (IDataObject)a).ToArray(), x);
                            });
                            lock (failedSaves)
                                failedSaves.Add(x);
                        }
                    }
                };

                if (rs.Count > cut) {
                    wq.Enqueue(WorkFn);
                } else {
                    WorkFn.Invoke();
                }

                i2++;
            }
            wq?.Stop(true);
            transaction?.Benchmarker.Mark($"End SaveList process");

            transaction?.Benchmarker.Mark($"Dispatch Successful Save events");
            if (successfulSaves.Any()) {
                if (recoverIds) {
                    var q = Query(transaction, QueryGenerator.QueryIds(rs));
                    foreach (DataRow dr in q.Rows) {
                        successfulSaves.FirstOrDefault(it => it.RID == dr[1] as String).Id = Int64.Parse(dr[0] as String);
                    }
                    failedObjects.AddRange(successfulSaves.Where(a => a.Id <= 0).Select(a => (IDataObject)a));
                    successfulSaves.RemoveAll(a => a.Id <= 0);
                }
                Fi.Tech.RunAndForget(() => {
                    int newHash = 0;
                    successfulSaves.ForEach(it => {
                        newHash = it.SpFthComputeDataFieldsHash();
                        if (it.PersistedHash != newHash) {
                            it.PersistedHash = newHash;
                            it.AlteredBy = IDataObjectExtensions.localInstanceId;
                        }
                    });
                    transaction.NotifyChange(successfulSaves.ToArray());
                    OnSuccessfulSave?.Invoke(typeof(T), successfulSaves.ToArray());
                });
            }
            transaction?.Benchmarker.Mark($"SaveList all done");
            if (failedSaves.Any()) {
                throw new BDadosException("Not everything could be saved", failedObjects, new AggregateException(failedSaves));
            }
            if (failedObjects.Any()) {
                var ex = new BDadosException("Some objects did not persist correctly", failedObjects, null);
                Fi.Tech.RunAndForget(() => {
                    OnFailedSave?.Invoke(typeof(T), failedObjects.Select(a => (IDataObject)a).ToArray(), ex);
                });
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

        public bool DeleteWhereRidNotIn<T>(ConnectionInfo transaction, Expression<Func<T, bool>> cnd, IList<T> list) where T : IDataObject, new() {
            int retv = 0;
            if (list == null)
                return true;

            var id = GetIdColumn<T>();
            var rid = GetRidColumn<T>();
            QueryBuilder query = Qb.Fmt($"SELECT * FROM {typeof(T).Name} WHERE ");
            if (cnd != null) {
                PrefixMaker pm = new PrefixMaker();
                query.Append($"{rid} IN (SELECT {rid} FROM (SELECT {rid} FROM {typeof(T).Name} AS {pm.GetAliasFor("root", typeof(T).Name, String.Empty)} WHERE ");
                query.Append(new ConditionParser(pm).ParseExpression<T>(cnd));
                query.Append(") sub)");
            }
            if (list.Count > 0) {

                query += Qb.And();
                query += Qb.NotIn(rid, list, l => l.RID);
                //for (var i = 0; i < list.Count; i++) {
                //    query.Append($"@{IntEx.GenerateShortRid()}", list[i].RID);
                //    if (i < list.Count - 1)
                //        query.Append(",");
                //}
                //query.Append(")");
            }

            var results = Query<T>(transaction, query);
            if (results.Any()) {
                OnObjectsDeleted?.Invoke(typeof(T), results.Select(t => t as IDataObject).ToArray());
                var query2 = Qb.Fmt($"DELETE FROM {typeof(T).Name} WHERE ") + Qb.In(rid, results, r => r.RID);
                retv = Execute(transaction, query2);
                return retv > 0;
            }
            return true;

            //var id = GetIdColumn<T>();
            //var rid = GetRidColumn<T>();
            //var query = Qb.Fmt($"DELETE FROM {typeof(T).Name} WHERE ");
            //if (cnd != null) {
            //    PrefixMaker pm = new PrefixMaker();
            //    query.Append($"{rid} IN (SELECT {rid} FROM (SELECT {rid} FROM {typeof(T).Name} AS {pm.GetAliasFor("root", typeof(T).Name, String.Empty)} WHERE ");
            //    query.Append(new ConditionParser(pm).ParseExpression<T>(cnd));
            //    query.Append(") sub)");
            //}
            //if (list.Count > 0) {
            //    query.Append($"AND {rid} NOT IN (");
            //    for (var i = 0; i < list.Count; i++) {
            //        query.Append($"@{IntEx.GenerateShortRid()}", list[i].RID);
            //        if (i < list.Count - 1)
            //            query.Append(",");
            //    }
            //    query.Append(")");
            //}
            //retv = Execute(transaction, query);
            //return retv > 0;
        }

        public bool SaveList<T>(IList<T> rs, bool recoverIds = false) where T : IDataObject {
            if (CurrentTransaction != null) {
                return SaveList<T>(CurrentTransaction, rs, recoverIds);
            }
            return Access((transaction) => {
                return SaveList<T>(transaction, rs, recoverIds);
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
                        var jh = q.AggregateRoot(typeof(T), p.GetAliasFor("root", typeof(T).Name, String.Empty)).As(p.GetAliasFor("root", typeof(T).Name, String.Empty));
                        jh.OnlyFields(
                            ReflectionTool.FieldsWithAttribute<FieldAttribute>(typeof(T))
                            .Select(a => a.Name)
                        );
                        MakeQueryAggregations(ref q, typeof(T), "root", typeof(T).Name, String.Empty, p, false);
                    });

            var query = Qb.Fmt($"DELETE FROM {typeof(T).Name} WHERE ");
            query.Append($"{id} IN (SELECT tba_{id} as {id} FROM (");
            query.Append(join.GenerateQuery(Plugin.QueryGenerator, new ConditionParser(p).ParseExpression<T>(conditions)));
            query.Append(") as outmost )");
            retv = Execute(transaction, query) > 0;
            return retv;
        }

        //public DbType GetDbTypeOf(object value) {
        //    if(value != null) {
        //        switch(value) {
        //            case String s:
        //                return DbType.String;
        //            case short s:
        //                return DbType.Int16;
        //            case int i:
        //                return DbType.Int32;
        //            case long l:
        //                return DbType.Int64;
        //            case float f:
        //                return DbType.Single;
        //            case double d:
        //            case decimal m:
        //                return DbType.Double;
        //            case DateTime dt:
        //                return DbType.DateTime;
        //            case bool b:
        //                return DbType.Boolean;
        //        }
        //        if(value.GetType().IsEnum) {
        //            return DbType.Int32;
        //        }
        //    }
        //    return DbType.Object;
        //}

        public IList<T> Query<T>(ConnectionInfo transaction, IQueryBuilder query) where T : new() {
            if (query == null || query.GetCommandText() == null) {
                return new List<T>();
            }
            DateTime Inicio = DateTime.Now;
            String QueryText = query.GetCommandText();
            using (var command = transaction.CreateCommand()) {
                command.CommandText = QueryText;
                command.CommandTimeout = Plugin.CommandTimeout;
                WriteLog($"[{accessId}] -- Query: {QueryText}");
                transaction?.Benchmarker?.Mark($"[{accessId}] Prepare Statement");
                // Adiciona os parametros
                foreach (KeyValuePair<String, Object> param in query.GetParameters()) {
                    var cmdParam = command.CreateParameter();
                    cmdParam.ParameterName = param.Key;
                    if (param.Value is String str) {
                        cmdParam.Value = str;
                        cmdParam.DbType = DbType.String;
                        var paramRefl = new ObjectReflector(cmdParam);
                        paramRefl["Encoding"] = Encoding.UTF8;
                    } else {
                        cmdParam.Value = param.Value;
                    }
                    cmdParam.Direction = ParameterDirection.Input;

                    var pval = $"'{param.Value?.ToString() ?? "null"}'";
                    if (param.Value is DateTime || param.Value is DateTime? && ((DateTime?)param.Value).HasValue) {
                        pval = ((DateTime)param.Value).ToString("yyyy-MM-dd HH:mm:ss");
                        pval = $"'{pval}'";
                    }

                    command.Parameters.Add(cmdParam);
                    WriteLog($"[{accessId}] SET @{param.Key} = {pval} -- {cmdParam.DbType.ToString()}");
                    //if (Debugger.IsAttached) {
                    //    Debugger.Break();
                    //}
                }
                // --
                IList<T> retv;
                lock (transaction) {
                    retv = GetObjectList<T>(transaction, command);
                }
                if (retv == null) {
                    throw new Exception("Null list generated");
                }
                var elaps = transaction?.Benchmarker?.Mark($"[{accessId}] Built List Size: {retv.Count}");
                transaction?.Benchmarker?.Mark($"[{accessId}] Avg Build speed: {((double) elaps / (double) retv.Count).ToString("0.00")}ms/item");

                try {
                    int resultados = 0;
                    resultados = retv.Count;
                    WriteLog($"[{accessId}] -------- Queried [OK] ({resultados} results) [{elaps} ms]");
                    return retv;
                } catch (Exception x) {
                    WriteLog($"[{accessId}] -------- Error: {x.Message} ([{DateTime.Now.Subtract(Inicio).TotalMilliseconds} ms]");
                    WriteLog(x.Message);
                    WriteLog(x.StackTrace);
                    throw x;
                } finally {
                    WriteLog("------------------------------------");
                }
            }
        }

        public T LoadById<T>(ConnectionInfo transaction, long Id) where T : IDataObject, new() {
            var id = GetIdColumn(typeof(T));
            return LoadAll<T>(transaction, new Qb().Append($"{id}=@id", Id), null, 1).FirstOrDefault();
        }

        public T LoadByRid<T>(ConnectionInfo transaction, String RID) where T : IDataObject, new() {
            var rid = GetRidColumn(typeof(T));
            return LoadAll<T>(transaction, new Qb().Append($"{rid}=@rid", RID), null, 1).FirstOrDefault();
        }

        public IList<T> LoadAll<T>(ConnectionInfo transaction, Expression<Func<T, bool>> conditions = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new() {
            return Fetch<T>(transaction, conditions, skip, limit, orderingMember, ordering).ToList();
        }

        public IList<T> LoadAll<T>(ConnectionInfo transaction, IQueryBuilder condicoes, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new() {
            return Fetch<T>(transaction, condicoes, skip, limit, orderingMember, ordering).ToList();
        }

        public bool Delete(ConnectionInfo transaction, IDataObject obj) {
            bool retv = false;

            //var id = GetIdColumn(obj.GetType());
            var rid = obj.RID;
            var ridcol = FiTechBDadosExtensions.RidColumnOf[obj.GetType()];
            string ridname = "RID";
            if (ridcol != null) {
                ridname = ridcol;
            }
            OnObjectsDeleted(obj.GetType(), obj.ToSingleElementList().ToArray());
            var query = new Qb().Append($"DELETE FROM {obj.GetType().Name} WHERE {ridname}=@rid", obj.RID);
            retv = Execute(transaction, query) > 0;
            return retv;
        }

        public bool Delete<T>(ConnectionInfo transaction, IEnumerable<T> obj) where T : IDataObject, new() {
            bool retv = false;

            var ridcol = FiTechBDadosExtensions.RidColumnOf[typeof(T)];
            string ridname = "RID";
            if (ridcol != null) {
                ridname = ridcol;
            }
            OnObjectsDeleted(typeof(T), obj.Select(t => t as IDataObject).ToArray());
            var query = Qb.Fmt($"DELETE FROM {typeof(T).Name} WHERE ") + Qb.In(ridname, obj.ToList(), o => o.RID);
            retv = Execute(transaction, query) > 0;
            return retv;
        }

        public bool SaveItem(ConnectionInfo transaction, IDataObject input) {

            if (input == null) {
                throw new ArgumentNullException("Input to SaveItem must be not-null");
            }

            bool retv = false;

            int rs = -33;

            var id = GetIdColumn(input.GetType());
            var rid = GetRidColumn(input.GetType());

            if (!input.IsReceivedFromSync) {
                input.UpdatedTime = DateTime.UtcNow;
            }

            input.IsPersisted = false;
            var persistedMap = Query(transaction, Plugin.QueryGenerator.QueryIds(input.ToSingleElementList()));
            foreach (DataRow a in persistedMap.Rows) {
                if (input.RID == a[1] as String) {
                    input.IsPersisted = true;
                }
            }


            if (input.IsPersisted) {
                rs = Execute(transaction, Plugin.QueryGenerator.GenerateUpdateQuery(input));
                retv = true;
                transaction.NotifyChange(input.ToSingleElementList().ToArray());
                return retv;
            }

            try {
                rs = Execute(transaction, Plugin.QueryGenerator.GenerateInsertQuery(input));
            } catch (Exception x) {
                Fi.Tech.RunAndForget(() => {
                    OnFailedSave?.Invoke(input.GetType(), new List<IDataObject> { input }.ToArray(), x);
                }, (xe) => {
                    Fi.Tech.Throw(xe);
                });
                throw x;
            }
            if (rs == 0) {
                WriteLog("** Something went SERIOUSLY NUTS in SaveItem<T> **");
            }

            retv = rs > 0;
            if (retv && !input.IsPersisted) {
                if (input.Id <= 0) {
                    long retvId = 0;

                    //var ridAtt = ReflectionTool.FieldsAndPropertiesOf(input.GetType()).Where((f) => f.GetCustomAttribute<ReliableIdAttribute>() != null);

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
                                Int64.TryParse(s, out retvId);
                            }
                        } catch (Exception) {

                        }
                    }

                    if (retvId <= 0) {
                        try {
                            var query = (IQueryBuilder)Plugin.QueryGenerator
                                .GetType()
                                .GetMethod(nameof(Plugin.QueryGenerator.GetLastInsertId))
                                .MakeGenericMethod(input.GetType())
                                .Invoke(Plugin.QueryGenerator, new Object[0]);
                            var gid1 = ScalarQuery(transaction, query);
                            if (gid1 is long l)
                                retvId = l;
                            if (retvId <= 0) {
                                var gid = ScalarQuery(transaction, query);
                                if (gid is long l2)
                                    retvId = l2;
                                if (gid is string s) {
                                    if (Int64.TryParse(s, out retvId)) {
                                    }
                                }
                            }
                        } catch (Exception x) {
                            Fi.Tech.RunAndForget(() => {
                                OnFailedSave?.Invoke(input.GetType(), new List<IDataObject> { input }.ToArray(), x);
                            }, (xe) => {
                                Fi.Tech.Throw(xe);
                            });
                        }
                    }

                    if (retvId > 0) {
                        input.Id = retvId;
                    }
                }

                var newHash = input.SpFthComputeDataFieldsHash();
                if (input.PersistedHash != newHash) {
                    input.PersistedHash = newHash;
                    input.AlteredBy = IDataObjectExtensions.localInstanceId;
                }
                transaction.NotifyChange(input.ToSingleElementList().ToArray());
                retv = true;
            }

            Fi.Tech.RunAndForget(() => {
                OnSuccessfulSave?.Invoke(input.GetType(), new List<IDataObject> { input }.ToArray());
            }, (xe) => {
                Fi.Tech.Throw(xe);
            });

            return retv;
        }

        static SelfInitializerDictionary<Type, PrefixMaker> CacheAutoPrefixer = new SelfInitializerDictionary<Type, PrefixMaker>(
            type => {
                return new PrefixMaker();
            }
        );
        static SelfInitializerDictionary<Type, PrefixMaker> CacheAutoPrefixerLinear = new SelfInitializerDictionary<Type, PrefixMaker>(
            type => {
                return new PrefixMaker();
            }
        );

        static SelfInitializerDictionary<Type, JoinDefinition> CacheAutoJoinLinear = new SelfInitializerDictionary<Type, JoinDefinition>(
            type => {
                var retv = CacheAutomaticJoinBuilderLinear[type].GetJoin();
                var _buildParameters = new BuildParametersHelper(retv);
                MakeBuildAggregations(_buildParameters, type, "root", type.Name, String.Empty, CacheAutoPrefixerLinear[type], true);

                return retv;
            }
        );
        static SelfInitializerDictionary<Type, JoinDefinition> CacheAutoJoin = new SelfInitializerDictionary<Type, JoinDefinition>(
            type => {
                var retv = CacheAutomaticJoinBuilder[type].GetJoin();
                var _buildParameters = new BuildParametersHelper(retv);
                MakeBuildAggregations(_buildParameters, type, "root", type.Name, String.Empty, CacheAutoPrefixer[type], false);

                return retv;
            }
        );

        static SelfInitializerDictionary<Type, IJoinBuilder> CacheAutomaticJoinBuilder = new SelfInitializerDictionary<Type, IJoinBuilder>(
            type => {
                var prefixer = CacheAutoPrefixer[type];
                return MakeJoin(
                    (query) => {
                        // Starting with T itself
                        var jh = query.AggregateRoot(type, prefixer.GetAliasFor("root", type.Name, String.Empty)).As(prefixer.GetAliasFor("root", type.Name, String.Empty));
                        jh.OnlyFields(
                            ReflectionTool.FieldsWithAttribute<FieldAttribute>(type)
                            .Select(a => a.Name)
                        );
                        MakeQueryAggregations(ref query, type, "root", type.Name, String.Empty, prefixer, false);
                    });
            }
        );
        static SelfInitializerDictionary<Type, IJoinBuilder> CacheAutomaticJoinBuilderLinear = new SelfInitializerDictionary<Type, IJoinBuilder>(
            type => {
                var prefixer = CacheAutoPrefixerLinear[type];
                return MakeJoin(
                    (query) => {
                        // Starting with T itself
                        var jh = query.AggregateRoot(type, prefixer.GetAliasFor("root", type.Name, String.Empty)).As(prefixer.GetAliasFor("root", type.Name, String.Empty));
                        jh.OnlyFields(
                            ReflectionTool.FieldsWithAttribute<FieldAttribute>(type)
                            .Select(a => a.Name)
                        );
                        MakeQueryAggregations(ref query, type, "root", type.Name, String.Empty, prefixer, true);
                    });
            }
        );

        public IList<T> AggregateLoad<T>
            (ConnectionInfo transaction,
            Expression<Func<T, bool>> cnd = null, int? skip = null, int? limit = null,
            Expression<Func<T, object>> orderingMember = null, OrderingType otype = OrderingType.Asc,
            MemberInfo GroupingMember = null, bool Linear = false) where T : IDataObject, new() {
            if (limit < 0) {
                limit = DefaultQueryLimit;
            }
            transaction?.Benchmarker?.Mark("Begin AggregateLoad");
            var Members = ReflectionTool.FieldsAndPropertiesOf(typeof(T));
            var prefixer = Linear ? CacheAutoPrefixerLinear[typeof(T)] : CacheAutoPrefixer[typeof(T)];
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

            WriteLog($"Running Aggregate Load All for {typeof(T).Name}? {hasAnyAggregations}. Linear? {Linear}");
            // CLUMSY
            if (hasAnyAggregations) {
                var membersOfT = ReflectionTool.FieldsAndPropertiesOf(typeof(T));

                transaction?.Benchmarker?.Mark("Construct Join Definition");

                var builtConditions = (cnd == null ? Qb.Fmt("TRUE") : new ConditionParser(prefixer).ParseExpression(cnd));
                var builtConditionsRoot = (cnd == null ? Qb.Fmt("TRUE") : new ConditionParser(prefixer).ParseExpression(cnd, false));

                transaction?.Benchmarker?.Mark("Resolve ordering Member");
                var om = GetOrderingMember(orderingMember);
                transaction?.Benchmarker?.Mark("--");

                using (var command = transaction?.CreateCommand()) {
                    var join = Linear ? CacheAutoJoinLinear[typeof(T)] : CacheAutoJoin[typeof(T)];
                    transaction?.Benchmarker?.Mark("Generate Join Query");
                    //var _buildParameters = Linear ? CacheBuildParamsLinear[typeof(T)] : CacheBuildParams[typeof(T)];
                    var query = Plugin.QueryGenerator.GenerateJoinQuery(join, builtConditions, skip, limit, om, otype, builtConditionsRoot);
                    transaction?.Benchmarker?.Mark("--");
                    command.CommandText = query.GetCommandText();
                    WriteLog($"[{accessId}] {command.CommandText}");
                    foreach (KeyValuePair<String, Object> param in query.GetParameters()) {
                        var cmdParam = command.CreateParameter();
                        cmdParam.ParameterName = param.Key;
                        if (param.Value is String str) {
                            cmdParam.Value = str;
                            cmdParam.DbType = DbType.String;
                            var paramRefl = new ObjectReflector(cmdParam);
                            paramRefl["Encoding"] = Encoding.UTF8;
                        } else {
                            cmdParam.Value = param.Value;
                        }

                        var pval = $"'{param.Value?.ToString() ?? "null"}'";
                        if (param.Value is DateTime || param.Value is DateTime? && ((DateTime?)param.Value).HasValue) {
                            pval = ((DateTime)param.Value).ToString("yyyy-MM-dd HH:mm:ss");
                            pval = $"'{pval}'";
                        }

                        command.Parameters.Add(cmdParam);
                        WriteLog($"[{accessId}] SET @{param.Key} = {pval} -- {cmdParam.DbType.ToString()}");
                    }
                    var retv = BuildAggregateListDirect<T>(transaction, command, join, 0);
                    return retv;
                }

            } else {
                WriteLog(cnd?.ToString());
                var pm = new PrefixMaker();
                var cndb = new ConditionParser(pm).ParseExpression<T>(cnd);
                return Fetch<T>(transaction, cndb, skip, limit, orderingMember, otype).ToList();
            }
        }

        public T LoadFirstOrDefault<T>(ConnectionInfo transaction, Expression<Func<T, bool>> condicoes, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new() {
            return LoadAll<T>(transaction, condicoes, skip, limit, orderingMember, ordering).FirstOrDefault();
        }

        public IList<T> Fetch<T>(ConnectionInfo transaction, Expression<Func<T, bool>> conditions = null, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new() {
            var query = new ConditionParser().ParseExpression(conditions);
            return Fetch<T>(transaction, query, skip, limit, orderingMember, ordering);
        }

        public int DefaultQueryLimit { get; set; } = 50;

        public IList<T> Fetch<T>(ConnectionInfo transaction, IQueryBuilder condicoes, int? skip, int? limit, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc) where T : IDataObject, new() {
            if (limit < 0) {
                limit = DefaultQueryLimit;
            }
            if (condicoes == null) {
                condicoes = Qb.Fmt("TRUE");
            }

            if (transaction == null) {
                throw new BDadosException("Fatal inconsistency error: Fetch<T> Expects a functional initialized ConnectionInfo object.");
            }

            transaction.Benchmarker?.Mark("--");

            transaction.Benchmarker?.Mark("Data Load ---");
            MemberInfo ordMember = GetOrderingMember<T>(orderingMember);
            transaction.Benchmarker?.Mark("Generate SELECT");
            var selectQuery = Plugin.QueryGenerator.GenerateSelect<T>(condicoes, skip, limit, ordMember, ordering);
            transaction.Benchmarker?.Mark("Execute SELECT");
            var mapFn = Query<T>(transaction, selectQuery);
            transaction.Benchmarker?.Mark("Run AfterLoads");
            var retv = RunAfterLoads(mapFn, false);

            return retv;
        }

        public DataTable Query(ConnectionInfo transaction, IQueryBuilder query) {
            if (query == null || query.GetCommandText() == null) {
                return new DataTable();
            }
            DateTime Inicio = DateTime.Now;
            String QueryText = query.GetCommandText();
            DataTable retv = new DataTable();
            using (var command = transaction.CreateCommand()) {
                command.CommandText = QueryText;
                command.CommandTimeout = Plugin.CommandTimeout;
                WriteLog($"[{accessId}] -- Query: {QueryText}");
                transaction?.Benchmarker?.Mark($"[{accessId}] Prepare Statement");
                // Adiciona os parametros
                foreach (KeyValuePair<String, Object> param in query.GetParameters()) {
                    var cmdParam = command.CreateParameter();
                    cmdParam.ParameterName = param.Key;
                    if (param.Value is String str) {
                        cmdParam.Value = str;
                        cmdParam.DbType = DbType.String;
                        var paramRefl = new ObjectReflector(cmdParam);
                        paramRefl["Encoding"] = Encoding.UTF8;
                    } else {
                        cmdParam.Value = param.Value;
                    }

                    var pval = $"'{param.Value?.ToString() ?? "null"}'";
                    if (param.Value is DateTime || param.Value is DateTime? && ((DateTime?)param.Value).HasValue) {
                        pval = ((DateTime)param.Value).ToString("yyyy-MM-dd HH:mm:ss");
                        pval = $"'{pval}'";
                    }

                    command.Parameters.Add(cmdParam);
                    WriteLog($"[{accessId}] SET @{param.Key} = {pval} -- {cmdParam.DbType.ToString()}");
                }
                // --
                transaction?.Benchmarker?.Mark($"[{accessId}] Build Dataset");
                DataSet ds;
                lock (transaction) {
                    ds = GetDataSet(command);
                }
                var elaps = transaction?.Benchmarker?.Mark($"[{accessId}] --");

                try {
                    int resultados = 0;
                    if (ds.Tables.Count < 1) {
                        throw new BDadosException("Database did not return any table.");
                    }
                    resultados = ds.Tables[0].Rows.Count;
                    WriteLog($"[{accessId}] -------- Queried [OK] ({resultados} results) [{elaps} ms]");
                    return ds.Tables[0];
                } catch (Exception x) {
                    WriteLog($"[{accessId}] -------- Error: {x.Message} ([{DateTime.Now.Subtract(Inicio).TotalMilliseconds} ms]");
                    WriteLog(x.Message);
                    WriteLog(x.StackTrace);
                    var ex = new BDadosException("Error executing Query", x);
                    ex.Data["query"] = query;
                    throw ex;
                } finally {
                    WriteLog("------------------------------------");
                }
            }
        }

        public int Execute(ConnectionInfo transaction, IQueryBuilder query) {
            if (query == null)
                return 0;
            int result = -1;
            transaction.Benchmarker?.Mark($"[{accessId}] Prepare statement");
            transaction.Benchmarker?.Mark("--");
            WriteLog($"[{accessId}] -- Execute: {query.GetCommandText()} [{Plugin.CommandTimeout}s timeout]");
            foreach (var param in query.GetParameters()) {
                WriteLog($"[{accessId}] @{param.Key} = {param.Value?.ToString() ?? "null"}");
            }
            using (var command = transaction.CreateCommand()) {
                try {
                    command.CommandText = query.GetCommandText();
                    foreach (KeyValuePair<String, Object> param in query.GetParameters()) {
                        var cmdParam = command.CreateParameter();
                        cmdParam.ParameterName = param.Key;
                        if (param.Value is String str) {
                            cmdParam.Value = str;
                            cmdParam.DbType = DbType.String;
                            var paramRefl = new ObjectReflector(cmdParam);
                            paramRefl["Encoding"] = Encoding.UTF8;
                        } else {
                            cmdParam.Value = param.Value;
                        }

                        var pval = $"'{param.Value?.ToString() ?? "null"}'";
                        if (param.Value is DateTime || param.Value is DateTime? && ((DateTime?)param.Value).HasValue) {
                            pval = ((DateTime)param.Value).ToString("yyyy-MM-dd HH:mm:ss");
                            pval = $"'{pval}'";
                        }
                        WriteLog($"[{accessId}] SET @{param.Key} = {pval} -- {cmdParam.DbType.ToString()}");

                        command.Parameters.Add(cmdParam);
                    }
                    command.CommandTimeout = Plugin.CommandTimeout;
                    WriteLog(command.CommandText);
                    transaction.Benchmarker?.Mark($"[{accessId}] Execute");
                    lock (transaction) {
                        result = command.ExecuteNonQuery();
                    }
                    var elaps = transaction.Benchmarker?.Mark("--");
                    WriteLog($"[{accessId}] --------- Executed [OK] ({result} lines affected) [{elaps} ms]");
                } catch (Exception x) {
                    WriteLog($"[{accessId}] -------- Error: {x.Message} ([{transaction.Benchmarker?.Mark("Error")} ms]");
                    WriteLog(x.Message);
                    WriteLog(x.StackTrace);
                    WriteLog($"BDados Execute: {x.Message}");
                    throw x;
                } finally {
                    WriteLog("------------------------------------");
                }
            }
            return result;
        }

        #endregion

    }
}