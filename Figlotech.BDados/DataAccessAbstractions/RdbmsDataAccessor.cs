using Figlotech.BDados.Builders;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using Figlotech.BDados.Helpers;
using Figlotech.Core;
using Figlotech.Core.BusinessModel;
using Figlotech.Core.Extensions;
using Figlotech.Core.Helpers;
using Figlotech.Core.Interfaces;
using Figlotech.Data;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.BDados.DataAccessAbstractions {

    public sealed class QueryIdsReturnValueModel {
        public string Id { get; set; }
        public string RID { get; set; }
    }

    public sealed class BDadosTransaction : IDisposable, IAsyncDisposable {
        public IDbConnection Connection { get; private set; }
        private IDbTransaction Transaction { get; set; }
        public Benchmarker Benchmarker { get; set; }
        private FiAsyncLock _lock { get; set; } = new FiAsyncLock();
        internal RdbmsDataAccessor DataAccessor { get; set; }
        public ConnectionState ConnectionState => Connection.State;
        public Object ContextTransferObject { get; set; }

        static long _idGen = 0;
        public int NumberOfWriteCommandsCreated { get; private set; }
        public Activity LoggerActivity { get; set; }

        int WriteOperationsCount { get; set; }

        public long Id { get; private set; } = ++_idGen;

        internal bool usingExternalBenchmarker { get; set; }

        public bool IsUsingRdbmsTransaction => Transaction != null;

        private static List<BDadosTransaction> DebugOnlyGlobalTransactions = new List<BDadosTransaction>();

        private List<(MemberInfo Member, IDataObject Target, object Value)> AutoMutateTargets = new List<(MemberInfo Member, IDataObject Target, object Value)>();

        public void AddMutateTarget(MemberInfo Member, IDataObject Target, object Value) {
            AutoMutateTargets.Add((Member, Target, Value));
        }

        public BDadosTransaction(RdbmsDataAccessor rda, IDbConnection connection) {
            DataAccessor = rda;
            Connection = connection;
            LoggerActivity = Fi.Tech.CreateTelemetryActivity($"Database Transaction", ActivityKind.Internal);
            if(LoggerActivity != null) {
                foreach(var (k, v) in rda.AdditionalTelemetryTags) {
                    LoggerActivity.AddTag(k, v);
                }
                LoggerActivity.AddTag("SourceLib", "Figlotech.BDados");
                LoggerActivity.AddTag("TransactionId", Id.ToString());
                LoggerActivity?.Start();
            }
            if (FiTechCoreExtensions.DebugConnectionLifecycle) {
                lock (DebugOnlyGlobalTransactions)
                    DebugOnlyGlobalTransactions.Add(this);
            }
        }

        ~BDadosTransaction() {
            Dispose();
        }

        public List<string[]> FrameHistory { get; private set; } = new List<string[]>(200);

        private List<IDataObject> ObjectsToNotify { get; set; } = new List<IDataObject>();
        public Action OnTransactionEnding { get; internal set; }
        private List<(Func<Task> Action, Func<Exception, Task> Handler)> ActionsToExecuteAfterSuccess { get; set; } = new List<(Func<Task> Action, Func<Exception, Task> Handler)>();

        List<IDbCommand> _commands { get; set; } = new List<IDbCommand>();

        public IDbCommand[] Commands {
            get {
                lock (_commands)
                    return _commands.ToArray();
            }
        }

        public void ExecuteWhenSuccess(Action fn, Action<Exception> handler = null) {
            lock (ActionsToExecuteAfterSuccess)
                ActionsToExecuteAfterSuccess.Add((async () => {
                    await Task.Run(() => {
                        fn();
                    });
                }, async (x) => {
                    await Task.Run(() => {
                        handler(x);
                    });
                }
                ));
        }
        public void ExecuteWhenSuccess(Func<Task> fn, Func<Exception, Task> handler = null) {
            lock (ActionsToExecuteAfterSuccess)
                ActionsToExecuteAfterSuccess.Add((fn, handler));
        }

        public void NotifyChange(IDataObject[] ido) {
            if (Transaction == null) {
                DataAccessor.RaiseForChangeIn(ido);
            } else {
                lock (ObjectsToNotify)
                    ObjectsToNotify.AddRange(ido);
            }
        }

        public async Task Step() {
            if (isDisposed || isTransactionEnded) {
                Debugger.Break();
                throw new BDadosException("Trying to use a transaction that has already been disposed or ended.");
            }
            if (CancellationToken.IsCancellationRequested) {
                throw new OperationCanceledException("The transaction was cancelled");
            }
            if (Connection.State != ConnectionState.Open) {
                await DataAccessor.OpenConnectionAsync(this.CancellationToken, Connection).ConfigureAwait(false);
            }
            if (!FiTechCoreExtensions.EnableDebug)
                return;
            try {
                StackTrace trace = new StackTrace(0, false);
                var frames = trace.GetFrames();
                int i = 0;
                while (frames[i].GetMethod().DeclaringType == typeof(RdbmsDataAccessor)) {
                    i++;
                }
                FrameHistory.Add(trace.GetFrames().Skip(i).Take(20).Select((frame) => {
                    var type = frame.GetMethod().DeclaringType;
                    return $"{(type?.Name ?? "")} -> " + frame.ToString();
                }).ToArray());
            } catch (Exception x) {
                if (Debugger.IsAttached) {
                    Debugger.Break();
                }
            }
        }

        public async Task<IDbCommand> CreateCommand(IQueryBuilder query, bool forWrite = false) {
            if (isDisposed || isTransactionEnded) {
                Debugger.Break();
                throw new BDadosException("Trying to use a transaction that has already been disposed or ended.");
            }
            var retv = await CreateCommand(forWrite).ConfigureAwait(false);
            query.ApplyToCommand(retv, this.DataAccessor.Plugin.ProcessParameterValue);
            return retv;
        }

        public async Task<IDbCommand> CreateCommand(string query, bool forWrite = false) {
            if (isDisposed || isTransactionEnded) {
                Debugger.Break();
                throw new BDadosException("Trying to use a transaction that has already been disposed or ended.");
            }
            var retv = await CreateCommand(forWrite).ConfigureAwait(false);
            retv.CommandText = query;
            return retv;
        }

        public void NotifyWriteOperation() {
            WriteOperationsCount++;
        }

        public async Task<IDbCommand> CreateCommand(bool forWrite = false) {
            if (isDisposed || isTransactionEnded) {
                Debugger.Break();
                throw new BDadosException("Trying to use a transaction that has already been disposed or ended.");
            }
            await Step().ConfigureAwait(false);
            var retv = Connection?.CreateCommand();
            retv.Transaction = Transaction;
            lock (this._commands) {
                this._commands.Add(retv);
            }
            if(forWrite) {
                NumberOfWriteCommandsCreated++;
            }
            return retv;
        }
        public void BeginTransaction(IsolationLevel ilev = IsolationLevel.ReadUncommitted) {
            BeginTransactionAsync(ilev)
                .ConfigureAwait(false)
                .GetAwaiter()
                .GetResult();
        }
        public async ValueTask BeginTransactionAsync(IsolationLevel ilev = IsolationLevel.ReadUncommitted) {
            if (Connection is DbConnection acon) {
                Transaction = await acon.BeginTransactionAsync(ilev, this.CancellationToken).ConfigureAwait(false);
            } else {
                Transaction = Connection?.BeginTransaction(ilev);
            }
        }

        public bool IsCommited { get; set; }
        public bool IsRolledBack { get; set; }
        internal DateTime CreatedTime { get; private set; } = DateTime.UtcNow;
        internal DateTime? DisposedTime { get; private set; }
        internal TimeSpan TimeAlive => (DisposedTime ?? DateTime.UtcNow) - CreatedTime; 
        internal StackTrace StackTrace { get; set; }
        internal CancellationTokenSource _cancellationTokenSource { get; set; } = new CancellationTokenSource();
        public CancellationToken CancellationToken => _cancellationTokenSource.Token;

        private void ApplySuccessActions() {
            Dictionary<object, bool> MutatedObjects = new Dictionary<object, bool>();
            for (int i = 0; i < AutoMutateTargets.Count; i++) {
                ReflectionTool.SetMemberValue(AutoMutateTargets[i].Member, AutoMutateTargets[i].Target, AutoMutateTargets[i].Value);
                if (!MutatedObjects.ContainsKey(AutoMutateTargets[i].Target)) {
                    MutatedObjects.Add(AutoMutateTargets[i].Target, true);
                    AutoMutateTargets[i].Target.UpdatedTime = DateTime.UtcNow;
                }
            }
            AutoMutateTargets.Clear();
            lock (ActionsToExecuteAfterSuccess) {
                for (int i = 0; i < ActionsToExecuteAfterSuccess.Count; i++) {
                    var fn = ActionsToExecuteAfterSuccess[i];
                    try {
                        var task = fn.Action?.Invoke();
                        if (task != null) {
                            task.GetAwaiter().GetResult();
                        }
                    } catch (Exception x) {
                        Debugger.Break();
                        try {
                            var task = fn.Handler?.Invoke(x);
                            if (task != null) {
                                task.GetAwaiter().GetResult();
                            }
                        } catch (Exception ex) {
                            Fi.Tech.Throw(ex);
                        }
                    }
                }
                ActionsToExecuteAfterSuccess.Clear();
            }

        }

        private async ValueTask ApplySuccessActionsAsync() {
            Dictionary<object, bool> MutatedObjects = new Dictionary<object, bool>();
            for (int i = 0; i < AutoMutateTargets.Count; i++) {
                ReflectionTool.SetMemberValue(AutoMutateTargets[i].Member, AutoMutateTargets[i].Target, AutoMutateTargets[i].Value);
                if (!MutatedObjects.ContainsKey(AutoMutateTargets[i].Target)) {
                    MutatedObjects.Add(AutoMutateTargets[i].Target, true);
                    AutoMutateTargets[i].Target.UpdatedTime = DateTime.UtcNow;
                }
            }
            AutoMutateTargets.Clear();

            for (int i = 0; i < ActionsToExecuteAfterSuccess.Count; i++) {
                var fn = ActionsToExecuteAfterSuccess[i];
                try {
                    var task = fn.Action?.Invoke();
                    if (task != null) {
                        await task.ConfigureAwait(false);
                    }
                } catch (Exception x) {
                    Debugger.Break();
                    try {
                        var task = fn.Handler?.Invoke(x);
                        if (task != null) {
                            await task.ConfigureAwait(false);
                        }
                    } catch (Exception ex) {
                        Fi.Tech.Throw(ex);
                    }
                }
            }
            ActionsToExecuteAfterSuccess.Clear();
        }

        public void Commit() {
            if(isDisposed || isTransactionEnded) {
                Debugger.Break();
                throw new BDadosException("Trying to commit a transaction that has already been disposed or ended.");
            }
            if (Transaction?.Connection?.State == ConnectionState.Open) {
                lock (_commands) {
                    if (_commands.Count == 0) {
                        // Nothing to commit 
                        // Weird
                        return;
                    }
                }
                if (IsCommited || IsRolledBack) {
                    Debugger.Break();
                    return;
                    // This transaction has already been committed or rolled back
                }
                if (WriteOperationsCount > 0) {
                    Transaction?.Commit();
                }
                if (ActionsToExecuteAfterSuccess.Count > 0 || AutoMutateTargets.Count > 0) {
                    ApplySuccessActions();
                }
                IsCommited = true;
                lock (ObjectsToNotify) {
                    DataAccessor.RaiseForChangeIn(ObjectsToNotify.ToArray());
                    ObjectsToNotify.Clear();
                }
            }
        }

        bool _hasOnAfterSuccessBeenRun = false;
        public async ValueTask CommitAsync() {
            if (isDisposed || isTransactionEnded) {
                Debugger.Break();
                throw new BDadosException("Trying to commit a transaction that has already been disposed or ended.");
            }
            if (IsCommited || IsRolledBack) {
                Debugger.Break();
                return;
                // This transaction has already been committed or rolled back
            }
            if (Transaction?.Connection?.State == ConnectionState.Open) {
                lock (_commands) {
                    if (_commands.Count == 0) {
                        // Nothing to commit 
                        // Weird
                        return;
                    }
                }
                if (WriteOperationsCount > 0) {
                    if (Transaction is DbTransaction tsn) {
                        await tsn.CommitAsync(this.CancellationToken).ConfigureAwait(false);
                    } else {
                        Transaction?.Commit();
                    }
                }
                IsCommited = true;
            }
            if(!_hasOnAfterSuccessBeenRun) {
                _hasOnAfterSuccessBeenRun = true;
                if (ActionsToExecuteAfterSuccess.Count > 0 || AutoMutateTargets.Count > 0) {
                    await ApplySuccessActionsAsync().ConfigureAwait(false);
                }
                LoggerActivity?.SetStatus(ActivityStatusCode.Ok);
                if (ObjectsToNotify.Count > 0) {
                    lock (ObjectsToNotify) {
                        DataAccessor.RaiseForChangeIn(ObjectsToNotify.ToArray());
                        ObjectsToNotify.Clear();
                    }
                }
            }
        }

        public void Rollback() {
            if (isDisposed || isTransactionEnded) {
                Debugger.Break();
                throw new BDadosException("Trying to rollback a transaction that has already been disposed or ended.");
            }
            if (Transaction?.Connection?.State == ConnectionState.Open) {
                Transaction?.Rollback();
                IsRolledBack = true;
            }
            lock (ObjectsToNotify)
                ObjectsToNotify.Clear();
        }
        public async ValueTask RollbackAsync() {
            if(isDisposed || isTransactionEnded) {
                Debugger.Break();
                throw new BDadosException("Trying to rollback a transaction that has already been disposed or ended.");
            }
            if (Transaction?.Connection?.State == ConnectionState.Open) {
                if (Transaction is DbTransaction tsn) {
                    await tsn.RollbackAsync(this.CancellationToken).ConfigureAwait(false);
                } else {
                    Transaction?.Rollback();
                }
                IsRolledBack = true;
            }
            lock (ObjectsToNotify)
                ObjectsToNotify.Clear();
        }

        public void EndTransaction() {
            EndTransactionAsync()
                .ConfigureAwait(false)
                .GetAwaiter()
                .GetResult();
        }
        public async ValueTask EndTransactionAsync() {
            if(isDisposed || isTransactionEnded) {
                return;
            }
            if (OnTransactionEnding != null) {
                this.OnTransactionEnding.Invoke();
            }
            if (Transaction is DbTransaction tsn) {
                await tsn.DisposeAsync().ConfigureAwait(false);
            } else {
                Transaction?.Dispose();
            }
            Transaction = null;
            await DisposeConnectionIfNotYetDisposed();
            Connection = null;
            if(!usingExternalBenchmarker) {
                LoggerActivity?.AddTag("Status", string.Join("\r\n", Benchmarker?.VerboseLog()));
            }
            LoggerActivity?.SetEndTime(DateTime.UtcNow);
            LoggerActivity?.Dispose();
            LoggerActivity = null;
            lock (this.DataAccessor.ActiveConnections) {
                this.DataAccessor.ActiveConnections.Remove(Id);
            }
            isTransactionEnded = true;

            this.DataAccessor.WriteLog($"Transaction Closed {Id}");
        }
        bool isTransactionEnded = false;
        bool Errored = false;
        internal void MarkAsErrored() {
            Errored = true;
            LoggerActivity?.SetStatus(ActivityStatusCode.Error);
        }

        public void Throw(Exception x) {
            Errored = true;
            LoggerActivity?.AddTag("Exception",
                JsonConvert.SerializeObject(ExceptionExtensions.ToRecursiveInnerExceptions(x))
            );
            throw Exception(x);
        }

        public Exception Exception<T>(T x) where T: Exception {
            Errored = true;
            if (x is MainLogicGeneratedException) {
                return Activator.CreateInstance(x.GetType(), x.Message, x) as T;
            }
            return new BDadosException("Transaction interruped by an error", x);
        }

        public async Task AutoCommit() {
            if(isDisposed || isTransactionEnded) {
                return;
            }
            if (Connection.State == ConnectionState.Open && !IsCommited && !IsRolledBack) {
                bool hasException = false;

                try {
                    var tMarshal = Type.GetType("System.Runtime.InteropServices.Marshal");
                    var hasError = Marshal.GetExceptionCode() != 0;
                    var hasExpeptionPtr = (IntPtr) tMarshal.GetMethod("GetExceptionPointers")?.Invoke(null, Array.Empty<object>()) != IntPtr.Zero;
                    hasException |= hasError || hasExpeptionPtr;
                } catch(Exception x) {
                    
                }
                
                if (Errored || hasException || CancellationToken.IsCancellationRequested) {
                    await RollbackAsync().ConfigureAwait(false);
                } else {
                    try {
                        await CommitAsync().ConfigureAwait(false);
                    } catch (Exception ex) {
                        await RollbackAsync().ConfigureAwait(false);
                        Fi.Tech.WriteLine($"Warning disposing BDadosTransaction: {ex.Message}");
                        if (Debugger.IsAttached) {
                            Debugger.Break();
                        }
                    }
                }
            }
        }

        public void Dispose() {
            DisposeAsync().GetAwaiter().GetResult();
        }
        bool isDisposed = false;
        bool isConnectionDisposed = false;
        public async ValueTask DisposeAsync() {
            try {
                if (isDisposed) {
                    return;
                }
                try {
                    await AutoCommit().ConfigureAwait(false);
                    if(!isTransactionEnded) {
                        await EndTransactionAsync();
                    }
                    if (FiTechCoreExtensions.DebugConnectionLifecycle) {
                        lock (DebugOnlyGlobalTransactions)
                            DebugOnlyGlobalTransactions.Remove(this);
                    }
                } catch (Exception ex) {
                    Fi.Tech.WriteLine($"Warning disposing BDadosTransaction: {ex.Message}");
                    if(Debugger.IsAttached) {
                        Debugger.Break();
                    }
                } finally {
                    DisposedTime = DateTime.UtcNow;
                    isDisposed = true;
                }
                try {
                    _cancellationTokenSource.Dispose();
                } catch (Exception) {

                }
            } finally {
                await DisposeConnectionIfNotYetDisposed().ConfigureAwait(false);
            }
        }

        private async Task DisposeConnectionIfNotYetDisposed() {
            if (!isConnectionDisposed) {
                if(Connection.State == ConnectionState.Open) {
                    Connection.Close();
                }
                if (Connection is DbConnection aconn) {
                    await aconn.DisposeAsync().ConfigureAwait(false);
                    isConnectionDisposed = true;
                } else {
                    Connection?.Dispose();
                    isConnectionDisposed = true;
                }
                Connection = null;
                DataAccessor._concurrentConnectionsSemaphoreSlim.Release();
            }
        }

        internal async Task<FiAsyncDisposableLock> Lock() {
            return await _lock.Lock().ConfigureAwait(false);
        }
    }

    public partial class RdbmsDataAccessor : IRdbmsDataAccessor, IDisposable, IAsyncDisposable {

        public Dictionary<long, BDadosTransaction> ActiveConnections = new Dictionary<long, BDadosTransaction>();

        public ITextToFileLogger Logger { get; set; }
        public static int DefaultMaxOpenAttempts { get; set; } = 5;
        public static int DefaultOpenAttemptInterval { get; set; } = 100;
        public string Description { get; set; }
        public Benchmarker Benchmarker { get; set; }

        public Type[] _workingTypes = Array.Empty<Type>();
        public Type[] WorkingTypes {
            get { return _workingTypes; }
            set {
                _workingTypes = value.Where(t => t.GetInterfaces().Contains(typeof(IDataObject))).ToArray();
            }
        }

        public Dictionary<string, object> AdditionalTelemetryTags = new Dictionary<string, object>();

        internal void RaiseForChangeIn(IDataObject[] ido) {
            if (!ido.Any()) {
                return;
            }
            OnDataObjectAltered?.Invoke(ido.First().GetType(), ido);
        }
        public static RdbmsDataAccessor Using<T>(IDictionary<String, object> Configuration) where T : IRdbmsPluginAdapter, new() {
            var Plugin = new T();
            Plugin.SetConfiguration(Configuration);
            return new RdbmsDataAccessor(
                Plugin
            );
        }

        #region **** Global Declaration ****
        internal IRdbmsPluginAdapter Plugin;

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

        internal SemaphoreSlim _concurrentConnectionsSemaphoreSlim;
        public RdbmsDataAccessor(IRdbmsPluginAdapter extension) {
            Plugin = extension;
            _concurrentConnectionsSemaphoreSlim = new SemaphoreSlim(Plugin.PoolSize);
        }
        ~RdbmsDataAccessor() {
            Dispose();
        }

        public async ValueTask EnsureDatabaseExistsAsync() {
            if (_isDisposed || _isDisposing) {
                if(Debugger.IsAttached) {
                    Debugger.Break();
                }
                throw new BDadosException("Error trying to open connection in a Disposed RdbmsDataAccessor");
            }

            await using (var conn = (DbConnection)await this.GetNewOpenSchemalessConnectionAsync(CancellationToken.None).ConfigureAwait(false)) {
                var query = Plugin.QueryGenerator.CreateDatabase(Plugin.SchemaName);
                using (var command = conn.CreateCommand()) {
                    query.ApplyToCommand(command, Plugin.ProcessParameterValue);

                    if (command is DbCommand acom) {
                        await acom.PrepareAsync().ConfigureAwait(false);
                        await acom.ExecuteNonQueryAsync().ConfigureAwait(false);
                    } else {
                        command.Prepare();
                        command.ExecuteNonQuery();
                    }
                }
                _concurrentConnectionsSemaphoreSlim.Release();
            }
        }

        public T ForceExist<T>(Func<T> Default, IQueryBuilder qb) where T : IDataObject, new() {
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
            return Task.Run(async () => {
                await using var transaction = await CreateNewTransactionAsync(CancellationToken.None).ConfigureAwait(false);
                var f = LoadAll<T>(transaction, Core.Interfaces.LoadAll.Where<T>(qb));
                if (f.Any()) {
                    return f.First();
                } else {
                    T quickSave = Default();
                    SaveItem(transaction, quickSave);
                    return quickSave;
                }
            }).ConfigureAwait(false).GetAwaiter().GetResult();
        }

        bool isOnline = true;

        public bool Test() {
            try {
                Execute("SELECT 1");
                return isOnline = true;
            } catch (Exception x) {
                return isOnline = false;
                throw new BDadosException("Error testing connection to the database", x);
            }
        }

        #endregion ***************************

        public T LoadFirstOrDefault<T>(LoadAllArgs<T> args = null) where T : IDataObject, new() {
            return LoadAll<T>(args).FirstOrDefault();
        }

        public async Task<T> LoadFirstOrDefaultAsync<T>(BDadosTransaction tsn, LoadAllArgs<T> args = null) where T : IDataObject, new() {
            var list = await LoadAllAsync<T>(tsn, args).ConfigureAwait(false);
            return list.FirstOrDefault();
        }
        public async Task<T> LoadFirstOrDefaultAsync<T>(LoadAllArgs<T> args = null) where T : IDataObject, new() {
            return await AccessAsync(async tsn=> {
                var list = await LoadAllAsync<T>(tsn, args).ConfigureAwait(false);
                return list.FirstOrDefault();
            }, CancellationToken.None).ConfigureAwait(false);
        }
        public async Task<T> LoadFirstOrDefaultAsync<T>(BDadosTransaction tsn, Expression<Func<T, bool>> predicate) where T : IDataObject, new() {
            var list = await LoadAllAsync<T>(tsn, Figlotech.Core.Interfaces.LoadAll.From<T>().Where(predicate).Limit(1)).ConfigureAwait(false);
            return list.FirstOrDefault();
        }
        public async Task<T> LoadFirstOrDefaultAsync<T>(Expression<Func<T, bool>> predicate) where T : IDataObject, new() {
            return await AccessAsync(async tsn=> {
                return await LoadFirstOrDefaultAsync<T>(tsn, predicate).ConfigureAwait(false);
            }, CancellationToken.None).ConfigureAwait(false);
        }

        public async Task<List<FieldAttribute>> GetInfoSchemaColumns() {
            var dbName = this.SchemaName;
            var map = Plugin.InfoSchemaColumnsMap;

            List<FieldAttribute> retv = new List<FieldAttribute>();

            return await UseTransactionAsync(async (conn) => {
                using (var cmd = await conn.CreateCommand(this.QueryGenerator.InformationSchemaQueryColumns(dbName))) {
                    cmd.Prepare();
                    if (cmd is DbCommand acom) {
                        using (var reader = await acom.ExecuteReaderAsync(CommandBehavior.SingleResult).ConfigureAwait(false)) {
                            return await Fi.Tech.ReaderToObjectListUsingMapAsync<FieldAttribute>(reader, map).ConfigureAwait(false);
                        }
                    } else {
                        using (var reader = cmd.ExecuteReader(CommandBehavior.SingleResult)) {
                            return Fi.Tech.ReaderToObjectListUsingMap<FieldAttribute>(reader, map);
                        }
                    }
                }
            }, CancellationToken.None).ConfigureAwait(false);
        }

        public IRdbmsDataAccessor Fork() {
            return new RdbmsDataAccessor(Plugin);
        }

        public async Task<BDadosTransaction> CreateNonDbLevelTransaction(CancellationToken cancellationToken, Benchmarker bmark = null) {
            return await CreateNewTransactionAsync(cancellationToken, null, bmark);
        }

        public async Task<BDadosTransaction> CreateNewTransactionAsync(CancellationToken cancellationToken, IsolationLevel? ilev = IsolationLevel.ReadUncommitted, Benchmarker bmark = null) {
            if(_isDisposed || _isDisposing) {
                if (Debugger.IsAttached) {
                    Debugger.Break();
                }
                throw new BDadosException("Error trying to open connection in a Disposed RdbmsDataAccessor");
            }
            
            BDadosTransaction retv;
            //if (FiTechCoreExtensions.EnableDebug) {
            //    WriteLog(Environment.StackTrace);
            //}
            var connection = await GetNewOpenConnectionAsync(cancellationToken).ConfigureAwait(false);

            retv = new BDadosTransaction(this, connection);
            WriteLog($"Database Acess Opened {retv.Id} (using DB Transaction: {ilev.HasValue})");
            var trace = new StackTrace();
            if(ilev.HasValue) {
                await retv.BeginTransactionAsync(ilev.Value).ConfigureAwait(false);
            }
            cancellationToken.Register(() => {
                try {
                    if(!retv.DisposedTime.HasValue && !retv._cancellationTokenSource.IsCancellationRequested) {
                        retv._cancellationTokenSource?.Cancel();
                    }
                } catch(Exception x) {

                }
            });
            retv.StackTrace = trace;
            retv.Benchmarker = bmark ?? new Benchmarker("Database Access", FiTechCoreExtensions.IsTelemetryLoggingEnabled) {
                Active = true
            };
            retv.usingExternalBenchmarker = bmark != null;
            lock (ActiveConnections) {
                ActiveConnections[retv.Id] = retv;
            }
            WriteLog("Transaction Open");
            return retv;
        }

        public BDadosTransaction CreateNewTransaction(CancellationToken cancellationToken, IsolationLevel? ilev = IsolationLevel.ReadUncommitted, Benchmarker bmark = null) {
            return CreateNewTransactionAsync(cancellationToken, ilev, bmark)
                .ConfigureAwait(false)
                .GetAwaiter()
                .GetResult();
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
            try {
                return Task.Run(async () => {
                    await using var transaction = await CreateNewTransactionAsync(CancellationToken.None);
                    return await SaveItemAsync(transaction, input);
                }).ConfigureAwait(false).GetAwaiter().GetResult();
            } catch (Exception x) {
                throw new BDadosException("Error saving item", x);
            }
        }

        public List<T> Query<T>(IQueryBuilder query) where T : new() {
            return Access((transaction) => Query<T>(transaction, query), null);
        }

        private static String GetIdColumn<T>() where T : IDataObject, new() { return GetIdColumn(typeof(T)); }
        private static String GetIdColumn(Type type) {
            var retv = ReflectionTool.GetAttributedMemberValues<PrimaryKeyAttribute>(type)
                .FirstOrDefault()
                .Member?.Name
                ?? "Id";
            return retv;
        }

        private static String GetRidColumn<T>() where T : IDataObject, new() { return GetRidColumn(typeof(T)); }
        private static String GetRidColumn(Type type) {
            var fields = ReflectionTool.FieldsAndPropertiesOf(type);
            var retv = fields
                .Where((f) => f.GetCustomAttribute<ReliableIdAttribute>() != null)
                .FirstOrDefault()
                ?.Name
                ?? "RID";
            return retv;
        }

        public T LoadById<T>(long Id) where T : IDataObject, new() {
            return Access((transaction) => LoadById<T>(transaction, Id), null);
        }

        public T LoadByRid<T>(String RID) where T : IDataObject, new() {
            return Access((transaction) => LoadByRid<T>(transaction, RID), null);
        }

        public async Task<T> LoadByRidAsync<T>(BDadosTransaction tsn, string RID) where T : IDataObject, new() {
            return (await LoadAllAsync<T>(tsn, Figlotech.Core.Interfaces.LoadAll.From<T>().Where(x => x.RID == RID).Limit(1))).FirstOrDefault();
        }

        public List<T> LoadAll<T>(LoadAllArgs<T> args = null) where T : IDataObject, new() {
            return Fetch<T>(args).ToList();
        }

        public List<T> LoadAll<T>(IQueryBuilder conditions, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc, object contextObject = null) where T : IDataObject, new() {
            return Access((transaction) => {
                return LoadAll<T>(transaction, conditions, skip, limit, orderingMember, ordering).ToList();
            }, null);
        }

        public IEnumerable<T> Fetch<T>(LoadAllArgs<T> args = null) where T : IDataObject, new() {
            using var transaction = CreateNewTransaction(CancellationToken.None);
            var retv = Fetch<T>(transaction, args);
            foreach(var item in retv) {
                yield return item;
            }
        }

        public IEnumerable<T> Fetch<T>(IQueryBuilder conditions, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc, object contextObject = null) where T : IDataObject, new() {
            using var transaction = CreateNewTransaction(CancellationToken.None);
            var retv = Fetch<T>(transaction, conditions, skip, limit, orderingMember, ordering, contextObject);
            foreach (var item in retv) {
                yield return item;
            }
        }

        private T RunAfterLoad<T>(T target, bool isAggregateLoad, object transferObject = null) {
            if (target is IBusinessObject ibo) {
                ibo.OnAfterLoad(new DataLoadContext {
                    DataAccessor = this,
                    IsAggregateLoad = isAggregateLoad,
                    ContextTransferObject = transferObject
                });
            }
            return target;
        }

        public bool Delete<T>(IEnumerable<T> obj) where T : IDataObject, new() {
            return Access((transaction) => Delete(transaction, obj), null);
        }

        public bool Delete(IDataObject obj) {
            return Access((transaction) => Delete(transaction, obj), null);
        }

        public bool Delete<T>(Expression<Func<T, bool>> conditions) where T : IDataObject, new() {
            return Access((transaction) => Delete(transaction, conditions));
        }

        #region **** BDados API ****

        int fail = 0;

        private List<Task<Object>> Workers = new List<Task<Object>>();

        private static int accessCount = 0;


        public Object ScalarQuery(IQueryBuilder qb) {
            return Access(transaction => ScalarQuery(transaction, qb), null);
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

        public event Action<Type, IDataObject[]> OnSuccessfulSave;
        public event Action<Type, IDataObject[], Exception> OnFailedSave;
        public event Action<Type, IDataObject[]> OnDataObjectAltered;
        public event Action<Type, IDataObject[]> OnObjectsDeleted;

        public void Access(Action<BDadosTransaction> functions, IsolationLevel? ilev = IsolationLevel.ReadUncommitted) {
            var i = Access<int>((transaction) => {
                functions?.Invoke(transaction);
                return 0;
            }, ilev);
        }

        public async ValueTask AccessAsync(Func<BDadosTransaction, ValueTask> functions, CancellationToken cancellationToken,IsolationLevel? ilev = IsolationLevel.ReadUncommitted) {
            _ = await AccessAsync<int>(async (transaction) => {
                await functions.Invoke(transaction).ConfigureAwait(false);
                return 0;
            }, cancellationToken, ilev).ConfigureAwait(false);
        }
        public async Task<T> AccessAsync<T>(Func<BDadosTransaction, Task<T>> functions, CancellationToken cancellationToken,IsolationLevel? ilev = IsolationLevel.ReadUncommitted) {
            if (functions == null) return default(T);

            return await UseTransactionAsync(async (transaction) => {
                var aid = transaction.Id;

                if (transaction.Benchmarker == null) {
                    transaction.Benchmarker = Benchmarker ?? new Benchmarker($"---- Access [{++aid}]");
                    transaction.usingExternalBenchmarker = Benchmarker != null;
                    transaction.Benchmarker.WriteToStdout = FiTechCoreExtensions.EnableStdoutLogs;
                }

                var retv = await functions.Invoke(transaction).ConfigureAwait(false);
                if (retv is Task t) {
                    t.GetAwaiter().GetResult();
                }
                if (!transaction?.usingExternalBenchmarker ?? false) {
                    var total = transaction?.Benchmarker.FinalMark();
                    WriteLog($"---- Access [{Description}:{aid}] Finished in {total}ms");
                } else {
                    WriteLog($"---- Access [{Description}:{0}] Finished");
                }
                return retv;
            }, cancellationToken, ilev).ConfigureAwait(false);
        }

        public T Access<T>(Func<BDadosTransaction, T> functions, IsolationLevel? ilev = IsolationLevel.ReadUncommitted) {
            if (functions == null) return default(T);

            //if (transactionHandle != null && transactionHandle.State == transactionState.Open) {
            //    return functions.Invoke(transaction);
            //}
            return UseTransaction((transaction) => {
                var aid = transaction.Id;

                if (transaction.Benchmarker == null) {
                    transaction.Benchmarker = Benchmarker ?? new Benchmarker($"---- Access [{++aid}]");
                    transaction.usingExternalBenchmarker = Benchmarker != null;
                    transaction.Benchmarker.WriteToStdout = FiTechCoreExtensions.EnableStdoutLogs;
                }

                var retv = functions.Invoke(transaction);
                if (retv is Task t) {
                    t.GetAwaiter().GetResult();
                }
                if (!transaction?.usingExternalBenchmarker ?? false) {
                    var total = transaction?.Benchmarker.FinalMark();
                    WriteLog($"---- Access [{Description}:{aid}] Finished in {total}ms");
                } else {
                    WriteLog($"---- Access [{Description}:{aid}] Finished");
                }
                return retv;
            }, ilev);
        }

        public DataTable Query(IQueryBuilder query) {
            return Access((transaction) => {
                return Query(transaction, query);
            }, null);
        }

        private const string RDB_SYSTEM_LOGID = "FTH:RDB";
        public void WriteLog(String s) {
            if (string.IsNullOrEmpty(s)) {
                return;
            }
            if (!FiTechCoreExtensions.EnableStdoutLogs) {
                return;
            }
            Logger?.WriteLog(s);
            Fi.Tech.WriteLine(RDB_SYSTEM_LOGID, s);
        }

        public int Execute(String str, params object[] args) {
            return Execute(Qb.Fmt(str, args));
        }

        public int Execute(IQueryBuilder query) {
            return Access((transaction) => {
                return Execute(transaction, query);
            });
        }

        static long ConnectionTracks = 0;

        internal async Task<IDbConnection> GetNewOpenConnectionAsync(CancellationToken cancellation) {
            //using var lockHandle = await OpenConnectionLock.Lock(Plugin.ConnectionString, TimeSpan.FromSeconds(300)).ConfigureAwait(false);
            var connection = Plugin.GetNewConnection();
            try {
                await OpenConnectionAsync(cancellation, connection).ConfigureAwait(false);
                return connection;
            } catch (Exception x) {
                try {
                    if (connection is DbConnection acon) {
                        await acon.DisposeAsync().ConfigureAwait(false);
                    } else {
                        connection?.Dispose();
                    }
                } catch (Exception) {

                }
                throw new BDadosException("Error when trying to Open Connection", x);
            }
            
        }
        internal async Task<IDbConnection> GetNewOpenSchemalessConnectionAsync(CancellationToken cancellation) {
            //using var lockHandle = await OpenConnectionLock.Lock(Plugin.ConnectionString, TimeSpan.FromSeconds(300)).ConfigureAwait(false);
            var connection = Plugin.GetNewSchemalessConnection();
            try {
                await OpenConnectionAsync(cancellation, connection).ConfigureAwait(false);
                return connection;
            } catch (Exception x) {
                try {
                    if (connection is DbConnection acon) {
                        await acon.DisposeAsync().ConfigureAwait(false);
                    } else {
                        connection?.Dispose();
                    }
                } catch (Exception) {

                }
                throw new BDadosException("Error when trying to Open Connection", x);
            }
        }

        internal async ValueTask OpenConnectionAsync(CancellationToken cancellation, IDbConnection connection) {
            int attempts = DefaultMaxOpenAttempts;
            Exception ex = null;
            while (connection?.State != ConnectionState.Open && attempts-- >= 0) {
                try {
                    await ExclusiveOpenConnectionAsync(cancellation, connection).ConfigureAwait(false);
                    isOnline = true;
                    break;
                } catch (Exception x) {
                    isOnline = false;
                    ex = x;
                    if (x.Message.Contains("Unable to connect")) {
                        break;
                    }
                    await Task.Delay(DefaultOpenAttemptInterval).ConfigureAwait(false);
                }
            }
            if (connection?.State != ConnectionState.Open) {
                throw new BDadosException($"Cannot open connection to the RDBMS database service (Using {Plugin.GetType().Name}).", ex);
            }
        }

        //FiAsyncMultiLock OpenConnectionLock = new FiAsyncMultiLock();
        private void ExclusiveOpenConnection(CancellationToken cancellation, IDbConnection connection) {
            ExclusiveOpenConnectionAsync(cancellation, connection).ConfigureAwait(false).GetAwaiter().GetResult();
        }

        FiAsyncLock ExclusiveOpenConnectionLock = new FiAsyncLock();
        private async ValueTask ExclusiveOpenConnectionAsync(CancellationToken cancellation, IDbConnection connection) {
            await _concurrentConnectionsSemaphoreSlim.WaitAsync(Plugin.CommandTimeout, cancellation);
            await using (var handle = await ExclusiveOpenConnectionLock.Lock().ConfigureAwait(false)) {
                try {
                    if (connection is DbConnection idbconn) {
                        await idbconn.OpenAsync(CancellationToken.None).ConfigureAwait(false);
                    } else {
                        connection.Open();
                    }
                } catch(Exception) {
                    _concurrentConnectionsSemaphoreSlim.Release();
                    throw;
                }
            }
        }

        internal void OpenConnection(CancellationToken cancellation, IDbConnection connection) {
            int attempts = DefaultMaxOpenAttempts;
            Exception ex = null;
            while (connection?.State != ConnectionState.Open && attempts-- >= 0) {
                try {
                    ExclusiveOpenConnection(cancellation, connection);

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

        private async Task<T> UseTransactionAsync<T>(Func<BDadosTransaction, Task<T>> func, CancellationToken cancellationToken,IsolationLevel? ilev = IsolationLevel.ReadUncommitted) {

            if (func == null) return default(T);

            await using (var transaction = await CreateNewTransactionAsync(cancellationToken, ilev).ConfigureAwait(false)) {
                var b = transaction.Benchmarker;
                if (FiTechCoreExtensions.EnableDebug) {
                    try {
                        int maxFrames = 6;
                        var stack = new StackTrace();
                        foreach (var f in stack.GetFrames()) {
                            var m = f.GetMethod();
                            if (m != null) {
                                var mName = m.Name;
                                var t = m.DeclaringType;
                                if (t != null) {
                                    if (t.IsNested) {
                                        t = t.DeclaringType;
                                    }
                                    var tName = t.Name;
                                    if (m.DeclaringType.Assembly != GetType().Assembly) {
                                        b.Mark($" at {tName}->{mName}");
                                        if (maxFrames-- <= 0) {
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    } catch (Exception) {
                        if (Debugger.IsAttached) {
                            Debugger.Break();
                        }
                    }
                }
                try {
                    b.Mark("Run User Code");
                    var retv = await func.Invoke(transaction).ConfigureAwait(false);

                    if (retv is Task awaitable) {
                        await awaitable.ConfigureAwait(false);
                    }

                    if (transaction.CancellationToken.IsCancellationRequested) {
                        WriteLog($"[{Description}:{transaction.Id}] Transaction was cancelled via token");
                        b.Mark($"[{Description}:{transaction.Id}] Begin Rollback");
                        await transaction.RollbackAsync().ConfigureAwait(false);
                        b.Mark($"[{Description}:{transaction.Id}] End Rollback");
                        WriteLog($"[{Description}:{transaction.Id}] Rollback OK ");
                    } else {
                        WriteLog($"[{Description}:{transaction.Id}] Committing");
                        b.Mark($"[{Description}:{transaction.Id}] Begin Commit");
                        await transaction.CommitAsync().ConfigureAwait(false);
                        b.Mark($"[{Description}:{transaction.Id}] End Commit");
                        WriteLog($"[{Description}:{transaction.Id}] Commited OK ");
                    }
                    return retv;
                } catch (TaskCanceledException x) {
                    if(!transaction.IsRolledBack && transaction.IsCommited) {
                        await transaction.RollbackAsync().ConfigureAwait(false);
                    }
                    throw x;
                } catch (Exception x) {
                    if (Debugger.IsAttached) {
                        Debugger.Break();
                    }
                    WriteLog($"[{Description}:{transaction.Id}] Begin Rollback : {x.Message} {x.StackTrace}");
                    b.Mark($"[{Description}:{transaction.Id}] Begin Rollback");
                    try {
                        await transaction.RollbackAsync().ConfigureAwait(false);
                    } catch (Exception rbex) {
                        Debugger.Break();
                    }
                    b.Mark($"[{Description}:{transaction.Id}] End Rollback");
                    WriteLog($"[{Description}:{transaction.Id}] Transaction rolled back ");
                    transaction?.MarkAsErrored();
                    throw new BDadosException("Error accessing the database", transaction?.FrameHistory, null, x);
                } finally {
                    if (!(transaction?.usingExternalBenchmarker ?? true)) {
                        b?.FinalMark();
                    }
                    await transaction.EndTransactionAsync().ConfigureAwait(false);
                    await transaction.DisposeAsync().ConfigureAwait(false);
                }

                return default(T);
            }
        }

        private T UseTransaction<T>(Func<BDadosTransaction, T> func, IsolationLevel? ilev = IsolationLevel.ReadUncommitted) {
            try {
                return UseTransactionAsync<T>(
                    async (tsn) => {
                        return await Task.Run(()=> func(tsn)).ConfigureAwait(false);
                    },
                    CancellationToken.None,
                    ilev
                ).ConfigureAwait(false)
                .GetAwaiter()
                .GetResult();
            } catch (Exception x) {
                throw new BDadosException("Error accessing the database", x);
            }
        }

        //private PrefixMaker prefixer = new PrefixMaker();
        /*
         * HERE BE DRAGONS
         * jk.
         * It works and it is actually really good
         * But the logic behind this is crazy,
         * it took a lot of coffee to achieve.
         */
        private static void MakeQueryAggregations(ref JoinDefinition query, Type theType, String parentAlias, String nameofThis, String pKey, PrefixMaker prefixer, bool Linear = false) {

            //var reflectedJoinMethod = query.GetType().GetMethod("Join");

            String thisAlias = prefixer.GetAliasFor(parentAlias, nameofThis, pKey);
            var selfRIDColumn = FiTechBDadosExtensions.RidColumnNameOf[theType];

            // Iterating through AggregateFields
            var aggregateFieldAttributes = ReflectionTool.GetAttributedMemberValues<AggregateFieldAttribute>(theType);
            for (int i = 0; i < aggregateFieldAttributes.Length; i++) {
                var refl = aggregateFieldAttributes[i];
                var field = refl.Member;
                var info = refl.Attribute;
                if (
                    (info.ExplodedFlags.Contains("root") && parentAlias != "root") ||
                    (info.ExplodedFlags.Contains("child") && parentAlias == "root")
                ) {
                    continue;
                }
                var type = info?.RemoteObjectType;
                var key = info?.ObjectKey;
                String childAlias;
                var tname = type.Name;
                var pkey = key;
                // This inversion principle might be fucktastic.
                childAlias = prefixer.GetNewAliasFor(thisAlias,
                    tname,
                    pkey);

                String OnClause = $"{thisAlias}.{key}={childAlias}.{FiTechBDadosExtensions.RidColumnNameOf[type]}";

                if (!ReflectionTool.TypeContains(theType, key)) {
                    OnClause = $"{thisAlias}.{selfRIDColumn}={childAlias}.{key}";
                }
                var joh = query.Join(type, childAlias, OnClause, JoinType.LEFT);

                joh.As(childAlias);

                var qjoins = query.Joins.Where((a) => a.Alias == childAlias);
                if (qjoins.Any() && !qjoins.First().Columns.Contains(info?.RemoteField)) {
                    qjoins.First().Columns.Add(info?.RemoteField);
                    //continue;
                }

                if (field.GetCustomAttribute<AggregateFieldAttribute>() != null) {
                    joh.GetType().GetMethod("OnlyFields").Invoke(joh, new object[] { new string[] { field.GetCustomAttribute<AggregateFieldAttribute>().RemoteField } });
                }
            }

            // Iterating through AggregateFarFields
            var aggregateFarFieldAttributes = ReflectionTool.GetAttributedMemberValues<AggregateFarFieldAttribute>(theType);
            for (int i = 0; i < aggregateFarFieldAttributes.Length; i++) {
                var refl = aggregateFarFieldAttributes[i];
                var field = refl.Member;
                var info = refl.Attribute;
                var memberType = ReflectionTool.GetTypeOf(field);
                if (
                    (info.ExplodedFlags.Contains("root") && parentAlias != "root") ||
                    (info.ExplodedFlags.Contains("child") && parentAlias == "root")
                ) {
                    continue;
                }
                String childAlias = prefixer.GetAliasFor(thisAlias, info.ImediateType.Name, info.ImediateKey);
                String farAlias = prefixer.GetAliasFor(childAlias, info.FarType.Name, info.FarKey);

                var qimediate = query.Joins.Where((j) => j.Alias == childAlias);
                if (!qimediate.Any()) {

                    string OnClause = $"{thisAlias}.{info.ImediateKey}={childAlias}.{FiTechBDadosExtensions.RidColumnNameOf[info.ImediateType]}";
                    // This inversion principle will be fucktastic.
                    // But has to be this way for now.
                    if (!ReflectionTool.TypeContains(theType, info.ImediateKey)) {
                        OnClause = $"{thisAlias}.{selfRIDColumn}={childAlias}.{info.ImediateKey}";
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
                    String OnClause2 = $"{childAlias}.{info.FarKey}={farAlias}.{FiTechBDadosExtensions.RidColumnNameOf[info.FarType]}";
                    // This inversion principle will be fucktastic.
                    // But has to be this way for now.
                    if (!ReflectionTool.TypeContains(info.ImediateType, info.FarKey)) {
                        OnClause2 = $"{childAlias}.{FiTechBDadosExtensions.RidColumnNameOf[info.ImediateType]}={farAlias}.{info.FarKey}";
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

            var aggregateObjectAttributes = ReflectionTool.GetAttributedMemberValues<AggregateObjectAttribute>(theType);
            for (int i = 0; i < aggregateObjectAttributes.Length; i++) {
                var refl = aggregateObjectAttributes[i];
                var field = refl.Member;
                var memberType = ReflectionTool.GetTypeOf(field);
                var type = ReflectionTool.GetTypeOf(field);
                var key = field.GetCustomAttribute<AggregateObjectAttribute>()?.ObjectKey;
                var info = refl.Attribute;
                if (
                    (info.ExplodedFlags.Contains("root") && parentAlias != "root") ||
                    (info.ExplodedFlags.Contains("child") && parentAlias == "root")
                ) {
                    continue;
                }
                String childAlias;
                var tname = type.Name;
                var pkey = key;
                // This inversion principle might be fucktastic.
                childAlias = prefixer.GetAliasFor(thisAlias,
                    tname,
                    pkey);

                String OnClause = $"{thisAlias}.{key}={childAlias}.RID";

                if (!ReflectionTool.TypeContains(theType, key)) {
                    OnClause = $"{thisAlias}.{selfRIDColumn}={childAlias}.{key}";
                }

                var joh = query.Join(type, childAlias, OnClause, JoinType.LEFT);

                joh.As(childAlias);

                var qjoins = query.Joins.Where((a) => a.Alias == childAlias);
                if (qjoins.Any()) {
                    var members = ReflectionTool.GetAttributedMemberValues<FieldAttribute>(memberType);
                    qjoins.First().Columns.AddRange(
                        members
                            .Select(m => m.Member.Name)
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
            var aggregateListAttributes = ReflectionTool.GetAttributedMemberValues<AggregateListAttribute>(theType);
            for (int i = 0; i < aggregateListAttributes.Length; i++) {
                var refl = aggregateListAttributes[i];
                var field = refl.Member;
                var memberType = ReflectionTool.GetTypeOf(field);
                var info = refl.Attribute;
                if (
                     (info.ExplodedFlags.Contains("root") && parentAlias != "root") ||
                     (info.ExplodedFlags.Contains("child") && parentAlias == "root")
                ) {
                    continue;
                }
                String childAlias = prefixer.GetAliasFor(thisAlias, info.RemoteObjectType.Name, info.RemoteField);

                String OnClause = $"{childAlias}.{info.RemoteField}={thisAlias}.{selfRIDColumn}";
                // Yuck
                if (!ReflectionTool.TypeContains(info.RemoteObjectType, info.RemoteField)) {
                    OnClause = $"{childAlias}.{FiTechBDadosExtensions.RidColumnNameOf[info.RemoteObjectType]}={thisAlias}.{info.RemoteField}";
                }
                var joh = query.Join(info.RemoteObjectType, childAlias, OnClause, JoinType.RIGHT);
                // The ultra supreme gimmick mode reigns supreme here.
                joh.GetType().GetMethod("As").Invoke(joh, new object[] { childAlias });

                var qjoins = query.Joins.Where((a) => a.Alias == childAlias);
                if (qjoins.Any()) {
                    var members = ReflectionTool.GetAttributedMemberValues<FieldAttribute>(info.RemoteObjectType);
                    qjoins.First().Columns.AddRange(
                        members
                            .Select(m => m.Member.Name)
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
            var membersOfT = ReflectionTool.FieldsAndPropertiesOf(theType);

            String thisAlias = prefixer.GetAliasFor(parentAlias, nameofThis, pKey);
            // Iterating through AggregateFields
            foreach (var field in membersOfT.Where((f) => f.GetCustomAttribute<AggregateFieldAttribute>() != null)) {
                var memberType = ReflectionTool.GetTypeOf(field);
                var info = field.GetCustomAttribute<AggregateFieldAttribute>();
                if (
                    (info.ExplodedFlags.Contains("root") && parentAlias != "root") ||
                    (info.ExplodedFlags.Contains("child") && parentAlias == "root")
                ) {
                    continue;
                }
                String childAlias = prefixer.GetAliasFor(thisAlias, info.RemoteObjectType.Name, info.ObjectKey);
                build.AggregateField(thisAlias, childAlias, info.RemoteField, field.Name);
            }
            // Iterating through AggregateFarFields
            foreach (var field in membersOfT.Where(
                    (f) =>
                        f.GetCustomAttribute<AggregateFarFieldAttribute>() != null)) {
                var memberType = ReflectionTool.GetTypeOf(field);
                var info = field.GetCustomAttribute<AggregateFarFieldAttribute>();
                if (
                    (info.ExplodedFlags.Contains("root") && parentAlias != "root") ||
                    (info.ExplodedFlags.Contains("child") && parentAlias == "root")
                ) {
                    continue;
                }
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
                if (
                     (info.ExplodedFlags.Contains("root") && parentAlias != "root") ||
                     (info.ExplodedFlags.Contains("child") && parentAlias == "root")
                ) {
                    continue;
                }
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
                if (
                    (info.ExplodedFlags.Contains("root") && parentAlias != "root") ||
                    (info.ExplodedFlags.Contains("child") && parentAlias == "root")
                ) {
                    continue;
                }
                String childAlias = prefixer.GetAliasFor(thisAlias, info.RemoteObjectType.Name, info.RemoteField);
                build.AggregateList(thisAlias, childAlias, field.Name);
                if (!Linear) {
                    MakeBuildAggregations(build, info.RemoteObjectType, thisAlias, info.RemoteObjectType.Name, info.RemoteField, prefixer);
                }
            }
        }

        public List<T> AggregateLoad<T>(LoadAllArgs<T> args = null) where T : IDataObject, new() {
            return Access((transaction) => AggregateLoad(transaction, args), null);
        }

        public List<object> ActiveConnectionStatus() {
            List<object> result = new List<object>();
            lock (ActiveConnections) {
                foreach (var conn in ActiveConnections.Values) {
                    result.Add(new {
                        conn.Connection.State,
                        conn.CreatedTime,
                        StackTrace = conn.StackTrace.ToString()
                    });
                }
            }
            return result;
        }

        public bool DeleteWhereRidNotIn<T>(Expression<Func<T, bool>> cnd, List<T> list) where T : IDataObject, new() {
            return Access((transaction) => DeleteWhereRidNotIn(transaction, cnd, list));
        }

        public void Dispose() {
            DisposeAsync().ConfigureAwait(false).GetAwaiter().GetResult();
        }

        bool _isDisposed = false;
        bool _isDisposing = false;
        public async ValueTask DisposeAsync() {
            if (!_isDisposed && !_isDisposing) {
                _isDisposing = true;
                try {
                    if (ActiveConnections != null) {
                        foreach(var i in ActiveConnections.Keys) {
                            try {
                                var connection = ActiveConnections[i].Connection;
                                if (connection is DbConnection dbc) {
                                    await dbc.DisposeAsync();
                                } else {
                                    connection.Dispose();
                                }
                                lock (ActiveConnections) {
                                    ActiveConnections.Remove(i);
                                }
                            } catch (Exception x) {
                                Debugger.Break();
                            }
                        }
                    }
                } catch (Exception x) {
                    Fi.Tech.Throw(x);
                } finally {
                    _isDisposed = true;
                    _isDisposing = false;
                }
            }
        }
        #endregion *****************
        //
        #region Default Transaction Using Core Functions.
        public T ForceExist<T>(BDadosTransaction transaction, Func<T> Default, IQueryBuilder qb) where T : IDataObject, new() {
            var f = LoadAll<T>(transaction, qb, null, 1);
            if (f.Any()) {
                return f.First();
            } else {
                T quickSave = Default();
                SaveItem(quickSave);
                return quickSave;
            }
        }

        public async Task<bool> SaveListAsync<T>(BDadosTransaction transaction, List<T> rs, bool recoverIds = false) where T : IDataObject {
            await transaction.Step().ConfigureAwait(false);
            bool retv = true;

            if (rs.Count == 0)
                return true;
            //if (rs.Count == 1) {
            //    return SaveItem(transaction, rs.First());
            //}
            for (int it = 0; it < rs.Count; it++) {
                if (rs[it].RID == null) {
                    rs[it].RID = new RID().ToString();
                }
            }

            rs.ForEach(item => {
                item.IsPersisted = false;
                if (!item.IsReceivedFromSync) {
                    item.UpdatedTime = Fi.Tech.GetUtcTime();
                }
            });
            DateTime d1 = DateTime.UtcNow;
            List<T> conflicts = new List<T>();

            var chunkSz = 5000;
            var chunks = rs.Fracture(chunkSz);
            foreach (var x in chunks) {
                var xlist = new List<T>(Math.Min(rs.Count, chunkSz));
                xlist.AddRange(x);
                await QueryReaderAsync(transaction, Plugin.QueryGenerator.QueryIds(xlist), async reader => {
                    var areader = reader as DbDataReader;
                    while (await areader.ReadAsync().ConfigureAwait(false)) {
                        var vId = (long?)Convert.ChangeType(reader[0], typeof(long));
                        var vRid = (string)Convert.ChangeType(reader[1], typeof(string));
                        for (int i = 0; i < xlist.Count; i++) {
                            var item = xlist[i];
                            if (item.Id == vId || item.RID == vRid) {
                                item.Id = vId ?? item.Id;
                                item.RID = vRid ?? item.RID;
                                item.IsPersisted = true;
                                break;
                            }
                        }
                    }
                    return 0;
                }).ConfigureAwait(false);
            }
            var elaps = DateTime.UtcNow - d1;

            var members = ReflectionTool.GetAttributedMemberValues<FieldAttribute>(typeof(T));
            int i2 = 0;
            int cut = Math.Min(500, 63999 / ((members.Length + 1) * 3));

            int rst = 0;
            List<T> temp;

            if (rs.Count > cut) {
                temp = new List<T>(rs.Count);
                temp.AddRange(rs);
            } else {
                temp = rs;
            }
            List<Exception> failedSaves = new List<Exception>();
            List<IDataObject> successfulSaves = new List<IDataObject>();
            List<IDataObject> failedObjects = new List<IDataObject>();
            transaction?.Benchmarker.Mark($"Begin SaveList<{typeof(T).Name}> process");
            //WorkQueuer wq = rs.Count > cut ? new WorkQueuer("SaveList_Annonymous_Queuer", 1, true) : null;

            while (i2 * cut < rs.Count) {
                int i = i2;
                List<T> sub;
                lock (temp)
                    sub = temp.Skip(i * cut).Take(Math.Min(rs.Count - (i * cut), cut)).ToList();
                var inserts = sub.Where(it => !it.IsPersisted).ToList();
                var updates = sub.Where(it => it.IsPersisted).ToList();
                if (inserts.Count > 0) {
                    try {
                        transaction?.Benchmarker.Mark($"Generate MultiInsert Query for {inserts.Count} {typeof(T).Name}");
                        var query = Plugin.QueryGenerator.GenerateMultiInsert<T>(inserts, false);
                        transaction?.Benchmarker.Mark($"Execute MultiInsert Query {inserts.Count} {typeof(T).Name}");
                        rst += await ExecuteAsync(transaction, query).ConfigureAwait(false);
                        lock (successfulSaves)
                            successfulSaves.AddRange(inserts.Select(a => (IDataObject)a));
                    } catch (Exception x) {
                        if (OnFailedSave != null) {
                            Fi.Tech.FireAndForget(async () => {
                                await Task.Yield();
                                OnFailedSave?.Invoke(typeof(T), inserts.Select(a => (IDataObject)a).ToArray(), x);
                            });
                        }

                        lock (failedSaves)
                            failedSaves.Add(x);
                    }
                    if (recoverIds) {
                        var queryIds = await QueryAsync<QueryIdsReturnValueModel>(transaction, QueryGenerator.QueryIds(inserts)).ConfigureAwait(false);
                        foreach (var dr in queryIds) {
                            var psave = inserts.FirstOrDefault(it => it.RID == dr.RID);
                            if (psave != null) {
                                psave.Id = Int64.Parse(dr.Id as String);
                            }
                        }
                    }
                }

                if (updates.Count > 0) {
                    try {
                        transaction?.Benchmarker.Mark($"Generate MultiUpdate Query for {updates.Count} {typeof(T).Name}");
                        var query = Plugin.QueryGenerator.GenerateMultiUpdate(updates);
                        transaction?.Benchmarker.Mark($"Execute MultiUpdate Query for {updates.Count} {typeof(T).Name}");
                        rst += await ExecuteAsync(transaction, query).ConfigureAwait(false);
                        lock (successfulSaves)
                            successfulSaves.AddRange(updates.Select(a => (IDataObject)a));
                    } catch (Exception x) {
                        if (OnFailedSave != null) {
                            Fi.Tech.FireAndForget(async () => {
                                await Task.Yield();
                                OnFailedSave?.Invoke(typeof(T), updates.Select(a => (IDataObject)a).ToArray(), x);
                            });
                        }
                        lock (failedSaves)
                            failedSaves.Add(x);
                    }
                }
                i2++;
            }
            //wq?.Stop(true);
            transaction?.Benchmarker.Mark($"End SaveList<{typeof(T).Name}> process");

            transaction?.Benchmarker.Mark($"Dispatch Successful Save events {typeof(T).Name}");
            if (successfulSaves.Any()) {
                //if (recoverIds) {
                //    var q = Query(transaction, QueryGenerator.QueryIds(rs));
                //    foreach (DataRow dr in q.Rows) {
                //        successfulSaves.FirstOrDefault(it => it.RID == dr[1] as String).Id = Int64.Parse(dr[0] as String);
                //    }
                //    failedObjects.AddRange(successfulSaves.Where(a => a.Id <= 0).Select(a => (IDataObject)a));
                //    successfulSaves.RemoveAll(a => a.Id <= 0);
                //}
                int newHash = 0;
                successfulSaves.ForEach(it => {
                    newHash = it.SpFthComputeDataFieldsHash();
                    if (it.PersistedHash != newHash) {
                        it.PersistedHash = newHash;
                        it.AlteredBy = IDataObjectExtensions.localInstanceId;
                    }
                });
                transaction.NotifyChange(successfulSaves.ToArray());
                if (OnSuccessfulSave != null) {
                    Fi.Tech.FireAndForget(async () => {
                        await Task.Yield();
                        OnSuccessfulSave?.Invoke(typeof(T), successfulSaves.ToArray());
                    }, async ex => {
                        await Task.Yield();
                        Fi.Tech.Throw(ex);
                    });
                }
            }
            transaction?.Benchmarker.Mark($"SaveList all done");
            if (failedSaves.Any()) {
                transaction?.MarkAsErrored();
                throw new BDadosException($"Not everything could be saved list of type {typeof(T).Name}", transaction.FrameHistory, failedObjects, new AggregateException(failedSaves));
            }
            if (failedObjects.Any()) {
                var ex = new BDadosException("Some objects did not persist correctly", transaction.FrameHistory, failedObjects, null);
                if (OnFailedSave != null) {
                    Fi.Tech.FireAndForget(async () => {
                        await Task.Yield();
                        OnFailedSave?.Invoke(typeof(T), failedObjects.Select(a => (IDataObject)a).ToArray(), ex);
                    });
                }
            }

            return retv;
        }

        public bool SaveList<T>(BDadosTransaction transaction, List<T> rs, bool recoverIds = false) where T : IDataObject {
            return SaveListAsync<T>(transaction, rs, recoverIds)
                .ConfigureAwait(false)
                .GetAwaiter()
                .GetResult();
        }

        public Object ScalarQuery(BDadosTransaction transaction, IQueryBuilder qb) {
            transaction.Step().ConfigureAwait(false).GetAwaiter().GetResult();
            Object retv = null;
            retv = Query(transaction, qb).Rows[0][0];
            return retv;
        }
        public bool DeleteWhereRidNotIn<T>(BDadosTransaction transaction, Expression<Func<T, bool>> cnd, List<T> list) where T : IDataObject, new() {
            return DeleteWhereRidNotInAsync(transaction, cnd, list)
                .ConfigureAwait(false)
                .GetAwaiter()
                .GetResult();
        }
        public async Task<bool> DeleteWhereRidNotInAsync<T>(BDadosTransaction transaction, Expression<Func<T, bool>> cnd, List<T> list) where T : IDataObject, new() {
            int retv = 0;
            if (list == null)
                return true;

            var id = GetIdColumn<T>();
            var rid = GetRidColumn<T>();
            IQueryBuilder query = QueryGenerator.GenerateSelectAll<T>().Append("WHERE");
            if (cnd != null) {
                PrefixMaker pm = new PrefixMaker();
                query.Append($"{rid} IN (SELECT {rid} FROM (SELECT {rid} FROM {typeof(T).Name} AS {pm.GetAliasFor("root", typeof(T).Name, String.Empty)} WHERE ");
                query.Append(new ConditionParser(pm).ParseExpression<T>(cnd));
                query.Append(") sub)");
            }
            if (list.Count > 0) {

                query.Append("AND");
                query.Append(Qb.NotIn(rid, list, l => l.RID));
                //for (var i = 0; i < list.Count; i++) {
                //    query.Append($"@{IntEx.GenerateShortRid()}", list[i].RID);
                //    if (i < list.Count - 1)
                //        query.Append(",");
                //}
                //query.Append(")");
            }

            var results = await QueryAsync<T>(transaction, query).ConfigureAwait(false);
            if (results.Any()) {
                OnObjectsDeleted?.Invoke(typeof(T), results.Select(t => t as IDataObject).ToArray());
                var query2 = Qb.Fmt($"DELETE FROM {typeof(T).Name} WHERE ") + Qb.In(rid, results, r => r.RID);
                retv = await ExecuteAsync(transaction, query2).ConfigureAwait(false);
                return retv > 0;
            }
            return true;
        }

        public bool SaveList<T>(List<T> rs, bool recoverIds = false) where T : IDataObject {
            return Access((transaction) => {
                return SaveList<T>(transaction, rs, recoverIds);
            });
        }

        public async Task<List<IDataObject>> LoadUpdatedItemsSince(IEnumerable<Type> types, DateTime dt) {
            return await AccessAsync (async (transaction) => {
                return await LoadUpdatedItemsSince(transaction, types, dt);
            }, CancellationToken.None, null);
        }

        public async Task<List<IDataObject>> LoadUpdatedItemsSince(BDadosTransaction transaction, IEnumerable<Type> types, DateTime dt) {
            var workingTypes = types.Where(t => t.Implements(typeof(IDataObject))).ToList();
            var fields = new Dictionary<Type, MemberInfo[]>();
            foreach (var type in workingTypes) {
                fields[type] = ReflectionTool.GetAttributedMemberValues<FieldAttribute>(type)
                    .Select(x => x.Member)
                    .ToArray();
            }

            var query = Plugin.QueryGenerator.GenerateGetStateChangesQuery(workingTypes, fields, dt);

            using (var command = await transaction.CreateCommand().ConfigureAwait(false)) {
                VerboseLogQueryParameterization(transaction, query);
                query.ApplyToCommand(command, Plugin.ProcessParameterValue);
                transaction?.Benchmarker?.Mark($"Execute Query <{query.Id}>");
                lock (transaction) {
                    command.Prepare();
                    using (var reader = command.ExecuteReader()) {
                        return BuildStateUpdateQueryResult(transaction, reader, workingTypes, fields);
                    }
                }
            }
        }

        private void SortTypesByDep(List<Type> workingTypes) {
            Dictionary<Type, Type[]> DepTrees = new Dictionary<Type, Type[]>();

            foreach (var a in workingTypes) {
                DepTrees[a] = TypeDepScore.ExplodeDependencyTree(a).ToArray();
            }
            foreach (var a in workingTypes) {
                foreach (var b in workingTypes) {

                    var aDependsOnB = DepTrees[a].Any(t => t == b);
                    var bDependsOnA = DepTrees[b].Any(t => t == a);

                    if (aDependsOnB && bDependsOnA)
                        Console.WriteLine($"Cross dependency: {a.Name} <-> {b.Name}");
                }
            }
            List<Type> finalList = new List<Type>();
            List<Type> tempList = new List<Type>(workingTypes);
            while (finalList.Count < workingTypes.Count) {
                for (int i = 0; i < tempList.Count; i++) {
                    if (!finalList.Contains(tempList[i])) {
                        // Check if all items in dependency tree meets
                        // - Is the type itself (self-dependency) or
                        // - Is already in the final list or
                        // - Is not in the temp list (dependency blatantly missing)
                        // If all dependencies meet, then tempList[i] is clear to enter the final list 
                        if (DepTrees[tempList[i]].All(t => t == tempList[i] || finalList.Contains(t) || !tempList.Contains(t))) {
                            finalList.Add(tempList[i]);
                        }
                    }
                }
                tempList.RemoveAll(i => finalList.Contains(i));
            }
            workingTypes.Clear();
            workingTypes.AddRange(finalList);
        }

        public async Task SendLocalUpdates(IEnumerable<Type> types, DateTime dt, Stream stream) {
            await AccessAsync(async tsn => await SendLocalUpdates(tsn, types, dt, stream), CancellationToken.None);
        }
        public void ReceiveRemoteUpdatesAndPersist(IEnumerable<Type> types, Stream stream) {
            Access(tsn => ReceiveRemoteUpdatesAndPersist(tsn, types, stream));
        }

        public async Task SendLocalUpdates(BDadosTransaction transaction, IEnumerable<Type> types, DateTime dt, Stream stream) {
            var workingTypes = types.Where(t => !t.IsInterface && t.GetCustomAttribute<ViewOnlyAttribute>() == null && !t.IsGenericType && t.Implements(typeof(IDataObject))).ToList();

            SortTypesByDep(workingTypes);

            var fields = new Dictionary<Type, MemberInfo[]>();
            foreach (var type in workingTypes) {
                fields[type] = ReflectionTool.GetAttributedMemberValues<FieldAttribute>(type)
                    .Select(x => x.Member)
                    .ToArray();
            }
            var memberTypeOf = new Dictionary<MemberInfo, Type>();
            foreach (var typeMembers in fields) {
                foreach (var member in typeMembers.Value) {
                    memberTypeOf[member] = ReflectionTool.GetTypeOf(member);
                }
            }
            var query = Plugin.QueryGenerator.GenerateGetStateChangesQuery(workingTypes, fields, dt);

            using (var cmd = await transaction.CreateCommand("set net_write_timeout=99999; set net_read_timeout=99999", true)) {
                cmd.ExecuteNonQuery();
            }

            using (var command = await transaction.CreateCommand()) {
                command.CommandTimeout = 999999;
                VerboseLogQueryParameterization(transaction, query);
                query.ApplyToCommand(command, Plugin.ProcessParameterValue);
                transaction?.Benchmarker?.Mark($"@SendLocalUpdates Execute Query <{query.Id}>");
                lock (transaction) {
                    command.Prepare();
                    using (var reader = command.ExecuteReader()) {
                        using (var writer = new StreamWriter(stream, new UTF8Encoding(false), 1024 * 64, true)) {
                            object[] values = new object[reader.FieldCount];
                            transaction?.Benchmarker?.Mark("Begin transmit data");
                            BinaryFormatter bf = new BinaryFormatter();
                            int readRows = 0;
                            try {
                                while (true) {
                                    if (transaction?.ConnectionState == ConnectionState.Closed) {
                                        Debugger.Break();
                                    }
                                    if (reader.IsClosed || !reader.Read()) {
                                        break;
                                    }
                                    readRows++;
                                    // 0x09 is Tab in the ASCII table,
                                    // this character is chosen because it's 100% sure it will not
                                    // appear in the JSON serialized values
                                    reader.GetValues(values);
                                    var type = workingTypes.FirstOrDefault(t => t.Name == values[0] as string);
                                    if (type == null) {
                                        continue;
                                    }
                                    for (int i = 0; i < fields[type].Length; i++) {
                                        values[i + 1] = ReflectionTool.TryCast(values[i + 1], memberTypeOf[fields[type][i]]);
                                    }
                                    // +1 because we need to add the type name too
                                    var outv = new object[fields[type].Length + 1];
                                    Array.Copy(values, 0, outv, 0, outv.Length);
                                    writer.WriteLine(JsonConvert.SerializeObject(outv));
                                    //writer.WriteLine(String.Join(((char) 0x09).ToString(), values.Select(v => JsonConvert.SerializeObject(v))));
                                }
                            } catch (Exception x) {
                                var elaps = transaction?.Benchmarker?.Mark("End data transmission");
                                Console.WriteLine($"Error in {elaps}ms");
                                transaction?.MarkAsErrored();
                                throw new BDadosException("Error sending updates to peers", x);
                            }
                            transaction?.Benchmarker?.Mark($"End data transmission {readRows} items");
                        }
                    }
                }
            }
        }

        public IEnumerable<IDataObject> ReceiveRemoteUpdates(IEnumerable<Type> types, Stream stream) {
            var workingTypes = types.Where(t => !t.IsInterface && t.GetCustomAttribute<ViewOnlyAttribute>() == null && !t.IsGenericType && t.Implements(typeof(IDataObject))).ToList();
            var fields = new Dictionary<Type, MemberInfo[]>();
            foreach (var type in workingTypes) {
                fields[type] = ReflectionTool.GetAttributedMemberValues<FieldAttribute>(type)
                    .Select(x => x.Member)
                    .ToArray();
            }
            var memberTypeOf = new Dictionary<MemberInfo, Type>();
            foreach (var typeMembers in fields) {
                foreach (var member in typeMembers.Value) {
                    memberTypeOf[member] = ReflectionTool.GetTypeOf(member);
                }
            }

            var cache = new Queue<IDataObject>();
            var objAssembly = new WorkQueuer("rcv_updates_objasm", 32, true);

            using (var reader = new StreamReader(stream, new UTF8Encoding(false), false, 1024 * 1024 * 8)) {
                String line;
                while (!string.IsNullOrEmpty(line = reader.ReadLine())) {
                    var values = JsonConvert.DeserializeObject<object[]>(line);
                    objAssembly.Enqueue(async () => {
                        await Task.Yield();
                        var v = values;
                        Type type = types.FirstOrDefault(t => t.Name == values[0] as String);
                        if (type == null)
                            return;
                        var instance = NewInstance(type);
                        var ft = fields[type];
                        for (int i = 0; i < fields[type].Length; i++) {
                            ReflectionTool.SetMemberValue(ft[i], instance, v[i + 1]);
                        }
                        var add = instance as IDataObject;
                        if (add != null) {
                            lock (cache) {
                                cache.Enqueue(add);
                            }
                        } else {
                            Debugger.Break();
                        }
                        //yield return instance as IDataObject;
                    });
                }
            }
            while (cache.Count > 0) {
                lock (cache) {
                    var ret = cache.Dequeue();
                    if (ret != null) {
                        yield return ret;
                    } else {
                        Debugger.Break();
                    }
                }
            }

            while (objAssembly.WorkDone < objAssembly.TotalWork) {
                while (cache.Count > 0) {
                    lock (cache) {
                        var ret = cache.Dequeue();
                        if (ret != null) {
                            yield return ret;
                        } else {
                            Debugger.Break();
                        }
                        lock (cache) {
                        }
                    }
                }
            }
            objAssembly.Stop(true).Wait();
        }

        public void ReceiveRemoteUpdatesAndPersist(BDadosTransaction transaction, IEnumerable<Type> types, Stream stream) {

            var cache = new List<IDataObject>();
            int maxCacheLenBeforeFlush = 5000;
            var persistenceQueue = new WorkQueuer("rcv_updates_persist", 1, true);

            Action flushAndPersist = () => {
                lock (cache) {
                    var persistenceBatch = new List<IDataObject>(cache);
                    cache.Clear();
                    persistenceQueue.Enqueue(async () => {
                        await Task.Yield();
                        var grouping = persistenceBatch.GroupBy(item => item.GetType());
                        foreach (var g in grouping) {
                            var listOfType = g.ToList();
                            Console.WriteLine($"Saving batch of type {listOfType.First().GetType().Name} {listOfType.Count} items");
                            listOfType.ForEach(i => i.IsReceivedFromSync = true);
                            SaveList(transaction, listOfType, false);
                        }
                    }, async x => {
                        await Task.Yield();
                        transaction?.MarkAsErrored();
                        Console.WriteLine($"Error persisting batch {x.Message}");
                    });
                }
            };

            foreach (var instance in ReceiveRemoteUpdates(types, stream)) {
                lock (cache) {
                    cache.Add(instance);
                }
                if (persistenceQueue.TotalWork - persistenceQueue.WorkDone > 2) {
                    Thread.Sleep(100);
                }
                if (cache.Count >= maxCacheLenBeforeFlush) {
                    flushAndPersist();
                }
            }

            flushAndPersist();
            persistenceQueue.Stop(true).Wait();
        }

        public bool Delete<T>(BDadosTransaction transaction, Expression<Func<T, bool>> conditions) where T : IDataObject, new() {
            return DeleteAsync(transaction, conditions)
                .ConfigureAwait(false)
                .GetAwaiter()
                .GetResult();
        }

        public async Task<bool> DeleteAsync<T>(BDadosTransaction transaction, Expression<Func<T, bool>> conditions) where T : IDataObject, new() {
            await transaction.Step().ConfigureAwait(false);
            bool retv = false;
            var prefixMaker = new PrefixMaker();
            var cnd = new ConditionParser(prefixMaker).ParseExpression<T>(conditions);
            var rid = GetRidColumn<T>();

            var query = Qb.Fmt($"DELETE FROM {typeof(T).Name} WHERE {rid} in (SELECT {rid} FROM (SELECT {rid} FROM {typeof(T).Name} tba WHERE ") + cnd + Qb.Fmt(") a);");
            retv = (await ExecuteAsync(transaction, query).ConfigureAwait(false)) > 0;
            return retv;
        }

        private void VerboseLogQueryParameterization(BDadosTransaction transaction, IQueryBuilder query) {
            if (!FiTechCoreExtensions.EnabledSystemLogs[RDB_SYSTEM_LOGID]) {
                return;
            }
            String QueryText = query.GetCommandText();
            WriteLog($"[{Description}:{transaction.Id}] -- Query <{query.Id}>:\n {QueryText}");
            transaction?.Benchmarker?.Mark($"[{Description}:{transaction.Id}] Prepare Statement <{query.Id}>");
            // Adiciona os parametros
            foreach (KeyValuePair<String, Object> param in query.GetParameters()) {

                var pval = $"'{param.Value?.ToString() ?? "null"}'";
                if (param.Value is DateTime || param.Value is DateTime? && ((DateTime?)param.Value).HasValue) {
                    pval = ((DateTime)param.Value).ToString("yyyy-MM-dd HH:mm:ss");
                    pval = $"'{pval}'";
                }

                WriteLog($"[{Description}:{transaction.Id}] SET @{param.Key} = {pval} -- {param.Value?.GetType()?.Name}");
            }
        }

        public async ValueTask QueryToJsonAsync<T>(BDadosTransaction transaction, IQueryBuilder query, TextWriter writer) where T : new() {
            await transaction.Step().ConfigureAwait(false);

            if (query == null || query.GetCommandText() == null) {
                await writer.WriteAsync("[]").ConfigureAwait(false);
                return;
            }
            var tName = typeof(T).Name;
            DateTime Inicio = DateTime.Now;
            using (var command = (DbCommand) await transaction.CreateCommand().ConfigureAwait(false)) {
                command.CommandTimeout = Plugin.CommandTimeout;
                query.ApplyToCommand(command, Plugin.ProcessParameterValue);
                VerboseLogQueryParameterization(transaction, query);
                // --
                transaction?.Benchmarker?.Mark($"[{Description}:{transaction.Id}] Enter lock region");
                var c = 0;
                transaction?.Benchmarker?.Mark($"[{Description}:{transaction.Id}] Execute QueryCoroutinely<{tName}> <{query.Id}>");
                await GetJsonStringFromQueryAsync<T>(transaction, command, writer).ConfigureAwait(false);
                transaction?.Benchmarker?.Mark($"[{Description}:{transaction.Id}] Build<{tName}> completed <{query.Id}>");

                var elaps = transaction?.Benchmarker?.Mark($"[{Description}:{transaction.Id}] Fully consumed resultset (coroutinely) <{query.Id}> Size: {c}");
                transaction?.Benchmarker?.Mark($"[{Description}:{transaction.Id}] Avg consumption speed: {((double)elaps / (double)c).ToString("0.00")}ms/item");

                try {
                    int nResults = 0;
                    nResults = c;
                    WriteLog($"[{Description}:{transaction.Id}] -------- Query<{tName}> <{query.Id}> [OK] ({nResults} results) [{elaps} ms]");
                } catch (Exception x) {
                    WriteLog($"[{Description}:{transaction.Id}] -------- Error<{tName}> <{query.Id}>: {x.Message} ([{DateTime.Now.Subtract(Inicio).TotalMilliseconds} ms]");
                    WriteLog(x.Message);
                    WriteLog(x.StackTrace);
                    transaction?.MarkAsErrored();
                    throw new BDadosException("Error in query", x) {
                        Data = {
                            ["Query"] = query,
                        }
                    };
                } finally {
                    WriteLog("------------------------------------");
                }
            }
        }

        public async IAsyncEnumerable<T> QueryCoroutinely<T>(BDadosTransaction transaction, IQueryBuilder query) where T : new() {
            await transaction.Step().ConfigureAwait(false);

            if (query == null || query.GetCommandText() == null) {
                yield break;
            }
            var tName = typeof(T).Name;
            DateTime Inicio = DateTime.Now;
            using (var command = (DbCommand) await transaction.CreateCommand().ConfigureAwait(false)) {
                command.CommandTimeout = Plugin.CommandTimeout;
                query.ApplyToCommand(command, Plugin.ProcessParameterValue);
                VerboseLogQueryParameterization(transaction, query);
                // --
                transaction?.Benchmarker?.Mark($"[{Description}:{transaction.Id}] Enter lock region");
                var c = 0;
                transaction?.Benchmarker?.Mark($"[{Description}:{transaction.Id}] Execute QueryCoroutinely<{tName}> <{query.Id}>");
                await foreach (var item in GetObjectEnumerableAsync<T>(transaction, command).ConfigureAwait(false)) {
                    c++;
                    yield return item;
                }
                transaction?.Benchmarker?.Mark($"[{Description}:{transaction.Id}] Build<{tName}> completed <{query.Id}>");

                var elaps = transaction?.Benchmarker?.Mark($"[{Description}:{transaction.Id}] Fully consumed resultset (coroutinely) <{query.Id}> Size: {c}");
                transaction?.Benchmarker?.Mark($"[{Description}:{transaction.Id}] Avg consumption speed: {((double)elaps / (double)c).ToString("0.00")}ms/item");

                try {
                    int nResults = 0;
                    nResults = c;
                    WriteLog($"[{Description}:{transaction.Id}] -------- Query<{tName}> <{query.Id}> [OK] ({nResults} results) [{elaps} ms]");
                    yield break;
                } catch (Exception x) {
                    WriteLog($"[{Description}:{transaction.Id}] -------- Error<{tName}> <{query.Id}>: {x.Message} ([{DateTime.Now.Subtract(Inicio).TotalMilliseconds} ms]");
                    WriteLog(x.Message);
                    WriteLog(x.StackTrace);
                    transaction?.MarkAsErrored();
                    throw new BDadosException("Error in query", x) {
                        Data = {
                            ["Query"] = query,
                        }
                    };
                } finally {
                    WriteLog("------------------------------------");
                }
            }
        }

        public async Task<List<T>> QueryAsync<T>(BDadosTransaction transaction, IQueryBuilder query) where T : new() {
            await Task.Yield();
            await transaction.Step().ConfigureAwait(false);

            if (query == null || query.GetCommandText() == null) {
                return new List<T>();
            }
            var tName = typeof(T).Name;
            DateTime Inicio = DateTime.Now;
            using (var command = (DbCommand) await transaction.CreateCommand().ConfigureAwait(false)) {
                command.CommandTimeout = Plugin.CommandTimeout;
                query.ApplyToCommand(command, Plugin.ProcessParameterValue);
                VerboseLogQueryParameterization(transaction, query);
                // --
                List<T> retv;
                transaction?.Benchmarker?.Mark($"[{Description}:{transaction.Id}] Enter lock region");
                transaction?.Benchmarker?.Mark($"[{Description}:{transaction.Id}] Execute Query<{tName}> <{query.Id}>");
                retv = await GetObjectListAsync<T>(transaction, command).ConfigureAwait(false);
                transaction?.Benchmarker?.Mark($"[{Description}:{transaction.Id}] Build<{tName}> completed <{query.Id}>");
                if (retv == null) {
                    throw new Exception("Null list generated");
                }
                var elaps = transaction?.Benchmarker?.Mark($"[{Description}:{transaction.Id}] Built List <{query.Id}> Size: {retv.Count}");
                transaction?.Benchmarker?.Mark($"[{Description}:{transaction.Id}] Avg Build speed: {((double)elaps / (double)retv.Count).ToString("0.00")}ms/item");

                try {
                    int nResults = 0;
                    nResults = retv.Count;
                    WriteLog($"[{Description}:{transaction.Id}] -------- Query<{tName}> <{query.Id}> [OK] ({nResults} results) [{elaps} ms]");
                    return retv;
                } catch (Exception x) {
                    WriteLog($"[{Description}:{transaction.Id}] -------- Error<{tName}> <{query.Id}>: {x.Message} ([{DateTime.Now.Subtract(Inicio).TotalMilliseconds} ms]");
                    WriteLog(x.Message);
                    WriteLog(x.StackTrace);
                    transaction?.MarkAsErrored();
                    throw new BDadosException("Error in Query", x) {
                        Data = {
                            ["Query"] = query,
                        }
                    };
                } finally {
                    WriteLog("------------------------------------");
                }
            }
        }

        public List<T> Query<T>(BDadosTransaction transaction, IQueryBuilder query) where T : new() {
            return QueryAsync<T>(transaction, query).ConfigureAwait(false).GetAwaiter().GetResult();
        }

        public T LoadById<T>(BDadosTransaction transaction, long Id) where T : IDataObject, new() {
            transaction.Step().ConfigureAwait(false).GetAwaiter().GetResult();

            var id = GetIdColumn(typeof(T));
            return LoadAll<T>(transaction, new Qb().Append($"{id}=@id", Id), null, 1).FirstOrDefault();
        }

        public T LoadByRid<T>(BDadosTransaction transaction, String RID) where T : IDataObject, new() {
            transaction.Step().ConfigureAwait(false).GetAwaiter().GetResult();

            var rid = GetRidColumn(typeof(T));
            return LoadAll<T>(transaction, new Qb().Append($"{rid}=@rid", RID), null, 1).FirstOrDefault();
        }

        public async Task<List<T>> LoadAllAsync<T>(BDadosTransaction transaction, LoadAllArgs<T> args = null) where T : IDataObject, new() {
            await transaction.Step().ConfigureAwait(false);

            return await FetchAsync<T>(transaction, args).ToListAsync().ConfigureAwait(false);
        }
        public List<T> LoadAll<T>(BDadosTransaction transaction, LoadAllArgs<T> args = null) where T : IDataObject, new() {
            transaction.Step().ConfigureAwait(false).GetAwaiter().GetResult();

            return Fetch<T>(transaction, args).ToList();
        }

        public async Task<List<T>> LoadAllAsync<T>(BDadosTransaction transaction, IQueryBuilder conditions, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc, object contextObject = null) where T : IDataObject, new() {
            transaction.Step().ConfigureAwait(false).GetAwaiter().GetResult();

            return await FetchAsync<T>(transaction, conditions, skip, limit, orderingMember, ordering, contextObject).ToListAsync().ConfigureAwait(false);
        }
        public List<T> LoadAll<T>(BDadosTransaction transaction, IQueryBuilder conditions, int? skip = null, int? limit = null, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc, object contextObject = null) where T : IDataObject, new() {
            transaction.Step().ConfigureAwait(false).GetAwaiter().GetResult();

            return Fetch<T>(transaction, conditions, skip, limit, orderingMember, ordering, contextObject).ToList();
        }
        public bool Delete(BDadosTransaction transaction, IDataObject obj) {
            return DeleteAsync(transaction, obj)
                .ConfigureAwait(false)
                .GetAwaiter()
                .GetResult();
        }
        public async Task<bool> DeleteAsync(BDadosTransaction transaction, IDataObject obj) {
            await transaction.Step().ConfigureAwait(false);

            bool retv = false;

            //var id = GetIdColumn(obj.GetType());
            var rid = obj.RID;
            var ridcol = FiTechBDadosExtensions.RidColumnNameOf[obj.GetType()];
            string ridname = "RID";
            if (ridcol != null) {
                ridname = ridcol;
            }
            OnObjectsDeleted?.Invoke(obj.GetType(), obj.ToSingleElementList().ToArray());
            var query = new Qb().Append($"DELETE FROM {obj.GetType().Name} WHERE {ridname}=@rid", obj.RID);
            retv = (await ExecuteAsync(transaction, query).ConfigureAwait(false)) > 0;
            return retv;
        }

        public bool Delete<T>(BDadosTransaction transaction, IEnumerable<T> obj) where T : IDataObject, new() {
            return DeleteAsync(transaction, obj)
                .ConfigureAwait(false)
                .GetAwaiter()
                .GetResult();
        }

        public async Task<bool> DeleteAsync<T>(BDadosTransaction transaction, IEnumerable<T> obj) where T : IDataObject, new() {
            await transaction.Step().ConfigureAwait(false);

            bool retv = false;

            var ridcol = FiTechBDadosExtensions.RidColumnNameOf[typeof(T)];
            string ridname = "RID";
            if (ridcol != null) {
                ridname = ridcol;
            }
            OnObjectsDeleted?.Invoke(typeof(T), obj.Select(t => t as IDataObject).ToArray());
            var query = Qb.Fmt($"DELETE FROM {typeof(T).Name} WHERE ") + Qb.In(ridname, obj.ToList(), o => o.RID);
            retv = (await ExecuteAsync(transaction, query).ConfigureAwait(false)) > 0;
            return retv;
        }

        public async ValueTask UpdateAsync<T>(BDadosTransaction transaction, T input, params (Expression<Func<T, object>> parameterExpression, object Value)[] updates) where T : IDataObject {
            await transaction.Step().ConfigureAwait(false);

            if (input == null) {
                transaction?.MarkAsErrored();
                throw new BDadosException("Error updating item", transaction.FrameHistory, new List<IDataObject>(), new ArgumentNullException("Input to SaveItem must be not-null"));
            }
            transaction.Benchmarker?.Mark($"[{Description}:{transaction.Id}] UpdateAsync<{typeof(T).Name}>");
            var query = QueryGenerator.GenerateUpdateQuery(input, updates);
            await ExecuteAsync(transaction, query).ConfigureAwait(false);
        }
        public async ValueTask UpdateAndMutateAsync<T>(BDadosTransaction transaction, T input, params (Expression<Func<T, object>> parameterExpression, object Value)[] updates) where T : IDataObject {
            await UpdateAsync(transaction, input, updates).ConfigureAwait(false);
            for (int i = 0; i < updates.Length; i++) {
                var check = updates[i].parameterExpression.Body;
                if (check is UnaryExpression unaex) {
                    check = unaex.Operand;
                }
                if (check is MemberExpression mex) {
                    transaction.AddMutateTarget(mex.Member, input, updates[i].Value);
                }
            }
        }

        public async Task<bool> SaveItemAsync(BDadosTransaction transaction, IDataObject input) {
            await transaction.Step().ConfigureAwait(false);

            if (input == null) {
                transaction?.MarkAsErrored();
                throw new BDadosException("Error saving item", transaction.FrameHistory, new List<IDataObject>(), new ArgumentNullException("Input to SaveItem must be not-null"));
            }

            var t = input.GetType();
            bool retv = false;

            int rs = -33;

            var id = GetIdColumn(t);
            var rid = GetRidColumn(t);

            if (!input.IsReceivedFromSync) {
                input.UpdatedTime = Fi.Tech.GetUtcTime();
            }

            transaction?.Benchmarker.Mark($"SaveItem<{t.Name}> check persistence");
            input.IsPersisted = false;
            var persistedMap = await QueryAsync<QueryIdsReturnValueModel>(transaction, Plugin.QueryGenerator.QueryIds(input.ToSingleElementList())).ConfigureAwait(false);
            foreach (var a in persistedMap) {
                if (input.RID == (string)Convert.ChangeType(a.RID, typeof(String))) {
                    input.IsPersisted = true;
                    input.Id = Int64.Parse(a.Id);
                }
            }
            transaction?.Benchmarker.Mark($"SaveItem<{input.GetType().Name}> isPersisted? {input.IsPersisted}");

            try {
                if (input.IsPersisted) {
                    transaction?.Benchmarker.Mark($"SaveItem<{input.GetType().Name}> generating UPDATE query");
                    var query = Plugin.QueryGenerator.GenerateUpdateQuery(input);
                    transaction?.Benchmarker.Mark($"SaveItem<{input.GetType().Name}> executing query");
                    rs = await ExecuteAsync(transaction, query).ConfigureAwait(false);
                    transaction?.Benchmarker.Mark($"SaveItem<{input.GetType().Name}> query executed OK");
                    retv = true;
                    transaction.NotifyChange(input.ToSingleElementList().ToArray());
                    return retv;
                } else {
                    transaction?.Benchmarker.Mark($"SaveItem<{input.GetType().Name}> generating INSERT query");
                    var query = Plugin.QueryGenerator.GenerateInsertQuery(input);
                    transaction?.Benchmarker.Mark($"SaveItem<{input.GetType().Name}> executing query");
                    retv = true;
                    rs = await ExecuteAsync(transaction, query).ConfigureAwait(false);
                    transaction?.Benchmarker.Mark($"SaveItem<{input.GetType().Name}> query executed OK");
                }
            } catch (Exception x) {
                if (OnFailedSave != null) {
                    Fi.Tech.FireAndForget(async () => {
                        await Task.Yield();
                        OnFailedSave?.Invoke(input.GetType(), new List<IDataObject> { input }.ToArray(), x);
                    }, async (xe) => {
                        await Task.Yield();
                        Fi.Tech.Throw(xe);
                    });
                }
                transaction?.MarkAsErrored();
                throw new BDadosException("Error Saving Item", x);
            }

            if (input.Id <= 0) {
                long retvId = 0;
                var queryIds = await QueryAsync<QueryIdsReturnValueModel>(transaction, Plugin.QueryGenerator.QueryIds(input.ToSingleElementList())).ConfigureAwait(false);
                foreach (var dr in queryIds) {
                    if (input != null && dr.RID == input.RID) {
                        input.Id = Int64.Parse(dr.Id as String);
                    }
                }
            }

            transaction.OnTransactionEnding += () => {
                if (retv) {
                    var newHash = input.SpFthComputeDataFieldsHash();
                    if (input.PersistedHash != newHash) {
                        input.PersistedHash = newHash;
                        input.AlteredBy = IDataObjectExtensions.localInstanceId;
                    }
                    transaction.NotifyChange(new[] { input });

                    if (OnSuccessfulSave != null) {
                        Fi.Tech.FireAndForget(async () => {
                            await Task.Yield();
                            OnSuccessfulSave?.Invoke(input.GetType(), new List<IDataObject> { input }.ToArray());
                        }, async (xe) => {
                            await Task.Yield();
                            Fi.Tech.Throw(xe);
                        });
                    }
                    retv = true;
                }
            };

            return rs > 0;
        }

        public bool SaveItem(BDadosTransaction transaction, IDataObject input) {
            return SaveItemAsync(transaction, input).ConfigureAwait(false).GetAwaiter().GetResult();
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

        public async Task<bool> ExistsByRIDAsync<T>(BDadosTransaction transaction, string RID) where T : IDataObject{
            return ((await QueryAsync<ValueBox<int>>(transaction, Plugin.QueryGenerator.CheckExistsByRID<T>(RID))).FirstOrDefault()?.Value ?? 0) > 0;
        }
        public async Task<bool> ExistsByIdAsync<T>(BDadosTransaction transaction, long Id) where T : IDataObject{
            return ((await QueryAsync<ValueBox<int>>(transaction, Plugin.QueryGenerator.CheckExistsById<T>(Id))).FirstOrDefault()?.Value ?? 0) > 0;
        }
        public async Task<bool> ExistsByRIDAsync<T>(string RID) where T : IDataObject{
            return await AccessAsync(async (tsn) => {
                return await ExistsByRIDAsync<T>(tsn, RID);
            }, CancellationToken.None);
        }
        public async Task<bool> ExistsByIdAsync<T>(long Id) where T : IDataObject {
            return await AccessAsync(async (tsn) => {
                return await ExistsByIdAsync<T>(tsn, Id);
            }, CancellationToken.None);
        }


        static SelfInitializerDictionary<Type, IJoinBuilder> CacheAutomaticJoinBuilder = new SelfInitializerDictionary<Type, IJoinBuilder>(
            type => {
                var prefixer = CacheAutoPrefixer[type];
                var retv = MakeJoin(
                    (query) => {
                        // Starting with T itself
                        var jh = query.AggregateRoot(type, prefixer.GetAliasFor("root", type.Name, String.Empty)).As(prefixer.GetAliasFor("root", type.Name, String.Empty));
                        jh.OnlyFields(
                            ReflectionTool.GetAttributedMemberValues<FieldAttribute>(type)
                            .Select(a => a.Member.Name)
                            .ToArray()
                        );
                        MakeQueryAggregations(ref query, type, "root", type.Name, String.Empty, prefixer, false);
                    });

                return retv;
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
                            ReflectionTool.GetAttributedMemberValues<FieldAttribute>(type)
                            .Select(a => a.Member.Name)
                            .ToArray()
                        );
                        MakeQueryAggregations(ref query, type, "root", type.Name, String.Empty, prefixer, true);
                    });
            }
        );

        public async IAsyncEnumerable<T> AggregateLoadAsyncCoroutinely<T>(
            BDadosTransaction transaction,
            LoadAllArgs<T> args = null) where T : IDataObject, new() {
            await transaction.Step().ConfigureAwait(false);
            args = args ?? new LoadAllArgs<T>();
            int? queryLimit = args.Linear ? args.RowLimit : null; // args.RowLimit ?? DefaultQueryLimit;
            int? querySkip = args.Linear ? args.RowSkip : null;

            transaction?.Benchmarker?.Mark($"Begin AggregateLoad<{typeof(T).Name}>");
            var Members = ReflectionTool.FieldsAndPropertiesOf(typeof(T));
            var prefixer = args.Linear ? CacheAutoPrefixerLinear[typeof(T)] : CacheAutoPrefixer[typeof(T)];
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

            WriteLog($"Running Aggregate Load All for {typeof(T).Name}? {hasAnyAggregations}. Linear? {args.Linear}");
            // CLUMSY
            if (hasAnyAggregations) {

                transaction?.Benchmarker?.Mark("Construct Join Definition");

                transaction?.Benchmarker?.Mark("Resolve ordering Member");
                var om = FiTechBDadosExtensions.IdColumnOf[typeof(T)];

                transaction?.Benchmarker?.Mark("--");

                using (var command = (DbCommand) await transaction.CreateCommand().ConfigureAwait(false)) {
                    var join = args.Linear ? CacheAutoJoinLinear[typeof(T)] : CacheAutoJoin[typeof(T)];

                    var builtConditions = (args.Conditions == null ? Qb.Fmt("TRUE") : new ConditionParser(prefixer).ParseExpression(args.Conditions));
                    var builtConditionsRoot = (args.Conditions == null ? Qb.Fmt("TRUE") : new ConditionParser(prefixer).ParseExpression(args.Conditions, false));

                    transaction?.Benchmarker?.Mark($"Parsed Conditions: {builtConditions.GetCommandText()}");

                    var query = Plugin.QueryGenerator.GenerateJoinQuery(join, builtConditions, querySkip, queryLimit, om, args.OrderingType, builtConditionsRoot);
                    transaction?.Benchmarker?.Mark($"Generate Join Query");
                    //var _buildParameters = Linear ? CacheBuildParamsLinear[typeof(T)] : CacheBuildParams[typeof(T)];
                    query.ApplyToCommand(command, Plugin.ProcessParameterValue);
                    transaction?.Benchmarker?.Mark($"Start build AggregateListDirect<{typeof(T).Name}> ({query.Id})");

                    var dlc = new DataLoadContext {
                        DataAccessor = this,
                        IsAggregateLoad = true,
                        ContextTransferObject = args.ContextObject ?? transaction?.ContextTransferObject
                    };

                    var implementsAfterLoad = CacheImplementsAfterLoad[typeof(T)];
                    var implementsAfterAggregateLoad = CacheImplementsAfterAggregateLoad[typeof(T)];

                    // One day this could be great, but right now it sucks;
                    //
                    //await foreach(var item in Fi.Tech.ParallelFlow<T>(async yield => {
                    //    await foreach (var item in BuildAggregateListDirectCoroutinely<T>(transaction, command, join, 0).ConfigureAwait(false)) {
                    //        yield.Return(item);
                    //    }
                    //}).Then<T>(async i => {
                    //    if (implementsAfterLoad) {
                    //        ((IBusinessObject)i).OnAfterLoad(dlc);
                    //    }
                    //    if (implementsAfterAggregateLoad) {
                    //        await ((IBusinessObject<T>)i).OnAfterAggregateLoadAsync(dlc).ConfigureAwait(false);
                    //    }
                    //    return i;
                    //})) {
                    //    yield return item;
                    //}
                    var yieldedItems = 0;
                    var rowSkip = args.Linear ? 0 : (args.RowSkip ?? 0);
                    await foreach (var item in BuildAggregateListDirectCoroutinely<T>(transaction, command, join, 0).ConfigureAwait(false)) {
                        if (implementsAfterLoad) {
                            ((IBusinessObject)item).OnAfterLoad(dlc);
                        }
                        if (implementsAfterAggregateLoad) {
                            await ((IBusinessObject<T>)item).OnAfterAggregateLoadAsync(dlc).ConfigureAwait(false);
                        }
                        if(rowSkip > 0) {
                            rowSkip--;
                            continue;
                        }
                        yield return item;
                        yieldedItems++;
                        if(!args.Linear && args.RowLimit.HasValue && yieldedItems >= args.RowLimit) {
                            break;
                        }
                    }
                    transaction?.Benchmarker?.Mark($"Finished building the result AggregateListDirect<{typeof(T).Name}> ({query.Id})");
                }
            } else {
                WriteLog(args.Conditions?.ToString());
                await foreach (var item in FetchAsync<T>(transaction, args).ConfigureAwait(false)) {
                    yield return item;
                }
            }
        }

        public async Task<List<T>> AggregateLoadAsync<T>
            (BDadosTransaction transaction,
            LoadAllArgs<T> args = null) where T : IDataObject, new() {
            List<T> retv = new List<T>();

            await foreach (var item in AggregateLoadAsyncCoroutinely(transaction, args).ConfigureAwait(false)) {
                retv.Add(item);
            }

            var dlc = new DataLoadContext {
                DataAccessor = this,
                IsAggregateLoad = true,
                ContextTransferObject = args?.ContextObject ?? transaction?.ContextTransferObject
            };
            if (retv.Count > 0 && CacheImplementsAfterListAggregateLoad[typeof(T)]) {
                await ((IBusinessObject<T>)retv.First()).OnAfterListAggregateLoadAsync(dlc, retv).ConfigureAwait(false);
            }

            return retv;
        }

        public List<T> AggregateLoad<T>
            (BDadosTransaction transaction,
            LoadAllArgs<T> args = null) where T : IDataObject, new() {
            return AggregateLoadAsync(transaction, args)
                .ConfigureAwait(false)
                .GetAwaiter()
                .GetResult();
        }

        public T LoadFirstOrDefault<T>(BDadosTransaction transaction, LoadAllArgs<T> args = null) where T : IDataObject, new() {
            transaction.Step().ConfigureAwait(false).GetAwaiter().GetResult();
            return LoadAll<T>(transaction, args).FirstOrDefault();
        }

        public IAsyncEnumerable<T> FetchAsync<T>(BDadosTransaction transaction, LoadAllArgs<T> args = null) where T : IDataObject, new() {
            transaction.Step().ConfigureAwait(false).GetAwaiter().GetResult();
            var cndParse = new ConditionParser();
            var cnd = cndParse.ParseExpression(args?.Conditions);
            return FetchAsync<T>(transaction, cnd, args.RowSkip, args.RowLimit, args.OrderingMember, args.OrderingType, args.ContextObject);
        }
        public IEnumerable<T> Fetch<T>(BDadosTransaction transaction, LoadAllArgs<T> args = null) where T : IDataObject, new() {
            transaction.Step().ConfigureAwait(false).GetAwaiter().GetResult();
            var cndParse = new ConditionParser();
            var cnd = cndParse.ParseExpression(args?.Conditions);
            return Fetch<T>(transaction, cnd, args?.RowSkip, args?.RowLimit, args?.OrderingMember, args?.OrderingType ?? OrderingType.Asc, args?.ContextObject);
        }

        public int DefaultQueryLimit { get; set; } = 50;

        public async IAsyncEnumerable<T> FetchAsync<T>(BDadosTransaction transaction, IQueryBuilder conditions, int? skip, int? limit, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc, object transferObject = null) where T : IDataObject, new() {
            await transaction.Step().ConfigureAwait(false);
            if (limit < 0) {
                limit = DefaultQueryLimit;
            }
            if (transaction.CancellationToken.IsCancellationRequested) {
                throw new OperationCanceledException("The transaction was cancelled");
            }
            if (conditions == null) {
                conditions = Qb.Fmt("TRUE");
            }

            if (transaction == null) {
                throw new BDadosException("Fatal inconsistency error: FetchAsync<T> Expects a functional initialized ConnectionInfo object.");
            }

            transaction.Benchmarker?.Mark("--");

            transaction.Benchmarker?.Mark("Data Load ---");
            MemberInfo ordMember = GetOrderingMember<T>(orderingMember);
            transaction.Benchmarker?.Mark($"Generate SELECT<{typeof(T).Name}>");
            var query = Plugin.QueryGenerator.GenerateSelect<T>(conditions, skip, limit, ordMember, ordering);
            transaction.Benchmarker?.Mark($"Execute SELECT<{typeof(T).Name}>");
            await transaction.Step().ConfigureAwait(false);
            if (query == null || query.GetCommandText() == null) {
                yield break;
            }
            Stopwatch sw = Stopwatch.StartNew();
            await using (var command = (DbCommand) await transaction.CreateCommand().ConfigureAwait(false)) {
                command.CommandTimeout = Plugin.CommandTimeout;
                VerboseLogQueryParameterization(transaction, query);
                query.ApplyToCommand(command, Plugin.ProcessParameterValue);
                DbDataReader reader = null;
                try {
                    transaction?.Benchmarker?.Mark($"[{Description}:{transaction.Id}] Wait for locked region");
                    using (await transaction.Lock().ConfigureAwait(false)) {
                        transaction?.Benchmarker?.Mark($"[{Description}:{transaction.Id}] Execute Query <{query.Id}>");
                        await command.PrepareAsync().ConfigureAwait(false);
                        reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, transaction.CancellationToken).ConfigureAwait(false);
                    }
                    transaction?.Benchmarker?.Mark($"[{Description}:{transaction.Id}] Query <{query.Id}> executed OK");

                } catch (Exception x) {
                    sw.Stop();
                    transaction?.Benchmarker?.Mark($"Error executing query: {x.Message}\r\n\tQuery: {query.GetCommandText()}");
                    WriteLog($"[{Description}:{transaction.Id}] -------- Error: {x.Message} ([{sw.ElapsedMilliseconds} ms]");
                    WriteLog(x.Message);
                    WriteLog(x.StackTrace);
                    try {
                        var ret = reader?.DisposeAsync();
                        if (ret is ValueTask t) {
                            await t.ConfigureAwait(false);
                        }
                    } catch (Exception) {
                        // empty catch
                    }
                    transaction?.MarkAsErrored();
                    throw new BDadosException("Error loading data", x);
                } finally {
                    WriteLog("------------------------------------");
                }
                transaction?.Benchmarker?.Mark($"[{Description}:{transaction.Id}] Reader executed OK <{query.Id}>");
                await using (reader) {
                    var cols = new string[reader.FieldCount];
                    for (int i = 0; i < cols.Length; i++)
                        cols[i] = reader.GetName(i);
                    transaction?.Benchmarker?.Mark($"[{Description}:{transaction.Id}] Build retv List<{typeof(T).Name}> ({query.Id})");

                    var existingKeys = new MemberInfo[reader.FieldCount];
                    for (int i = 0; i < reader.FieldCount; i++) {
                        var name = cols[i];
                        if (name != null) {
                            var m = ReflectionTool.GetMember(typeof(T), name);
                            if (m != null) {
                                existingKeys[i] = m;
                            }
                        }
                    }
                    int c = 0;
                    var swBuild = Stopwatch.StartNew();
                    while (await reader.ReadAsync(transaction.CancellationToken).ConfigureAwait(false)) {
                        object[] values = new object[reader.FieldCount];
                        for (int i = 0; i < reader.FieldCount; i++) {
                            values[i] = reader.GetValue(i);
                        }
                        T obj = new T();
                        for (int i = 0; i < existingKeys.Length; i++) {
                            try {
                                if (existingKeys[i] != null) {
                                    ReflectionTool.SetMemberValue(existingKeys[i], obj, Fi.Tech.ProperMapValue(values[i]));
                                }
                            } catch (Exception x) {
                                throw x;
                            }
                        }
                        RunAfterLoad(obj, false, transferObject ?? transaction?.ContextTransferObject);
                        yield return (obj);
                        c++;
                    }
                    sw.Stop();
                    swBuild.Stop();
                    double elaps = sw.ElapsedMilliseconds;
                    transaction?.Benchmarker?.Mark($"[{Description}:{transaction.Id}] Build retv List<{typeof(T).Name}> ({query.Id}) completed");

                    WriteLog($"[{Description}:{transaction.Id}] -------- <{query.Id}> FetchAsync [OK] ({c} results) [{elaps} ms] [{swBuild.ElapsedMilliseconds}ms build]");
                }
            }
        }
        public IEnumerable<T> Fetch<T>(BDadosTransaction transaction, IQueryBuilder conditions, int? skip, int? limit, Expression<Func<T, object>> orderingMember = null, OrderingType ordering = OrderingType.Asc, object transferObject = null) where T : IDataObject, new() {
            transaction.Step().ConfigureAwait(false).GetAwaiter().GetResult();
            if (limit < 0) {
                limit = DefaultQueryLimit;
            }
            if (transaction.CancellationToken.IsCancellationRequested) {
                throw new OperationCanceledException("The transaction was cancelled");
            }
            if (conditions == null) {
                conditions = Qb.Fmt("TRUE");
            }

            if (transaction == null) {
                throw new BDadosException("Fatal inconsistency error: FetchAsync<T> Expects a functional initialized ConnectionInfo object.");
            }

            transaction.Benchmarker?.Mark("--");

            transaction.Benchmarker?.Mark("Data Load ---");
            MemberInfo ordMember = GetOrderingMember<T>(orderingMember);
            transaction.Benchmarker?.Mark($"Generate SELECT<{typeof(T).Name}>");
            var query = Plugin.QueryGenerator.GenerateSelect<T>(conditions, skip, limit, ordMember, ordering);
            transaction.Benchmarker?.Mark($"Execute SELECT<{typeof(T).Name}>");
            transaction.Step().ConfigureAwait(false).GetAwaiter().GetResult();
            if (query == null || query.GetCommandText() == null) {
                yield break;
            }
            Stopwatch sw = Stopwatch.StartNew();
            using (var command = transaction.CreateCommand().ConfigureAwait(false).GetAwaiter().GetResult()) {
                command.CommandTimeout = Plugin.CommandTimeout;
                VerboseLogQueryParameterization(transaction, query);
                query.ApplyToCommand(command, Plugin.ProcessParameterValue);
                IDataReader reader = null;
                try {
                    transaction?.Benchmarker?.Mark($"[{Description}:{transaction.Id}] Wait for locked region");
                    using (transaction.Lock().ConfigureAwait(false).GetAwaiter().GetResult()) {
                        transaction?.Benchmarker?.Mark($"[{Description}:{transaction.Id}] Execute Query <{query.Id}>");
                        command.Prepare();
                        reader = command.ExecuteReader(CommandBehavior.SequentialAccess);
                    }
                    transaction?.Benchmarker?.Mark($"[{Description}:{transaction.Id}] Query <{query.Id}> executed OK");

                } catch (Exception x) {
                    sw.Stop();
                    transaction?.Benchmarker?.Mark($"Error executing query: {x.Message}\r\n\tQuery: {query.GetCommandText()}");
                    WriteLog($"[{Description}:{transaction.Id}] -------- Error: {x.Message} ([{sw.ElapsedMilliseconds} ms]");
                    WriteLog(x.Message);
                    WriteLog(x.StackTrace);
                    try {
                        reader?.Dispose();
                    } catch (Exception) {
                        // empty catch
                    }
                    transaction?.MarkAsErrored();
                    throw new BDadosException("Error loading data", x);
                } finally {
                    WriteLog("------------------------------------");
                }
                transaction?.Benchmarker?.Mark($"[{Description}:{transaction.Id}] Reader executed OK <{query.Id}>");
                using (reader) {
                    var cols = new string[reader.FieldCount];
                    for (int i = 0; i < cols.Length; i++)
                        cols[i] = reader.GetName(i);
                    transaction?.Benchmarker?.Mark($"[{Description}:{transaction.Id}] Build retv List<{typeof(T).Name}> ({query.Id})");

                    var existingKeys = new MemberInfo[reader.FieldCount];
                    for (int i = 0; i < reader.FieldCount; i++) {
                        var name = cols[i];
                        if (name != null) {
                            var m = ReflectionTool.GetMember(typeof(T), name);
                            if (m != null) {
                                existingKeys[i] = m;
                            }
                        }
                    }
                    int c = 0;
                    var swBuild = Stopwatch.StartNew();
                    while (reader.Read()) {
                        object[] values = new object[reader.FieldCount];
                        for (int i = 0; i < reader.FieldCount; i++) {
                            values[i] = reader.GetValue(i);
                        }
                        T obj = new T();
                        for (int i = 0; i < existingKeys.Length; i++) {
                            try {
                                if (existingKeys[i] != null) {
                                    ReflectionTool.SetMemberValue(existingKeys[i], obj, Fi.Tech.ProperMapValue(values[i]));
                                }
                            } catch (Exception x) {
                                throw x;
                            }
                        }
                        RunAfterLoad(obj, false, transferObject ?? transaction?.ContextTransferObject);
                        yield return (obj);
                        c++;
                    }
                    sw.Stop();
                    swBuild.Stop();
                    double elaps = sw.ElapsedMilliseconds;
                    transaction?.Benchmarker?.Mark($"[{Description}:{transaction.Id}] Build retv List<{typeof(T).Name}> ({query.Id}) completed");

                    WriteLog($"[{Description}:{transaction.Id}] -------- <{query.Id}> FetchAsync [OK] ({c} results) [{elaps} ms] [{swBuild.ElapsedMilliseconds}ms build]");
                }
            }
        }

        public void QueryReader(BDadosTransaction transaction, IQueryBuilder query, Action<IDataReader> actionRead) {
            QueryReader<int>(transaction, query, (reader) => { actionRead(reader); return 0; });
        }
        public async Task<T> QueryReaderAsync<T>(BDadosTransaction transaction, IQueryBuilder query, Func<IDataReader, Task<T>> actionRead) {
            await transaction.Step().ConfigureAwait(false);
            if (query == null || query.GetCommandText() == null) {
                return default(T);
            }
            DateTime Inicio = DateTime.Now;
            DataTable retv = new DataTable();
            await using (var command = (DbCommand) await transaction.CreateCommand().ConfigureAwait(false)) {
                VerboseLogQueryParameterization(transaction, query);
                query.ApplyToCommand(command, Plugin.ProcessParameterValue);
                // --
                transaction?.Benchmarker?.Mark($"[{Description}:{transaction.Id}] Build Dataset");
                using (await transaction.Lock().ConfigureAwait(false)) {
                    if (command is DbCommand acom) {
                        await acom.PrepareAsync().ConfigureAwait(false);
                        var dataReader = await acom.ExecuteReaderAsync(CommandBehavior.SequentialAccess, transaction.CancellationToken).ConfigureAwait(false);

                        await using (dataReader) {
                            return await actionRead(dataReader).ConfigureAwait(false);
                        }
                    } else {
                        command.Prepare();
                        var dataReader = command.ExecuteReader(CommandBehavior.SequentialAccess);
                        using (dataReader) {
                            return await actionRead(dataReader).ConfigureAwait(false);
                        }
                    }

                }
            }
        }

        public T QueryReader<T>(BDadosTransaction transaction, IQueryBuilder query, Func<IDataReader, T> actionRead) {
            return QueryReaderAsync<T>(transaction, query, async (dr) => {
                    return await Task.Run(()=> actionRead(dr)).ConfigureAwait(false);
                })
                .ConfigureAwait(false)
                .GetAwaiter()
                .GetResult();
        }

        public DataTable Query(BDadosTransaction transaction, IQueryBuilder query) {
            transaction.Step().ConfigureAwait(false).GetAwaiter().GetResult();
            if (query == null || query.GetCommandText() == null) {
                return new DataTable();
            }
            DateTime Inicio = DateTime.Now;
            DataTable retv = new DataTable();
            using (var command = transaction.CreateCommand().ConfigureAwait(false).GetAwaiter().GetResult()) {
                VerboseLogQueryParameterization(transaction, query);
                query.ApplyToCommand(command, Plugin.ProcessParameterValue);
                // --
                transaction?.Benchmarker?.Mark($"[{Description}:{transaction.Id}] Build Dataset");
                DataSet ds;
                lock (transaction) {
                    ds = GetDataSet(command);
                }
                var elaps = transaction?.Benchmarker?.Mark($"[{Description}:{transaction.Id}] --");

                try {
                    int resultados = 0;
                    if (ds.Tables.Count < 1) {
                        transaction?.MarkAsErrored();
                        throw new BDadosException("Database did not return any table.");
                    }
                    resultados = ds.Tables[0].Rows.Count;
                    transaction?.Benchmarker?.Mark($"[{Description}:{transaction.Id}] -------- Queried [OK] ({resultados} results) [{elaps} ms]");
                    return ds.Tables[0];
                } catch (Exception x) {
                    transaction?.Benchmarker?.Mark($"[{Description}:{transaction.Id}] -------- Error: {x.Message} ([{DateTime.Now.Subtract(Inicio).TotalMilliseconds} ms]");
                    WriteLog(x.Message);
                    WriteLog(x.StackTrace);
                    var ex = new BDadosException("Error executing Query", x);
                    ex.Data["Query"] = query;
                    transaction?.MarkAsErrored();
                    throw ex;
                } finally {
                    WriteLog("------------------------------------");
                }
            }
        }

        public async Task<int> ExecuteAsync(BDadosTransaction transaction, IQueryBuilder query) {
            await transaction.Step().ConfigureAwait(false);
            if (query == null)
                return 0;
            if (transaction.CancellationToken.IsCancellationRequested) {
                throw new OperationCanceledException("The transaction was cancelled");
            }
            int result = -1;
            transaction.Benchmarker?.Mark($"[{Description}:{transaction.Id}] Prepare statement");
            transaction.Benchmarker?.Mark("--");
            WriteLog($"[{Description}:{transaction.Id}] -- Execute Statement <{query.Id}> [{Plugin.CommandTimeout}s timeout]");
            using (var command = await transaction.CreateCommand(true).ConfigureAwait(false)) {
                try {
                    VerboseLogQueryParameterization(transaction, query);
                    query.ApplyToCommand(command, Plugin.ProcessParameterValue);
                    transaction.Benchmarker?.Mark($"[{Description}:{transaction.Id}] Execute");
                    using (await transaction.Lock().ConfigureAwait(false)) {
                        if (command is DbCommand acom) {
                            await acom.PrepareAsync().ConfigureAwait(false);
                            result = await acom.ExecuteNonQueryAsync(transaction.CancellationToken).ConfigureAwait(false);
                        } else {
                            command.Prepare();
                            result = command.ExecuteNonQuery();
                        }
                    }
                    var elaps = transaction.Benchmarker?.Mark("--");
                    transaction.NotifyWriteOperation();
                    WriteLog($"[{Description}:{transaction.Id}] --------- Executed [OK] ({result} lines affected) [{elaps} ms]");
                } catch (Exception x) {
                    var elapsed = transaction?.Benchmarker?.Mark($"Error executing query: {x.Message}\r\n\tQuery: {query.GetCommandText()}");
                    WriteLog($"[{Description}:{transaction.Id}] -------- Error: {x.Message} ([{elapsed} ms]");
                    WriteLog(x.Message);
                    WriteLog(x.StackTrace);
                    WriteLog($"BDados Execute: {x.Message}");
                    transaction?.MarkAsErrored();
                    throw new BDadosException("Error Executing Statement", x) {
                        Data = {
                            ["Query"] = query
                        }
                    };
                } finally {
                    WriteLog("------------------------------------");
                }
            }
            return result;
        }


        public int Execute(BDadosTransaction transaction, IQueryBuilder query) {
            return ExecuteAsync(transaction, query).ConfigureAwait(false).GetAwaiter().GetResult();
        }

        #endregion

    }
}