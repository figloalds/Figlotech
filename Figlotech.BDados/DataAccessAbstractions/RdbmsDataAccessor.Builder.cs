using Figlotech.BDados.Business;
using Figlotech.Core;
using Figlotech.Core.Extensions;
using Figlotech.Core.Helpers;
using Figlotech.Core.Interfaces;
using System;
using System.Collections.Concurrent;
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
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.BDados.DataAccessAbstractions {

    public partial class RdbmsDataAccessor : IRdbmsDataAccessor, IDisposable, IAsyncDisposable {
        public List<T> GetObjectList<T>(BDadosTransaction transaction, IDbCommand command) where T : new() {
            transaction?.Benchmarker.Mark("Enter lock command");
            using var handle = transaction.Lock();
            transaction?.Benchmarker.Mark("- Starting Execute Query");
            using (var reader = command.ExecuteReader(CommandBehavior.SingleResult | CommandBehavior.SequentialAccess | CommandBehavior.KeyInfo)) {
                transaction?.Benchmarker.Mark("- Starting build");
                return Fi.Tech.MapFromReader<T>(reader).ToList();
            }
        }

        readonly FiAsyncMultiLock _lock = new FiAsyncMultiLock();
        public async Task<List<T>> GetObjectListAsync<T>(BDadosTransaction transaction, DbCommand command) where T : new() {
            transaction?.Benchmarker.Mark("Enter lock command");
            await using var handle = await transaction.LockAsync().ConfigureAwait(false);
            transaction?.Benchmarker.Mark("- Starting Execute Query");
            using (var reader = await command.ExecuteReaderAsync(CommandBehavior.SingleResult | CommandBehavior.SequentialAccess | CommandBehavior.KeyInfo, transaction.CancellationToken).ConfigureAwait(false)) {
                transaction?.Benchmarker.Mark("- Starting build");
                return await Fi.Tech.MapFromReaderAsync<T>(reader, transaction.CancellationToken).ToListAsync().ConfigureAwait(false);
            }
        }

        public async IAsyncEnumerable<T> GetObjectEnumerableAsync<T>(BDadosTransaction transaction, DbCommand command, [EnumeratorCancellation] CancellationToken cancellationToken = default) where T : new() {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(transaction.CancellationToken, cancellationToken);
            var combinedToken = cts.Token;
            transaction?.Benchmarker.Mark("Enter lock command");
            await using var handle = await transaction.LockAsync().ConfigureAwait(false);
            transaction?.Benchmarker.Mark("- Starting Execute Query");
            using (var reader = await command.ExecuteReaderAsync(CommandBehavior.SingleResult | CommandBehavior.SequentialAccess | CommandBehavior.KeyInfo, combinedToken).ConfigureAwait(false)) {
                transaction?.Benchmarker.Mark("- Starting build");
                await foreach (var item in Fi.Tech.MapFromReaderAsync<T>(reader, combinedToken).ConfigureAwait(false)) {
                    yield return item;
                }
            }
        }

        public async Task GetJsonStringFromQueryAsync<T>(BDadosTransaction transaction, DbCommand command, TextWriter writer) where T : new() {
            transaction?.Benchmarker.Mark("Enter lock command");
            transaction?.Benchmarker.Mark("- Starting Execute Query");
            using (var reader = await command.ExecuteReaderAsync(CommandBehavior.SingleResult | CommandBehavior.SequentialAccess | CommandBehavior.KeyInfo, transaction.CancellationToken).ConfigureAwait(false)) {
                transaction?.Benchmarker.Mark("- Starting build");

                // Precompute per-ordinal bindings: whether the column maps to a T member,
                // its name (JSON property name), and its reader-side type for typed access.
                var bindings = new (bool mapped, string name, Type fieldType)[reader.FieldCount];
                for (int i = 0; i < reader.FieldCount; i++) {
                    var name = reader.GetName(i);
                    if (name != null && ReflectionTool.DoesTypeHaveFieldOrProperty(typeof(T), name)) {
                        bindings[i] = (true, name, reader.GetFieldType(i) ?? typeof(object));
                    } else {
                        bindings[i] = (false, null, null);
                    }
                }

                // Utf8JsonWriter emits UTF-8 bytes; bridge them to the caller\'s TextWriter
                // via a small Stream adapter that decodes and forwards incrementally.
                // This eliminates per-row StringBuilder + string allocations and the
                // per-field JsonConvert.SerializeObject reflection cost.
                using var bridge = new TextWriterUtf8Stream(writer);
                var jsonOpts = new System.Text.Json.JsonWriterOptions { Indented = false };
                using var json = new System.Text.Json.Utf8JsonWriter(bridge, jsonOpts);
                json.WriteStartArray();
                while (await reader.ReadAsync(transaction.CancellationToken).ConfigureAwait(false)) {
                    transaction.CancellationToken.ThrowIfCancellationRequested();
                    json.WriteStartObject();
                    for (int i = 0; i < bindings.Length; i++) {
                        var b = bindings[i];
                        if (!b.mapped)
                            continue;
                        if (reader.IsDBNull(i)) {
                            json.WriteNull(b.name);
                            continue;
                        }
                        WriteJsonValue(json, reader, i, b.name, b.fieldType);
                    }
                    json.WriteEndObject();
                }
                json.WriteEndArray();
                await json.FlushAsync(transaction.CancellationToken).ConfigureAwait(false);
            }
        }

        private static void WriteJsonValue(System.Text.Json.Utf8JsonWriter w, DbDataReader reader, int ordinal, string name, Type fieldType) {
            // Typed fast paths avoid boxing and JsonConvert reflection. Order: common DB types first.
            if (fieldType == typeof(string)) {
                w.WriteString(name, reader.GetString(ordinal));
            } else if (fieldType == typeof(int)) {
                w.WriteNumber(name, reader.GetInt32(ordinal));
            } else if (fieldType == typeof(long)) {
                w.WriteNumber(name, reader.GetInt64(ordinal));
            } else if (fieldType == typeof(decimal)) {
                w.WriteNumber(name, reader.GetDecimal(ordinal));
            } else if (fieldType == typeof(double)) {
                w.WriteNumber(name, reader.GetDouble(ordinal));
            } else if (fieldType == typeof(float)) {
                w.WriteNumber(name, reader.GetFloat(ordinal));
            } else if (fieldType == typeof(bool)) {
                w.WriteBoolean(name, reader.GetBoolean(ordinal));
            } else if (fieldType == typeof(DateTime)) {
                w.WriteString(name, reader.GetDateTime(ordinal));
            } else if (fieldType == typeof(DateTimeOffset)) {
                w.WriteString(name, (DateTimeOffset)reader.GetValue(ordinal));
            } else if (fieldType == typeof(Guid)) {
                w.WriteString(name, reader.GetGuid(ordinal));
            } else if (fieldType == typeof(short)) {
                w.WriteNumber(name, reader.GetInt16(ordinal));
            } else if (fieldType == typeof(byte)) {
                w.WriteNumber(name, reader.GetByte(ordinal));
            } else {
                // Fallback for uncommon types: boxed object dispatch.
                var value = reader.GetValue(ordinal);
                switch (value) {
                    case null: w.WriteNull(name); break;
                    case string s: w.WriteString(name, s); break;
                    case int ii: w.WriteNumber(name, ii); break;
                    case long ll: w.WriteNumber(name, ll); break;
                    case decimal dd: w.WriteNumber(name, dd); break;
                    case double db: w.WriteNumber(name, db); break;
                    case float f: w.WriteNumber(name, f); break;
                    case bool bb: w.WriteBoolean(name, bb); break;
                    case DateTime dt: w.WriteString(name, dt); break;
                    case DateTimeOffset dto: w.WriteString(name, dto); break;
                    case Guid g: w.WriteString(name, g); break;
                    default: w.WriteString(name, value.ToString()); break;
                }
            }
        }

        public DataSet GetDataSet(BDadosTransaction transaction, IDbCommand command) {
            using var handle = transaction.Lock();
            using (var reader = command.ExecuteReader()) {
                DataTable dt = new DataTable();
                int fieldCount = reader.FieldCount;
                var fieldTypes = new Type[fieldCount];
                for (int i = 0; i < fieldCount; i++) {
                    fieldTypes[i] = reader.GetFieldType(i) ?? typeof(object);
                    dt.Columns.Add(new DataColumn(reader.GetName(i), fieldTypes[i]));
                }

                while (reader.Read()) {
                    var dr = dt.NewRow();
                    for (int i = 0; i < fieldCount; i++) {
                        if (reader.IsDBNull(i)) {
                            dr[i] = DBNull.Value;
                        } else {
                            var val = reader.GetValue(i);
                            var type = fieldTypes[i];
                            dr[i] = type.IsInstanceOfType(val) ? val : Convert.ChangeType(val, type);
                        }
                    }
                    dt.Rows.Add(dr);
                }

                var ds = new DataSet();
                ds.Tables.Add(dt);
                return ds;
            }
        }

        public (Type, object) cacheId(IDataReader reader, string myRidCol, Type t) {
            var rid = ReflectionTool.DbDeNull(reader[myRidCol]);
            return (t, rid ?? throw new InvalidDataException($"Column '{myRidCol}' returned NULL for the record identifier; cannot build a cache/grouping key."));
        }
        public (Type, object) cacheId(object[] reader, int myRidCol, Type t) {
            var rid = ReflectionTool.DbDeNull(reader[myRidCol]);
            return (t, rid ?? throw new InvalidDataException($"Column '{myRidCol}' returned NULL for the record identifier; cannot build a cache/grouping key."));
        }

        public (int, Type, object) cacheId(IDataReader reader, string myRidCol, Type t, int tableIndex) {
            var rid = ReflectionTool.DbDeNull(reader[myRidCol]);
            return (tableIndex, t, rid ?? throw new InvalidDataException($"Column '{myRidCol}' returned NULL for the record identifier; cannot build a cache/grouping key."));
        }
        public (int, Type, object) cacheId(object[] reader, int myRidCol, Type t, int tableIndex) {
            var rid = ReflectionTool.DbDeNull(reader[myRidCol]);
            return (tableIndex, t, rid ?? throw new InvalidDataException($"Column '{myRidCol}' returned NULL for the record identifier; cannot build a cache/grouping key."));
        }

        public async IAsyncEnumerable<T> BuildAggregateListDirectCoroutinely<T>(BDadosTransaction transaction, DbCommand command, DefinitiveJoinPlan plan, CompiledAggregateMaterializerPlan materializer, [EnumeratorCancellation] CancellationToken cancellationToken = default) where T : IDataObject, new() {
            ValidateFrozenAggregateInputs<T>(command, plan, materializer);
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(transaction?.CancellationToken ?? CancellationToken.None, cancellationToken);
            CancellationToken combinedToken = cts.Token;
            combinedToken.ThrowIfCancellationRequested();

            transaction?.Benchmarker?.Mark($"Executing query for AggregateListDirect<{typeof(T).Name}>");
            using (DbDataReader reader = await command.ExecuteReaderAsync(CommandBehavior.SingleResult | CommandBehavior.KeyInfo, combinedToken).ConfigureAwait(false)) {
                ReaderSchemaValidator.Validate(reader, plan);
                var context = materializer.CreateContext();
                object currentIdentifier = null;
                T currentRoot = default(T);
                int row = 0;
                int objects = 0;
                var values = new object[materializer.ProjectionLength];
                while (await reader.ReadAsync(combinedToken).ConfigureAwait(false)) {
                    combinedToken.ThrowIfCancellationRequested();
                    reader.GetValues(values);
                    object identifier = materializer.ReadRootIdentifier(values, row);
                    if (identifier == null) {
                        row++;
                        continue;
                    }

                    if (currentRoot != null && !Equals(currentIdentifier, identifier)) {
                        objects++;
                        yield return currentRoot;
                        context = materializer.CreateContext();
                        currentRoot = default(T);
                    }

                    object root = materializer.GetOrCreateRoot(identifier, context, out bool isNew);
                    currentRoot = (T)root;
                    currentIdentifier = identifier;
                    materializer.PopulateRoot(values, root, isNew, context);
                    row++;
                }
                if (currentRoot != null) {
                    objects++;
                    yield return currentRoot;
                }
                transaction?.Benchmarker?.Mark($"[{transaction.Id}] Built List Size: {objects} / {row} rows");
            }
        }

        [Obsolete("Legacy JoinDefinition execution is not safe for execution. Freeze to DefinitiveJoinPlan and call the frozen overload.")]
        public async IAsyncEnumerable<T> BuildAggregateListDirectCoroutinely<T>(BDadosTransaction transaction, DbCommand command, JoinDefinition join, int thisIndex, [EnumeratorCancellation] CancellationToken cancellationToken = default) where T : IDataObject, new() {
            DefinitiveJoinPlan plan = join.Freeze(typeof(T), AggregateJoinShape.FullGraph);
            ValidateLegacyRootIndex(plan, thisIndex);
            CompiledAggregateMaterializerPlan materializer = CompiledAggregateMaterializerPlan.GetOrCreate(plan);
            await foreach (T item in BuildAggregateListDirectCoroutinely<T>(transaction, command, plan, materializer, cancellationToken).ConfigureAwait(false)) {
                yield return item;
            }
        }

        public async Task<List<T>> BuildAggregateListDirectAsync<T>(BDadosTransaction transaction, DbCommand command, DefinitiveJoinPlan plan, CompiledAggregateMaterializerPlan materializer, object overrideContext) where T : IDataObject, new() {
            ValidateFrozenAggregateInputs<T>(command, plan, materializer);
            CancellationToken token = transaction?.CancellationToken ?? CancellationToken.None;
            var retv = new List<T>();
            var dlc = new DataLoadContext {
                DataAccessor = this,
                IsAggregateLoad = true,
                Transaction = transaction,
                ContextTransferObject = overrideContext ?? transaction?.ContextTransferObject
            };
            transaction?.Benchmarker?.Mark($"Executing query for AggregateListDirect<{typeof(T).Name}>");
            using (DbDataReader reader = await command.ExecuteReaderAsync(CommandBehavior.SingleResult | CommandBehavior.KeyInfo, token).ConfigureAwait(false)) {
                ReaderSchemaValidator.Validate(reader, plan);
                var context = materializer.CreateContext();
                var values = new object[materializer.ProjectionLength];
                int row = 0;
                while (await reader.ReadAsync(token).ConfigureAwait(false)) {
                    token.ThrowIfCancellationRequested();
                    reader.GetValues(values);
                    object identifier = materializer.ReadRootIdentifier(values, row);
                    if (identifier != null) {
                        object root = materializer.GetOrCreateRoot(identifier, context, out bool isNew);
                        if (isNew) {
                            retv.Add((T)root);
                        }
                        materializer.PopulateRoot(values, root, isNew, context);
                    }
                    row++;
                }
                transaction?.Benchmarker?.Mark($"[{transaction.Id}] Built List Size: {retv.Count} / {row} rows");
            }

            transaction?.Benchmarker?.Mark("Run afterloads");
            if (retv.Count > 0 && CacheImplementsAfterListAggregateLoad[typeof(T)]) {
                await ((IBusinessObject<T>)retv.First()).OnAfterListAggregateLoadAsync(dlc, retv).ConfigureAwait(false);
            }

            bool implementsAfterLoad = CacheImplementsAfterLoad[typeof(T)];
            bool implementsAfterAggregateLoad = CacheImplementsAfterAggregateLoad[typeof(T)];
            if (implementsAfterLoad || implementsAfterAggregateLoad) {
                using var afterLoads = new WorkQueuer("AfterLoads");
                var requests = new List<WorkJobExecutionRequest>();
                foreach (T item in retv) {
                    T captured = item;
                    requests.Add(afterLoads.Enqueue(new WorkJob(async () => {
                        if (implementsAfterAggregateLoad) {
                            await ((IBusinessObject<T>)captured).OnAfterAggregateLoadAsync(dlc).ConfigureAwait(false);
                        }
                        if (implementsAfterLoad) {
                            ((IBusinessObject)captured).OnAfterLoad(dlc);
                        }
                    })));
                }
                await afterLoads.Stop(true).ConfigureAwait(false);
                await Task.WhenAll(requests.Select(request => request.GetAwaiterInternal())).ConfigureAwait(false);
            }
            return retv;
        }

        [Obsolete("Legacy JoinDefinition execution is not safe for execution. Freeze to DefinitiveJoinPlan and call the frozen overload.")]
        public Task<List<T>> BuildAggregateListDirectAsync<T>(BDadosTransaction transaction, DbCommand command, JoinDefinition join, int thisIndex, object overrideContext) where T : IDataObject, new() {
            DefinitiveJoinPlan plan = join.Freeze(typeof(T), AggregateJoinShape.FullGraph);
            ValidateLegacyRootIndex(plan, thisIndex);
            return BuildAggregateListDirectAsync<T>(transaction, command, plan, CompiledAggregateMaterializerPlan.GetOrCreate(plan), overrideContext);
        }

        private readonly SelfInitializerDictionary<Type, bool> CacheImplementsAfterLoad = new SelfInitializerDictionary<Type, bool>(
            t => t.Implements(typeof(IBusinessObject)) && t.GetMethod(nameof(IBusinessObject.OnAfterLoad)).DeclaringType == t
        );
        private readonly SelfInitializerDictionary<Type, bool> CacheImplementsAfterAggregateLoad = new SelfInitializerDictionary<Type, bool>(
            t => t.Implements(typeof(IBusinessObject<>).MakeGenericType(t)) && t.GetMethod("OnAfterAggregateLoadAsync").DeclaringType == t
        );
        private readonly SelfInitializerDictionary<Type, bool> CacheImplementsAfterListAggregateLoad = new SelfInitializerDictionary<Type, bool>(
            t => t.Implements(typeof(IBusinessObject<>).MakeGenericType(t)) && t.GetMethod("OnAfterListAggregateLoadAsync").DeclaringType == t
        );

        public List<T> BuildAggregateListDirect<T>(BDadosTransaction transaction, IDbCommand command, DefinitiveJoinPlan plan, CompiledAggregateMaterializerPlan materializer, object overrideContext) where T : IDataObject, new() {
            ValidateFrozenAggregateInputs<T>(command, plan, materializer);
            var retv = new List<T>();
            var dlc = new DataLoadContext {
                DataAccessor = this,
                IsAggregateLoad = true,
                Transaction = transaction,
                ContextTransferObject = overrideContext ?? transaction?.ContextTransferObject
            };
            transaction?.Benchmarker?.Mark($"Executing query for AggregateListDirect<{typeof(T).Name}>");
            using (IDataReader reader = command.ExecuteReader(CommandBehavior.SingleResult | CommandBehavior.KeyInfo)) {
                ReaderSchemaValidator.Validate(reader, plan);
                var context = materializer.CreateContext();
                var values = new object[materializer.ProjectionLength];
                int row = 0;
                while (reader.Read()) {
                    transaction?.CancellationToken.ThrowIfCancellationRequested();
                    reader.GetValues(values);
                    object identifier = materializer.ReadRootIdentifier(values, row);
                    if (identifier != null) {
                        object root = materializer.GetOrCreateRoot(identifier, context, out bool isNew);
                        if (isNew) {
                            retv.Add((T)root);
                        }
                        materializer.PopulateRoot(values, root, isNew, context);
                    }
                    row++;
                }
                transaction?.Benchmarker?.Mark($"[{transaction.Id}] Built List Size: {retv.Count} / {row} rows");
            }

            transaction?.Benchmarker?.Mark("Run afterloads");
            if (retv.Count > 0 && CacheImplementsAfterListAggregateLoad[typeof(T)]) {
                ((IBusinessObject<T>)retv.First()).OnAfterListAggregateLoadAsync(dlc, retv).ConfigureAwait(false).GetAwaiter().GetResult();
            }
            if (CacheImplementsAfterAggregateLoad[typeof(T)]) {
                foreach (T item in retv) {
                    ((IBusinessObject<T>)item).OnAfterAggregateLoadAsync(dlc).ConfigureAwait(false).GetAwaiter().GetResult();
                }
            }
            if (CacheImplementsAfterLoad[typeof(T)]) {
                foreach (T item in retv) {
                    ((IBusinessObject)item).OnAfterLoad(dlc);
                }
            }
            return retv;
        }

        [Obsolete("Legacy JoinDefinition execution is not safe for execution. Freeze to DefinitiveJoinPlan and call the frozen overload.")]
        public List<T> BuildAggregateListDirect<T>(BDadosTransaction transaction, IDbCommand command, JoinDefinition join, int thisIndex, object overrideContext) where T : IDataObject, new() {
            DefinitiveJoinPlan plan = join.Freeze(typeof(T), AggregateJoinShape.FullGraph);
            ValidateLegacyRootIndex(plan, thisIndex);
            return BuildAggregateListDirect<T>(transaction, command, plan, CompiledAggregateMaterializerPlan.GetOrCreate(plan), overrideContext);
        }

        private static void ValidateFrozenAggregateInputs<T>(IDbCommand command, DefinitiveJoinPlan plan, CompiledAggregateMaterializerPlan materializer) where T : IDataObject, new() {
            if (plan == null) {
                throw new ArgumentNullException(nameof(plan));
            }
            if (materializer == null) {
                throw new ArgumentNullException(nameof(materializer));
            }
            materializer.ValidateSourcePlan(plan);
            materializer.ValidateRootType<T>();
            if (command == null) {
                throw new ArgumentNullException(nameof(command));
            }
        }

        private static void ValidateLegacyRootIndex(DefinitiveJoinPlan plan, int thisIndex) {
            if (thisIndex != plan.RootTableIndex) {
                throw new ArgumentOutOfRangeException(nameof(thisIndex), thisIndex, $"Legacy aggregate builder root index must match frozen plan root table index {plan.RootTableIndex}.");
            }
        }

        private static readonly ConcurrentDictionary<Type, (Func<object> Constructor, Action<object, object>[] Setters)> _stateUpdateMetadataCache = new ConcurrentDictionary<Type, (Func<object>, Action<object, object>[])>();

        private static Func<object> BuildStateUpdateConstructor(Type type) {
            NewExpression constructor = Expression.New(type);
            return Expression.Lambda<Func<object>>(Expression.Convert(constructor, typeof(object))).Compile();
        }

        private static Action<object, object> BuildStateUpdateSetter(Type ownerType, string memberName) {
            MemberInfo member = ReflectionTool.GetMember(ownerType, memberName);
            if (member == null
                || member is PropertyInfo property && property.SetMethod == null
                || member is FieldInfo field && field.IsInitOnly) {
                return null;
            }

            Type memberType = member is PropertyInfo memberProperty
                ? memberProperty.PropertyType
                : ((FieldInfo)member).FieldType;
            ParameterExpression target = Expression.Parameter(typeof(object), "target");
            ParameterExpression value = Expression.Parameter(typeof(object), "value");
            UnaryExpression typedTarget = Expression.Convert(target, ownerType);
            MemberExpression memberAccess = member is PropertyInfo writableProperty
                ? Expression.Property(typedTarget, writableProperty)
                : Expression.Field(typedTarget, (FieldInfo)member);
            Expression convertedValue = ReflectionTool.BuildObjectToTargetConversionExpression(value, memberType);
            if (convertedValue.Type != memberType) {
                convertedValue = Expression.Convert(convertedValue, memberType);
            }

            BinaryExpression assignment = Expression.Assign(memberAccess, convertedValue);
            Expression body = assignment;
            if (memberType.IsValueType && Nullable.GetUnderlyingType(memberType) == null) {
                MemberExpression dbNull = Expression.Field(null, typeof(DBNull).GetField(nameof(DBNull.Value)));
                BinaryExpression isNull = Expression.Equal(value, Expression.Constant(null));
                BinaryExpression isDbNull = Expression.ReferenceEqual(value, dbNull);
                body = Expression.IfThen(Expression.Not(Expression.OrElse(isNull, isDbNull)), assignment);
            }

            return Expression.Lambda<Action<object, object>>(body, target, value).Compile();
        }

        private List<ILegacyDataObject> BuildStateUpdateQueryResult(BDadosTransaction transaction, IDataReader reader, List<Type> workingTypes, Dictionary<Type, MemberInfo[]> fields) {
            var retv = new List<ILegacyDataObject>();
            transaction?.Benchmarker?.Mark("Init Build Result");

            var typeByName = new Dictionary<string, Type>(StringComparer.Ordinal);
            var constructors = new Dictionary<Type, Func<object>>();
            var setters = new Dictionary<Type, Action<object, object>[]>();
            var dataOrdinals = new Dictionary<Type, int[]>();

            foreach (var type in workingTypes) {
                typeByName[type.Name] = type;
                var metadata = _stateUpdateMetadataCache.GetOrAdd(type, t => {
                    var typeFields = fields[t];
                    var typeSetters = new Action<object, object>[typeFields.Length];
                    for (int i = 0; i < typeFields.Length; i++) {
                        typeSetters[i] = BuildStateUpdateSetter(t, typeFields[i].Name);
                    }
                    return (BuildStateUpdateConstructor(t), typeSetters);
                });
                constructors[type] = metadata.Constructor;
                setters[type] = metadata.Setters;
                var typeFieldsLocal = fields[type];
                var ordinals = new int[typeFieldsLocal.Length];
                for (int i = 0; i < typeFieldsLocal.Length; i++) {
                    ordinals[i] = reader.GetOrdinal($"data_{i}");
                }
                dataOrdinals[type] = ordinals;
            }

            var typeNameOrdinal = reader.GetOrdinal("TypeName");

            while (reader.Read()) {
                var typename = reader.GetValue(typeNameOrdinal) as string;
                if (string.IsNullOrEmpty(typename)) {
                    continue;
                }
                if (!typeByName.TryGetValue(typename, out var type)) {
                    continue;
                }

                var instance = constructors[type]();
                var typeSettersArr = setters[type];
                var typeOrdinals = dataOrdinals[type];
                for (int i = 0; i < typeSettersArr.Length; i++) {
                    typeSettersArr[i]?.Invoke(instance, reader.GetValue(typeOrdinals[i]));
                }
                retv.Add((ILegacyDataObject)instance);
            }

            transaction?.Benchmarker?.Mark("Build process finished");
            return retv;
        }

    }
}
