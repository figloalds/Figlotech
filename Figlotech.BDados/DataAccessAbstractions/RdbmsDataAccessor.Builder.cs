using Figlotech.BDados.Business;
using Figlotech.Core;
using Figlotech.Core.Extensions;
using Figlotech.Core.Helpers;
using Figlotech.Core.Interfaces;
using Newtonsoft.Json;
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

    public sealed class AggregateRelationPlan {
        public AggregateBuildOptions BuildOption;
        public int ChildIndex;
        public int ParentRIDOrdinal;
        public int ChildRIDOrdinal;
        public Type ChildType;

        // AggregateField
        public int ValueOrdinal;
        public Action<object, object> SetField;

        // AggregateList
        public Action<object, object> SetList;
        public Func<object, object> GetList;
        public Func<object> CreateListInstance;
        public Func<object> CreateChildInstance;
        public Action<object, object> AddToList;

        // AggregateObject
        public Action<object, object> SetObject;
        public Func<object> CreateInstance;
        public bool HasAggregateList;
    }

    internal struct AggregateConstructionContext {
        public Dictionary<(Type Type, object Rid), object> ObjectCache;
        public ConditionalWeakTable<object, object[]> ListCache;
    }

    public sealed class AggregateMaterializerPlan {
        public Action<object, object[]>[] FieldPopulators;
        public AggregateRelationPlan[][] RelationPlans;

        static readonly ConcurrentDictionary<JoinDefinition, AggregateMaterializerPlan> _planCache = new ConcurrentDictionary<JoinDefinition, AggregateMaterializerPlan>();

        public static AggregateMaterializerPlan GetOrCreate(
            JoinDefinition join,
            Dictionary<string, (int[], string[], Dictionary<string, int>)> fieldNamesDict,
            IDictionary<int, Relation[]> cachedRelations) {
            return _planCache.GetOrAddWithLocking(join, _ => Build(join, fieldNamesDict, cachedRelations));
        }

        static AggregateMaterializerPlan Build(
            JoinDefinition join,
            Dictionary<string, (int[], string[], Dictionary<string, int>)> fieldNamesDict,
            IDictionary<int, Relation[]> cachedRelations) {

            var joinTables = join.Joins;
            int tableCount = joinTables.Count;

            var fieldPopulators = new Action<object, object[]>[tableCount];
            var relationPlans = new AggregateRelationPlan[tableCount][];

            for (int ti = 0; ti < tableCount; ti++) {
                var prefix = joinTables[ti].Prefix;
                var type = joinTables[ti].ValueObject;

                // Build field populator
                if (fieldNamesDict.TryGetValue(prefix, out var fieldInfo)) {
                    fieldPopulators[ti] = BuildFieldPopulator(type, fieldInfo.Item1, fieldInfo.Item2);
                }

                // Build relation plans
                var relations = cachedRelations[ti];
                var plans = new AggregateRelationPlan[relations.Length];
                for (int ri = 0; ri < relations.Length; ri++) {
                    var rel = relations[ri];
                    plans[ri] = BuildRelationPlan(type, rel, joinTables, fieldNamesDict, cachedRelations);
                }
                relationPlans[ti] = plans;
            }

            return new AggregateMaterializerPlan {
                FieldPopulators = fieldPopulators,
                RelationPlans = relationPlans
            };
        }

        static Action<object, object[]> BuildFieldPopulator(Type type, int[] ordinals, string[] fieldNames) {
            // Build: (object target, object[] values) => { ((T)target).Field1 = convert(values[ord1]); ... }
            var pTarget = Expression.Parameter(typeof(object), "target");
            var pValues = Expression.Parameter(typeof(object[]), "values");
            var typed = Expression.Variable(type, "obj");
            var block = new List<Expression>();
            block.Add(Expression.Assign(typed, Expression.Convert(pTarget, type)));

            var members = ReflectionTool.FieldsAndPropertiesOf(type);
            var memberDict = new Dictionary<string, MemberInfo>(StringComparer.OrdinalIgnoreCase);
            foreach (var m in members) {
                memberDict[m.Name] = m;
            }

            var dbNullValue = Expression.Field(null, typeof(DBNull).GetField(nameof(DBNull.Value)));

            for (int i = 0; i < ordinals.Length; i++) {
                if (!memberDict.TryGetValue(fieldNames[i], out var member))
                    continue;
                if (member is PropertyInfo pi && pi.SetMethod == null)
                    continue;
                if (member is FieldInfo fi && fi.IsInitOnly)
                    continue;

                var targetType = member is PropertyInfo p ? p.PropertyType : ((FieldInfo)member).FieldType;
                var ordinalConst = Expression.Constant(ordinals[i]);
                // values[ordinal]
                var rawValue = Expression.ArrayIndex(pValues, ordinalConst);

                MemberExpression memberExpr = member switch {
                    PropertyInfo prop => Expression.Property(typed, prop),
                    FieldInfo field => Expression.Field(typed, field),
                    _ => throw new NotSupportedException()
                };

                // Inline object->target conversion (handles null/DBNull internally)
                Expression convertedValue = ReflectionTool.BuildObjectToTargetConversionExpression(rawValue, targetType);
                if (convertedValue.Type != targetType) {
                    convertedValue = Expression.Convert(convertedValue, targetType);
                }
                block.Add(Expression.Assign(memberExpr, convertedValue));
            }

            block.Add(Expression.Empty());
            var body = Expression.Block(new[] { typed }, block);
            var lambda = Expression.Lambda<Action<object, object[]>>(body, pTarget, pValues);
            return lambda.Compile();
        }

        static internal Func<object> BuildConstructor(Type type) {
            var newExpr = Expression.New(type);
            var lambda = Expression.Lambda<Func<object>>(Expression.Convert(newExpr, typeof(object)));
            return lambda.Compile();
        }

        static Action<object, object> BuildSetter(Type ownerType, string memberName) {
            var member = ReflectionTool.GetMember(ownerType, memberName);
            if (member == null) return null;

            var pTarget = Expression.Parameter(typeof(object), "target");
            var pValue = Expression.Parameter(typeof(object), "value");
            var typed = Expression.Convert(pTarget, ownerType);
            var memberType = member is PropertyInfo pi ? pi.PropertyType : ((FieldInfo)member).FieldType;
            var typedValue = Expression.Convert(pValue, memberType);

            MemberExpression memberExpr;
            if (member is PropertyInfo prop) {
                if (prop.SetMethod == null) return null;
                memberExpr = Expression.Property(typed, prop);
            } else {
                memberExpr = Expression.Field(typed, (FieldInfo)member);
            }

            var assign = Expression.Assign(memberExpr, typedValue);
            var lambda = Expression.Lambda<Action<object, object>>(assign, pTarget, pValue);
            return lambda.Compile();
        }

        static internal Action<object, object> BuildConvertingSetter(Type ownerType, string memberName) {
            var member = ReflectionTool.GetMember(ownerType, memberName);
            if (member == null) return null;
            if (member is PropertyInfo prop && prop.SetMethod == null) return null;
            if (member is FieldInfo fi && fi.IsInitOnly) return null;

            var memberType = member is PropertyInfo pi2 ? pi2.PropertyType : ((FieldInfo)member).FieldType;

            var pTarget = Expression.Parameter(typeof(object), "target");
            var pValue = Expression.Parameter(typeof(object), "value");
            var typed = Expression.Convert(pTarget, ownerType);

            MemberExpression memberExpr = member switch {
                PropertyInfo p => Expression.Property(typed, p),
                FieldInfo f => Expression.Field(typed, f),
                _ => throw new NotSupportedException()
            };

            // Inline object->memberType conversion (handles null/DBNull internally)
            Expression convertedValue = ReflectionTool.BuildObjectToTargetConversionExpression(pValue, memberType);
            if (convertedValue.Type != memberType) {
                convertedValue = Expression.Convert(convertedValue, memberType);
            }
            var body = Expression.Assign(memberExpr, convertedValue);

            var lambda = Expression.Lambda<Action<object, object>>(body, pTarget, pValue);
            return lambda.Compile();
        }

        static Func<object, object> BuildGetter(Type ownerType, string memberName) {
            var member = ReflectionTool.GetMember(ownerType, memberName);
            if (member == null) return null;

            var pTarget = Expression.Parameter(typeof(object), "target");
            var typed = Expression.Convert(pTarget, ownerType);

            Expression access;
            if (member is PropertyInfo prop) {
                access = Expression.Property(typed, prop);
            } else {
                access = Expression.Field(typed, (FieldInfo)member);
            }

            var lambda = Expression.Lambda<Func<object, object>>(Expression.Convert(access, typeof(object)), pTarget);
            return lambda.Compile();
        }

        static Action<object, object> BuildListAdd(Type listType, Type elementType) {
            var addMethod = listType.GetMethod("Add", new[] { elementType });
            if (addMethod == null) return null;

            var pList = Expression.Parameter(typeof(object), "list");
            var pItem = Expression.Parameter(typeof(object), "item");
            var typedList = Expression.Convert(pList, listType);
            var typedItem = Expression.Convert(pItem, elementType);
            var call = Expression.Call(typedList, addMethod, typedItem);

            var lambda = Expression.Lambda<Action<object, object>>(call, pList, pItem);
            return lambda.Compile();
        }

        static AggregateRelationPlan BuildRelationPlan(
            Type parentType,
            Relation rel,
            List<JoiningTable> joinTables,
            Dictionary<string, (int[], string[], Dictionary<string, int>)> fieldNamesDict,
            IDictionary<int, Relation[]> cachedRelations) {

            var plan = new AggregateRelationPlan {
                BuildOption = rel.AggregateBuildOption,
                ChildIndex = rel.ChildIndex,
            };

            switch (rel.AggregateBuildOption) {
                case AggregateBuildOptions.AggregateField: {
                        string childPrefix = joinTables[rel.ChildIndex].Prefix;
                        string name = rel.NewName ?? (childPrefix + "_" + rel.Fields[0]);
                        var childFields = fieldNamesDict[childPrefix];
                        plan.ValueOrdinal = childFields.Item3[rel.Fields[0]];
                        plan.SetField = BuildConvertingSetter(parentType, name);
                        break;
                    }

                case AggregateBuildOptions.AggregateList: {
                        string fieldAlias = rel.NewName ?? joinTables[rel.ChildIndex].Alias;
                        var member = ReflectionTool.GetMember(parentType, fieldAlias);
                        var objectType = member != null ? ReflectionTool.GetTypeOf(member) : null;
                        if (objectType == null) break;
                        var ulType = objectType.GetGenericArguments().FirstOrDefault();
                        if (ulType == null) break;

                        plan.ChildType = ulType;
                        plan.SetList = BuildSetter(parentType, fieldAlias);
                        plan.GetList = BuildGetter(parentType, fieldAlias);
                        plan.CreateListInstance = BuildConstructor(objectType);
                        plan.CreateChildInstance = BuildConstructor(ulType);
                        plan.AddToList = BuildListAdd(objectType, ulType);

                        var ridCol = FiTechBDadosExtensions.RidColumnNameOf[ulType];
                        var parentPrefix = joinTables[rel.ParentIndex].Prefix;
                        var parentFields = fieldNamesDict[parentPrefix];
                        plan.ParentRIDOrdinal = parentFields.Item3[rel.ParentKey];
                        var childFieldsLookup = fieldNamesDict[joinTables[rel.ChildIndex].Prefix];
                        plan.ChildRIDOrdinal = childFieldsLookup.Item3[ridCol];
                        break;
                    }

                case AggregateBuildOptions.AggregateObject: {
                        string fieldAlias = rel.NewName ?? joinTables[rel.ChildIndex].Alias;
                        var member = ReflectionTool.GetMember(parentType, fieldAlias);
                        var ulType = member != null ? ReflectionTool.GetTypeOf(member) : null;
                        if (ulType == null) break;

                        plan.ChildType = ulType;
                        plan.SetObject = BuildSetter(parentType, fieldAlias);
                        plan.CreateInstance = BuildConstructor(ulType);

                        var ridCol = FiTechBDadosExtensions.RidColumnNameOf[ulType];
                        var parentFieldsLookup = fieldNamesDict[joinTables[rel.ParentIndex].Prefix];
                        plan.ParentRIDOrdinal = parentFieldsLookup.Item3[rel.ParentKey];
                        var childFieldsLookup = fieldNamesDict[joinTables[rel.ChildIndex].Prefix];
                        plan.ChildRIDOrdinal = childFieldsLookup.Item3[ridCol];

                        var childRelations = cachedRelations[rel.ChildIndex];
                        plan.HasAggregateList = false;
                        for (int ci = 0; ci < childRelations.Length; ci++) {
                            if (childRelations[ci].AggregateBuildOption == AggregateBuildOptions.AggregateList) {
                                plan.HasAggregateList = true;
                                break;
                            }
                        }
                        break;
                    }
            }

            return plan;
        }
    }

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
                if (!transaction.CancellationToken.IsCancellationRequested) {
                    return await Fi.Tech.MapFromReaderAsync<T>(reader, transaction.CancellationToken).ToListAsync().ConfigureAwait(false);
                } else {
                    return new List<T>();
                }
            }
        }

        public async IAsyncEnumerable<T> GetObjectEnumerableAsync<T>(BDadosTransaction transaction, DbCommand command, [EnumeratorCancellation] CancellationToken cancellationToken = default) where T : new() {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(transaction.CancellationToken, cancellationToken);
            var combinedToken = cts.Token;
            transaction?.Benchmarker.Mark("Enter lock command");
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
                var existingKeys = new string[reader.FieldCount];
                var jsonKeyPrefixes = new string[reader.FieldCount];
                for (int i = 0; i < reader.FieldCount; i++) {
                    var name = reader.GetName(i);
                    if (name != null) {
                        if (ReflectionTool.DoesTypeHaveFieldOrProperty(typeof(T), name)) {
                            existingKeys[i] = name;
                            jsonKeyPrefixes[i] = $"\"{name}\":";
                        }
                    }
                }
                await writer.WriteAsync("[").ConfigureAwait(false);
                var isFirst = true;
                var sb = new System.Text.StringBuilder(256);
                while (await reader.ReadAsync(transaction.CancellationToken).ConfigureAwait(false)) {
                    transaction.CancellationToken.ThrowIfCancellationRequested();
                    sb.Clear();
                    if (!isFirst) {
                        sb.Append(',');
                    }
                    sb.Append('{');
                    bool isFirstField = true;
                    for (int i = 0; i < existingKeys.Length; i++) {
                        if (existingKeys[i] != null) {
                            var o = reader.GetValue(i);
                            if (!isFirstField) {
                                sb.Append(',');
                            }
                            sb.Append(jsonKeyPrefixes[i]);
                            sb.Append(JsonConvert.SerializeObject(o));
                            isFirstField = false;
                        }
                    }
                    sb.Append('}');
                    await writer.WriteAsync(sb.ToString()).ConfigureAwait(false);
                    isFirst = false;
                }
                await writer.WriteAsync("]").ConfigureAwait(false);
            }
        }

        public DataSet GetDataSet(BDadosTransaction transaction, IDbCommand command) {
            using var handle = transaction.Lock();
            using (var reader = command.ExecuteReader()) {
                DataTable dt = new DataTable();
                for (int i = 0; i < reader.FieldCount; i++) {
                    dt.Columns.Add(new DataColumn(reader.GetName(i)));
                }

                while (reader.Read()) {
                    var dr = dt.NewRow();
                    for (int i = 0; i < reader.FieldCount; i++) {
                        var type = reader.GetFieldType(i);
                        if (reader.IsDBNull(i)) {
                            dr[i] = null;
                        } else {
                            var val = reader.GetValue(i);
                            dr[i] = Convert.ChangeType(val, type);
                        }
                    }
                    dt.Rows.Add(dr);
                }

                var ds = new DataSet();
                ds.Tables.Add(dt);
                return ds;
            }
        }

        static readonly ConcurrentDictionary<JoinDefinition, Dictionary<string, (int[], string[], Dictionary<string, int>)>> _autoAggregateCache = new ConcurrentDictionary<JoinDefinition, Dictionary<string, (int[], string[], Dictionary<string, int>)>>();

        public (Type, object) cacheId(IDataReader reader, string myRidCol, Type t) {
            var rid = ReflectionTool.DbDeNull(reader[myRidCol]);
            return (t, rid);
        }
        public (Type, object) cacheId(object[] reader, int myRidCol, Type t) {
            var rid = ReflectionTool.DbDeNull(reader[myRidCol]);
            return (t, rid);
        }

        private (Type, object) cacheId(IDataReader reader, int ordinal, Type t) {
            var rid = ReflectionTool.DbDeNull(reader.GetValue(ordinal));
            return (t, rid);
        }

        internal void BuildAggregateObject(
            AggregateMaterializerPlan plan,
            object[] reader, object obj,
            int thisIndex, bool isNew,
            AggregateConstructionContext ctx, int recDepth) {

            if (isNew) {
                var populator = plan.FieldPopulators[thisIndex];
                if (populator != null) {
                    populator(obj, reader);
                }
            }

            var relations = plan.RelationPlans[thisIndex];

            for (var i = 0; i < relations.Length; i++) {
                var rel = relations[i];
                if (isNew
                    || rel.BuildOption == AggregateBuildOptions.AggregateList
                    || rel.BuildOption == AggregateBuildOptions.AggregateObject) {
                    switch (rel.BuildOption) {
                        case AggregateBuildOptions.AggregateField: {
                                if (rel.SetField != null) {
                                    rel.SetField(obj, reader[rel.ValueOrdinal]);
                                }
                                break;
                            }

                        case AggregateBuildOptions.AggregateList: {
                                if (rel.AddToList == null || rel.CreateChildInstance == null)
                                    continue;

                                var parentRid = ReflectionTool.DbDeNull(reader[rel.ParentRIDOrdinal]);
                                var childRid = ReflectionTool.DbDeNull(reader[rel.ChildRIDOrdinal]);
                                if (parentRid == null || childRid == null)
                                    continue;

                                // Get or create the list on this parent instance
                                object[] lists;
                                if (!ctx.ListCache.TryGetValue(obj, out lists)) {
                                    lists = new object[plan.RelationPlans.Length];
                                    ctx.ListCache.Add(obj, lists);
                                }
                                object list = lists[rel.ChildIndex];
                                if (list == null) {
                                    list = rel.GetList(obj);
                                    if (list == null) {
                                        list = rel.CreateListInstance();
                                        rel.SetList(obj, list);
                                    }
                                    lists[rel.ChildIndex] = list;
                                }

                                var childRidCacheId = cacheId(reader, rel.ChildRIDOrdinal, rel.ChildType);
                                bool isUlNew = false;
                                if (!ctx.ObjectCache.TryGetValue(childRidCacheId, out var newObj)) {
                                    newObj = rel.CreateChildInstance();
                                    rel.AddToList(list, newObj);
                                    ctx.ObjectCache[childRidCacheId] = newObj;
                                    isUlNew = true;
                                }

                                BuildAggregateObject(plan, reader, newObj, rel.ChildIndex, isUlNew, ctx, recDepth + 1);
                                break;
                            }

                        case AggregateBuildOptions.AggregateObject: {
                                if (rel.ChildType == null || rel.CreateInstance == null)
                                    continue;

                                var parentRid = ReflectionTool.DbDeNull(reader[rel.ParentRIDOrdinal]);
                                var childRid = ReflectionTool.DbDeNull(reader[rel.ChildRIDOrdinal]);
                                if (parentRid == null || childRid == null)
                                    continue;

                                var childRidCacheId = cacheId(reader, rel.ChildRIDOrdinal, rel.ChildType);
                                bool isUlNew = false;
                                if (!ctx.ObjectCache.TryGetValue(childRidCacheId, out var newObj)) {
                                    newObj = rel.CreateInstance();
                                    ctx.ObjectCache[childRidCacheId] = newObj;
                                    isUlNew = true;
                                }
                                if (rel.SetObject != null && (isNew || isUlNew)) {
                                    rel.SetObject(obj, newObj);
                                }

                                if (isUlNew || rel.HasAggregateList) {
                                    BuildAggregateObject(plan, reader, newObj, rel.ChildIndex, isUlNew, ctx, recDepth + 1);
                                }
                                break;
                            }
                    }
                }
            }
        }

        public async IAsyncEnumerable<T> BuildAggregateListDirectCoroutinely<T>(BDadosTransaction transaction, DbCommand command, JoinDefinition join, int thisIndex, [EnumeratorCancellation] CancellationToken cancellationToken = default) where T : IDataObject, new() {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(transaction.CancellationToken, cancellationToken);
            var combinedToken = cts.Token;
            var myPrefix = join.Joins[thisIndex].Prefix;
            var joinTables = join.Joins;
            var joinRelations = join.Relations;
            var ridcol = FiTechBDadosExtensions.RidColumnNameOf[typeof(T)];
            if (Debugger.IsAttached && string.IsNullOrEmpty(ridcol)) {
                Debugger.Break();
            }

            transaction?.Benchmarker?.Mark($"Executing query for AggregateListDirect<{typeof(T).Name}>");
            using (var reader = await command.ExecuteReaderAsync(CommandBehavior.SingleResult | CommandBehavior.KeyInfo, combinedToken).ConfigureAwait(false)) {
                transaction?.Benchmarker?.Mark("Prepare caches");
                combinedToken.ThrowIfCancellationRequested();
                Dictionary<string, (int[], string[], Dictionary<string, int>)> fieldNamesDict;
                Benchmarker.Assert(() => join != null);
                fieldNamesDict = _autoAggregateCache.GetOrAddWithLocking(join, _ => CreateFieldNamesDict(reader, join));

                var cachedRelations = new SelfInitializerDictionary<int, Relation[]>(rel => {
                    return joinRelations.Where(a => a.ParentIndex == rel).ToArray();
                });

                var plan = AggregateMaterializerPlan.GetOrCreate(join, fieldNamesDict, cachedRelations);

                var myRidCol = $"{myPrefix}_{ridcol}";
                var myRidOrdinal = reader.GetOrdinal(myRidCol);
                bool isNew;
                var ctx = new AggregateConstructionContext {
                    ObjectCache = new Dictionary<(Type Type, object Rid), object>(),
                    ListCache = new ConditionalWeakTable<object, object[]>()
                };
                if (myRidCol == null && Debugger.IsAttached) {
                    Debugger.Break();
                }

                transaction?.Benchmarker?.Mark("Enter Build Result");
                int row = 0;
                int objs = 0;
                T currentObject = default(T);
                var objArray = new object[reader.FieldCount];
                while (await reader.ReadAsync(combinedToken).ConfigureAwait(false)) {
                    combinedToken.ThrowIfCancellationRequested();
                    isNew = true;
                    T iterationObject;
                    var thisCacheId = cacheId(reader, myRidOrdinal, typeof(T));
                    if (!ctx.ObjectCache.TryGetValue(thisCacheId, out var iterationObjectObj)) {
                        iterationObject = new T();
                        if (currentObject != null) {
                            objs++;
                            yield return currentObject;
                        }
                        ctx.ObjectCache.Clear();
                        ctx.ListCache.Clear();
                        currentObject = iterationObject;
                        ctx.ObjectCache[thisCacheId] = iterationObject;
                    } else {
                        iterationObject = (T)iterationObjectObj;
                        isNew = false;
                    }

                    reader.GetValues(objArray);

                    BuildAggregateObject(plan, objArray, iterationObject, thisIndex, isNew, ctx, 0);

                    row++;
                }
                if (currentObject != null) {
                    objs++;
                    yield return currentObject;
                }
                var elaps = transaction?.Benchmarker?.Mark($"[{transaction.Id}] Built List Size: {objs} / {row} rows");
                transaction.Benchmarker.Mark($"[{transaction.Id}] Avg Build speed: {((double)elaps / (double)objs).ToString("0.00")}ms/obj | {((double)elaps / (double)row).ToString("0.00")}ms/row");

                transaction?.Benchmarker?.Mark("Clear cache");
                ctx.ObjectCache.Clear();
            }
        }

        public async Task<List<T>> BuildAggregateListDirectAsync<T>(BDadosTransaction transaction, DbCommand command, JoinDefinition join, int thisIndex, object overrideContext) where T : ILegacyDataObject, new() {
            List<T> retv = new List<T>();
            var myPrefix = join.Joins[thisIndex].Prefix;
            var joinTables = join.Joins;
            var joinRelations = join.Relations.ToArray();
            var ridcol = FiTechBDadosExtensions.RidColumnNameOf[typeof(T)];
            if (Debugger.IsAttached && string.IsNullOrEmpty(ridcol)) {
                Debugger.Break();
            }

            transaction?.Benchmarker?.Mark($"Executing query for AggregateListDirect<{typeof(T).Name}>");
            var dlc = new DataLoadContext {
                DataAccessor = this,
                IsAggregateLoad = true,
                Transaction = transaction,
                ContextTransferObject = overrideContext ?? transaction?.ContextTransferObject
            };
            using (var reader = await command.ExecuteReaderAsync(CommandBehavior.SingleResult | CommandBehavior.KeyInfo, transaction.CancellationToken).ConfigureAwait(false)) {
                transaction?.Benchmarker?.Mark("Prepare caches");
                Dictionary<string, (int[], string[], Dictionary<string, int>)> fieldNamesDict;
                Benchmarker.Assert(() => join != null);
                fieldNamesDict = _autoAggregateCache.GetOrAddWithLocking(join, _ => CreateFieldNamesDict(reader, join));

                var cachedRelations = new SelfInitializerDictionary<int, Relation[]>(rel => {
                    return joinRelations.Where(a => a.ParentIndex == rel).ToArray();
                });

                var plan = AggregateMaterializerPlan.GetOrCreate(join, fieldNamesDict, cachedRelations);

                var myRidCol = $"{myPrefix}_{ridcol}";
                var myRidOrdinal = reader.GetOrdinal(myRidCol);
                bool isNew;
                var ctx = new AggregateConstructionContext {
                    ObjectCache = new Dictionary<(Type Type, object Rid), object>(),
                    ListCache = new ConditionalWeakTable<object, object[]>()
                };
                if (myRidCol == null && Debugger.IsAttached) {
                    Debugger.Break();
                }
                transaction?.Benchmarker?.Mark("Enter Build Result");
                int row = 0;

                var implementsAfterLoad = CacheImplementsAfterLoad[typeof(T)];
                var implementsAfterAggregateLoad = CacheImplementsAfterAggregateLoad[typeof(T)];
                const int AfterLoadInlineThreshold = 1000;
                WorkQueuer afterLoads = implementsAfterLoad || implementsAfterAggregateLoad ? new WorkQueuer("AfterLoads") : null;
                bool useInlineAfterLoad = false;
                var objArray = new object[reader.FieldCount];
                while (await reader.ReadAsync(transaction.CancellationToken).ConfigureAwait(false)) {
                    transaction.CancellationToken.ThrowIfCancellationRequested();
                    isNew = true;
                    T newObj;
                    var thisCacheId = cacheId(reader, myRidOrdinal, typeof(T));
                    if (!ctx.ObjectCache.TryGetValue(thisCacheId, out var newObjObj)) {
                        newObj = new T();
                        ctx.ObjectCache[thisCacheId] = newObj;
                        if (implementsAfterLoad || implementsAfterAggregateLoad) {
                            if (retv.Count >= AfterLoadInlineThreshold) {
                                useInlineAfterLoad = true;
                            }
                            if (useInlineAfterLoad) {
                                if (implementsAfterLoad) {
                                    ((IBusinessObject)newObj).OnAfterLoad(dlc);
                                }
                                if (implementsAfterAggregateLoad) {
                                    await ((IBusinessObject<T>)newObj).OnAfterAggregateLoadAsync(dlc).ConfigureAwait(false);
                                }
                            } else {
                                var capturedNewObj = newObj;
                                afterLoads.Enqueue(async () => {
                                    if (implementsAfterLoad) {
                                        ((IBusinessObject)capturedNewObj).OnAfterLoad(dlc);
                                    }

                                    if (implementsAfterAggregateLoad) {
                                        await ((IBusinessObject<T>)capturedNewObj).OnAfterAggregateLoadAsync(dlc).ConfigureAwait(false);
                                    }
                                });
                            }
                        }
                        retv.Add(newObj);
                    } else {
                        newObj = (T)newObjObj;
                        isNew = false;
                    }

                    reader.GetValues(objArray);
                    BuildAggregateObject(plan, objArray, newObj, thisIndex, isNew, ctx, 0);

                    row++;
                }

                if (afterLoads != null) {
                    await afterLoads.Stop(true).ConfigureAwait(false);
                }
                var elaps = transaction?.Benchmarker?.Mark($"[{transaction.Id}] Built List Size: {retv.Count} / {row} rows");
                transaction.Benchmarker.Mark($"[{transaction.Id}] Avg Build speed: {((double)elaps / (double)retv.Count).ToString("0.00")}ms/item | {((double)elaps / (double)row).ToString("0.00")}ms/row");

                transaction?.Benchmarker?.Mark("Clear cache");
                ctx.ObjectCache.Clear();
            }
            transaction?.Benchmarker?.Mark("Run afterloads");

            if (retv.Count > 0 && CacheImplementsAfterListAggregateLoad[typeof(T)]) {
                await ((IBusinessObject<T>)retv.First()).OnAfterListAggregateLoadAsync(dlc, retv).ConfigureAwait(false);
            }

            transaction?.Benchmarker?.Mark("Build process finished");
            return retv;
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

        public List<T> BuildAggregateListDirect<T>(BDadosTransaction transaction, IDbCommand command, JoinDefinition join, int thisIndex, object overrideContext) where T : ILegacyDataObject, new() {
            List<T> retv = new List<T>();
            var myPrefix = join.Joins[thisIndex].Prefix;
            var joinTables = join.Joins;
            var joinRelations = join.Relations.ToArray();
            var ridcol = FiTechBDadosExtensions.RidColumnNameOf[typeof(T)];
            if (Debugger.IsAttached && string.IsNullOrEmpty(ridcol)) {
                Debugger.Break();
            }
            transaction?.Benchmarker?.Mark($"Executing query for AggregateListDirect<{typeof(T).Name}>");
            using (var reader = command.ExecuteReader(CommandBehavior.SingleResult | CommandBehavior.KeyInfo)) {
                transaction?.Benchmarker?.Mark("Prepare caches");
                Dictionary<string, (int[], string[], Dictionary<string, int>)> fieldNamesDict;
                Benchmarker.Assert(() => join != null);
                fieldNamesDict = _autoAggregateCache.GetOrAddWithLocking(join, _ => CreateFieldNamesDict(reader, join));

                var cachedRelations = new SelfInitializerDictionary<int, Relation[]>(rel => {
                    return joinRelations.Where(a => a.ParentIndex == rel).ToArray();
                });

                var plan = AggregateMaterializerPlan.GetOrCreate(join, fieldNamesDict, cachedRelations);

                var myRidCol = $"{myPrefix}_{ridcol}";
                var myRidOrdinal = reader.GetOrdinal(myRidCol);
                bool isNew;
                var ctx = new AggregateConstructionContext {
                    ObjectCache = new Dictionary<(Type Type, object Rid), object>(),
                    ListCache = new ConditionalWeakTable<object, object[]>()
                };
                if (myRidCol == null && Debugger.IsAttached) {
                    Debugger.Break();
                }
                transaction?.Benchmarker?.Mark("Enter Build Result");
                int row = 0;
                var objArray = new object[reader.FieldCount];
                while (reader.Read()) {
                    isNew = true;
                    T newObj;
                    var thisCacheId = cacheId(reader, myRidOrdinal, typeof(T));
                    if (!ctx.ObjectCache.TryGetValue(thisCacheId, out var newObjObj)) {
                        newObj = new T();
                        ctx.ObjectCache[thisCacheId] = newObj;
                        retv.Add(newObj);
                    } else {
                        newObj = (T)newObjObj;
                        isNew = false;
                    }

                    reader.GetValues(objArray);
                    BuildAggregateObject(plan, objArray, newObj, thisIndex, isNew, ctx, 0);

                    row++;
                }
                var elaps = transaction?.Benchmarker?.Mark($"[{transaction.Id}] Built List Size: {retv.Count} / {row} rows");
                transaction.Benchmarker.Mark($"[{transaction.Id}] Avg Build speed: {((double)elaps / (double)retv.Count).ToString("0.00")}ms/item | {((double)elaps / (double)row).ToString("0.00")}ms/row");

                transaction?.Benchmarker?.Mark("Clear cache");
                ctx.ObjectCache.Clear();
            }
            var dlc = new DataLoadContext {
                DataAccessor = this,
                IsAggregateLoad = true,
                Transaction = transaction,
                ContextTransferObject = overrideContext ?? transaction?.ContextTransferObject
            };
            transaction?.Benchmarker?.Mark("Run afterloads");

            if (retv.Count > 0 && CacheImplementsAfterListAggregateLoad[typeof(T)]) {
                ((IBusinessObject<T>)retv.First()).OnAfterListAggregateLoadAsync(dlc, retv).ConfigureAwait(false).GetAwaiter().GetResult();
            }
            if (CacheImplementsAfterAggregateLoad[typeof(T)]) {
                foreach (var a in retv) {
                    ((IBusinessObject<T>)a).OnAfterAggregateLoadAsync(dlc).ConfigureAwait(false).GetAwaiter().GetResult();
                }
            }

            if (CacheImplementsAfterLoad[typeof(T)]) {
                foreach (var a in retv) {
                    ((IBusinessObject)a).OnAfterLoad(dlc);
                }
            }

            transaction?.Benchmarker?.Mark("Build process finished");
            return retv;
        }

        private static readonly ConcurrentDictionary<Type, (Func<object> Constructor, Action<object, object>[] Setters)> _stateUpdateMetadataCache = new ConcurrentDictionary<Type, (Func<object>, Action<object, object>[])>();

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
                        typeSetters[i] = AggregateMaterializerPlan.BuildConvertingSetter(t, typeFields[i].Name);
                    }
                    return (AggregateMaterializerPlan.BuildConstructor(t), typeSetters);
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
        private Dictionary<string, (int[], string[], Dictionary<string, int>)> CreateFieldNamesDict(IDataReader reader, JoinDefinition join) {
            var groups = new Dictionary<string, List<(int Index, string FieldName)>>(StringComparer.OrdinalIgnoreCase);
            for (int i = 0; i < reader.FieldCount; i++) {
                var name = reader.GetName(i);
                if (name == null) continue;
                var underscoreIdx = name.IndexOf('_');
                var prefix = underscoreIdx >= 0 ? name.Substring(0, underscoreIdx).ToLowerInvariant() : name.ToLowerInvariant();
                var fieldName = underscoreIdx >= 0 ? name.Substring(underscoreIdx + 1) : name;
                if (!groups.TryGetValue(prefix, out var list)) {
                    list = new List<(int, string)>();
                    groups[prefix] = list;
                }
                list.Add((i, fieldName));
            }

            var result = new Dictionary<string, (int[], string[], Dictionary<string, int>)>(StringComparer.OrdinalIgnoreCase);
            foreach (var kvp in groups) {
                var list = kvp.Value;
                var indices = new int[list.Count];
                var names = new string[list.Count];
                var lookup = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                for (int i = 0; i < list.Count; i++) {
                    indices[i] = list[i].Index;
                    names[i] = list[i].FieldName;
                    lookup[names[i]] = indices[i];
                }
                result[kvp.Key] = (indices, names, lookup);
            }
            return result;
        }
    }
}
