using Figlotech.Core;
using Figlotech.Core.BusinessModel;
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

    public sealed class AggregateMaterializerPlan {
        public Action<object, object[]>[] FieldPopulators;
        public AggregateRelationPlan[][] RelationPlans;

        static ConcurrentDictionary<JoinDefinition, AggregateMaterializerPlan> _planCache = new ConcurrentDictionary<JoinDefinition, AggregateMaterializerPlan>();

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

                // Check for DBNull: values[ordinal] is DBNull || values[ordinal] == null
                var isDbNull = Expression.OrElse(
                    Expression.TypeIs(rawValue, typeof(DBNull)),
                    Expression.ReferenceEqual(rawValue, Expression.Constant(null, typeof(object))));

                // Get converter from object -> targetType
                var converter = ReflectionTool.GetConverterDelegate(typeof(object), targetType);
                var converterFuncType = converter.GetType();
                var invokeMethod = converterFuncType.GetMethod("Invoke");
                var converterConst = Expression.Constant(converter, converterFuncType);
                Expression convertedValue = Expression.Call(converterConst, invokeMethod, rawValue);

                // GetConverterDelegate unwraps Nullable<T> -> T, so the return type may differ from targetType
                if (convertedValue.Type != targetType) {
                    convertedValue = Expression.Convert(convertedValue, targetType);
                }

                if (!targetType.IsValueType || Nullable.GetUnderlyingType(targetType) != null) {
                    var conditionalExpr = Expression.Condition(
                        isDbNull,
                        Expression.Default(targetType),
                        convertedValue);
                    block.Add(Expression.Assign(memberExpr, conditionalExpr));
                } else {
                    var assign = Expression.Assign(memberExpr, convertedValue);
                    block.Add(Expression.IfThen(Expression.Not(isDbNull), assign));
                }
            }

            block.Add(Expression.Empty());
            var body = Expression.Block(new[] { typed }, block);
            var lambda = Expression.Lambda<Action<object, object[]>>(body, pTarget, pValues);
            return lambda.Compile();
        }

        static Func<object> BuildConstructor(Type type) {
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

        static Action<object, object> BuildConvertingSetter(Type ownerType, string memberName) {
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

            // Handle DBNull/null
            var isDbNull = Expression.OrElse(
                Expression.TypeIs(pValue, typeof(DBNull)),
                Expression.ReferenceEqual(pValue, Expression.Constant(null, typeof(object))));

            var converter = ReflectionTool.GetConverterDelegate(typeof(object), memberType);
            var converterFuncType = converter.GetType();
            var invokeMethod = converterFuncType.GetMethod("Invoke");
            var converterConst = Expression.Constant(converter, converterFuncType);
            Expression convertedValue = Expression.Call(converterConst, invokeMethod, pValue);
            if (convertedValue.Type != memberType) {
                convertedValue = Expression.Convert(convertedValue, memberType);
            }

            Expression body;
            if (!memberType.IsValueType || Nullable.GetUnderlyingType(memberType) != null) {
                body = Expression.Assign(memberExpr,
                    Expression.Condition(isDbNull, Expression.Default(memberType), convertedValue));
            } else {
                body = Expression.IfThen(Expression.Not(isDbNull),
                    Expression.Assign(memberExpr, convertedValue));
            }

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
                    var objectType = ReflectionTool.GetTypeOf(
                        ReflectionTool.FieldsAndPropertiesOf(parentType)
                            .Where(m => m.Name == fieldAlias).FirstOrDefault());
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
                    var ulType = ReflectionTool.GetTypeOf(
                        ReflectionTool.FieldsAndPropertiesOf(parentType)
                            .Where(m => m.Name == fieldAlias).FirstOrDefault());
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
            lock (command) {
                transaction?.Benchmarker.Mark("- Starting Execute Query");
                using (var reader = command.ExecuteReader(CommandBehavior.SingleResult | CommandBehavior.SequentialAccess | CommandBehavior.KeyInfo)) {
                    transaction?.Benchmarker.Mark("- Starting build");
                    return Fi.Tech.MapFromReader<T>(reader).ToList();
                }
            }
        }

        FiAsyncMultiLock _lock = new FiAsyncMultiLock();
        public async Task<List<T>> GetObjectListAsync<T>(BDadosTransaction transaction, DbCommand command) where T : new() {
            transaction?.Benchmarker.Mark("Enter lock command");
            using (await _lock.Lock($"TRANSACTION_{transaction.Id}").ConfigureAwait(false)) {
                transaction?.Benchmarker.Mark("- Starting Execute Query");
                using (var reader = await command.ExecuteReaderAsync(CommandBehavior.SingleResult | CommandBehavior.SequentialAccess | CommandBehavior.KeyInfo, transaction.CancellationToken).ConfigureAwait(false)) {
                    transaction?.Benchmarker.Mark("- Starting build");
                    if(!transaction.CancellationToken.IsCancellationRequested) {
                        return await Fi.Tech.MapFromReaderAsync<T>(reader, transaction.CancellationToken).ToListAsync().ConfigureAwait(false);
                    } else {
                        return new List<T>();
                    }
                }
            }
        }
        public async IAsyncEnumerable<T> GetObjectEnumerableAsync<T>(BDadosTransaction transaction, DbCommand command) where T : new() {
            transaction?.Benchmarker.Mark("Enter lock command");
            transaction?.Benchmarker.Mark("- Starting Execute Query");
            using (var reader = await command.ExecuteReaderAsync(CommandBehavior.SingleResult | CommandBehavior.SequentialAccess | CommandBehavior.KeyInfo, transaction.CancellationToken).ConfigureAwait(false)) {
                transaction?.Benchmarker.Mark("- Starting build");
                await foreach (var item in Fi.Tech.MapFromReaderAsync<T>(reader, transaction.CancellationToken).ConfigureAwait(false)) {
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
                    if (transaction.CancellationToken.IsCancellationRequested) {
                        break;
                    }
                    sb.Clear();
                    if(!isFirst) {
                        sb.Append(',');
                    }
                    sb.Append('{');
                    bool isFirstField = true;
                    for (int i = 0; i < existingKeys.Length; i++) {
                        if (existingKeys[i] != null) {
                            try {
                                var o = reader.GetValue(i);
                                if (!isFirstField) {
                                    sb.Append(',');
                                }
                                sb.Append(jsonKeyPrefixes[i]);
                                sb.Append(JsonConvert.SerializeObject(o));
                                isFirstField = false;
                            } catch (Exception x) {
                                Debugger.Break();
                                //throw x;
                            }
                        }
                    }
                    sb.Append('}');
                    await writer.WriteAsync(sb.ToString()).ConfigureAwait(false);
                    isFirst = false;
                }
                await writer.WriteAsync("]").ConfigureAwait(false);
            }
        }

        public DataSet GetDataSet(IDbCommand command) {
            lock (command) {
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
        }

        static readonly SelfInitializerDictionary<Type, ConstructorInfo> ConstructorCache = new SelfInitializerDictionary<Type, ConstructorInfo>(
            x => x.GetConstructor(Array.Empty<Type>())
        );
        private object NewInstance(Type t) {
            return ConstructorCache[t].Invoke(Array.Empty<object>());
        }

        static ConcurrentDictionary<JoinDefinition, Dictionary<string, (int[], string[], Dictionary<string, int>)>> _autoAggregateCache = new ConcurrentDictionary<JoinDefinition, Dictionary<string, (int[], string[], Dictionary<string, int>)>>();

        public (string, string) cacheId(IDataReader reader, string myRidCol, Type t) {
            var rid = ReflectionTool.DbDeNull(reader[myRidCol])?.ToString();
            return (t.Name, rid);
        }
        public (string, string) cacheId(object[] reader, int myRidCol, Type t) {
            var rid = ReflectionTool.DbDeNull(reader[myRidCol])?.ToString();
            return (t.Name, rid);
        }

        public void BuildAggregateObject(
            AggregateMaterializerPlan plan,
            object[] reader, object obj,
            int thisIndex, bool isNew,
            Dictionary<(string, string), object> constructionCache, int recDepth) {

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
                                var listCacheId = ("AGLIST_REF", $"{obj.GetHashCode()}_{rel.ChildIndex}");
                                object list;
                                if (!constructionCache.TryGetValue(listCacheId, out list)) {
                                    list = rel.GetList(obj);
                                    if (list == null) {
                                        list = rel.CreateListInstance();
                                        rel.SetList(obj, list);
                                    }
                                    constructionCache[listCacheId] = list;
                                }

                                var childRidCacheId = cacheId(reader, rel.ChildRIDOrdinal, rel.ChildType);
                                bool isUlNew = false;
                                if (!constructionCache.TryGetValue(childRidCacheId, out var newObj)) {
                                    newObj = rel.CreateChildInstance();
                                    rel.AddToList(list, newObj);
                                    constructionCache[childRidCacheId] = newObj;
                                    isUlNew = true;
                                }

                                BuildAggregateObject(plan, reader, newObj, rel.ChildIndex, isUlNew, constructionCache, recDepth + 1);
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
                                if (!constructionCache.TryGetValue(childRidCacheId, out var newObj)) {
                                    newObj = rel.CreateInstance();
                                    constructionCache[childRidCacheId] = newObj;
                                    isUlNew = true;
                                }
                                rel.SetObject(obj, newObj);

                                if (isUlNew || rel.HasAggregateList) {
                                    BuildAggregateObject(plan, reader, newObj, rel.ChildIndex, isUlNew, constructionCache, recDepth + 1);
                                }
                                break;
                            }
                    }
                }
            }
        }

        public async IAsyncEnumerable<T> BuildAggregateListDirectCoroutinely<T>(BDadosTransaction transaction, DbCommand command, JoinDefinition join, int thisIndex) where T : IDataObject, new() {
            var myPrefix = join.Joins[thisIndex].Prefix;
            var joinTables = join.Joins;
            var joinRelations = join.Relations;
            var ridcol = FiTechBDadosExtensions.RidColumnNameOf[typeof(T)];
            if (Debugger.IsAttached && string.IsNullOrEmpty(ridcol)) {
                Debugger.Break();
            }

            transaction?.Benchmarker?.Mark($"Executing query for AggregateListDirect<{typeof(T).Name}>");
            using (var reader = await command.ExecuteReaderAsync(CommandBehavior.SingleResult | CommandBehavior.KeyInfo, transaction.CancellationToken).ConfigureAwait(false)) {
                transaction?.Benchmarker?.Mark("Prepare caches");
                if ((transaction?.CancellationToken.IsCancellationRequested) ?? false) {
                    throw transaction.Exception(new TaskCanceledException("Task was cancelled"));
                }
                Dictionary<string, (int[], string[], Dictionary<string, int>)> fieldNamesDict;
                Benchmarker.Assert(() => join != null);
                fieldNamesDict = _autoAggregateCache.GetOrAddWithLocking(join, _ => CreateFieldNamesDict(reader, join));

                var cachedRelations = new SelfInitializerDictionary<int, Relation[]>(rel => {
                    return joinRelations.Where(a => a.ParentIndex == rel).ToArray();
                });

                var plan = AggregateMaterializerPlan.GetOrCreate(join, fieldNamesDict, cachedRelations);

                var myRidCol = $"{myPrefix}_{ridcol}";
                bool isNew;
                var constructionCache = new Dictionary<(string, string), object>();
                if (myRidCol == null && Debugger.IsAttached) {
                    Debugger.Break();
                }

                transaction?.Benchmarker?.Mark("Enter Build Result");
                int row = 0;
                int objs = 0;
                T currentObject = default(T);
                var objArray = new object[reader.FieldCount];
                while (await reader.ReadAsync(transaction.CancellationToken).ConfigureAwait(false)) {
                    isNew = true;
                    T iterationObject;
                    var thisCacheId = cacheId(reader, myRidCol, typeof(T));
                    if (!constructionCache.TryGetValue(thisCacheId, out var iterationObjectObj)) {
                        iterationObject = new T();
                        if (currentObject != null) {
                            objs++;
                            yield return currentObject;
                        }
                        constructionCache.Clear();
                        currentObject = iterationObject;
                        constructionCache[thisCacheId] = iterationObject;
                    } else {
                        iterationObject = (T)iterationObjectObj;
                        isNew = false;
                    }

                    reader.GetValues(objArray);

                    BuildAggregateObject(plan, objArray, iterationObject, thisIndex, isNew, constructionCache, 0);

                    row++;
                }
                if (currentObject != null) {
                    objs++;
                    yield return currentObject;
                }
                var elaps = transaction?.Benchmarker?.Mark($"[{transaction.Id}] Built List Size: {objs} / {row} rows");
                if (transaction?.Benchmarker != null) {
                    transaction.Benchmarker.Mark($"[{transaction.Id}] Avg Build speed: {((double)elaps / (double)objs).ToString("0.00")}ms/obj | {((double)elaps / (double)row).ToString("0.00")}ms/row");
                }

                transaction?.Benchmarker?.Mark("Clear cache");
                constructionCache.Clear();
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
                bool isNew;
                var constructionCache = new Dictionary<(string, string), object>();
                if (myRidCol == null && Debugger.IsAttached) {
                    Debugger.Break();
                }
                constructionCache.Add(("_INIT_", myRidCol), new Dictionary<string, object>());
                transaction?.Benchmarker?.Mark("Enter Build Result");
                int row = 0;

                var implementsAfterLoad = CacheImplementsAfterLoad[typeof(T)];
                var implementsAfterAggregateLoad = CacheImplementsAfterAggregateLoad[typeof(T)];
                WorkQueuer afterLoads = implementsAfterLoad || implementsAfterAggregateLoad ? new WorkQueuer("AfterLoads") : null;
                var objArray = new object[reader.FieldCount];
                while (await reader.ReadAsync(transaction.CancellationToken).ConfigureAwait(false)) {
                    isNew = true;
                    T newObj;
                    var thisCacheId = cacheId(reader, myRidCol, typeof(T));
                    if (!constructionCache.TryGetValue(thisCacheId, out var newObjObj)) {
                        newObj = new T();
                        constructionCache[thisCacheId] = newObj;
                        if (implementsAfterLoad || implementsAfterAggregateLoad) {
                            afterLoads.Enqueue(async () => {
                                if (implementsAfterLoad) {
                                    ((IBusinessObject)newObj).OnAfterLoad(dlc);
                                }

                                if (implementsAfterAggregateLoad) {
                                    await ((IBusinessObject<T>)newObj).OnAfterAggregateLoadAsync(dlc).ConfigureAwait(false);
                                }
                            });
                        }
                        retv.Add(newObj);
                    } else {
                        newObj = (T)newObjObj;
                        isNew = false;
                    }

                    reader.GetValues(objArray);
                    BuildAggregateObject(plan, objArray, newObj, thisIndex, isNew, constructionCache, 0);

                    row++;
                }

                if (afterLoads != null) {
                    await afterLoads.Stop(true).ConfigureAwait(false);
                }
                var elaps = transaction?.Benchmarker?.Mark($"[{transaction.Id}] Built List Size: {retv.Count} / {row} rows");
                if (transaction?.Benchmarker != null) {
                    transaction.Benchmarker.Mark($"[{transaction.Id}] Avg Build speed: {((double)elaps / (double)retv.Count).ToString("0.00")}ms/item | {((double)elaps / (double)row).ToString("0.00")}ms/row");
                }

                transaction?.Benchmarker?.Mark("Clear cache");
                constructionCache.Clear();
            }
            transaction?.Benchmarker?.Mark("Run afterloads");

            if (retv.Count > 0 && CacheImplementsAfterListAggregateLoad[typeof(T)]) {
                await ((IBusinessObject<T>)retv.First()).OnAfterListAggregateLoadAsync(dlc, retv).ConfigureAwait(false);
            }

            transaction?.Benchmarker?.Mark("Build process finished");
            return retv;
        }

        private SelfInitializerDictionary<Type, bool> CacheImplementsAfterLoad = new SelfInitializerDictionary<Type, bool>(
            t => t.Implements(typeof(IBusinessObject)) && t.GetMethod(nameof(IBusinessObject.OnAfterLoad)).DeclaringType == t
        );
        private SelfInitializerDictionary<Type, bool> CacheImplementsAfterAggregateLoad = new SelfInitializerDictionary<Type, bool>(
            t => t.Implements(typeof(IBusinessObject<>).MakeGenericType(t)) && t.GetMethod("OnAfterAggregateLoadAsync").DeclaringType == t
        );
        private SelfInitializerDictionary<Type, bool> CacheImplementsAfterListAggregateLoad = new SelfInitializerDictionary<Type, bool>(
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
            lock (command) {
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
                    bool isNew;
                    var constructionCache = new Dictionary<(string, string), object>();
                    if (myRidCol == null && Debugger.IsAttached) {
                        Debugger.Break();
                    }
                    constructionCache.Add(("_INIT_", myRidCol), new Dictionary<string, object>());
                    transaction?.Benchmarker?.Mark("Enter Build Result");
                    int row = 0;
                    var objArray = new object[reader.FieldCount];
                    while (reader.Read()) {
                        isNew = true;
                        T newObj;
                        var thisCacheId = cacheId(reader, myRidCol, typeof(T));
                        if (!constructionCache.TryGetValue(thisCacheId, out var newObjObj)) {
                            newObj = new T();
                            constructionCache[thisCacheId] = newObj;
                            retv.Add(newObj);
                        } else {
                            newObj = (T)newObjObj;
                            isNew = false;
                        }

                        reader.GetValues(objArray);
                        BuildAggregateObject(plan, objArray, newObj, thisIndex, isNew, constructionCache, 0);

                        row++;
                    }
                    var elaps = transaction?.Benchmarker?.Mark($"[{transaction.Id}] Built List Size: {retv.Count} / {row} rows");
                    if (transaction?.Benchmarker != null) {
                        transaction.Benchmarker.Mark($"[{transaction.Id}] Avg Build speed: {((double)elaps / (double)retv.Count).ToString("0.00")}ms/item | {((double)elaps / (double)row).ToString("0.00")}ms/row");
                    }

                    transaction?.Benchmarker?.Mark("Clear cache");
                    constructionCache.Clear();
                }
            }
            var dlc = new DataLoadContext {
                DataAccessor = this,
                IsAggregateLoad = true,
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

        private List<ILegacyDataObject> BuildStateUpdateQueryResult(BDadosTransaction transaction, IDataReader reader, List<Type> workingTypes, Dictionary<Type, MemberInfo[]> fields) {
            var retv = new List<ILegacyDataObject>();
            transaction?.Benchmarker?.Mark("Init Build Result");
            while (reader.Read()) {
                var typename = reader["TypeName"] as String;
                var type = workingTypes.FirstOrDefault(wt => wt.Name == typename);
                if (type == null) {
                    continue;
                }

                var instance = NewInstance(type);
                var tFields = fields[type];
                for (int i = 0; i < tFields.Length; i++) {
                    ReflectionTool.SetMemberValue(tFields[i], instance, reader[$"data_{i}"]);
                }
                retv.Add((ILegacyDataObject) instance);
            }

            transaction?.Benchmarker?.Mark("Build process finished");
            return retv;
        }
        private Dictionary<string, (int[], string[], Dictionary<string, int>)> CreateFieldNamesDict(IDataReader reader, JoinDefinition join) {
            var fieldNames = new string[reader.FieldCount];
            for (int i = 0; i < fieldNames.Length; i++)
                fieldNames[i] = reader.GetName(i);
            int idx = 0;
            var newEntryGrp = fieldNames.Select<string, (string, int, string)>(name => {
                var underscoreIdx = name.IndexOf('_');
                var prefix = underscoreIdx >= 0 ? name.Substring(0, underscoreIdx).ToLowerInvariant() : name.ToLowerInvariant();
                var fieldName = underscoreIdx >= 0 ? name.Substring(underscoreIdx + 1) : name;
                return (prefix, idx++, fieldName);
            })
            .Where(i => i.Item1 != null)
            .GroupBy(i => i.Item1);
            return newEntryGrp.ToDictionary(
                i => i.First().Item1,
                i => {
                    var indices = i.Select(j => j.Item2).ToArray();
                    var names = i.Select(j => j.Item3).ToArray();
                    var lookup = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
                    for (int k = 0; k < names.Length; k++) {
                        lookup[names[k]] = indices[k];
                    }
                    return (indices, names, lookup);
                });
        }
    }
}
