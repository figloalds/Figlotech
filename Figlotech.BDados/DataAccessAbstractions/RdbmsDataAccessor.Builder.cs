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
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.BDados.DataAccessAbstractions {
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

        public sealed class AggregateListCachedMetadata {
            public Type UlType { get; set; }
            public int ParentRIDIndex { get; set; }
            public int ChildRIDIndex { get; set; }
            public object List { get; set; }
            public MethodInfo AddMethod { get; set; }
        }

        public sealed class AggregateObjectCachedMetadata {
            public Type UlType { get; set; }
            public string FieldAlias { get; set; }
            public int ParentRIDIndex { get; set; }
            public int ChildRIDIndex { get; set; }
            public bool HasAggregateList { get; set; }
        }

        public static SelfInitializerDictionary<Type, SelfInitializerDictionary<string, Type>> ObjectTypeCache = new SelfInitializerDictionary<Type, SelfInitializerDictionary<string, Type>>(
            t =>
                new SelfInitializerDictionary<string, Type>(fieldAlias => {
                    var objectType = ReflectionTool.GetTypeOf(
                                    ReflectionTool.FieldsAndPropertiesOf(t)
                                    .Where(m => m.Name == fieldAlias)
                                    .FirstOrDefault());
                    return objectType;
                })
        );
        public static SelfInitializerDictionary<Type, Type> UlTypeCache = new SelfInitializerDictionary<Type, Type>(
            objectType => objectType
                    .GetGenericArguments().FirstOrDefault()
        );
        public static SelfInitializerDictionary<Type, MethodInfo> AddMethodCache = new SelfInitializerDictionary<Type, MethodInfo>(
            objectType => objectType.GetMethods()
                    .Where(m => m.Name == "Add")
                    .FirstOrDefault()
        );

        static ConcurrentDictionary<JoinDefinition, Dictionary<string, (int[], string[], Dictionary<string, int>)>> _autoAggregateCache = new ConcurrentDictionary<JoinDefinition, Dictionary<string, (int[], string[], Dictionary<string, int>)>>();

        public (string, string) cacheId(IDataReader reader, string myRidCol, Type t) {
            var rid = ReflectionTool.DbDeNull(reader[myRidCol])?.ToString();
            return (t.Name, rid);
        }
        public (string, string) cacheId(object[] reader, int myRidCol, Type t) {
            var rid = ReflectionTool.DbDeNull(reader[myRidCol])?.ToString();
            return (t.Name, rid);
        }

        public void BuildAggregateObject(BDadosTransaction transaction,
            Type t, object[] reader, object obj, Dictionary<string, (int[], string[], Dictionary<string, int>)> fieldNamesDict, List<JoiningTable> joinTables, IDictionary<int, Relation[]> joinRelations,
            int thisIndex, bool isNew,
            Dictionary<(string, string), object> constructionCache, int recDepth) {
            var myPrefix = joinTables[thisIndex].Prefix;
            if (isNew) {
                if (!fieldNamesDict.TryGetValue(myPrefix, out var fieldNames)) {
                    throw new BDadosException($"Failed to build aggregate list: Aggregated reference {myPrefix} of type {joinTables[thisIndex].ValueObject.Name} was not present in the resulting fields");
                }

                var membersKey = ("_MEMBERS_", $"{myPrefix}_{t.Name}");
                if (!constructionCache.TryGetValue(membersKey, out var membersObj)) {
                    var members = new MemberInfo[fieldNames.Item2.Length];
                    for (int i = 0; i < members.Length; i++) {
                        members[i] = ReflectionTool.GetMember(t, fieldNames.Item2[i]);
                    }
                    membersObj = members;
                    constructionCache[membersKey] = membersObj;
                }
                var cachedMembers = (MemberInfo[])membersObj;

                for (int i = 0; i < fieldNames.Item1.Length; i++) {
                    var val = reader[fieldNames.Item1[i]];
                    if (val is DateTime dt && dt.Kind != DateTimeKind.Utc) {
                        // Opinion, the correct way to save dates is in UTC
                        val = new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, dt.Second, dt.Millisecond, DateTimeKind.Utc);
                    }
                    if (cachedMembers[i] != null) {
                        ReflectionTool.SetMemberValue(cachedMembers[i], obj, val);
                    }
                }
            }

            var relations = joinRelations[thisIndex].AsSpan();

            for (var i = 0; i < relations.Length; i++) {
                var rel = relations[i];
                if (isNew
                    || rel.AggregateBuildOption == AggregateBuildOptions.AggregateList
                    || rel.AggregateBuildOption == AggregateBuildOptions.AggregateObject) {
                    switch (rel.AggregateBuildOption) {
                        case AggregateBuildOptions.AggregateField: {
                                String childPrefix = joinTables[rel.ChildIndex].Prefix;
                                String name = rel.NewName ?? (childPrefix + "_" + rel.Fields[0]);
                                var childFields = fieldNamesDict[childPrefix];
                                var valueIndex = childFields.Item3[rel.Fields[0]];
                                var value = reader[valueIndex];
                                ReflectionTool.SetValue(obj, name, value);

                                break;
                            }

                        case AggregateBuildOptions.AggregateList: {
                                String fieldAlias = rel.NewName ?? joinTables[rel.ChildIndex].Alias;
                                var aglistId = ("AGLIST", $"{obj.GetHashCode()}_{fieldAlias}");
                                if (!constructionCache.TryGetValue(aglistId, out var metadataObj)) {
                                    var objectType = ObjectTypeCache[t][fieldAlias];
                                    var ulType = UlTypeCache[objectType];
                                    var addMethod = AddMethodCache[objectType];
                                    if (addMethod == null)
                                        continue;

                                    object li = ReflectionTool.GetValue(obj, fieldAlias);
                                    if (li == null) {
                                        li = NewInstance(objectType);
                                        ReflectionTool.SetValue(obj, fieldAlias, li);
                                    }

                                    var ridCol = FiTechBDadosExtensions.RidColumnNameOf[ulType];
                                    var childRidCol = joinTables[rel.ChildIndex].Prefix + "_" + ridCol;
                                    var parentPrefix = joinTables[rel.ParentIndex].Prefix;
                                    var parentFields = fieldNamesDict[parentPrefix];
                                    var parentRidIndex = parentFields.Item3[rel.ParentKey];
                                    var childFieldsLookup = fieldNamesDict[joinTables[rel.ChildIndex].Prefix];
                                    var childRidIndex = childFieldsLookup.Item3[ridCol];

                                    metadataObj = new AggregateListCachedMetadata {
                                        UlType = ulType,
                                        ParentRIDIndex = parentRidIndex,
                                        ChildRIDIndex = childRidIndex,
                                        List = li,
                                        AddMethod = addMethod
                                    };
                                    constructionCache[aglistId] = metadataObj;
                                }
                                var metadata = (AggregateListCachedMetadata)metadataObj;

                                var parentRid = ReflectionTool.DbDeNull(reader[metadata.ParentRIDIndex]);
                                var childRid = ReflectionTool.DbDeNull(reader[metadata.ChildRIDIndex]);
                                var childRidCacheId = cacheId(reader, metadata.ChildRIDIndex, metadata.UlType);
                                object newObj;
                                if (parentRid == null || childRid == null) {
                                    continue;
                                }
                                bool isUlNew = false;

                                if (!constructionCache.TryGetValue(childRidCacheId, out newObj)) {
                                    newObj = NewInstance(metadata.UlType);
                                    metadata.AddMethod.Invoke(metadata.List, new object[] { newObj });
                                    constructionCache[childRidCacheId] = newObj;
                                    isUlNew = true;
                                }

                                //transaction.Benchmarker.Mark($"Aggregate List Enter Item {myPrefix}::{childRid}");
                                BuildAggregateObject(transaction, metadata.UlType, reader, newObj, fieldNamesDict, joinTables, joinRelations, rel.ChildIndex, isUlNew, constructionCache, recDepth + 1);

                                break;
                            }

                        // this one is almost the same as previous one.
                        case AggregateBuildOptions.AggregateObject: {
                                var agobjId = ("AGOBJ", $"{thisIndex}_{rel.ChildIndex}");
                                if (!constructionCache.TryGetValue(agobjId, out var agobjMetadataObj)) {
                                    String fieldAlias = rel.NewName ?? joinTables[rel.ChildIndex].Alias;
                                    var ulType = ObjectTypeCache[t][fieldAlias];
                                    if (ulType == null) {
                                        continue;
                                    }
                                    var ridCol = FiTechBDadosExtensions.RidColumnNameOf[ulType];
                                    var parentFieldsLookup = fieldNamesDict[joinTables[rel.ParentIndex].Prefix];
                                    var parentRidIndex = parentFieldsLookup.Item3[rel.ParentKey];
                                    var childFieldsLookup = fieldNamesDict[joinTables[rel.ChildIndex].Prefix];
                                    var childRidIndex = childFieldsLookup.Item3[ridCol];

                                    var childRelations = joinRelations[rel.ChildIndex];
                                    bool hasAggregateList = false;
                                    for (int ci = 0; ci < childRelations.Length; ci++) {
                                        if (childRelations[ci].AggregateBuildOption == AggregateBuildOptions.AggregateList) {
                                            hasAggregateList = true;
                                            break;
                                        }
                                    }

                                    agobjMetadataObj = new AggregateObjectCachedMetadata {
                                        UlType = ulType,
                                        FieldAlias = fieldAlias,
                                        ParentRIDIndex = parentRidIndex,
                                        ChildRIDIndex = childRidIndex,
                                        HasAggregateList = hasAggregateList
                                    };
                                    constructionCache[agobjId] = agobjMetadataObj;
                                }
                                var agobjMeta = (AggregateObjectCachedMetadata)agobjMetadataObj;

                                var parentRid = ReflectionTool.DbDeNull(reader[agobjMeta.ParentRIDIndex]);
                                var childRid = ReflectionTool.DbDeNull(reader[agobjMeta.ChildRIDIndex]);
                                if (parentRid == null || childRid == null) {
                                    continue;
                                }

                                var childRidCacheId = cacheId(reader, agobjMeta.ChildRIDIndex, agobjMeta.UlType);
                                object newObj;
                                bool isUlNew = false;

                                if (!constructionCache.TryGetValue(childRidCacheId, out newObj)) {
                                    newObj = NewInstance(agobjMeta.UlType);
                                    constructionCache[childRidCacheId] = newObj;
                                    isUlNew = true;
                                }
                                ReflectionTool.SetValue(obj, agobjMeta.FieldAlias, newObj);

                                if (isUlNew || agobjMeta.HasAggregateList) {
                                    BuildAggregateObject(transaction, agobjMeta.UlType, reader, newObj, fieldNamesDict, joinTables, joinRelations, rel.ChildIndex, isUlNew, constructionCache, recDepth + 1);
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

                    BuildAggregateObject(transaction, typeof(T), objArray, iterationObject, fieldNamesDict, joinTables, cachedRelations, thisIndex, isNew, constructionCache, 0);

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
                    BuildAggregateObject(transaction, typeof(T), objArray, newObj, fieldNamesDict, joinTables, cachedRelations, thisIndex, isNew, constructionCache, 0);

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
                        BuildAggregateObject(transaction, typeof(T), objArray, newObj, fieldNamesDict, joinTables, cachedRelations, thisIndex, isNew, constructionCache, 0);

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
