using Figlotech.Core;
using Figlotech.Core.BusinessModel;
using Figlotech.Core.Helpers;
using Figlotech.Core.Interfaces;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using static System.Net.Mime.MediaTypeNames;

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
        static readonly object[] emptyParameters = Array.Empty<object>();
        private object NewInstance(Type t) {
            return ConstructorCache[t].Invoke(emptyParameters);
        }

        public sealed class AggregateListCachedMetadata {
            public Type UlType { get; set; }
            public int ParentRIDIndex { get; set; }
            public int ChildRIDIndex { get; set; }
            public object List { get; set; }
            public MethodInfo AddMethod { get; set; }
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

        static AtomicDictionary<string, Dictionary<string, (int[], string[])>> _autoAggregateCache = new AtomicDictionary<string, Dictionary<string, (int[], string[])>>();

        public string cacheId(IDataReader reader, string myRidCol, Type t) {
            var rid = ReflectionTool.DbDeNull(reader[myRidCol]) as string;
            var retv = $"{t.Name}_{rid}";

            return retv;
        }
        public string cacheId(object[] reader, int myRidCol, Type t) {
            var rid = ReflectionTool.DbDeNull(reader[myRidCol]) as string;
            var retv = $"{t.Name}_{rid}";

            return retv;
        }

        public void BuildAggregateObject(BDadosTransaction transaction,
            Type t, object[] reader, object obj, Dictionary<string, (int[], string[])> fieldNamesDict, List<JoiningTable> joinTables, IDictionary<int, Relation[]> joinRelations,
            int thisIndex, bool isNew,
            Dictionary<string, object> constructionCache, int recDepth) {
            var myPrefix = joinTables[thisIndex].Prefix;
            if (isNew) {
                if (!fieldNamesDict.ContainsKey(myPrefix)) {
                    throw new BDadosException($"Failed to build aggregate list: Aggregated reference {myPrefix} of type {joinTables[thisIndex].ValueObject.Name} was not present in the resulting fields");
                }

                var fieldNames = fieldNamesDict[myPrefix];
                for (int i = 0; i < fieldNames.Item1.Length; i++) {
                    var val = reader[fieldNames.Item1[i]];
                    if (val is DateTime dt && dt.Kind != DateTimeKind.Utc) {
                        // Opinion, the correct way to save dates is in UTC
                        val = new DateTime(dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, dt.Second, dt.Millisecond, DateTimeKind.Utc);
                    }
                    ReflectionTool.SetValue(obj, fieldNames.Item2[i], val);
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
                                var valueIndex = fieldNamesDict[childPrefix].Item1[fieldNamesDict[childPrefix].Item2.GetIndexOf(rel.Fields[0])];
                                var value = reader[valueIndex];
                                ReflectionTool.SetValue(obj, name, reader[valueIndex]);

                                break;
                            }

                        case AggregateBuildOptions.AggregateList: {
                                String fieldAlias = rel.NewName ?? joinTables[rel.ChildIndex].Alias;
                                var aglistId = $"AGLIST_{obj.GetHashCode()}_{fieldAlias}";
                                if (!constructionCache.ContainsKey(aglistId)) {
                                    var objectType = ObjectTypeCache[t][fieldAlias];
                                    var ulType = UlTypeCache[objectType];
                                    var addMethod = AddMethodCache[objectType];
                                    if (addMethod == null)
                                        continue;

                                    object li = ReflectionTool.GetValue(obj, fieldAlias);
                                    if (li == null) {
                                        li = NewInstance(ulType);
                                        ReflectionTool.SetValue(obj, fieldAlias, li);
                                    }

                                    var ridCol = FiTechBDadosExtensions.RidColumnOf[ulType];
                                    var childRidCol = joinTables[rel.ChildIndex].Prefix + "_" + ridCol;
                                    var parentPrefix = joinTables[rel.ParentIndex].Prefix;
                                    var parentRidIndex = fieldNamesDict[parentPrefix].Item1[fieldNamesDict[parentPrefix].Item2.GetIndexOf(rel.ParentKey)];
                                    var childRidIndex = fieldNamesDict[joinTables[rel.ChildIndex].Prefix].Item1[fieldNamesDict[joinTables[rel.ChildIndex].Prefix].Item2.GetIndexOf(ridCol)];

                                    constructionCache[aglistId] = new AggregateListCachedMetadata {
                                        UlType = ulType,
                                        ParentRIDIndex = parentRidIndex,
                                        ChildRIDIndex = childRidIndex,
                                        List = li,
                                        AddMethod = addMethod
                                    };
                                }
                                var metadata = (AggregateListCachedMetadata)constructionCache[aglistId];

                                string parentRid = (string) ReflectionTool.DbDeNull(reader[metadata.ParentRIDIndex]);
                                string childRid = (string) ReflectionTool.DbDeNull(reader[metadata.ChildRIDIndex]);
                                string childRidCacheId = cacheId(reader, metadata.ChildRIDIndex, metadata.UlType);
                                object newObj;
                                if (parentRid == null || childRid == null) {
                                    continue;
                                }
                                bool isUlNew = false;

                                if (constructionCache.ContainsKey(childRidCacheId)) {
                                    newObj = constructionCache[childRidCacheId];
                                } else {
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
                                String fieldAlias = rel.NewName ?? joinTables[rel.ChildIndex].Alias;
                                //transaction.Benchmarker.Mark($"Aggregate Object Enter Item {myPrefix}::{fieldAlias}");
                                var ulType = ObjectTypeCache[t][fieldAlias];
                                if (ulType == null) {
                                    continue;
                                }
                                var ridCol = FiTechBDadosExtensions.RidColumnOf[ulType];
                                var parentRidCol = joinTables[rel.ParentIndex].Prefix + "_" + rel.ParentKey;
                                var parentRidIndex = fieldNamesDict[joinTables[rel.ParentIndex].Prefix].Item1[fieldNamesDict[joinTables[rel.ParentIndex].Prefix].Item2.GetIndexOf(rel.ParentKey)];
                                var childRidCol = joinTables[rel.ChildIndex].Prefix + "_" + ridCol;
                                var childRidIndex = fieldNamesDict[joinTables[rel.ChildIndex].Prefix].Item1[fieldNamesDict[joinTables[rel.ChildIndex].Prefix].Item2.GetIndexOf(ridCol)];

                                string parentRid = ReflectionTool.DbDeNull(reader[parentRidIndex]) as string;
                                string childRid = ReflectionTool.DbDeNull(reader[childRidIndex]) as string;
                                string childRidCacheId = cacheId(reader, childRidIndex, ulType);
                                object newObj = null;
                                if (parentRid == null || childRid == null) {
                                    continue;
                                }
                                bool isUlNew = false;

                                if (constructionCache.ContainsKey(childRidCacheId)) {
                                    newObj = constructionCache[childRidCacheId];
                                } else {
                                    newObj = NewInstance(ulType);
                                    constructionCache[childRidCacheId] = newObj;
                                    isUlNew = true;
                                }
                                ReflectionTool.SetValue(obj, fieldAlias, newObj);

                                if (isUlNew || joinRelations[rel.ChildIndex].Any(x => x.AggregateBuildOption == AggregateBuildOptions.AggregateList)) {
                                    BuildAggregateObject(transaction, ulType, reader, newObj, fieldNamesDict, joinTables, joinRelations, rel.ChildIndex, isUlNew, constructionCache, recDepth + 1);
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
            var ridcol = FiTechBDadosExtensions.RidColumnOf[typeof(T)];
            if (Debugger.IsAttached && string.IsNullOrEmpty(ridcol)) {
                Debugger.Break();
            }

            transaction?.Benchmarker?.Mark($"Executing query for AggregateListDirect<{typeof(T).Name}>");
            using (var reader = await command.ExecuteReaderAsync(CommandBehavior.SingleResult | CommandBehavior.KeyInfo, transaction.CancellationToken).ConfigureAwait(false)) {
                transaction?.Benchmarker?.Mark("Prepare caches");
                Dictionary<string, (int[], string[])> fieldNamesDict;
                Benchmarker.Assert(() => join != null);
                var jstr = String.Intern(join.ToString());
                lock (jstr) {
                    if (!_autoAggregateCache.ContainsKey(jstr)) {
                        // This is only ever used in the auto aggregations
                        // So it would be a waste of processing power to reflect these fieldNames and their indexes every time
                        var fieldNames = new string[reader.FieldCount];
                        for (int i = 0; i < fieldNames.Length; i++)
                            fieldNames[i] = reader.GetName(i);
                        int idx = 0;
                        // With this I make a reusable cache and reduce the "per-object" field 
                        // probing when copying values from the reader
                        var newEntryGrp = fieldNames.Select<string, (string, int, string)>(name => {
                            var prefix = name.Split('_')[0].ToLower();
                            return (prefix, idx++, name.Replace($"{prefix}_", ""));
                        })
                        .Where(i => i.Item1 != null)
                        .GroupBy(i => i.Item1);
                        var newEntry = newEntryGrp.ToDictionary(i => i.First().Item1, i => (i.Select(j => j.Item2).ToArray(), i.Select(j => j.Item3).ToArray()));

                        _autoAggregateCache.Add(jstr, newEntry);
                    }
                }
                Benchmarker.Assert(() => _autoAggregateCache.ContainsKey(jstr));
                fieldNamesDict = _autoAggregateCache[jstr];

                var cachedRelations = new SelfInitializerDictionary<int, Relation[]>(rel => {
                    return joinRelations.Where(a => a.ParentIndex == rel).ToArray();
                });

                var myRidCol = $"{myPrefix}_{ridcol}";
                bool isNew;
                var constructionCache = new Dictionary<string, object>();
                if (myRidCol == null && Debugger.IsAttached) {
                    Debugger.Break();
                }

                transaction?.Benchmarker?.Mark("Enter Build Result");
                int row = 0;
                int objs = 0;
                T currentObject = default(T);
                while (await reader.ReadAsync(transaction.CancellationToken).ConfigureAwait(false)) {
                    //transaction.Benchmarker.Mark($"Enter result row {row}");
                    isNew = true;
                    T iterationObject;
                    var thisCacheId = cacheId(reader, myRidCol, typeof(T));
                    if (!constructionCache.ContainsKey(thisCacheId)) {

                        iterationObject = new T();

                        if (currentObject != null) {
                            objs++;
                            yield return currentObject;
                        }
                        constructionCache.Clear();
                        currentObject = iterationObject;
                        constructionCache[thisCacheId] = iterationObject;
                    } else {
                        iterationObject = (T)constructionCache[thisCacheId];
                        isNew = false;
                    }

                    var objArray = new object[reader.FieldCount];
                    reader.GetValues(objArray);

                    BuildAggregateObject(transaction, typeof(T), objArray, iterationObject, fieldNamesDict, joinTables, cachedRelations, thisIndex, isNew, constructionCache, 0);

                    //transaction.Benchmarker.Mark($"End result row {row}");
                    row++;
                }
                if(currentObject != null) {
                    objs++;
                    yield return currentObject;
                }
                var elaps = transaction?.Benchmarker?.Mark($"[{accessId}] Built List Size: {objs} / {row} rows");
                transaction?.Benchmarker?.Mark($"[{accessId}] Avg Build speed: {((double)elaps / (double)objs).ToString("0.00")}ms/obj | {((double)elaps / (double)row).ToString("0.00")}ms/row");

                transaction?.Benchmarker?.Mark("Clear cache");
                constructionCache.Clear();
            }
        }

        public async Task<List<T>> BuildAggregateListDirectAsync<T>(BDadosTransaction transaction, DbCommand command, JoinDefinition join, int thisIndex, object overrideContext) where T : IDataObject, new() {
            List<T> retv = new List<T>();
            var myPrefix = join.Joins[thisIndex].Prefix;
            var joinTables = join.Joins;
            var joinRelations = join.Relations.ToArray();
            var ridcol = FiTechBDadosExtensions.RidColumnOf[typeof(T)];
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
                Dictionary<string, (int[], string[])> fieldNamesDict;
                Benchmarker.Assert(() => join != null);
                var jstr = String.Intern(join.ToString());
                lock (jstr) {
                    if (!_autoAggregateCache.ContainsKey(jstr)) {
                        // This is only ever used in the auto aggregations
                        // So it would be a waste of processing power to reflect these fieldNames and their indexes every time
                        var fieldNames = new string[reader.FieldCount];
                        for (int i = 0; i < fieldNames.Length; i++)
                            fieldNames[i] = reader.GetName(i);
                        int idx = 0;
                        // With this I make a reusable cache and reduce the "per-object" field 
                        // probing when copying values from the reader
                        var newEntryGrp = fieldNames.Select<string, (string, int, string)>(name => {
                            var prefix = name.Split('_')[0].ToLower();
                            return (prefix, idx++, name.Replace($"{prefix}_", ""));
                        })
                        .Where(i => i.Item1 != null)
                        .GroupBy(i => i.Item1);
                        var newEntry = newEntryGrp.ToDictionary(i => i.First().Item1, i => (i.Select(j => j.Item2).ToArray(), i.Select(j => j.Item3).ToArray()));

                        _autoAggregateCache.Add(jstr, newEntry);
                    }
                }
                Benchmarker.Assert(() => _autoAggregateCache.ContainsKey(jstr));
                fieldNamesDict = _autoAggregateCache[jstr];

                var cachedRelations = new SelfInitializerDictionary<int, Relation[]>(rel => {
                    return joinRelations.Where(a => a.ParentIndex == rel).ToArray();
                });

                var myRidCol = $"{myPrefix}_{ridcol}";
                bool isNew;
                var constructionCache = new Dictionary<string, object>();
                if (myRidCol == null && Debugger.IsAttached) {
                    Debugger.Break();
                }
                constructionCache.Add(myRidCol, new Dictionary<string, object>());
                transaction?.Benchmarker?.Mark("Enter Build Result");
                int row = 0;

                var implementsAfterLoad = CacheImplementsAfterLoad[typeof(T)];
                var implementsAfterAggregateLoad = CacheImplementsAfterAggregateLoad[typeof(T)];
                WorkQueuer afterLoads = implementsAfterLoad || implementsAfterAggregateLoad ? new WorkQueuer("AfterLoads") : null;
                while (await reader.ReadAsync(transaction.CancellationToken).ConfigureAwait(false)) {
                    //transaction.Benchmarker.Mark($"Enter result row {row}");
                    isNew = true;
                    T newObj;
                    if (!constructionCache.ContainsKey(cacheId(reader, myRidCol, typeof(T)))) {
                        newObj = new T();
                        constructionCache[cacheId(reader, myRidCol, typeof(T))] = newObj;
                        if(implementsAfterLoad || implementsAfterAggregateLoad) {
                            afterLoads.Enqueue(async () => {
                                if(implementsAfterLoad) {
                                    ((IBusinessObject)newObj).OnAfterLoad(dlc);
                                }

                                if(implementsAfterAggregateLoad) {
                                    await ((IBusinessObject<T>)newObj).OnAfterAggregateLoadAsync(dlc).ConfigureAwait(false);
                                }
                            });
                        }
                        retv.Add(newObj);
                    } else {
                        newObj = (T)constructionCache[cacheId(reader, myRidCol, typeof(T))];
                        isNew = false;
                    }

                    var objArray = new object[reader.FieldCount];
                    reader.GetValues(objArray);
                    BuildAggregateObject(transaction, typeof(T), objArray, newObj, fieldNamesDict, joinTables, cachedRelations, thisIndex, isNew, constructionCache, 0);

                    //transaction.Benchmarker.Mark($"End result row {row}");
                    row++;
                }

                if(afterLoads != null) {
                    await afterLoads.Stop(true).ConfigureAwait(false);
                }
                var elaps = transaction?.Benchmarker?.Mark($"[{accessId}] Built List Size: {retv.Count} / {row} rows");
                transaction?.Benchmarker?.Mark($"[{accessId}] Avg Build speed: {((double)elaps / (double)retv.Count).ToString("0.00")}ms/item | {((double)elaps / (double)row).ToString("0.00")}ms/row");

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

        public List<T> BuildAggregateListDirect<T>(BDadosTransaction transaction, IDbCommand command, JoinDefinition join, int thisIndex, object overrideContext) where T : IDataObject, new() {
            List<T> retv = new List<T>();
            var myPrefix = join.Joins[thisIndex].Prefix;
            var joinTables = join.Joins;
            var joinRelations = join.Relations.ToArray();
            var ridcol = FiTechBDadosExtensions.RidColumnOf[typeof(T)];
            if (Debugger.IsAttached && string.IsNullOrEmpty(ridcol)) {
                Debugger.Break();
            }
            lock (command) {
                transaction?.Benchmarker?.Mark($"Executing query for AggregateListDirect<{typeof(T).Name}>");
                using (var reader = command.ExecuteReader(CommandBehavior.SingleResult | CommandBehavior.KeyInfo)) {
                    transaction?.Benchmarker?.Mark("Prepare caches");
                    Dictionary<string, (int[], string[])> fieldNamesDict;
                    Benchmarker.Assert(() => join != null);
                    var jstr = String.Intern(join.ToString());
                    lock (jstr) {
                        if (!_autoAggregateCache.ContainsKey(jstr)) {
                            // This is only ever used in the auto aggregations
                            // So it would be a waste of processing power to reflect these fieldNames and their indexes every time
                            var fieldNames = new string[reader.FieldCount];
                            for (int i = 0; i < fieldNames.Length; i++)
                                fieldNames[i] = reader.GetName(i);
                            int idx = 0;
                            // With this I make a reusable cache and reduce the "per-object" field 
                            // probing when copying values from the reader
                            var newEntryGrp = fieldNames.Select<string, (string, int, string)>(name => {
                                var prefix = name.Split('_')[0].ToLower();
                                return (prefix, idx++, name.Replace($"{prefix}_", ""));
                            })
                            .Where(i => i.Item1 != null)
                            .GroupBy(i => i.Item1);
                            var newEntry = newEntryGrp.ToDictionary(i => i.First().Item1, i => (i.Select(j => j.Item2).ToArray(), i.Select(j => j.Item3).ToArray()));

                            _autoAggregateCache.Add(jstr, newEntry);
                        }
                    }
                    Benchmarker.Assert(() => _autoAggregateCache.ContainsKey(jstr));
                    fieldNamesDict = _autoAggregateCache[jstr];

                    var cachedRelations = new SelfInitializerDictionary<int, Relation[]>(rel => {
                        return joinRelations.Where(a => a.ParentIndex == rel).ToArray();
                    });

                    var myRidCol = $"{myPrefix}_{ridcol}";
                    bool isNew;
                    var constructionCache = new Dictionary<string, object>();
                    if (myRidCol == null && Debugger.IsAttached) {
                        Debugger.Break();
                    }
                    constructionCache.Add(myRidCol, new Dictionary<string, object>());
                    transaction?.Benchmarker?.Mark("Enter Build Result");
                    int row = 0;
                    while (reader.Read()) {
                        //transaction.Benchmarker.Mark($"Enter result row {row}");
                        isNew = true;
                        T newObj;
                        if (!constructionCache.ContainsKey(cacheId(reader, myRidCol, typeof(T)))) {
                            newObj = new T();
                            constructionCache[cacheId(reader, myRidCol, typeof(T))] = newObj;
                            retv.Add(newObj);
                        } else {
                            newObj = (T)constructionCache[cacheId(reader, myRidCol, typeof(T))];
                            isNew = false;
                        }

                        var objArray = new object[reader.FieldCount];
                        reader.GetValues(objArray);
                        BuildAggregateObject(transaction, typeof(T), objArray, newObj, fieldNamesDict, joinTables, cachedRelations, thisIndex, isNew, constructionCache, 0);

                        //transaction.Benchmarker.Mark($"End result row {row}");
                        row++;
                    }
                    var elaps = transaction?.Benchmarker?.Mark($"[{accessId}] Built List Size: {retv.Count} / {row} rows");
                    transaction?.Benchmarker?.Mark($"[{accessId}] Avg Build speed: {((double)elaps / (double)retv.Count).ToString("0.00")}ms/item | {((double)elaps / (double)row).ToString("0.00")}ms/row");

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
                    ((IBusinessObject<T>) a).OnAfterAggregateLoadAsync(dlc).ConfigureAwait(false).GetAwaiter().GetResult();
                }
            }

            if(CacheImplementsAfterLoad[typeof(T)]) {
                foreach (var a in retv) {
                    ((IBusinessObject)a).OnAfterLoad(dlc);
                }
            }

            transaction?.Benchmarker?.Mark("Build process finished");
            return retv;
        }

        private List<IDataObject> BuildStateUpdateQueryResult(BDadosTransaction transaction, IDataReader reader, List<Type> workingTypes, Dictionary<Type, MemberInfo[]> fields) {
            var retv = new List<IDataObject>();
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
                retv.Add((IDataObject) instance);
            }

            transaction?.Benchmarker?.Mark("Build process finished");
            return retv;
        }
    }
}