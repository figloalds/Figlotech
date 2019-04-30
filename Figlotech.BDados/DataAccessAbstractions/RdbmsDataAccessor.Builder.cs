using Figlotech.Core;
using Figlotech.Core.BusinessModel;
using Figlotech.Core.Helpers;
using Figlotech.Core.Interfaces;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Reflection;

namespace Figlotech.BDados.DataAccessAbstractions {
    public partial class RdbmsDataAccessor : IRdbmsDataAccessor, IDisposable  {

        public List<T> GetObjectList<T>(ConnectionInfo transaction, IDbCommand command) where T : new() {
            var refl = new ObjectReflector();
            lock (command) {
                transaction?.Benchmarker?.Mark($"[{accessId}] Execute Query");
                using (var reader = command.ExecuteReader()) {
                    var cols = new string[reader.FieldCount];
                    for (int i = 0; i < cols.Length; i++)
                        cols[i] = reader.GetName(i);
                    transaction?.Benchmarker?.Mark($"[{accessId}] Build retv List");
                    return Fi.Tech.MapFromReader<T>(reader).ToList();
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

        public void BuildAggregateObject(ConnectionInfo transaction,
            Type t, IDataReader reader, ObjectReflector refl,
            object obj, Dictionary<string, (int[], string[])> fieldNamesDict, JoiningTable[] joinTables, IDictionary<int, Relation[]> joinRelations,
            int thisIndex, bool isNew,
            Dictionary<string, object> constructionCache, int recDepth) {
            var myPrefix = joinTables[thisIndex].Prefix.ToLower();
            refl.Slot(obj);
            if (isNew) {
                //transaction.Benchmarker.Mark($"Enter Build Object {myPrefix}");
                if(!fieldNamesDict.ContainsKey(myPrefix)) {
                    throw new BDadosException($"Failed to build aggregate list: Aggregated reference {myPrefix} of type {joinTables[thisIndex].ValueObject.Name} was not present in the resulting fields");
                }
                var fieldNames = fieldNamesDict[myPrefix];
                for (int i = 0; i < fieldNames.Item1.Length; i++) {
                    var val = reader.GetValue(fieldNames.Item1[i]);
                    string kind = "";
                    if (val is DateTime dt && dt.Kind != DateTimeKind.Utc) {
                        kind = dt.Kind.ToString();
                        // I reluctantly admit that I'm using this horrible gimmick
                        // It pains my soul to do this, because the MySQL Connector doesn't support
                        // Timezone field in the connection string
                        // And besides, this is code is supposed to be abstract for all ADO Plugins
                        // But I'll go with "If it isn't UTC, then you're saving dates wrong"
                        val = new DateTime(dt.Ticks, DateTimeKind.Utc);
                    }
                    refl[fieldNames.Item2[i]] = val;
                }
                //transaction.Benchmarker.Mark($"End Build Object {myPrefix}");
            }

            var relations = joinRelations[thisIndex];
            //if(!isNew) {
            //    relations = relations.Where(r => r.AggregateBuildOption == AggregateBuildOptions.AggregateList);
            //}
            //transaction.Benchmarker.Mark($"Enter Relations for {myPrefix}");
            foreach (var rel in relations) {
                switch (rel.AggregateBuildOption) {
                    // Aggregate fields are the beautiful easy ones to deal
                    case AggregateBuildOptions.AggregateField: {
                            String childPrefix = joinTables[rel.ChildIndex].Prefix;
                            var value = reader[childPrefix + "_" + rel.Fields[0]];
                            String name = rel.NewName ?? (childPrefix + "_" + rel.Fields[0]);
                            refl[name] = reader[childPrefix + "_" + rel.Fields[0]];

                            //transaction.Benchmarker.Mark($"Aggregate Field {myPrefix}::{name}");
                            break;
                        }
                    // this one is RAD and the most cpu intensive
                    // Sure needs optimization.
                    case AggregateBuildOptions.AggregateList: {
                            String fieldAlias = rel.NewName ?? joinTables[rel.ChildIndex].Alias;
                            var objectType = ObjectTypeCache[t][fieldAlias];
                            var ulType = UlTypeCache[objectType];
                            var addMethod = AddMethodCache[objectType];
                            if (addMethod == null)
                                continue;

                            if (refl[fieldAlias] == null) {
                                var newLi = Activator.CreateInstance(ulType, new object[] { });
                                refl[fieldAlias] = newLi;
                            }
                            var li = refl[fieldAlias];
                            if(li == null) {
                                continue;
                            }
                            var ridCol = FiTechBDadosExtensions.RidColumnOf[ulType];
                            var childRidCol = joinTables[rel.ChildIndex].Prefix + "_" + ridCol;
                            string parentRid = ReflectionTool.DbDeNull(reader[joinTables[rel.ParentIndex].Prefix + "_" + rel.ParentKey]) as string;
                            string childRid = ReflectionTool.DbDeNull(reader[childRidCol]) as string;
                            object newObj;
                            if (parentRid == null || childRid == null) {
                                continue;
                            }
                            bool isUlNew = false;
                            //if (!constructionCache.ContainsKey(childRidCol))
                            //    constructionCache.Add(childRidCol, new Dictionary<string, object>());
                            if (constructionCache.ContainsKey(childRid)) {
                                newObj = constructionCache[childRid];
                            } else {
                                newObj = Activator.CreateInstance(ulType, new object[] { });
                                addMethod.Invoke(li, new object[] { newObj });
                                constructionCache[childRid] = newObj;
                                isUlNew = true;
                            }

                            //transaction.Benchmarker.Mark($"Aggregate List Enter Item {myPrefix}::{childRid}");
                            BuildAggregateObject(transaction, ulType, reader, new ObjectReflector(), newObj, fieldNamesDict, joinTables, joinRelations, rel.ChildIndex, isUlNew, constructionCache, recDepth + 1);

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
                            var childRidCol = joinTables[rel.ChildIndex].Prefix + "_" + ridCol;
                            string parentRid = ReflectionTool.DbDeNull(reader[joinTables[rel.ParentIndex].Prefix + "_" + rel.ParentKey]) as string;
                            string childRid = ReflectionTool.DbDeNull(reader[childRidCol]) as string;
                            object newObj = null;
                            if (parentRid == null || childRid == null) {
                                continue;
                            }
                            bool isUlNew = false;
                            if (!constructionCache.ContainsKey(childRidCol))
                                constructionCache.Add(childRidCol, new Dictionary<string, object>());

                            if (constructionCache.ContainsKey(childRid)) {
                                newObj = constructionCache[childRid];
                            } else {
                                newObj = Activator.CreateInstance(ulType, new object[] { });
                                constructionCache[childRid] = newObj;
                                isUlNew = true;
                            }
                            refl[fieldAlias] = newObj;

                            BuildAggregateObject(transaction, ulType, reader, new ObjectReflector(), newObj, fieldNamesDict, joinTables, joinRelations, rel.ChildIndex, isUlNew, constructionCache, recDepth + 1);
                            break;
                        }
                }
            }
            //transaction.Benchmarker.Mark($"End Relations for {myPrefix}");
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

        static Dictionary<JoinDefinition, Dictionary<string, (int[], string[])>> _autoAggregateCache = new Dictionary<JoinDefinition, Dictionary<string, (int[], string[])>>();

        public List<T> BuildAggregateListDirect<T>(ConnectionInfo transaction, IDbCommand command, JoinDefinition join, int thisIndex, object overrideContext) where T : IDataObject, new() {
            List<T> retv = new List<T>();
            var myPrefix = join.Joins[thisIndex].Prefix;
            var joinTables = join.Joins.ToArray();
            var joinRelations = join.Relations.ToArray();
            var ridcol = FiTechBDadosExtensions.RidColumnOf[typeof(T)];
            transaction?.Benchmarker?.Mark("Execute Query");
            lock (command) {
                using (var reader = command.ExecuteReader()) {
                    transaction?.Benchmarker?.Mark("Prepare caches");
                    Dictionary<string, (int[], string[])> fieldNamesDict;
                    lock(join) {
                        if (!_autoAggregateCache.ContainsKey(join)) {
                            // This is only ever used in the auto aggregations
                            // So it would be a waste of processing power to reflect these fieldNames and their indexes every time
                            var fieldNames = new string[reader.FieldCount];
                            for (int i = 0; i < fieldNames.Length; i++)
                                fieldNames[i] = reader.GetName(i);
                            int idx = 0;
                            // With this I make a reusable cache and reduce the "per-object" field 
                            // probing when copying values from the reader
                            var newEntry = fieldNames.Select<string, (string, int, string)>(name => {
                                var prefix = name.Split('_')[0].ToLower();
                                return (prefix, idx++, name.Replace($"{prefix}_", ""));
                            })
                            .GroupBy(i => i.Item1)
                            .ToDictionary(i => i.First().Item1, i => (i.Select(j => j.Item2).ToArray(), i.Select(j => j.Item3).ToArray()));
                            _autoAggregateCache.Add(join, newEntry);
                        }
                    }
                    Benchmarker.Assert(()=> _autoAggregateCache.ContainsKey(join));
                    fieldNamesDict = _autoAggregateCache[join];

                    var cachedRelations = new SelfInitializerDictionary<int, Relation[]>(rel => {
                        return joinRelations.Where(a => a.ParentIndex == rel).ToArray();
                    });
                    
                    var myRidCol = $"{myPrefix}_{ridcol}";
                    bool isNew;
                    var constructionCache = new Dictionary<string, object>();
                    constructionCache.Add(myRidCol, new Dictionary<string, object>());
                    transaction?.Benchmarker?.Mark("Enter Build Result");
                    int row = 0;
                    while (reader.Read()) {
                        //transaction.Benchmarker.Mark($"Enter result row {row}");
                        isNew = true;
                        T newObj;
                        if (!constructionCache.ContainsKey(reader[myRidCol] as string)) {
                            newObj = new T();
                            constructionCache[reader[myRidCol] as string] = newObj;
                            retv.Add(newObj);
                        } else {
                            newObj = (T)constructionCache[reader[myRidCol] as string];
                            isNew = false;
                        }

                        BuildAggregateObject(transaction, typeof(T), reader, new ObjectReflector(), newObj, fieldNamesDict, joinTables, cachedRelations, thisIndex, isNew, constructionCache, 0);

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

            if (retv.Any() && retv.First() is IBusinessObject<T> ibo2) {
                ibo2.OnAfterListAggregateLoad(dlc, retv);
            }
            foreach (var a in retv) {
                if (a is IBusinessObject<T> ibo) {
                    ibo.OnAfterAggregateLoad(dlc);
                }
            }

            foreach (var a in retv) {
                if (a is IBusinessObject ibo) {
                    ibo.OnAfterLoad(dlc);
                }
            }

            transaction?.Benchmarker?.Mark("Build process finished");
            return retv;
        }

        private List<IDataObject> BuildStateUpdateQueryResult(ConnectionInfo transaction, IDataReader reader, List<Type> workingTypes, Dictionary<Type, MemberInfo[]> fields) {
            var retv = new List<IDataObject>();
            transaction?.Benchmarker?.Mark("Init Build Result");
            ObjectReflector refl = new ObjectReflector();
            while(reader.Read()) {
                var typename = reader["TypeName"] as String;
                var type = workingTypes.FirstOrDefault(wt => wt.Name == typename);
                if(type == null) {
                    continue;
                }

                var instance = Activator.CreateInstance(type);
                refl.Slot(instance);
                var tFields = fields[type];
                for(int i = 0; i < tFields.Length; i++) {
                    refl[tFields[i]] = reader[$"data_{i}"];
                }
                retv.Add(refl.Retrieve() as IDataObject);
            }

            transaction?.Benchmarker?.Mark("Build process finished");
            return retv;
        }
    }
}