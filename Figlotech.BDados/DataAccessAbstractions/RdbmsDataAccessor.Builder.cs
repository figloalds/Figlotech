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
using System.Threading;
using System.Diagnostics;
using Figlotech.Core.Extensions;

namespace Figlotech.BDados.DataAccessAbstractions {
    public partial class RdbmsDataAccessor {

        public IList<T> GetObjectList<T>(IDbCommand command) where T : new() {
            var refl = new ObjectReflector();
            IList<T> retv = new List<T>();
            lock (command) {
                using (var reader = command.ExecuteReader()) {
                    var cols = new string[reader.FieldCount];
                    for (int i = 0; i < cols.Length; i++)
                        cols[i] = reader.GetName(i);

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

        public void BuildAggregateObject(
            Type t, IDataReader reader, ObjectReflector refl,
            object obj, string[] fieldNames, JoinDefinition join,
            int thisIndex, bool isNew,
            Dictionary<string, Dictionary<string, object>> constructionCache) {
            var myPrefix = join.Joins[thisIndex].Prefix;
            refl.Slot(obj);
            if (isNew) {
                for (int i = 0; i < fieldNames.Length; i++) {
                    if (fieldNames[i].StartsWith($"{myPrefix}_")) {
                        refl[fieldNames[i].Substring(myPrefix.Length + 1)] = reader.GetValue(i);
                    }
                }
            }

            var relations = join.Relations.Where(a => a.ParentIndex == thisIndex);
            foreach (var rel in relations) {
                switch (rel.AggregateBuildOption) {
                    // Aggregate fields are the beautiful easy ones to deal
                    case AggregateBuildOptions.AggregateField: {

                            String childPrefix = join.Joins[rel.ChildIndex].Prefix;
                            var value = reader[childPrefix + "_" + rel.Fields[0]];
                            String name = rel.NewName ?? (childPrefix + "_" + rel.Fields[0]);
                            refl[name] = reader[childPrefix + "_" + rel.Fields[0]];

                            break;
                        }
                    // this one is RAD and the most cpu intensive
                    // Sure needs optimization.
                    case AggregateBuildOptions.AggregateList: {
                            String fieldAlias = rel.NewName ?? join.Joins[rel.ChildIndex].Alias;
                            var objectType = ReflectionTool.GetTypeOf(
                                ReflectionTool.FieldsAndPropertiesOf(t)
                                .Where(m => m.Name == fieldAlias)
                                .FirstOrDefault());
                            var ulType = objectType
                                .GetGenericArguments().FirstOrDefault();
                            if (ulType == null) {
                                continue;
                            }
                            var addMethod = objectType.GetMethods()
                                .Where(m => m.Name == "Add")
                                .FirstOrDefault();
                            if (addMethod == null)
                                continue;

                            if (refl[fieldAlias] == null) {
                                var newLi = Activator.CreateInstance(ulType, new object[] { });
                                refl[fieldAlias] = newLi;
                            }
                            var li = refl[fieldAlias];
                            var ridCol = Fi.Tech.GetRidColumn(ulType);
                            var childRidCol = join.Joins[rel.ChildIndex].Prefix + "_" + ridCol;
                            string parentRid = ReflectionTool.DbDeNull(reader[join.Joins[rel.ParentIndex].Prefix + "_" + rel.ParentKey]) as string;
                            string childRid = ReflectionTool.DbDeNull(reader[childRidCol]) as string;
                            object newObj;
                            if (parentRid == null || childRid == null) {
                                continue;
                            }
                            bool isUlNew = false;
                            if (!constructionCache.ContainsKey(childRidCol))
                                constructionCache.Add(childRidCol, new Dictionary<string, object>());
                            if (constructionCache[childRidCol].ContainsKey(childRid)) {
                                newObj = constructionCache[childRidCol][childRid];
                            } else {
                                newObj = Activator.CreateInstance(ulType, new object[] { });
                                addMethod.Invoke(li, new object[] { newObj });
                                constructionCache[childRidCol][childRid] = newObj;
                                isUlNew = true;
                            }

                            BuildAggregateObject(ulType, reader, new ObjectReflector(), newObj, fieldNames, join, rel.ChildIndex, isUlNew, constructionCache);
                            break;
                        }

                    // this one is almost the same as previous one.
                    case AggregateBuildOptions.AggregateObject: {
                            String fieldAlias = rel.NewName ?? join.Joins[rel.ChildIndex].Alias;
                            var ulType = ReflectionTool.GetTypeOf(
                                ReflectionTool.FieldsAndPropertiesOf(t)
                                .Where((f) => f.GetCustomAttribute<AggregateObjectAttribute>() != null)
                                .Where(m => m.Name == fieldAlias)
                                .FirstOrDefault());
                            if (ulType == null) {
                                continue;
                            }
                            var ridCol = Fi.Tech.GetRidColumn(ulType);
                            var childRidCol = join.Joins[rel.ChildIndex].Prefix + "_" + ridCol;
                            string parentRid = ReflectionTool.DbDeNull(reader[join.Joins[rel.ParentIndex].Prefix + "_" + rel.ParentKey]) as string;
                            string childRid = ReflectionTool.DbDeNull(reader[childRidCol]) as string;
                            object newObj;
                            if (parentRid == null || childRid == null) {
                                continue;
                            }
                            bool isUlNew = false;
                            if (!constructionCache.ContainsKey(childRidCol))
                                constructionCache.Add(childRidCol, new Dictionary<string, object>());
                            if (constructionCache[childRidCol].ContainsKey(childRid)) {
                                newObj = constructionCache[childRidCol][childRid];
                            } else {
                                newObj = Activator.CreateInstance(ulType, new object[] { });

                                refl[fieldAlias] = newObj;
                                constructionCache[childRidCol][childRid] = newObj;
                                isUlNew = true;
                            }
                            BuildAggregateObject(ulType, reader, new ObjectReflector(), newObj, fieldNames, join, rel.ChildIndex, isUlNew, constructionCache);
                            break;
                        }
                }
            }
        }

        public IList<T> BuildAggregateListDirect<T>(ConnectionInfo transaction, IDbCommand command, JoinDefinition join, int thisIndex) where T : IDataObject, new() {
            IList<T> retv = new List<T>();
            var myPrefix = join.Joins[thisIndex].Prefix;
            var ridcol = Fi.Tech.GetRidColumn<T>();
            transaction?.Benchmarker?.Mark("Execute Query");
            lock (command) {
                using (var reader = command.ExecuteReader()) {
                    transaction?.Benchmarker?.Mark("--");
                    var fieldNames = new string[reader.FieldCount];
                    for (int i = 0; i < fieldNames.Length; i++)
                        fieldNames[i] = reader.GetName(i);
                    var myRidCol = fieldNames.FirstOrDefault(f => f == $"{myPrefix}_{ridcol}");
                    bool isNew;
                    var constructionCache = new Dictionary<string, Dictionary<string, object>>();
                    constructionCache.Add(myRidCol, new Dictionary<string, object>());
                    transaction?.Benchmarker?.Mark("Build Result");
                    while (reader.Read()) {
                        isNew = true;
                        T newObj;
                        if (!constructionCache[myRidCol].ContainsKey(reader[myRidCol] as string)) {
                            newObj = new T();
                            constructionCache[myRidCol][reader[myRidCol] as string] = newObj;
                            retv.Add(newObj);
                        } else {
                            newObj = (T)constructionCache[myRidCol][reader[myRidCol] as string];
                            isNew = false;
                        }

                        BuildAggregateObject(typeof(T), reader, new ObjectReflector(), newObj, fieldNames, join, thisIndex, isNew, constructionCache);
                    }
                    constructionCache.Clear();
                    transaction?.Benchmarker?.Mark("--");
                }
            }

            return retv;
        }
    }
}