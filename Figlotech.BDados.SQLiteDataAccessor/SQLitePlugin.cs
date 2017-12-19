using Figlotech.BDados.DataAccessAbstractions;
using Microsoft.Data.Sqlite;
using System;
using System.Data;
using System.Collections.Generic;
using Figlotech.Core.Helpers;
using System.Linq;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using System.Reflection;
using Figlotech.Core;

namespace Figlotech.BDados.SqliteDataAccessor {
    public class SqlitePlugin : IRdbmsPluginAdapter {
        public SqlitePlugin(SqlitePluginConfiguration cfg) {
            Config = cfg;
        }

        public SqlitePlugin() {
            
        }

        IQueryGenerator queryGenerator = new SqliteQueryGenerator();
        public IQueryGenerator QueryGenerator => queryGenerator;

        public SqlitePluginConfiguration Config { get; set; }

        public bool ContinuousConnection => true;

        public int CommandTimeout => throw new NotImplementedException();

        public string SchemaName => Config.Schema;

        public IDbConnection GetNewConnection() {
            return new SqliteConnection(Config.GetConnectionString());
        }


        static long idGen = 0;
        long myId = ++idGen;

        public void BuildAggregateObject(
            Type t, IDataReader reader, ObjectReflector refl,
            object obj, string[] fieldNames, JoinDefinition join,
            int thisIndex, bool isNew,
            Dictionary<string, object> constructionCache) {
            var myPrefix = join.Joins[thisIndex].Prefix;
            if (isNew) {
                refl.Slot(obj);
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
                            String nome = rel.NewName ?? (childPrefix + "_" + rel.Fields[0]);
                            refl[nome] = reader[childPrefix + "_" + rel.Fields[0]];

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
                                refl[fieldAlias] = Activator.CreateInstance(objectType);
                            }
                            var ridCol = Fi.Tech.GetRidColumn(ulType);
                            string parentRid = ReflectionTool.DbDeNull(reader[join.Joins[rel.ParentIndex].Prefix + "_" + rel.ParentKey]) as string;
                            string childRid = ReflectionTool.DbDeNull(reader[join.Joins[rel.ChildIndex].Prefix + "_" + ridCol]) as string;
                            object newObj;
                            if (parentRid == null) {
                                continue;
                            }
                            bool isUlNew = false;
                            if (constructionCache.ContainsKey(childRid)) {
                                newObj = constructionCache[childRid];
                            } else {
                                newObj = Activator.CreateInstance(t);
                                addMethod.Invoke(refl[fieldAlias], new object[] { newObj });
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
                            string parentRid = ReflectionTool.DbDeNull(reader[join.Joins[rel.ParentIndex].Prefix + "_" + rel.ParentKey]) as string;
                            string childRid = ReflectionTool.DbDeNull(reader[join.Joins[rel.ChildIndex].Prefix + "_" + ridCol]) as string;
                            object newObj;
                            if (parentRid == null) {
                                continue;
                            }
                            bool isUlNew = false;
                            if (constructionCache.ContainsKey(childRid)) {
                                newObj = constructionCache[childRid];
                            } else {
                                newObj = Activator.CreateInstance(t);
                                isUlNew = true;
                            }
                            BuildAggregateObject(ulType, reader, new ObjectReflector(), newObj, fieldNames, join, rel.ChildIndex, isUlNew, constructionCache);
                            break;
                        }
                }
            }
        }

        public List<T> BuildAggregateListDirect<T>(ConnectionInfo transaction, IDbCommand command, JoinDefinition join, int thisIndex) where T : IDataObject, new() {
            var refl = new ObjectReflector();
            List<T> retv = new List<T>();
            var myPrefix = join.Joins[thisIndex].Prefix;
            var ridcol = Fi.Tech.GetRidColumn<T>();
            transaction?.Benchmarker?.Mark("Execute Query");
            using (var reader = (command as SqliteCommand).ExecuteReader()) {
                transaction?.Benchmarker?.Mark("--");
                var fieldNames = new string[reader.FieldCount];
                for (int i = 0; i < fieldNames.Length; i++)
                    fieldNames[i] = reader.GetName(i);
                var myRidCol = fieldNames.FirstOrDefault(f => f == $"{myPrefix}_{ridcol}");
                if (reader.HasRows) {
                    bool isNew;
                    var constructionCache = new Dictionary<string, object>();
                    transaction?.Benchmarker?.Mark("Build Result");
                    while (reader.Read()) {
                        isNew = true;
                        T obj = retv.FirstOrDefault(o => o.RID == reader[myRidCol] as string);
                        if (obj == null) {
                            obj = new T();
                            retv.Add(obj);
                        } else {
                            isNew = false;
                        }

                        BuildAggregateObject(typeof(T), reader, refl, obj, fieldNames, join, thisIndex, isNew, constructionCache);
                    }
                    constructionCache.Clear();
                    transaction?.Benchmarker?.Mark("--");
                }
            }

            return retv;
        }


        public List<T> GetObjectList<T>(IDbCommand command) where T : new() {
            var refl = new ObjectReflector();
            List<T> retv = new List<T>();

            using (var reader = (command as SqliteCommand).ExecuteReader()) {
                var cols = new string[reader.FieldCount];
                for (int i = 0; i < cols.Length; i++)
                    cols[i] = reader.GetName(i);

                if (reader.HasRows) {
                    while(reader.Read()) {
                        T obj = new T();
                        refl.Slot(obj);
                        for (int i = 0; i < cols.Length; i++) {
                            refl[cols[i]] = reader.GetValue(i);
                        }
                        retv.Add((T)refl.Retrieve());
                    }
                }

            }
            return retv;
        }


        public DataSet GetDataSet(IDbCommand command) {
            //lock(command) {
            using (var reader = (command as SqliteCommand).ExecuteReader()) {
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
                //}
            }
        }

        public void SetConfiguration(IDictionary<string, object> settings) {
            Config = new SqlitePluginConfiguration();
            ObjectReflector o = new ObjectReflector(Config);
            foreach (var a in settings) {
                o[a.Key] = a.Value;
            }
        }
    }
}
