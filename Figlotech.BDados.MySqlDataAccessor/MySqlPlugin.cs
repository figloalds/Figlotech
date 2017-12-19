using Figlotech.BDados.DataAccessAbstractions;
using MySql.Data.MySqlClient;
using System;
using System.Data;
using System.Collections.Generic;
using Figlotech.Core.Helpers;
using System.Linq;
using Figlotech.Core;
using System.Reflection;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using Figlotech.BDados;
using System.Text.RegularExpressions;

namespace Figlotech.BDados.MySqlDataAccessor {
    public class MySqlPlugin : IRdbmsPluginAdapter {
        public MySqlPlugin(MySqlPluginConfiguration cfg) {
            Config = cfg;
        }

        public MySqlPlugin() {

        }

        IQueryGenerator queryGenerator = new MySqlQueryGenerator();
        public IQueryGenerator QueryGenerator => queryGenerator;

        public MySqlPluginConfiguration Config { get; set; }

        public bool ContinuousConnection => Config.ContinuousConnection;

        public int CommandTimeout => Config.Timeout;

        public string SchemaName => Config.Database;

        public IDbConnection GetNewConnection() {
            return new MySqlConnection(Config.GetConnectionString());
        }

        static long idGen = 0;
        long myId = ++idGen;

        public void BuildAggregateObject(
            Type t, IDataReader reader, ObjectReflector refl, 
            object obj, string[] fieldNames, JoinDefinition join, 
            int thisIndex, bool isNew,
            Dictionary<string, object> constructionCache) {
            var myPrefix = join.Joins[thisIndex].Prefix;
            refl.Slot(obj);
            if (isNew) {
                for (int i = 0; i < fieldNames.Length; i++) {
                    if(fieldNames[i].StartsWith($"{myPrefix}_")) {
                        refl[fieldNames[i].Substring(myPrefix.Length+1)] = reader.GetValue(i);
                    }
                }
            }

            var relations = join.Relations.Where(a=> a.ParentIndex == thisIndex);
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
                                var newLi = Activator.CreateInstance(objectType);
                                refl[fieldAlias] = newLi;
                            }
                            var li = refl[fieldAlias];
                            var ridCol = Fi.Tech.GetRidColumn(ulType);
                            string parentRid = ReflectionTool.DbDeNull(reader[join.Joins[rel.ParentIndex].Prefix + "_" + rel.ParentKey]) as string;
                            string childRid = ReflectionTool.DbDeNull(reader[join.Joins[rel.ChildIndex].Prefix + "_" + ridCol]) as string;
                            object newObj;
                            if(parentRid == null || childRid == null) {
                                continue;
                            }
                            bool isUlNew = false;
                            if (constructionCache.ContainsKey(childRid)) {
                                newObj = constructionCache[childRid];
                            } else {
                                newObj = Activator.CreateInstance(ulType);
                                addMethod.Invoke(li, new object[] { newObj });
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
                            if (parentRid == null || childRid == null) {
                                continue;
                            }
                            bool isUlNew = false;
                            if (constructionCache.ContainsKey(childRid)) {
                                newObj = constructionCache[childRid];
                            } else {
                                newObj = Activator.CreateInstance(ulType);
                                refl[fieldAlias] = newObj;
                                isUlNew = true;
                            }
                            BuildAggregateObject(ulType, reader, new ObjectReflector(), newObj, fieldNames, join, rel.ChildIndex, isUlNew, constructionCache);
                            break;
                        }
                }
            }
        }

        public List<T> BuildAggregateListDirect<T>(ConnectionInfo transaction, IDbCommand command, JoinDefinition join, int thisIndex) where T : IDataObject, new() {
            List<T> retv = new List<T>();
            var myPrefix = join.Joins[thisIndex].Prefix;
            var ridcol = Fi.Tech.GetRidColumn<T>();
            join.Relations = ValidateRelations(join);
            transaction?.Benchmarker?.Mark("Execute Query");
            using (var reader = (command as MySqlCommand).ExecuteReader()) {
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

                        BuildAggregateObject(typeof(T), reader, new ObjectReflector(), obj, fieldNames, join, thisIndex, isNew, constructionCache);
                    }
                    constructionCache.Clear();
                    transaction?.Benchmarker?.Mark("--");
                }
            }

            return retv;
        }

        internal List<Relation> ValidateRelations(JoinDefinition _join) {
            for (int i = 1; i < _join.Joins.Count; ++i) {
                Match m = Regex.Match(_join.Joins[i].Args, @"(?<PreA>\w+)\.(?<KeyA>\w+)=(?<PreB>\w+).(?<KeyB>\w+)");
                if (!m.Success) {
                    _join.Joins.Clear();
                    throw new BDadosException($"Join {i + 1} doesn't have an objective relation to any other joined object.");
                }
                int IndexA = _join.Joins.IndexOf((from a in _join.Joins where a.Prefix == m.Groups["PreA"].Value select a).First());
                int IndexB = _join.Joins.IndexOf((from b in _join.Joins where b.Prefix == m.Groups["PreB"].Value select b).First());
                if (IndexA < 0 || IndexB < 0) {
                    _join.Relations.Clear();
                    throw new BDadosException($"Arguments '{_join.Joins[i].Args}' in join {i + 1} aren't valid for this function.");
                }
                String ChaveA = m.Groups["KeyA"].Value;
                String ChaveB = m.Groups["KeyB"].Value;
                if (ChaveA == null || ChaveB == null) {
                    throw new BDadosException(String.Format("Invalid Relation {0}", _join.Joins[i].Args));
                }

                if (!Fi.Tech.FindColumn(ChaveA, _join.Joins[IndexA].ValueObject) || !Fi.Tech.FindColumn(ChaveB, _join.Joins[IndexB].ValueObject)) {
                    _join.Relations.Clear();
                    throw new BDadosException(String.Format("Column {0} specified doesn't exist in '{1} AS {2}'", ChaveA, _join.Joins[i].TableName, _join.Joins[i].Prefix));
                }
            }
            for (int i = 0; i < _join.Relations.Count; i++) {
                if (_join.Relations[i].ParentIndex < 0 || _join.Relations[i].ParentIndex > _join.Joins.Count - 1)
                    throw new BDadosException(String.Format("One of the specified relations is not valid."));
                if (_join.Relations[i].ChildIndex < 0 || _join.Relations[i].ChildIndex > _join.Joins.Count - 1)
                    throw new BDadosException(String.Format("One of the specified relations is not valid."));
            }
            return _join.Relations;
        }

        public List<T> GetObjectList<T>(IDbCommand command) where T : new() {
            var refl = new ObjectReflector();
            List<T> retv = new List<T>();
            using (var reader = (command as MySqlCommand).ExecuteReader()) {
                var cols = new string[reader.FieldCount];
                for (int i = 0; i < cols.Length; i++)
                    cols[i] = reader.GetName(i);

                if (reader.HasRows) {
                    while (reader.Read()) {
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
            using (var reader = (command as MySqlCommand).ExecuteReader()) {
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
            Config = new MySqlPluginConfiguration();
            ObjectReflector o = new ObjectReflector(Config);
            foreach (var a in settings) {
                o[a.Key] = a.Value;
            }
        }
    }
}
