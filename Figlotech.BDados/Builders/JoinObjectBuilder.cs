
/**
* Figlotech.BDados.Builders.JoinBuilder
* Default implementation for IJoinBuilder
* 
* @Author: Felype Rennan Alves dos Santos
* August/2014
* 
**/
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;

using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.DataAccessAbstractions.Attributes;
using Figlotech.Core.Helpers;
using Figlotech.Core.Interfaces;
using Figlotech.Core;
using Figlotech.Core.FileAcessAbstractions;
using Figlotech.BDados.Helpers;
using Figlotech.Core.BusinessModel;
using System.Threading.Tasks;
using System.Diagnostics;

namespace Figlotech.BDados.Builders {
    public class JoinObjectBuilder : IJoinBuilder
    {
        private JoinDefinition _join = new JoinDefinition();
        private BuildParametersHelper _buildParameters;
        //private IRdbmsDataAccessor _dataAccessor;
        //public delegate void JoinBuild(Join join);
        //public delegate void ParametrizeBuild(BuildParametersHelper parametros);

        ILogger _logger;
        ILogger Logger {
            get {
                return _logger = (_logger = new Logger(new FileAccessor("Logs")));
            }
        }

        public JoinObjectBuilder(Action<JoinDefinition> fn) {
            fn(_join);
            //_dataAccessor = dataAccessor;
            try {
                if (_join.GenerateRelations().Count < 1) {
                    throw new BDadosException($"This Join found no Relations.");
                }
            } catch (Exception x) {
                var tables = "";
                foreach (var a in _join.Joins) {
                    tables += $"{a.TableName} (AS {a.Alias})";
                    tables += a.Args != null ? $" (ON {a.Args})" : "";
                    tables += "\n";
                }
            }
        }

        public JoinDefinition GetJoin() {
            return _join;
        }

        public IQueryBuilder GenerateQuery(IQueryGenerator generator, IQueryBuilder conditions, MemberInfo orderingMember = null, OrderingType otype = OrderingType.Asc, int? p = null, int? limit = null, IQueryBuilder conditionsRoot = null) {
            return generator.GenerateJoinQuery(_join, conditions, p, limit, orderingMember, otype, conditionsRoot);
        }

        //public DataTable GenerateDataTable(ConnectionInfo transaction, IQueryGenerator generator, IQueryBuilder conditions, int? p = 1, int? limit = 200, MemberInfo orderingMember = null, OrderingType otype = OrderingType.Asc, IQueryBuilder conditionsRoot = null) {
        //    QueryBuilder query = (QueryBuilder)generator.GenerateJoinQuery(_join, conditions, p, limit, orderingMember, otype, conditionsRoot);
        //    DataTable dt = null;
        //    dt = _dataAccessor.Query(transaction, query);
        //    return dt;
        //}

        private List<IDataObject> BuildAggregateList(Type type, Object ParentVal, Relation relation, DataTable dt) {
            var method = GetType().GetMethods()
                .Where(m =>
                    m.Name == "BuildAggregateList" &&
                    m.GetGenericArguments().Length > 0)
                .FirstOrDefault();
            var retv = (IEnumerable<IDataObject>)
                method
                ?.MakeGenericMethod(type)
                .Invoke(this, new object[] { ParentVal, relation, dt });

            return new List<IDataObject>(retv).ToList();
        }

        public IList<T> BuildAggregateList<T>(Object ParentVal, Relation relation, DataTable dt) where T : IDataObject, new() {
            var type = typeof(T);
            int thisIndex = relation.ChildIndex;
            int parentIndex = relation.ParentIndex;
            IList<T> retv = new List<T>();
            var Relacoes = _join.Relations;
            String Prefix = _join.Joins[thisIndex].Prefix;
            String parentPrefix = _join.Joins[parentIndex].Prefix;
            string rid = FiTechBDadosExtensions.RidColumnOf[type];
            List<DataRow> rs = new List<DataRow>();
            List<object> ids = new List<object>();
            for (int i = 0; i < dt.Rows.Count; i++) {
                // 
                Object val = dt.Rows[i][Prefix + "_" + relation.ChildKey];
                if (val != null && val.ToString() == ParentVal.ToString()) {
                    object RID = dt.Rows[i][Prefix + $"_{rid}"];
                    if (!ids.Contains(RID)) {
                        rs.Add(dt.Rows[i]);
                        ids.Add(RID);
                    }
                }
            }
            MemberInfo[] fields = ReflectionTool.FieldsAndPropertiesOf(type)
                .Where(at =>
                    at.GetCustomAttribute<FieldAttribute>() != null
                )
                .ToArray();
            foreach (DataRow dr in rs) {
                T thisObject = new T();
                var objBuilder = new ObjectReflector(thisObject);
                // -- Add aggregate values
                for (int i = 0; i < fields.Length; i++) {
                    if (!_join.Joins[thisIndex].Columns.Contains(fields[i].Name))
                        continue;
                    var colName = Prefix + "_" + fields[i].Name;
                    if (!dt.Columns.Contains(colName)) continue;
                    var o = dr[colName];

                    objBuilder[fields[i].Name] = o;
                }

                // Here is where Recursivity gets real.
                foreach (var rel in (from a in Relacoes where a.ParentIndex == thisIndex select a)) {
                    switch (rel.AggregateBuildOption) {
                        case AggregateBuildOptions.AggregateField: {

                                String childPrefix = _join.Joins[rel.ChildIndex].Prefix;
                                var value = dr[childPrefix + "_" + rel.Fields[0]];
                                String fieldName = rel.NewName ?? (childPrefix + "_" + rel.Fields[0]);
                                objBuilder[fieldName] = dr[childPrefix + "_" + rel.Fields[0]];

                                break;
                            }
                        case AggregateBuildOptions.AggregateList: {
                                String fieldAlias = rel.NewName ?? _join.Joins[rel.ChildIndex].Alias;
                                var objectType = ReflectionTool.GetTypeOf(
                                    ReflectionTool.FieldsAndPropertiesOf(type)
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

                                Object parentRid = ReflectionTool.DbDeNull(dr[_join.Joins[rel.ParentIndex].Prefix + "_" + rel.ParentKey]);
                                var newList = BuildAggregateList(ulType, parentRid, rel, dt);
                                if (addMethod == null)
                                    continue;
                                if (objBuilder[fieldAlias] == null) {
                                    objBuilder[fieldAlias] = Activator.CreateInstance(objectType);
                                }
                                foreach (var a in newList) {
                                    var inVal = Convert.ChangeType(a, ulType);

                                    addMethod.Invoke(objBuilder[fieldAlias], new object[] { inVal });
                                }
                                break;
                            }
                        case AggregateBuildOptions.AggregateObject: {
                                String fieldAlias = rel.NewName ?? _join.Joins[rel.ChildIndex].Alias;
                                var objectType = ReflectionTool.GetTypeOf(
                                    ReflectionTool.FieldsAndPropertiesOf(type)
                                    .Where(m => m.Name == fieldAlias)
                                    .FirstOrDefault());
                                if (objectType == null) {
                                    continue;
                                }
                                Object parentRid = ReflectionTool.DbDeNull(dr[_join.Joins[rel.ParentIndex].Prefix + "_" + rel.ParentKey]);
                                if (parentRid == null)
                                    continue;

                                var newObject = BuildAggregateList(type, parentRid, rel, dt).FirstOrDefault();
                                objBuilder[fieldAlias] = newObject;

                                break;
                            }
                    }
                }

                String Alias = _join.Joins[thisIndex].Alias;
                foreach (var finalTransform in _buildParameters._honingParameters) {
                    if (finalTransform.Alias == Alias) {
                        if (finalTransform.Function == null) {
                            if (finalTransform.NewField != finalTransform.OldField) {
                                objBuilder[finalTransform.NewField] = objBuilder[finalTransform.OldField];
                            }
                        }
                        else {
                            objBuilder[finalTransform.NewField] = finalTransform.Function(thisObject);
                        }
                    }
                }

                if (thisObject is IBusinessObject bo) {
                    bo.OnAfterLoad();
                }
                retv.Add(thisObject);
            }
            return retv;
        }

        internal List<Relation> ValidateRelations() {
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

        //public IList<T> BuildObject<T>(ConnectionInfo transaction, Action<BuildParametersHelper> fn, IQueryBuilder conditions, int? p = 1, int? limit = 200, MemberInfo orderingMember = null, OrderingType otype = OrderingType.Asc, IQueryBuilder conditionsRoot = null) where T : IDataObject, new() {
        //    // May Jesus have mercy on your soul
        //    // If you intend on messing with this funciton.

        //    if (conditions == null) {
        //        conditions = new QbFmt("TRUE");
        //    }

        //    // First we generate the DataTable we'll be working with:

        //    transaction?.Benchmarker?.Mark("Query DB for DataTable");
        //    DataTable dt = GenerateDataTable(transaction, (_dataAccessor).QueryGenerator, conditions, p, limit, orderingMember, otype,  conditionsRoot);
        //    transaction?.Benchmarker?.Mark("--");
        //    _buildParameters = new BuildParametersHelper(ref _join, dt);
        //    fn(_buildParameters);
        //    // And validate everything;
        //    DateTime start = DateTime.Now;
        //    IList<T> cache = new List<T>();
        //    ValidateRelations();
        //    var Relations = _join.Relations;
        //    // Then we do this... Magic...
        //    // To initialize and group our 2D data table
        //    // You see, I need those values in a beautiful 3D Shaped
        //    // Hierarchic and Object Oriented Stuff
        //    transaction?.Benchmarker?.Mark("Start building result");
        //    int TopLevel = 0;
        //    String Prefix = _join.Joins[TopLevel].Prefix;
        //    var rs = new List<DataRow>();
        //    foreach (DataRow dr in dt.Rows)
        //        rs.Add(dr);
        //    string rid = FiTechBDadosExtensions.RidColumnOf[typeof(T)];
        //    rs = rs.GroupBy(c => c[Prefix + $"_{rid}"]).Select(grp => grp.First()).ToList();
        //    // This says: Foreach datarow at the 
        //    // "grouped by the Aggregate Root RID"
        //    Parallel.ForEach (rs, dr=> {
        //        // We will create 1 root level object
        //        // and then we will conduce some crazy 
        //        // recursiveness to objectize its children.
        //        T thisObject = new T();
        //        var objBuilder = new ObjectReflector(thisObject);

        //        var fields = ReflectionTool.FieldsAndPropertiesOf(_join.Joins[TopLevel].ValueObject)
        //            .Where((f) => f.GetCustomAttribute<FieldAttribute>() != null).ToArray();
        //        for (int i = 0; i < fields.Length; i++) {
        //            if (!_join.Joins[TopLevel].Columns.Contains(fields[i].Name))
        //                continue;
        //            var colName = Prefix + "_" + fields[i].Name;
        //            if (!dt.Columns.Contains(colName)) continue;
        //            var o = dr[colName];

        //            objBuilder[fields[i]] = o;
        //        }
        //        // -- Find all relations where current table is 'parent'.
        //        var relations = (from a in Relations where a.ParentIndex == TopLevel select a);
        //        foreach (var rel in relations) {
        //            switch (rel.AggregateBuildOption) {
        //                // Aggregate fields are the beautiful easy ones to deal
        //                case AggregateBuildOptions.AggregateField: {

        //                        String childPrefix = _join.Joins[rel.ChildIndex].Prefix;
        //                        var value = dr[childPrefix + "_" + rel.Fields[0]];
        //                        String nome = rel.NewName ?? (childPrefix + "_" + rel.Fields[0]);
        //                        objBuilder[nome] = dr[childPrefix + "_" + rel.Fields[0]];

        //                        break;
        //                    }
        //                // this one is RAD and the most cpu intensive
        //                // Sure needs optimization.
        //                case AggregateBuildOptions.AggregateList: {
        //                        String fieldAlias = rel.NewName ?? _join.Joins[rel.ChildIndex].Alias;
        //                        var objectType = ReflectionTool.GetTypeOf(
        //                            ReflectionTool.FieldsAndPropertiesOf(typeof(T))
        //                            .Where(m => m.Name == fieldAlias)
        //                            .FirstOrDefault());
        //                        var ulType = objectType
        //                            .GetGenericArguments().FirstOrDefault();
        //                        if (ulType == null) {
        //                            continue;
        //                        }
        //                        var addMethod = objectType.GetMethods()
        //                            .Where(m => m.Name == "Add")
        //                            .FirstOrDefault();

        //                        Object parentRid = ReflectionTool.DbDeNull(dr[_join.Joins[rel.ParentIndex].Prefix + "_" + rel.ParentKey]);
        //                        var newList = BuildAggregateList(ulType, parentRid, rel, dt);
        //                        if (addMethod == null)
        //                            continue;
        //                        if (objBuilder[fieldAlias] == null) {
        //                            objBuilder[fieldAlias] = Activator.CreateInstance(objectType);
        //                        }
        //                        foreach (var a in newList) {
        //                            var inVal = Convert.ChangeType(a, ulType);
        //                            addMethod.Invoke(objBuilder[fieldAlias], new object[] { inVal });
        //                        }
        //                        break;
        //                    }

        //                // this one is almost the same as previous one.
        //                case AggregateBuildOptions.AggregateObject: {
        //                        String fieldAlias = rel.NewName ?? _join.Joins[rel.ChildIndex].Alias;
        //                        var objectType = ReflectionTool.GetTypeOf(
        //                            ReflectionTool.FieldsAndPropertiesOf(typeof(T))
        //                            .Where((f) => f.GetCustomAttribute<AggregateObjectAttribute>() != null)
        //                            .Where(m => m.Name == fieldAlias)
        //                            .FirstOrDefault());
        //                        if (objectType == null) {
        //                            continue;
        //                        }
        //                        object parentValue = ReflectionTool.DbDeNull(dr[_join.Joins[rel.ParentIndex].Prefix + "_" + rel.ParentKey]);
        //                        if (parentValue == null) {
        //                            continue;
        //                        }
        //                        var newObjectList = BuildAggregateList(objectType, parentValue, rel, dt);
        //                        if (newObjectList.Any()) {
        //                            var newObject = newObjectList.First();
        //                            var newObjectMan = new ObjectReflector(newObject);
        //                            if (newObject == null) {
        //                                continue;
        //                            }
        //                            objBuilder[fieldAlias] = newObject;
        //                        }
        //                        break;
        //                    }
        //            }
        //        }

        //        String Alias = _join.Joins[TopLevel].Alias;
        //        foreach (var refinement in _buildParameters._honingParameters) {
        //            if (refinement.Alias == Alias) {
        //                if (refinement.Function == null) {
        //                    if (refinement.NewField != refinement.OldField) {
        //                        objBuilder[refinement.NewField] = objBuilder[refinement.OldField];
        //                    }
        //                }
        //                else {
        //                    objBuilder[refinement.NewField] = refinement.Function(thisObject);
        //                }
        //            }
        //        }

        //        if (thisObject is IBusinessObject bo) {
        //            bo.OnAfterLoad();
        //        }

        //        //lock(cache)
        //            cache.Add(thisObject);
        //        //}
        //    });
        //    Logger.WriteLog($"Fi.Tech JoinBuilder has coroutinely built the output object in {DateTime.Now.Subtract(start).TotalMilliseconds}ms");
        //    transaction?.Benchmarker?.Mark("-- Finished building result");
        //    return cache;
        //}
    }
}
