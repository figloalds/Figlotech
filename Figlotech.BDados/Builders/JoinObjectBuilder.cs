
using Figlotech.BDados.Attributes;
/**
* Figlotech.BDados.Builders.JoinBuilder
* Default implementation for IJoinBuilder
* 
* @Author: Felype Rennan Alves dos Santos
* August/2014
* 
**/
using Figlotech.BDados.Entity;
using Figlotech.BDados.Helpers;
using Figlotech.BDados.Interfaces;
using System;
using System.Collections.Generic;
using System.Data;
using System.Dynamic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Figlotech.BDados.Builders {
    public class JoinObjectBuilder : IJoinBuilder {
        private Entity.JoinDefinition _join = new Entity.JoinDefinition();
        private BuildParametersHelper _buildParameters;
        private IRdbmsDataAccessor _dataAccessor;
        //public delegate void JoinBuild(Join join);
        //public delegate void ParametrizeBuild(BuildParametersHelper parametros);

        ILogger _logger;
        ILogger Logger {
            get {
                return _logger = (_logger = new Logger(new FileAccessor(FTH.DefaultLogRepository)));
            }
        }

        public JoinObjectBuilder(IRdbmsDataAccessor dataAccessor, Action<Entity.JoinDefinition> fn) {
            fn(_join);
            _dataAccessor = dataAccessor;
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
                //throw new BDadosException($"Error validating relations: {x.Message}. \n\nJoined Tables were: \n {tables}");
            }
        }
        
        //private List<dynamic> AggregateList(int parentId, ref DataTable dt, JoiningTable parent, JoiningTable child, Relation parentJoin) {
        //    List<dynamic> retv = new List<dynamic>();
        //    List<Type> valueObjects = (from a in _join.Joins select a.ValueObject).ToList();
        //    List<String> vobjectNames = (from a in _join.Joins select a.TableName).ToList();
        //    List<String> prefixes = (from a in _join.Joins select a.Prefix).ToList();
        //    List<String> aliases = (from a in _join.Joins select a.Alias).ToList();
        //    List<String> onClauses = (from a in _join.Joins select a.Args).ToList();

        //    var rs = (from a in dt.AsEnumerable()
        //              where a.Field<int?>(prefixes[parentJoin.ParentIndex] + "_" + parentJoin.ParentKey) == parentId
        //              select a);
        //    FieldInfo[] fields = valueObjects[parentJoin.ChildIndex].GetFields();
        //    foreach (DataRow r in rs) {
        //        dynamic dyn = new ExpandoObject();
        //        int Id = 0;
        //        for (int i = 0; i < fields.Length; i++) {
        //            Object valor = r.Field<Object>(String.Format("{0}_{1}", prefixes[parentJoin.ChildIndex], fields[i].Name));
        //            if (valor == null)
        //                continue;
        //            if (fields[i].Name == "Id") {
        //                Id = (int)valor;
        //            }
        //            Type tipo = fields[i].FieldType;
        //            if (fields[i].FieldType.IsGenericType)
        //                tipo = fields[i].FieldType.GenericTypeArguments[0];
        //            (dyn as IDictionary<String, Object>).Add(
        //                fields[i].Name, Convert.ChangeType(valor, tipo)
        //            );
        //        }
        //        foreach (Relation juncao in (from j in _join.Relations where aliases[j.ChildIndex] == aliases[parentJoin.ParentIndex] select j)) {
        //            if (juncao.AssemblyOption == AggregateBuildOptions.AggregateList) {
        //                String nome = vobjectNames[juncao.ParentIndex];
        //                if (!nome.EndsWith("s"))
        //                    nome = nome + "s";
        //                (dyn as IDictionary<String, Object>).Add(
        //                    nome, AggregateList(Id, ref dt, parent, child, juncao)
        //                );
        //            }
        //        }
        //        retv.Add(dyn);
        //    }
        //    return retv;
        //}

        public IQueryBuilder GenerateQuery(IQueryGenerator generator, IQueryBuilder conditions, int p = 1, int limit = 200, IQueryBuilder conditionsRoot = null) {
            return generator.GenerateJoinQuery(_join, conditions, p, limit, conditionsRoot);
        }

        public DataTable GenerateDataTable(IQueryGenerator generator, IQueryBuilder conditions, int? p = 1, int? limit = 200, IQueryBuilder conditionsRoot = null) {
            QueryBuilder query = (QueryBuilder)generator.GenerateJoinQuery(_join, conditions, p, limit, conditionsRoot);
            DataTable dt = null;
            _dataAccessor.Access((bd) => {
                dt = bd.Query(query);
            }, (x) => {
                Logger.WriteLog("-- Failed to generate DataTable in JoinBuilder, verify your ON and WHERE clauses.");
                throw x;
            }
            );
            return dt;
        }

        /**
        <summary>
            NOT IN ENGLISH.    
        </summary>
        **/
        private String Pluralize(String str) {
            String retv = str;
            if (retv.EndsWith("s")) {
                return retv;
            }
            if (retv.ToLower().EndsWith("em")) {
                return retv.Substring(0, retv.ToLower().LastIndexOf("em")) + "ens";
            }
            if (retv.EndsWith("om")) {
                return retv.Substring(0, retv.ToLower().LastIndexOf("om")) + "ons";
            }
            if (retv.EndsWith("um")) {
                return retv.Substring(0, retv.ToLower().LastIndexOf("um")) + "uns";
            }
            if (retv.EndsWith("r")) {
                return retv.Substring(0, retv.ToLower().LastIndexOf("r")) + "res";
            }
            return retv + "s";
        }

        private List<object> BuildAggregateList(Type type, Object ParentVal, Relation relation, ref DataTable dt) {
            int thisIndex = relation.ChildIndex;
            int parentIndex = relation.ParentIndex;
            List<object> retv = new List<object>();
            var Relacoes = _join.Relations;
            String Prefix = _join.Joins[thisIndex].Prefix;
            String parentPrefix = _join.Joins[parentIndex].Prefix;
            List<DataRow> rs = new List<DataRow>();
            List<String> ids = new List<String>();
            for (int i = 0; i < dt.Rows.Count; i++) {
                // 
                Object val = dt.Rows[i].Field<Object>(Prefix + "_" + relation.ChildKey);
                if (val != null && val.ToString() == ParentVal.ToString()) {
                    String RID = dt.Rows[i].Field<String>(Prefix + "RID");
                    if (!ids.Contains(RID)) {
                        rs.Add(dt.Rows[i]);
                        ids.Add(RID);
                    }
                }
            }
            foreach (DataRow dr in rs) {
                object thisObject = Activator.CreateInstance(type);
                // -- Adicionar os valores selecionados
                MemberInfo[] fields = ReflectionTool.FieldsAndPropertiesOf(type)
                    .Where((f) => f.GetCustomAttribute<FieldAttribute>() != null)
                    .ToArray();
                for (int i = 0; i < fields.Length; i++) {
                    if (_join.Joins[thisIndex].Excludes.Contains(fields[i].Name))
                        continue;
                    ReflectionTool.SetValue(thisObject, fields[i].Name, dr.Field<Object>(Prefix + "_" + fields[i].Name));
                }

                // Here is where Recursivity gets real.
                foreach (var rel in (from a in Relacoes where a.ParentIndex == thisIndex select a)) {
                    switch (rel.AssemblyOption) {
                        case AggregateBuildOptions.AggregateField: {
                                try {
                                    String childPrefix = _join.Joins[rel.ChildIndex].Prefix;
                                    var value = dr.Field<Object>(childPrefix + "_" + rel.Fields[0]);
                                    String fieldName = rel.NewName ?? (childPrefix + "_" + rel.Fields[0]);
                                    ReflectionTool.SetValue(thisObject, fieldName, dr.Field<Object>(childPrefix + "_" + rel.Fields[0]));
                                } catch (Exception x) {
                                    Console.WriteLine(x.Message);
                                }
                                break;
                            }
                        case AggregateBuildOptions.AggregateList: {
                                String fieldAlias = rel.NewName ?? Pluralize(_join.Joins[rel.ChildIndex].Alias);
                                var objectType = ReflectionTool.GetTypeOf(
                                    ReflectionTool.FieldsAndPropertiesOf(type)
                                    .Where(m => m.Name == fieldAlias)
                                    .FirstOrDefault());
                                if (objectType == null) {
                                    continue;
                                }
                                List<object> newList = new List<object>();
                                Object parentRid = dr.Field<Object>(_join.Joins[rel.ParentIndex].Prefix + "_" + rel.ParentKey);
                                newList = BuildAggregateList(objectType, parentRid, rel, ref dt);
                                ReflectionTool.SetValue(thisObject, fieldAlias, newList);
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
                                object newObject = new ExpandoObject();
                                Object parentRid = dr.Field<Object>(_join.Joins[rel.ParentIndex].Prefix + "_" + rel.ParentKey);
                                try {
                                    newObject = BuildAggregateList(type, parentRid, rel, ref dt).FirstOrDefault();
                                    ReflectionTool.SetValue(thisObject, fieldAlias, newObject);
                                } catch (Exception) {
                                }
                                break;
                            }
                    }
                }

                String Alias = _join.Joins[thisIndex].Alias;
                foreach (var finalTransform in _buildParameters._honingParameters) {
                    if (finalTransform.Alias == Alias) {
                        if (finalTransform.Function == null) {
                            if (finalTransform.NewField != finalTransform.OldField) {
                                ReflectionTool.SetValue(thisObject, finalTransform.NewField, ReflectionTool.GetValue(thisObject, finalTransform.OldField));
                            }
                        } else {
                            ReflectionTool.SetValue(thisObject, finalTransform.NewField, finalTransform.Function(thisObject));
                        }
                    }
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
                    throw new BDadosException(String.Format("Relacao invalida {0}", _join.Joins[i].Args));
                }
                if (!FTH.FindColumn(ChaveA, _join.Joins[IndexA].ValueObject) || !FTH.FindColumn(ChaveB, _join.Joins[IndexB].ValueObject)) {
                    _join.Relations.Clear();
                    throw new BDadosException(String.Format("A coluna {0} especificada não existe na tabela '{1} AS {2}'", ChaveA, _join.Joins[i].TableName, _join.Joins[i].Prefix));
                }
            }
            for (int i = 0; i < _join.Relations.Count; i++) {
                if (_join.Relations[i].ParentIndex < 0 || _join.Relations[i].ParentIndex > _join.Joins.Count - 1)
                    throw new BDadosException(String.Format("Uma das Relacoes especificadas não é valida."));
                if (_join.Relations[i].ChildIndex < 0 || _join.Relations[i].ChildIndex > _join.Joins.Count - 1)
                    throw new BDadosException(String.Format("Uma das Relacoes especificadas não é valida."));
            }
            return _join.Relations;
        }

        public RecordSet<T> BuildObject<T>(Action<BuildParametersHelper> fn, IQueryBuilder conditions, int? p = 1, int? limit = 200, IQueryBuilder conditionsRoot = null) where T : IDataObject, new() {
            // May Jesus have mercy on your soul
            // If you intend on messing with this funciton.

            if (conditions == null) {
                conditions = new QueryBuilder("TRUE");
            }

            // First we generate the DataTable we'll be working with:
            DataTable dt = GenerateDataTable((_dataAccessor).GetQueryGenerator(), conditions, p, limit, conditionsRoot);
            _buildParameters = new BuildParametersHelper(ref _join, dt);
            fn(_buildParameters);
            // And validate everything;
            DateTime start = DateTime.Now;
            RecordSet<T> retv = new RecordSet<T>();
            ValidateRelations();
            var Relations = _join.Relations;
            // Then we do this... Magic...
            // To initialize and group our 2D data table
            // You see, I need those values in a beautiful 3D Shaped
            // Hierarchic and Object Oriented Stuff
            int TopLevel = 0;
            String Prefix = _join.Joins[TopLevel].Prefix;
            var rs = new List<DataRow>();
            foreach (DataRow dr in dt.Rows)
                rs.Add(dr);
            rs = rs.GroupBy(c => c.Field<String>(Prefix + "RID")).Select(grp => grp.First()).ToList();
            // This says: Foreach datarow at the 
            // "grouped by the Aggregate Root RID"
            foreach (DataRow dr in rs) {
                // We will create 1 root level object
                // and then we will conduce some crazy 
                // recursiveness to objectize its children.
                T thisObject = Activator.CreateInstance<T>();

                FieldInfo[] fields = _join.Joins[TopLevel].ValueObject
                    .GetFields().Where((f) => f.GetCustomAttribute<FieldAttribute>() != null).ToArray();
                for (int i = 0; i < fields.Length; i++) {
                    if (_join.Joins[TopLevel].Excludes.Contains(fields[i].Name))
                        continue;
                    ReflectionTool.SetValue(thisObject, fields[i].Name, dr.Field<Object>(Prefix + "_" + fields[i].Name));
                }
                // -- Find all relations where current table is 'parent'.
                var relations = (from a in Relations where a.ParentIndex == TopLevel select a);
                foreach (var rel in relations) {
                    switch (rel.AssemblyOption) {
                        // Aggregate fields are the beautiful easy ones to deal
                        case AggregateBuildOptions.AggregateField: {
                                try {
                                    String childPrefix = _join.Joins[rel.ChildIndex].Prefix;
                                    var value = dr.Field<Object>(childPrefix + "_" + rel.Fields[0]);
                                    String nome = rel.NewName ?? (childPrefix + "_" + rel.Fields[0]);
                                    ReflectionTool.SetValue(thisObject, nome, dr.Field<Object>(childPrefix + "_" + rel.Fields[0]));
                                } catch (Exception x) {
                                    Console.WriteLine(x.Message);
                                }
                                break;
                            }
                        // this one is RAD and the most cpu intensive
                        // Sure needs optimization.
                        case AggregateBuildOptions.AggregateList: {
                                String fieldAlias = rel.NewName;
                                var objectType = ReflectionTool.GetTypeOf(
                                    ReflectionTool.FieldsAndPropertiesOf(typeof(T))
                                    .Where(m => m.Name == fieldAlias)
                                    .FirstOrDefault());
                                if (objectType == null) {
                                    continue;
                                }
                                List<object> newObjects = new List<object>();
                                Object parentVal = dr.Field<Object>(_join.Joins[rel.ParentIndex].Prefix + "_" + rel.ParentKey);
                                if (parentVal == null)
                                    continue;
                                newObjects = BuildAggregateList(objectType, parentVal, rel, ref dt);
                                ReflectionTool.SetValue(thisObject, fieldAlias, newObjects);
                                break;
                            }
                        // this one is almost the same as previous one.
                        case AggregateBuildOptions.AggregateObject: {
                                String fieldAlias = rel.NewName ?? _join.Joins[rel.ChildIndex].Alias;
                                var objectType = ReflectionTool.GetTypeOf(
                                    ReflectionTool.FieldsAndPropertiesOf(typeof(T))
                                    .Where(m => m.Name == fieldAlias)
                                    .FirstOrDefault());
                                if (objectType == null) {
                                    continue;
                                }
                                var newObject = Activator.CreateInstance(objectType);
                                Object parentValue = dr.Field<Object>(_join.Joins[rel.ParentIndex].Prefix + "_" + rel.ParentKey);
                                try {
                                    if (parentValue == null) {
                                        continue;
                                    }
                                    newObject = BuildAggregateList(objectType, parentValue, rel, ref dt).FirstOrDefault();
                                    if (newObject == null) {
                                        continue;
                                    }
                                    ReflectionTool.SetValue(thisObject, fieldAlias, newObject);
                                } catch (Exception z) {

                                }
                                break;
                            }
                    }
                }

                // HOPE I DONT NEED THIS ANYMORE
                // SERIOUSLY
                //String Alias = _join.Joins[TopLevel].Alias;
                //foreach (var refinement in _buildParameters._honingParameters) {
                //    var dictObject = (thisObject as IDictionary<String, dynamic>);
                //    if (refinement.Alias == Alias) {
                //        if (refinement.Function == null) {
                //            if (refinement.NewField != refinement.OldField) {
                //                if (dictObject.ContainsKey(refinement.NewField))
                //                    dictObject.Remove(refinement.NewField);
                //                if (dictObject.ContainsKey(refinement.OldField)) {
                //                    dictObject.Add(refinement.NewField, dictObject[refinement.OldField]);
                //                    dictObject.Remove(refinement.OldField);
                //                }
                //            }
                //        } else {
                //            if (dictObject.ContainsKey(refinement.NewField))
                //                dictObject.Remove(refinement.NewField);
                //            try {
                //                dictObject.Add(refinement.NewField, refinement.Function(thisObject));
                //            } catch (Exception) { }
                //        }
                //    }
                //}
                retv.Add(thisObject);
            }
            //});
            Logger.WriteLog($"This clumsy JoinBuilder has built the output object in {DateTime.Now.Subtract(start).TotalMilliseconds}ms");
            return retv;
        }
    }
}
