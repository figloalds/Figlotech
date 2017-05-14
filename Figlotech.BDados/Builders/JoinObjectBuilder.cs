
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


        private List<dynamic> AggregateList(int parentId, ref DataTable dt, JoiningTable parent, JoiningTable child, Relation parentJoin) {
            List<dynamic> retv = new List<dynamic>();
            List<Type> valueObjects = (from a in _join.Joins select a.ValueObject).ToList();
            List<String> vobjectNames = (from a in _join.Joins select a.TableName).ToList();
            List<String> prefixes = (from a in _join.Joins select a.Prefix).ToList();
            List<String> aliases = (from a in _join.Joins select a.Alias).ToList();
            List<String> onClauses = (from a in _join.Joins select a.Args).ToList();

            var rs = (from a in dt.AsEnumerable()
                      where a.Field<int?>(prefixes[parentJoin.ParentIndex] + "_" + parentJoin.ParentKey) == parentId
                      select a);
            FieldInfo[] fields = valueObjects[parentJoin.ChildIndex].GetFields();
            foreach (DataRow r in rs) {
                dynamic dyn = new ExpandoObject();
                int Id = 0;
                for (int i = 0; i < fields.Length; i++) {
                    Object valor = r.Field<Object>(String.Format("{0}_{1}", prefixes[parentJoin.ChildIndex], fields[i].Name));
                    if (valor == null)
                        continue;
                    if (fields[i].Name == "Id") {
                        Id = (int)valor;
                    }
                    Type tipo = fields[i].FieldType;
                    if (fields[i].FieldType.IsGenericType)
                        tipo = fields[i].FieldType.GenericTypeArguments[0];
                    (dyn as IDictionary<String, Object>).Add(
                        fields[i].Name, Convert.ChangeType(valor, tipo)
                    );
                }
                foreach (Relation juncao in (from j in _join.Relations where aliases[j.ChildIndex] == aliases[parentJoin.ParentIndex] select j)) {
                    if (juncao.AssemblyOption == AggregateBuildOptions.AggregateList) {
                        String nome = vobjectNames[juncao.ParentIndex];
                        if (!nome.EndsWith("s"))
                            nome = nome + "s";
                        (dyn as IDictionary<String, Object>).Add(
                            nome, AggregateList(Id, ref dt, parent, child, juncao)
                        );
                    }
                }
                retv.Add(dyn);
            }
            return retv;
        }

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

        private List<dynamic> BuildAggregateList(Object ParentVal, Relation Relacao, ref DataTable dt) {
            int EsteIndex = Relacao.ChildIndex;
            int IndexPai = Relacao.ParentIndex;
            List<dynamic> retv = new List<dynamic>();
            var Relacoes = _join.Relations;
            String Prefixo = _join.Joins[EsteIndex].Prefix;
            String PrefixoPai = _join.Joins[IndexPai].Prefix;
            List<DataRow> rs = new List<DataRow>();
            List<String> ids = new List<String>();
            for (int i = 0; i < dt.Rows.Count; i++) {
                // É... Talvez seja uma gambiarra aqui, mas comparar Object com Object em C# faz comparação por referencia
                // E eu preciso de uma comparação por valor aqui.
                Object val = dt.Rows[i].Field<Object>(Prefixo + "_" + Relacao.ChildKey);
                if (val != null && val.ToString() == ParentVal.ToString()) {
                    String RID = dt.Rows[i].Field<String>(Prefixo + "RID");
                    if (!ids.Contains(RID)) {
                        rs.Add(dt.Rows[i]);
                        ids.Add(RID);
                    }
                }
            }
            foreach (DataRow dr in rs) {
                dynamic EsteObjeto = new ExpandoObject();
                // -- Adicionar os valores selecionados
                FieldInfo[] campos = _join.Joins[EsteIndex].ValueObject.GetFields().Where((f) => f.GetCustomAttribute<FieldAttribute>() != null).ToArray();
                for (int i = 0; i < campos.Length; i++) {
                    if (_join.Joins[EsteIndex].Excludes.Contains(campos[i].Name))
                        continue;
                    (EsteObjeto as IDictionary<String, Object>).Add(campos[i].Name, dr.Field<Object>(Prefixo + "_" + campos[i].Name));
                }

                // -- Agregar/Ramificar todas as relações em que a tabela atual é 'pai'.
                foreach (var rel in (from a in Relacoes where a.ParentIndex == EsteIndex select a)) {
                    if (rel.AssemblyOption == AggregateBuildOptions.AggregateField) {

                        try {
                            String PrefixoFilha = _join.Joins[rel.ChildIndex].Prefix;
                            var value = dr.Field<Object>(PrefixoFilha + "_" + rel.Fields[0]);
                            String nome = rel.NewName ?? (PrefixoFilha + "_" + rel.Fields[0]);
                            if ((EsteObjeto as IDictionary<String, dynamic>).ContainsKey(nome)) {
                                (EsteObjeto as IDictionary<String, dynamic>).Remove(nome);
                            }
                            (EsteObjeto as IDictionary<String, Object>).Add(nome, dr.Field<Object>(PrefixoFilha + "_" + rel.Fields[0]));
                        } catch (Exception x) {
                            Console.WriteLine(x.Message);
                        }

                        //foreach (var coluna in rel.Fields) {
                        //    String PrefixoFilha = _join.Joins[rel.ChildIndex].Prefix;
                        //    String nome = rel.NewName??(PrefixoFilha + "_" + coluna);
                        //    if ((EsteObjeto as IDictionary<String, dynamic>).ContainsKey(nome)) {
                        //        (EsteObjeto as IDictionary<String, dynamic>).Remove(nome);
                        //    }
                        //    (EsteObjeto as IDictionary<String, Object>).Add(nome, dr.Field<Object>(PrefixoFilha + "_" + coluna));
                        //}

                    } else
                    if (rel.AssemblyOption == AggregateBuildOptions.AggregateList) {
                        List<dynamic> novoObj = new List<dynamic>();
                        Object valPai = dr.Field<Object>(_join.Joins[rel.ParentIndex].Prefix + "_" + rel.ParentKey);
                        novoObj = BuildAggregateList(valPai, rel, ref dt);
                        String nome = rel.NewName ?? Pluralize(_join.Joins[rel.ChildIndex].Alias);
                        if ((EsteObjeto as IDictionary<String, dynamic>).ContainsKey(nome)) {
                            (EsteObjeto as IDictionary<String, dynamic>).Remove(nome);
                        }
                        (EsteObjeto as IDictionary<String, dynamic>).Add(nome, novoObj);
                    } else
                    if (rel.AssemblyOption == AggregateBuildOptions.AggregateObject) {
                        dynamic novoObj = new ExpandoObject();
                        Object valPai = dr.Field<Object>(_join.Joins[rel.ParentIndex].Prefix + "_" + rel.ParentKey);
                        try {
                            novoObj = BuildAggregateList(valPai, rel, ref dt)[0];
                            String nome = rel.NewName ?? _join.Joins[rel.ChildIndex].Alias;
                            if ((EsteObjeto as IDictionary<String, dynamic>).ContainsKey(nome)) {
                                (EsteObjeto as IDictionary<String, dynamic>).Remove(nome);
                            }
                            (EsteObjeto as IDictionary<String, dynamic>).Add(nome, novoObj);
                        } catch (Exception) {
                        }
                    }
                }
                String Alias = _join.Joins[EsteIndex].Alias;
                foreach (var refino in _buildParameters._honingParameters) {
                    var DictObjeto = (EsteObjeto as IDictionary<String, dynamic>);
                    if (refino.Alias == Alias) {
                        if (refino.Function == null) {
                            if (refino.NewField != refino.OldField) {
                                if (DictObjeto.ContainsKey(refino.NewField))
                                    DictObjeto.Remove(refino.NewField);
                                if (DictObjeto.ContainsKey(refino.OldField)) {
                                    DictObjeto.Add(refino.NewField, DictObjeto[refino.OldField]);
                                    DictObjeto.Remove(refino.OldField);
                                }
                            }
                        } else {
                            if (DictObjeto.ContainsKey(refino.NewField))
                                DictObjeto.Remove(refino.NewField);
                            try {
                                DictObjeto.Add(refino.NewField, refino.Function(EsteObjeto));
                            } catch (Exception) { }
                        }
                    }
                }
                retv.Add(EsteObjeto);
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
                if (!DataObject.FindColumn(ChaveA, _join.Joins[IndexA].ValueObject) || !DataObject.FindColumn(ChaveB, _join.Joins[IndexB].ValueObject)) {
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

        public DynamicList BuildObject(Action<BuildParametersHelper> fn, IQueryBuilder conditions, int? p = 1, int? limit = 200, IQueryBuilder conditionsRoot = null) {
            if (conditions == null) {
                throw new BDadosException("Conditions were null w'dafuq!");
            }
            DataTable dt = GenerateDataTable((_dataAccessor).GetQueryGenerator(), conditions, p, limit, conditionsRoot);
            _buildParameters = new BuildParametersHelper(ref _join, dt);
            fn(_buildParameters);
            DateTime begin = DateTime.Now;
            DynamicList retv = new DynamicList();
            ValidateRelations();
            var Relations = _join.Relations;
            // this gimmick needs fixing but i'm too sleepy right now.
            int TopLevel = 0;
            String Prefix = _join.Joins[TopLevel].Prefix;
            var rs = new List<DataRow>();
            foreach (DataRow dr in dt.Rows)
                rs.Add(dr);
            rs = rs.GroupBy(c => c.Field<String>(Prefix + "RID")).Select(grp => grp.First()).ToList();
            //Parallel.ForEach(rs, dr => {
            foreach (DataRow dr in rs) {
                dynamic thisObject = new ExpandoObject();
                // -- Add selected values
                FieldInfo[] fields = _join.Joins[TopLevel].ValueObject
                    .GetFields().Where((f) => f.GetCustomAttribute<FieldAttribute>() != null).ToArray();
                for (int i = 0; i < fields.Length; i++) {
                    if (_join.Joins[TopLevel].Excludes.Contains(fields[i].Name))
                        continue;
                    (thisObject as IDictionary<String, Object>).Add(fields[i].Name, dr.Field<Object>(Prefix + "_" + fields[i].Name));
                }
                // -- Aggregate or Branch all relations where current table is 'parent'.
                var relacoes = (from a in Relations where a.ParentIndex == TopLevel select a);
                //Parallel.ForEach(relacoes, rel => {
                foreach (var rel in relacoes) {
                    if (rel.AssemblyOption == AggregateBuildOptions.AggregateField) {

                        try {
                            String PrefixoFilha = _join.Joins[rel.ChildIndex].Prefix;
                            var value = dr.Field<Object>(PrefixoFilha + "_" + rel.Fields[0]);
                            String nome = rel.NewName ?? (PrefixoFilha + "_" + rel.Fields[0]);
                            if ((thisObject as IDictionary<String, dynamic>).ContainsKey(nome)) {
                                (thisObject as IDictionary<String, dynamic>).Remove(nome);
                            }
                            (thisObject as IDictionary<String, Object>).Add(nome, dr.Field<Object>(PrefixoFilha + "_" + rel.Fields[0]));
                        } catch (Exception x) {
                            Console.WriteLine(x.Message);
                        }

                        //foreach (var coluna in rel.Fields) {
                        //    String PrefixoFilha = _join.Joins[rel.ChildIndex].Prefix;
                        //    String nome = rel.NewName ?? (PrefixoFilha + "_" + coluna);
                        //    if ((EsteObjeto as IDictionary<String, dynamic>).ContainsKey(nome)) {
                        //        (EsteObjeto as IDictionary<String, dynamic>).Remove(nome);
                        //    }
                        //    (EsteObjeto as IDictionary<String, Object>).Add(nome, dr.Field<Object>(PrefixoFilha + "_" + coluna));
                        //}
                    } else
                    if (rel.AssemblyOption == AggregateBuildOptions.AggregateList) {
                        List<dynamic> newObject = new List<dynamic>();
                        Object parentVal = dr.Field<Object>(_join.Joins[rel.ParentIndex].Prefix + "_" + rel.ParentKey);
                        if (parentVal == null)
                            continue;
                        newObject = BuildAggregateList(parentVal, rel, ref dt);
                        String fieldAlias = rel.NewName ?? Pluralize(_join.Joins[rel.ChildIndex].Alias);
                        if ((thisObject as IDictionary<String, dynamic>).ContainsKey(fieldAlias)) {
                            (thisObject as IDictionary<String, dynamic>).Remove(fieldAlias);
                        }
                        (thisObject as IDictionary<String, dynamic>).Add(fieldAlias, newObject);
                    } else
                    if (rel.AssemblyOption == AggregateBuildOptions.AggregateObject) {
                        dynamic newObject = new ExpandoObject();
                        Object parentValue = dr.Field<Object>(_join.Joins[rel.ParentIndex].Prefix + "_" + rel.ParentKey);
                        try {
                            if (parentValue == null) {
                                continue;
                            }
                            newObject = BuildAggregateList(parentValue, rel, ref dt).FirstOrDefault();
                            if (newObject == null) {
                                continue;
                            }
                            String fieldAlias = rel.NewName ?? _join.Joins[rel.ChildIndex].Alias;
                            if ((thisObject as IDictionary<String, dynamic>).ContainsKey(fieldAlias)) {
                                (thisObject as IDictionary<String, dynamic>).Remove(fieldAlias);
                            }
                            (thisObject as IDictionary<String, dynamic>).Add(fieldAlias, newObject);
                        } catch (Exception z) {

                        }
                    }
                }
                //});
                String Alias = _join.Joins[TopLevel].Alias;
                foreach (var refinement in _buildParameters._honingParameters) {
                    var dictObject = (thisObject as IDictionary<String, dynamic>);
                    if (refinement.Alias == Alias) {
                        if (refinement.Function == null) {
                            if (refinement.NewField != refinement.OldField) {
                                if (dictObject.ContainsKey(refinement.NewField))
                                    dictObject.Remove(refinement.NewField);
                                if (dictObject.ContainsKey(refinement.OldField)) {
                                    dictObject.Add(refinement.NewField, dictObject[refinement.OldField]);
                                    dictObject.Remove(refinement.OldField);
                                }
                            }
                        } else {
                            if (dictObject.ContainsKey(refinement.NewField))
                                dictObject.Remove(refinement.NewField);
                            try {
                                dictObject.Add(refinement.NewField, refinement.Function(thisObject));
                            } catch (Exception) { }
                        }
                    }
                }
                retv.Add(thisObject);
            }
            //});
            Logger.WriteLog($"This clumsy JoinBuilder have built the dynamic object in {DateTime.Now.Subtract(begin).TotalMilliseconds}ms");
            return retv;
        }
    }
}
