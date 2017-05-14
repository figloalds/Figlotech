using Figlotech.BDados.Helpers;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Figlotech.BDados.Entity
{
    public class ObjectHoningOption
    {
        public delegate Object FuncaoRefinar(dynamic d);
        public String Alias;
        public String OldField; 
        public String NewField;
        public ComputeField Function;
        public ObjectHoningOption(String alias, String oldAntigo, String newCampo, ComputeField function)
        {
            Alias = alias;
            OldField = oldAntigo;
            NewField = newCampo;
            Function = function;
        }
    }

    public class BuildParametersHelper
    {
        internal JoinDefinition _join;
        internal List<ObjectHoningOption> _honingParameters = new List<ObjectHoningOption>();
        internal DataTable _dataTable;

        internal BuildParametersHelper(ref JoinDefinition join, DataTable table)
        {
            _join = join;
            _dataTable = table;
        }

        public RecordSet<T> CaptureEntities<T>(String Alias, int parentId) where T:DataObject, new()
        {
            var thisEntity = (from a in _join.Joins where a.Alias == Alias select a).FirstOrDefault();
            if(thisEntity == null) 
                throw new BDadosException("Invalid alias.");
            RecordSet<T> tabs = new RecordSet<T>();
            int myIndex = _join.Joins.IndexOf(thisEntity);
            String childKey = null;
            String parentKey = null;
            int parentIndex = -1;
            foreach (var rel in _join.Relations) {
                if (rel.ChildIndex == myIndex) {
                    childKey = rel.ChildKey;
                    parentKey = rel.ParentKey;
                    parentIndex = rel.ParentIndex;
                }
            }
            if (parentIndex != -1) {
                var rows = new List<DataRow>();
                foreach (DataRow dr in _dataTable.Rows)
                    rows.Add(dr);
                rows = rows.Where((a)=> { 
                    int? id = a.Field<int?>(_join.Joins[myIndex].Prefix+"_"+childKey);
                    return id.ToString() == parentId.ToString();
                })
                    .GroupBy(c => c.Field<long>(_join.Joins[myIndex].Prefix + "Id")).Select(grp => grp.First()).ToList();
                foreach (DataRow dr in rows) {
                    var newInstance = Activator.CreateInstance(typeof(T));
                    T thisValue = (T) newInstance;
                    foreach (var f in newInstance.GetType().GetFields()) {
                        f.SetValue(thisValue, dr.Field<Object>(_join.Joins[myIndex].Prefix +"_"+f.Name));
                    }
                    tabs.Add(thisValue);
                }
            }
            
            return tabs;
        }

        public void ComputeField(String Alias, String newField, ComputeField function)
        {
            _honingParameters.Add(new ObjectHoningOption(Alias, newField, newField, function));
        }
        public void Rename(String alias, String oldField, String newField)
        {
            _honingParameters.Add(new ObjectHoningOption(alias, oldField, newField, null));
        }

        public delegate Object SelecionarColunas<T>(T Tabela);

        public void AggregateField(String destinationAlias, String originAlias, String originField, String newName) {
            int originIndex = _join.Joins.IndexOf((from a in _join.Joins where a.Alias == originAlias select a).First());
            int destinationIndex = _join.Joins.IndexOf((from a in _join.Joins where a.Alias == destinationAlias select a).First());
            Relation newRelation = new Relation();

            var aliases = (from a in _join.Joins select a.Alias).ToList();
            newRelation.ChildIndex = aliases.IndexOf(originAlias);
            newRelation.ParentIndex = aliases.IndexOf(destinationAlias);
            if (newRelation.ChildIndex < 0)
                throw new BDadosException($"No table in this join corresponds to alias '{originAlias}'");
            if (newRelation.ParentIndex < 0)
                throw new BDadosException($"No table in this join corresponds to alias '{destinationAlias}'");

            newRelation.AssemblyOption = AggregateBuildOptions.AggregateField;

            newRelation.Fields = new List<string> { originField };
            newRelation.NewName = newName;

            _join.Relations.Add(newRelation);
        }

        // Good old stuff, too gimmicky, ignore.
        //public void Agregar<T>(String AliasDestino, String AliasOrigem, Expression<SelecionarColunas<T>> colunas = null, String NovoNome = null)
        //{
        //    int IndexOrigem = _join.Joins.IndexOf((from a in _join.Joins where a.Alias == AliasOrigem select a).First());
        //    int IndexDestino = _join.Joins.IndexOf((from a in _join.Joins where a.Alias == AliasDestino select a).First());
        //    Relacao novaRelacao = new Relacao();
        //    bool found = false;
        //    foreach (var rel in _join.Relations) {
        //        if (rel.ParentIndex == IndexDestino && rel.ChildIndex == IndexOrigem) {
        //            novaRelacao = rel;
        //            found = true; break;
        //        }
        //    }
        //    if (!found) {
        //        var Aliases = (from a in _join.Joins select a.Alias).ToList();
        //        novaRelacao.ChildIndex = Aliases.IndexOf(AliasOrigem);
        //        novaRelacao.ParentIndex = Aliases.IndexOf(AliasDestino);
        //        if (novaRelacao.ChildIndex < 0)
        //            throw new BDadosException(String.Format("Nenhuma tabela nessa junção corresponde ao Alias '{0}'", AliasOrigem));
        //        if (novaRelacao.ParentIndex < 0)
        //            throw new BDadosException(String.Format("Nenhuma tabela nessa junção corresponde ao Alias '{0}'", AliasDestino));
        //    }
        //    novaRelacao.AssemblyOption = AssemblyOptions.Aggregate;

        //    if (colunas != null) {
        //        if (colunas.Body is System.Linq.Expressions.NewExpression) {
        //            var args = (colunas.Body as NewExpression).Members;
        //            foreach (var arg in args) {
        //                novaRelacao.Fields.Add(arg.Name);
        //            }
        //        }
        //        else if (colunas.Body is MemberExpression) {
        //            novaRelacao.Fields.Add((colunas.Body as MemberExpression).Member.Name);
        //        }
        //        else {
        //            throw new BDadosException("Lambda para seleção de colunas ao Agregar está malformada. Esperando MemberExpression ou NewExpression");
        //        }
        //    } else {
        //        ReflectionTool.ForFields(typeof(T), (campo) => {
        //            novaRelacao.Fields.Add(campo.Name);
        //        });
        //    }

        //    if (!found) {
        //        _join.Relations.Add(novaRelacao);
        //    }
        //}

        public void RemoveField(String parentAlias, String field)
        {
            _honingParameters.Add(new ObjectHoningOption(parentAlias, field, field, null));
        }

        public void AggregateObject(String parentAlias, String childAlias, String newName = null)
        {
            int IndexPai = _join.Joins.IndexOf((from a in _join.Joins where a.Alias == parentAlias select a).First());
            int IndexFilha = _join.Joins.IndexOf((from a in _join.Joins where a.Alias == childAlias select a).First());
            Relation newRelation = new Relation();
            bool found = false;
            foreach (var rel in _join.Relations) {
                if (rel.ParentIndex == IndexPai && rel.ChildIndex == IndexFilha) {
                    newRelation = rel;
                    found = true; break;
                }
            }
            if (found) {
                _join.Relations.Remove(newRelation);
            }
            var Aliases = (from a in _join.Joins select a.Alias).ToList();
            newRelation.NewName = newName;
            newRelation.ChildIndex = Aliases.IndexOf(childAlias);
            newRelation.ParentIndex = Aliases.IndexOf(parentAlias);
            newRelation.AssemblyOption = AggregateBuildOptions.AggregateObject;
            _join.Relations.Add(newRelation);
        }

        public void AggregateList(String parentAlias, String childAlias, String newName = null)
        {
            int parentIndex = _join.Joins.IndexOf((from a in _join.Joins where a.Alias == parentAlias select a).First());
            int childIndex = _join.Joins.IndexOf((from a in _join.Joins where a.Alias == childAlias select a).First());

            Relation newRelation = new Relation();
            bool found = false;
            foreach (var rel in _join.Relations) {
                if (rel.ParentIndex == parentIndex && rel.ChildIndex == childIndex) {
                    newRelation = rel;
                    found = true; break;
                }
            }
            if (found) {
                _join.Relations.Remove(newRelation);
            }
            var Aliases = (from a in _join.Joins select a.Alias).ToList();
            newRelation.ChildIndex = Aliases.IndexOf(childAlias);
            newRelation.ParentIndex = Aliases.IndexOf(parentAlias);
            newRelation.AssemblyOption = AggregateBuildOptions.AggregateList;
            newRelation.NewName = newName;
            if (newRelation.ParentKey == null || newRelation.ChildKey == null) {
                throw new BDadosException($"No direct relation found between {parentAlias} and {childAlias} for aggregating list.");
            }
            _join.Relations.Add(newRelation);
        }

    }
}
