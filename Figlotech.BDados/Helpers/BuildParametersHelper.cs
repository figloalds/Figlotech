using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.Core;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace Figlotech.BDados.Helpers {
    public class ObjectHoningOption {
        public String Alias;
        public String OldField;
        public String NewField;
        private ComputeField function;
        public ObjectHoningOption(String alias, String oldAntigo, String newCampo, ComputeField function) {
            Alias = alias;
            OldField = oldAntigo;
            NewField = newCampo;
            Function = function;
        }

        public ComputeField Function { get => function; set => function = value; }
    }

    public class BuildParametersHelper {
        internal JoinDefinition _join;
        internal List<ObjectHoningOption> _honingParameters = new List<ObjectHoningOption>();
        internal DataTable _dataTable;

        internal BuildParametersHelper(ref JoinDefinition join, DataTable table) {
            _join = join;
            _dataTable = table;
        }

        public RecordSet<T> CaptureEntities<T>(String Alias, int parentId) where T : IDataObject, new() {
            var thisEntity = (from a in _join.Joins where a.Alias == Alias select a).FirstOrDefault();
            if (thisEntity == null)
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
                rows = rows.Where((a) => {
                    int? id = (int?) a[_join.Joins[myIndex].Prefix + "_" + childKey];
                    return id.ToString() == parentId.ToString();
                })
                    .GroupBy(c => c[_join.Joins[myIndex].Prefix + "_id"]).Select(grp => grp.First()).ToList();
                foreach (DataRow dr in rows) {
                    var newInstance = Activator.CreateInstance(typeof(T));
                    T thisValue = (T)newInstance;
                    foreach (var f in newInstance.GetType().GetFields()) {
                        f.SetValue(thisValue, dr[_join.Joins[myIndex].Prefix + "_" + f.Name]);
                    }
                    tabs.Add(thisValue);
                }
            }

            return tabs;
        }

        public void ComputeField(String Alias, String newField, ComputeField function) {
            _honingParameters.Add(new ObjectHoningOption(Alias, newField, newField, function));
        }
        public void Rename(String alias, String oldField, String newField) {
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

        public void RemoveField(String parentAlias, String field) {
            _honingParameters.Add(new ObjectHoningOption(parentAlias, field, field, null));
        }

        public void AggregateObject(String parentAlias, String childAlias, String newName = null) {
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

        public void AggregateList(String parentAlias, String childAlias, String newName = null) {
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

            _join.Relations.Add(newRelation);
        }

    }
}
