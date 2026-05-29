
/**
* Figlotech.BDados.Builders.JoinBuilder
* Default implementation for IJoinBuilder
* 
* @Author: Felype Rennan Alves dos Santos
* August/2014
* 
**/
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.Exceptions;
using Figlotech.BDados.Helpers;
using Figlotech.Core;
using Figlotech.Core.FileAcessAbstractions;
using Figlotech.Core.Interfaces;
using Figlotech.Data;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;

namespace Figlotech.BDados.Builders {
    public sealed class JoinObjectBuilder : IJoinBuilder {
        private readonly JoinDefinition _join = new JoinDefinition();

        private readonly BuildParametersHelper _buildParameters;

        //private IRdbmsDataAccessor _dataAccessor;
        //public delegate void JoinBuild(Join join);
        //public delegate void ParametrizeBuild(BuildParametersHelper parametros);

        ITextToFileLogger _logger;
        ITextToFileLogger Logger {
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

            } catch (Exception) {

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
    }
}
