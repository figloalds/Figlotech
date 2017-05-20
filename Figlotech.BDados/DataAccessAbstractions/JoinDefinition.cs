using Figlotech.BDados.Helpers;
using Figlotech.BDados.Interfaces;
using System;
using System.Collections.Generic;
using System.Data;
using System.Dynamic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Figlotech.BDados.DataAccessAbstractions {
    public enum AggregateBuildOptions
    {
        None, AggregateField, AggregateList, AggregateObject
    }
    public enum JoinType
    {
        LEFT, RIGHT, INNER, CROSS, LEFT_OUTER, RIGHT_OUTER, NATURAL, NATURAL_LEFT_OUTER, NATURAL_RIGHT_OUTER
    }

    public class JoinDefinition
    {
        private bool Validated = false;
        public List<JoiningTable> Joins = new List<JoiningTable>();
        public List<Relation> Relations = new List<Relation>();

        public JoinDefinition()
        {
        }

        private void ValidateOnClauses(String args, String prefix)
        {
            if (args == null || args.Length < 1)
                throw new BDadosException($"ON CLAUSE should be declared for {prefix}");

            if (args == "true" || args == "false") {
                return;
            }
            if (!args.Contains("=")) {
                throw new BDadosException($"ON CLAUSE {args} doesn't make sense, BDados doesn't know how to deal with this kind of crazy yet.");
            }

            List<String> usedAliases = new List<String>();
            foreach (String a in (from a in Joins select a.Prefix)) {
                if (args.Contains(a + ".")) {
                    usedAliases.Add(a);
                    break;
                }
            }
            if (args.Contains(prefix + "."))
                usedAliases.Add(prefix);
            if (usedAliases.Count < 2) {
                throw new BDadosException("ON CLAUSE should reference at least 2 different aliases");
            }
            // TODO: Adicionar validação dos nomes dos campos depois.
        }

        public JoinConfigureHelper<T> AggregateRoot<T>(String Alias) where T : IDataObject, new() 

        {
            // Tipo junção é ignorado para a primeira tabela, de qualquer forma.
            Join<T>(Alias, "", JoinType.LEFT);
            return GenerateNewHelper<T>(Joins.Count-1);
        }

        private JoinConfigureHelper<T> GenerateNewHelper<T>(int Index)
        {
            return new JoinConfigureHelper<T>(this, Index);
        }

        internal List<Relation> GenerateRelations()
        {
            Validated = false;
            Relations.Clear();
            for (int i = 1; i < Joins.Count; ++i) {
                try {
                    Match m = Regex.Match(Joins[i].Args, @"(?<PreA>\w+)\.(?<KeyA>\w+)=(?<PreB>\w+).(?<KeyB>\w+)");
                    int IndexA = Joins.IndexOf((from a in Joins where a.Prefix == m.Groups["PreA"].Value select a).First());
                    int IndexB = Joins.IndexOf((from b in Joins where b.Prefix == m.Groups["PreB"].Value select b).First());
                    if (IndexA < 0 || IndexB < 0) {
                        Relations.Clear();
                        throw new BDadosException(String.Format("Join arguments '{0}' in join {1} are invalid.", Joins[i].Args, i + 1));
                    }
                    String keyA = m.Groups["KeyA"].Value;
                    String keyB = m.Groups["KeyB"].Value;
                    if (!FTH.FindColumn(keyA, Joins[IndexA].ValueObject)) {
                        Relations.Clear();
                        throw new BDadosException($"Field {keyA} does not exist on '{Joins[IndexA].TableName} AS {Joins[IndexA].Prefix}'");
                    }
                    if (!FTH.FindColumn(keyB, Joins[IndexB].ValueObject)) {
                        Relations.Clear();
                        throw new BDadosException($"Field {keyB} does not exist on '{Joins[IndexB].TableName} AS {Joins[IndexB].Prefix}'");
                    }
                    Relation r1 = new Relation();
                    Relation r2 = new Relation();
                    r1.ParentKey = keyB;
                    r1.ParentIndex = IndexB;
                    r1.ChildKey = keyA;
                    r1.ChildIndex = IndexA;
                    r1.AssemblyOption = AggregateBuildOptions.None;

                    r2.ParentKey = keyA;
                    r2.ParentIndex = IndexA;
                    r2.ChildKey = keyB;
                    r2.ChildIndex = IndexB;
                    r2.AssemblyOption = AggregateBuildOptions.None;
                    Relations.Add(r1);
                    Relations.Add(r2);
                } catch (Exception x) {
                    throw new BDadosException($"This error was not supposed to happen: {x.Message}");
                }
            }

            Validated = true;
            return Relations;
        }

        /// <summary>
        /// Adds a DataObject into this join.
        /// </summary>
        /// <typeparam name="T">The IDataObject type of this join</typeparam>
        /// <param name="Alias">Table alias</param>
        /// <param name="Args">ON CLAUSE argument</param>
        /// <param name="joinType">Specifies the join type between LEFT, RIGHT or INNER</param>
        public JoinConfigureHelper<T> Join<T>(String Alias, String Args = "", JoinType joinType = JoinType.LEFT) where T : IDataObject, new() 

        {
            Validated = false;
            Relations.Clear();
            if ((from a in Joins select a.Args).Contains(Alias)) {
                throw new BDadosException("This Alias has already been used in this join.");
            }

            JoiningTable tj = new JoiningTable();
            tj.Alias = Alias;
            tj.TableName = typeof(T).Name.ToLower();
            tj.ValueObject = typeof(T);
            tj.Args = Args;
            tj.Type = joinType;
            tj.Prefix = GetAPrefix(Alias);
            Joins.Add(tj);
            return GenerateNewHelper<T>(Joins.Count - 1);
        }

        private bool ValidateTableCount()
        {
            int c = Joins.Count;
            return c>0;
        }
        
        private String GetAPrefix(String Alias)
        {
            List<String> prefixes = (from a in Joins select a.Prefix).ToList();
            int tam = 0;
            int c = 0;
            String retv = "";
            do {
                string tab = Regex.Replace(Alias, "[^\\w]", "");
                tab = tab.Replace("_", "");
                if (tam < tab.Length)
                    retv = tab.ToLower().Substring(0, ++tam);
                else
                    retv = tab.ToLower() + (++c);
            } while (prefixes.Contains(retv));
            return retv;
        }

    }
}
