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

namespace Figlotech.BDados.Entity
{
    public enum AggregateBuildOptions
    {
        None, AggregateField, AggregateList, AggregateObject
    }
    public enum TipoJuncao
    {
        LEFT, RIGHT, INNER, CROSS, LEFT_OUTER, RIGHT_OUTER, NATURAL, NATURAL_LEFT_OUTER, NATURAL_RIGHT_OUTER
    }

    public class Relation
    {
        internal int ChildIndex;
        internal int ParentIndex;
        internal String ParentKey;
        internal String ChildKey;
        internal AggregateBuildOptions AssemblyOption;
        internal String NewName;
        internal List<String> Fields = new List<String>();
    }

    public sealed class JoiningTable
    {
        public JoiningTable() { }
        public Type ValueObject = null;
        public String TableName = null;
        public TipoJuncao Type = TipoJuncao.LEFT;
        public String Args = null;
        public String Prefix = null;
        public String Alias = null;
        public List<String> Excludes = new List<String>();
    }

    public class JoinDefinition
    {
        private bool Validated = false;
        public List<JoiningTable> Joins = new List<JoiningTable>();
        public List<Relation> Relations = new List<Relation>();

        public JoinDefinition()
        {
        }

        private void ValidateOnClauses(String Args, String Prefixo)
        {
            if (Args == null || Args.Length < 1)
                throw new BDadosException("Faltando argumentos");

            if (Args == "true" || Args == "false") {
                return;
            }
            if (!Args.Contains("=")) {
                throw new BDadosException("Os argumentos de junção passados não estão comparando nada com nada. Veja http://Figlotech.com/IaeDados/Documentacao/ArgumentosJuncao para mais informações.");
            }

            List<String> usedAliases = new List<String>();
            foreach (String a in (from a in Joins select a.Prefix)) {
                if (Args.Contains(a + ".")) {
                    usedAliases.Add(a);
                    break;
                }
            }
            if (Args.Contains(Prefixo + "."))
                usedAliases.Add(Prefixo);
            if (usedAliases.Count < 2) {
                throw new BDadosException("Os argumentos de junção passados não estão comparando nada com nada. Eles deveriam fazer referencia a pelo menos dois Alias diferentes");
            }
            // TODO: Adicionar validação dos nomes dos campos depois.
        }

        public JoinConfigureHelper<T> AggregateRoot<T>(String Alias) where T : IDataObject<T>
        {
            // Tipo junção é ignorado para a primeira tabela, de qualquer forma.
            Join<T>(Alias, "", TipoJuncao.LEFT);
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
                        throw new BDadosException(String.Format("Os argumentos '{0}' na junção {1} não são válidos.", Joins[i].Args, i + 1));
                    }
                    String ChaveA = m.Groups["KeyA"].Value;
                    String ChaveB = m.Groups["KeyB"].Value;
                    if (!DataObject.FindColumn(ChaveA, Joins[IndexA].ValueObject)) {
                        Relations.Clear();
                        throw new BDadosException($"Field {ChaveA} does not exist on '{Joins[IndexA].TableName} AS {Joins[IndexA].Prefix}'");
                    }
                    if (!DataObject.FindColumn(ChaveB, Joins[IndexB].ValueObject)) {
                        Relations.Clear();
                        throw new BDadosException($"Field {ChaveB} does not exist on '{Joins[IndexB].TableName} AS {Joins[IndexB].Prefix}'");
                    }
                    Relation r1 = new Relation();
                    Relation r2 = new Relation();
                    r1.ParentKey = ChaveB;
                    r1.ParentIndex = IndexB;
                    r1.ChildKey = ChaveA;
                    r1.ChildIndex = IndexA;
                    r1.AssemblyOption = AggregateBuildOptions.None;

                    r2.ParentKey = ChaveA;
                    r2.ParentIndex = IndexA;
                    r2.ChildKey = ChaveB;
                    r2.ChildIndex = IndexB;
                    r2.AssemblyOption = AggregateBuildOptions.None;
                    Relations.Add(r1);
                    Relations.Add(r2);
                } catch (Exception x) {
                    throw new BDadosException($"Nasty unforeseen error: {x.Message}");
                }
            }

            Validated = true;
            return Relations;
        }

        /// <summary>
        /// Adiciona um tipo BDTabela ao prototipo de junção.
        /// </summary>
        /// <typeparam name="T">O tipo BDTabela a adicionar à junção</typeparam>
        /// <param name="Alias">O alias que essa tabela vai receber</param>
        /// <param name="Args">O argumento de junção que sera usado na clausula ON (deve ser em branco pra primeira tabela e válido paras as demais)</param>
        /// <param name="Tipo">Tipo de junção: Agregar adiciona os valores da tabela B (essa nova) à tabela referenciada nos args / Ramificar cria um vetor de tabelas B dentro da tabela A.
        /// Esse parametro só é usado na montagem dessa junção como objeto dinamico.</param>
        public JoinConfigureHelper<T> Join<T>(String Alias, String Args = "", TipoJuncao tipoJuncao = TipoJuncao.LEFT) where T : IDataObject<T>
        {
            Validated = false;
            Relations.Clear();
            if ((from a in Joins select a.Args).Contains(Alias)) {
                throw new BDadosException("Esse Alias já foi usado antes nessa mesma junção.");
            }
            //T insta = Activator.CreateInstance<T>();
            //DataObject genericInstance = insta as DataObject;
            //String Prefixo = ObterPrefixo(tbGenerica);
            //FieldInfo[] campos = tbGenerica.GetType().GetFields();
            //for (int i = 0; i < campos.Length; i++) {
            //    foreach (var attr in campos[i].CustomAttributes) {
            //        if (attr.AttributeType == typeof(AttColuna)) {
            //            tBuilder.DefineField(Prefixo + campos[i].Name, campos[i].DeclaringType, campos[i].Attributes);
            //        }
            //    }
            //}

            JoiningTable tj = new JoiningTable();
            tj.Alias = Alias;
            tj.TableName = typeof(T).Name.ToLower();
            tj.ValueObject = typeof(T);
            tj.Args = Args;
            tj.Type = tipoJuncao;
            tj.Prefix = GetAPrefix(Alias);
            Joins.Add(tj);
            return GenerateNewHelper<T>(Joins.Count - 1);
        }

        //private bool ValidarConsulta() {
        //    List<String> aliasesUsados = new List<String>();
        //    foreach (String b in (from a in Juncoes select a.Prefixo)) {
        //        if (Args.Contains(b + ".")) {
        //            aliasesUsados.Add(b);
        //            break;
        //        }
        //    }
        //    if (Juncoes.Count > 1) {
        //        if (Args.Contains(tj.Prefixo + "."))
        //            aliasesUsados.Add(tj.Prefixo);
        //        if (aliasesUsados.Count < 2) {
        //            throw new BDadosException("Os argumentos de junção passados não estão comparando nada com nada. Eles deveriam fazer referencia a pelo menos dois Alias diferentes");
        //        }
        //    }
        //}

        private bool ValidateTableCount()
        {
            int c = Joins.Count;
            return c>0;
        }
        
        private String GetAPrefix(String Alias)
        {
            List<String> Prefixos = (from a in Joins select a.Prefix).ToList();
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
            } while (Prefixos.Contains(retv));
            return retv;
        }

        private bool IsBDTabela(Type t)
        {
            while (t.BaseType != null) {
                if (t == typeof(DataObject)) {
                    return true;
                }
                t = t.BaseType;
            }
            return false;
        }

    }
}
