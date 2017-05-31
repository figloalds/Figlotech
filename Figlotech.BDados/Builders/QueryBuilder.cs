
/**
 * Figlotech.BDados.Builders.QueryBuilder
 * Default implementation for IQueryBuilder
 * 
 * @Author: Felype Rennan Alves dos Santos
 * August/2014
 * 
**/


using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.BDados.Interfaces;
using Figlotech.Core;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Figlotech.BDados.Builders {
    public class Qb : QueryBuilder {
    }

    public class QueryBuilder : IQueryBuilder {
        private bool _conditionalEngaged = false;
        private bool _elseEngaged = false;
        private bool _conditionalRetv = false;

        public QueryBuilder() { }
        public QueryBuilder(String fragment, params object[] args) {
            Append(fragment, args);
        }

        internal StringBuilder _queryString = new StringBuilder();
        internal Dictionary<String, Object> _objParams = new Dictionary<String, Object>();
        public bool IsEmpty {
            get {
                return _queryString.Length == 0;
            }
        }

        public static QueryBuilder FromResource(String path, params Object[] args) {
            return FromResource(path, null, args);
        }
        public static QueryBuilder FromResource(String path, String defaultNamespace = null, params Object[] args) {
            var assembly = Assembly.GetCallingAssembly();
            if (defaultNamespace == null) {
                defaultNamespace = assembly.GetName().Name;
            }
            var resourceName = $"{defaultNamespace}.{path.Replace("\\",".").Replace("/",".")}";
            try {
                using (Stream stream = assembly.GetManifestResourceStream(resourceName))
                using (StreamReader reader = new StreamReader(stream)) {
                    string result = reader.ReadToEnd();
                    return new QueryBuilder(result, args);
                }
            } catch(Exception x) {
                throw new BDadosException($"QueryBuilder couldn't load resource: {resourceName}");
            }

        }

        public static QueryBuilder operator +(QueryBuilder a, QueryBuilder b) {
            return (QueryBuilder) a.Append(b);
        }

        public static QueryBuilder operator +(String a, QueryBuilder b) {
            return (QueryBuilder)new QueryBuilder(a).Append(b);
        }

        public static QueryBuilder operator +(QueryBuilder a, String b) {
            return (QueryBuilder)a.Append(b);
        }

        public QueryBuilder t(string text) {
            return (QueryBuilder) Append(new QueryBuilder(text));
        }
        public QueryBuilder tif(bool condition, string text) {
            return (QueryBuilder)Append(condition ? t(text) : new QueryBuilder());
        }
        public QueryBuilder v(object value) {
            return (QueryBuilder)Append(new QueryBuilder($"@{IntEx.GenerateShortRid()}", value));
        }
        public QueryBuilder vif(bool condition, string text) {
            return (QueryBuilder)Append(condition ? v(text) : new QueryBuilder());
        }
        public QueryBuilder q(QueryBuilder qb) {
            return (QueryBuilder)Append(qb);
        }
        public QueryBuilder q(Func<QueryBuilder, QueryBuilder> fun) {
            return (QueryBuilder)Append(fun(new QueryBuilder()));
        }
        public QueryBuilder eq(string col, object val) {
            return t($"{col}=").v(val);
        }
        public QueryBuilder neq(string col, object val) {
            return t($"{col}!=").v(val);
        }
        public QueryBuilder gt(string col, object val) {
            return t($"{col}>").v(val);
        }
        public QueryBuilder lt(string col, object val) {
            return t($"{col}<").v(val);
        }
        public QueryBuilder gte(string col, object val) {
            return t($"{col}>=").v(val);
        }
        public QueryBuilder lte(string col, object val) {
            return t($"{col}<=").v(val);
        }
        public QueryBuilder isNull(string col) {
            return t($"{col} IS NULL");
        }
        public QueryBuilder notNull(string col) {
            return t($"{col} IS NOT NULL");
        }
    
        public QueryBuilder Select(string cols) {
            return (QueryBuilder)Append(new QueryBuilder().t($"SELECT {cols}"));
        }
        public QueryBuilder From<T>(string alias = null) {
            return t($"FROM {typeof(T).Name.ToLower()}").tif(alias != null, alias);
        }
        public QueryBuilder From(string table, string alias = null) {
            return t($"FROM {table}").tif(alias != null, alias);
        }
        public QueryBuilder Where(Func<QueryBuilder, QueryBuilder> fun) {
            return t($"WHERE ").q(fun);
        }
        public QueryBuilder InsertInto<T>(params string[] cols) {
            var retv = new QueryBuilder($"INSERT INTO {typeof(T).Name.ToLower()}");
            if (cols.Length > 0) {
                retv.Append("(");
                for (int i = 0; i < cols.Length; i++) {
                    retv.Append(cols[i]);
                    if (i < cols.Length - 1) {
                        retv.Append(",");
                    }
                }
                retv.Append(")");
            }
            return (QueryBuilder)Append(retv);
        }
        public QueryBuilder InsertInto(string tbl, params string[] cols) {
            var retv = new QueryBuilder($"INSERT INTO {tbl}");
            if(cols.Length > 0) {
                retv.Append("(");
                for (int i = 0; i < cols.Length; i++) {
                    retv.Append(cols[i]);
                    if(i < cols.Length-1) {
                        retv.Append(",");
                    }
                }
                retv.Append(")");
            }
            return (QueryBuilder)Append(retv);
        }
        public QueryBuilder Update(string tbl) {
            return t($"UPDATE {tbl}");
        }
        public QueryBuilder Set(IDictionary<String, Object> stuff) {
            var retv = new QueryBuilder("SET");
            for(int i = 0; i<stuff.Count; i++) {
                retv.eq(stuff.Keys.ElementAt(i), stuff.Values.ElementAt(i));
                if(i < stuff.Count - 1) {
                    retv.Append(",");
                }
            }
            return (QueryBuilder)Append(retv);
        }
        
        public IQueryBuilder Build(params IQueryBuilder[] args) {
            foreach(var a in args) {
                Append(a);
            }

            return this;
        }

        public IQueryBuilder Append(String fragment, params object[] args) {
            if(fragment == null) {
                return this;
            }
            if (!_conditionalEngaged || (_conditionalEngaged && _conditionalRetv)) {
                if (_queryString.Length > 0 && !fragment.StartsWith(" "))
                    fragment = $" {fragment}";
                _queryString.Append(fragment);
                var matchcol = Regex.Matches(fragment, "@(?<tagname>[a-zA-Z0-9_]*)");
                int iterator = -1;
                List<Match> matches = new List<Match>();
                foreach (Match match in matchcol) {
                    if (!matches.Where((m) => m.Groups["tagname"].Value == match.Groups["tagname"].Value).Any()) {
                        matches.Add(match);
                    }
                }
                if (args.Length > 0 && args.Length != matches.Count) {
                    throw new Exception($@"Parameter count mismatch on QueryBuilder.Append
                        Text was: {fragment}
                        Parameters: {string.Join(", ", args)}");
                        
                }
                foreach (Match match in matches) {
                    var pname = match.Groups["tagname"].Value;
                    if (_objParams.Where((p) => p.Key == pname).Any()) {
                        continue;
                    }
                    if (++iterator > args.Length - 1)
                        break;
                    _objParams.Add(pname, args[iterator]);
                }
            }
            return this;
        }

        public IQueryBuilder Append(IQueryBuilder other) {
            if (!_conditionalEngaged || (_conditionalEngaged && _conditionalRetv)) {
                this._queryString.Append(" ");
                this._queryString.Append(other.GetCommandText());
                foreach(var a in other.GetParameters()) {
                    this._objParams.Add(a.Key, a.Value);
                }
            }
            return this;
        }

        public IQueryBuilder If(bool Condition) {
            _conditionalRetv = Condition;
            _conditionalEngaged = true;
            return this;
        }

        public IQueryBuilder Then() {
            _conditionalEngaged = true;
            return this;
        }

        public IQueryBuilder Else() {
            _conditionalRetv = !_conditionalRetv;
            _elseEngaged = true;
            return this;
        }

        public IQueryBuilder EndIf() {
            _conditionalRetv = false;
            _elseEngaged = false;
            _conditionalEngaged = false;
            return this;
        }

        public Dictionary<String, Object> GetParameters() {
            return _objParams;
        }

        public string GetCommandText() {
            return _queryString.ToString();
        }
    }
}
