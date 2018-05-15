using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.Core;
using Figlotech.Core.Helpers;
/**
* Iaetec.BDados.Builders.QueryBuilder
* Default implementation for IQueryBuilder
* 
* @Author: Felype Rennan Alves dos Santos
* August/2014
* 
**/
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;

namespace Figlotech.BDados.Builders {
    public class QbParam : QueryParameter {
        public QbParam(object value) : base(value) {
        }

    }
    public class QbIfParam : QueryParameter {
        public bool Condition { get; private set; }
        public object ElseValue { get; private set; }
        public QbIfParam(bool condition, object value, object elseVal) : base(value) {
            this.Condition = condition;
            this.ElseValue = elseVal;
        }
    }
    public class QueryParameter {
        public object Value { get; private set; }
        public QueryParameter(object value) {
            this.Value = value;
        }
    }
    public class QbIf : QueryBuilder {
        public QbIf(bool condition, params object[] args) {
            If(condition);
            AppendQuery(args);
        }
    }
    public class Qb : QueryBuilder {
        public Qb(params object[] args) {
            AppendQuery(args);
        }

        public static QbParam Param(object o) {
            return new QbParam(o);
        }

        static readonly string defParam = IntEx.GenerateShortRid();
        static int pids = 0;
        private static string paramId => $"{defParam}{++pids}";

        private static QbFmt ListQueryFunction<T>(string column, IList<T> o, Func<T, object> fn, bool isIn) {
            if (!o.Any()) {
                return Qb.Fmt(isIn ? "FALSE" : "TRUE");
            }
            var retv = Qb.Fmt($"{column} {(isIn ? "IN" : "NOT IN")} (");
            var sRetv = IntEx.GenerateShortRid();
            for (int i = 0; i < o.Count; i++) {
                retv.Append($"@{paramId}", fn?.Invoke(o[i]));
                if (i < o.Count - 1) {
                    retv.Append(",");
                }
            }
            retv.Append(")");

            return retv;
        }

        public static QbFmt Eq(string column, object value) {
            return Qb.Fmt($"{column}=@{paramId}", value);
        }
        public static QbFmt Neq(string column, object value) {
            return Qb.Fmt($"{column}!=@{paramId}", value);
        }
        public static QbFmt Gt(string column, object value) {
            return Qb.Fmt($"{column}>@{paramId}", value);
        }
        public static QbFmt Ge(string column, object value) {
            return Qb.Fmt($"{column}>=@{paramId}", value);
        }
        public static QbFmt Lt(string column, object value) {
            return Qb.Fmt($"{column}<@{paramId}", value);
        }
        public static QbFmt Le(string column, object value) {
            return Qb.Fmt($"{column}<=@{paramId}", value);
        }

        public static QbFmt Or() {
            return Qb.Fmt("OR");
        }
        public static QbFmt And() {
            return Qb.Fmt("AND");
        }

        public static QbFmt In<T>(string column, IList<T> o, Func<T, object> fn) {
            return ListQueryFunction(column, o, fn, true);
        }
        public static QbFmt NotIn<T>(string column, IList<T> o, Func<T, object> fn) {
            return ListQueryFunction(column, o, fn, false);
        }

        public static QbListOf<T> List<T>(IList<T> o, Func<T, object> fn) {
            return new QbListOf<T>(o, fn);
        }
        public static QbFmt Fmt(String str, params object[] args) {
            return new QbFmt(str, args);
        }
        static string fmtPrefix = IntEx.GenerateShortRid();
        static int fmtCnt = 0;
        public static QbFmt S(FormattableString fmtstr) {
            var fmt = fmtstr.Format.RegExReplace(@"\{([^\}]*)\}", ()=> $"@{fmtPrefix}{fmtCnt++}");
            return new QbFmt(fmt, fmtstr.GetArguments());
        }

        public static QbIf If(bool condition, params object[] args) {
            return new QbIf(condition, args);
        }

        public static explicit operator Qb(string s) {
            return new Qb(s);
        }
    }
    public class QbFmt : QueryBuilder {
        public QbFmt(String str, params object[] args) {
            Append(str, args);
        }

        public static explicit operator QbFmt(KeyValuePair<string, object> s) {
            return new QbFmt(s.Key, s.Value);
        }
    }
    public class QbListOf<T> : QueryBuilder {
        public IList<T> List { get; private set; }
        public Func<T, object> Selector { get; private set; }
        public QbListOf(IList<T> list, Func<T, object> fn) {
            AppendListOf(list, fn);
        }
    }
    public class QueryBuilder : IQueryBuilder {
        private bool _conditionalEngaged = false;
        private bool _elseEngaged = false;
        private bool _conditionalRetv = false;

        public QueryBuilder() { }
        public QueryBuilder(params object[] args) {
            AppendQuery(args);
        }

        public QueryBuilder AppendQuery(params object[] args) {

            foreach (var a in args) {
                if (a is String s) {
                    Append(s);
                } else
                if (a is IQueryBuilder qb) {
                    Append(qb);
                } else
                if (a is QbParam p) {
                    Append("@???", p.Value);
                } else
                if (a is QbIfParam ip) {
                    Append("@???", ip.Condition ? ip.Value : ip.ElseValue);
                } else {
                    Append("@???", a);
                }
            }

            return this;
        }

        //public QueryBuilder(String fragment, params object[] args) {
        //    Append(fragment, args);
        //}

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
            var resourceName = $"{defaultNamespace}.{path.Replace("\\", ".").Replace("/", ".")}";
            try {
                using (Stream stream = assembly.GetManifestResourceStream(resourceName))
                using (StreamReader reader = new StreamReader(stream)) {
                    string result = reader.ReadToEnd();
                    return new QbFmt(result, args);
                }
            } catch (Exception x) {
                throw new BDadosException($"QueryBuilder couldn't load resource: {resourceName}");
            }

        }

        public QueryBuilder AppendListOf<T>(IList<T> list, Func<T, object> fn) {
            for (int i = 0; i < list.Count; i++) {
                this.Append("@???", fn(list[i]));
                if (i < list.Count - 1) {
                    this.Append(",");
                }
            }
            return this;
        }

        //public IQueryBuilder Append(dynamic b) {
        //    ObjectReflector or = b.AsReflectable();
        //    var members = ReflectionTool.FieldsAndPropertiesOf(b.GetType());
        //    var g = new QueryBuilder();
        //    for (int i = 0; i < members.Count; i++) {
        //        g.Append($"@{l}", or[members[i]]);
        //        if (i < members.Count - 1) {
        //            g.Append(",");
        //        }
        //    }

        //    return this.Append(g);
        //}

        //public static QueryBuilder operator +(QueryBuilder a, dynamic b) {
        //    return (QueryBuilder)a.Append(b);
        //}

        public static QueryBuilder operator +(QueryBuilder a, QueryBuilder b) {
            return (QueryBuilder)a.Append(b);
        }

        public static QueryBuilder operator +(String a, QueryBuilder b) {
            return (QueryBuilder)new QbFmt(a).Append(b);
        }

        public static QueryBuilder operator +(QueryBuilder a, String b) {
            return (QueryBuilder)a.Append(b);
        }

        static long _l = 17;
        Random r = new Random();
        string l {
            get {
                String retv = "";

                lock ("___QUERYBUILDER_AUTOPARAM__") {
                    _l += ((1 + r.Next(10)) * 17);
                    retv = _l.ToString("x");
                }

                return retv;
            }
        }

        public IQueryBuilder Append(String fragment, params object[] args) {
            if (!_conditionalEngaged || (_conditionalEngaged && _conditionalRetv)) {
                var pat = new Regex(Regex.Escape("@???"));
                var s = false;

                do {
                    s = pat.Match(fragment).Success;
                    fragment = pat.Replace(fragment, $"@{l}", 1);
                } while (s);
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
                var otherParams = other.GetParameters();
                for (int i = 0; i < otherParams.Count; i++) {
                    var Param = otherParams.ElementAt(i);
                    if (!this._objParams.ContainsKey(Param.Key)) {
                        this._objParams.Add(Param.Key, Param.Value);
                    }
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
