/**
* Iaetec.BDados.Builders.QueryBuilder
* Default implementation for IQueryBuilder
* 
* @Author: Felype Rennan Alves dos Santos
* August/2014
* 
* Comment on comments, yep they do tend to become a lie over time.
**/
using Figlotech.Core.Helpers;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;

namespace Figlotech.Data {
    public sealed class QbParam : QueryParameter {
        public QbParam(object value) : base(value) {
        }

    }
    public sealed class QbIfParam : QueryParameter {
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
    public sealed class QbIf : QueryBuilder {
        bool Condition;
        public QbIf(bool condition) {
            this.Condition = condition;
        }

        public QbIf Then(string query, params object[] args) {
            if (Condition) {
                return (QbIf)this.Append(query, args);
            } else {
                return (QbIf)this;
            }
        }

        public QbIf Else(string query, params object[] args) {
            if (Condition) {
                return (QbIf)this;
            } else {
                return (QbIf)this.Append(query, args);
            }
        }
        public QbIf Then(IQueryBuilder qb) {
            if (Condition) {
                return (QbIf)this.Append(qb);
            } else {
                return (QbIf)this;
            }
        }
        public QbIf Else(IQueryBuilder qb) {
            if (Condition) {
                return (QbIf)this;
            } else {
                return (QbIf)this.Append(qb);
            }
        }

    }
    public sealed class Qb : QueryBuilder {
        public Qb(params object[] args) {
            AppendQuery(args);
        }

        public static Qb Param(object o) {
            return new Qb($"@{paramId}", o);
        }

        static int pids = 0;
        public static string paramId => $"_qb{++pids}";

        private static QbFmt ListQueryFunction<T>(string column, List<T> o, Func<T, object> fn, bool isIn) {
            if (o == null || !o.Any()) {
                return Qb.Fmt(isIn ? "FALSE" : "TRUE");
            }
            var retv = Qb.Fmt($"{column} {(isIn ? "IN" : "NOT IN")} (");
            for (int i = 0; i < o.Count; i++) {
                retv.Append($"@{paramId}", fn?.Invoke(o[i]));
                if (i < o.Count - 1) {
                    retv.Append(",");
                }
            }
            retv.Append(")");

            return retv;
        }

        public static QueryBuilder Compose(params IQueryBuilder[] args) {
            var Qb = new Qb();
            args.ForEach(
                a => Qb.Append(a)
            );
            return Qb;
        }

        public static QueryBuilder Wrap(params IQueryBuilder[] args) {
            var Qb = new Qb();
            Qb.Append("(");
            args.ForEach(
                a => Qb.Append(a)
            );
            Qb.Append(")");
            return Qb;
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
        public static QueryBuilder Or(IQueryBuilder left, IQueryBuilder right) {
            var customOr = Wrap(left, Or(), right);
            return customOr;
        }
        public static QueryBuilder And(IQueryBuilder left, IQueryBuilder right) {
            var customAnd = Wrap(left, And(), right);
            return customAnd;
        }

        public static QueryBuilder And(IQueryBuilder right) {
            var customAnd = Qb.And().Append(right);
            return (QueryBuilder)customAnd;
        }
        public static QueryBuilder And(string query, params object[] args) {
            var customAnd = Qb.And().Append(query, args);
            return (QueryBuilder)customAnd;
        }

        public static QbFmt In<T>(string column, List<T> o, Func<T, object> fn) {
            return ListQueryFunction(column, o, fn, true);
        }
        public static QbFmt NotIn<T>(string column, List<T> o, Func<T, object> fn) {
            return ListQueryFunction(column, o, fn, false);
        }

        public static QbListOf<T> List<T>(List<T> o, Func<T, object> fn) {
            return new QbListOf<T>(o, fn);
        }
        public static QbFmt Fmt(String str, params object[] args) {
            return new QbFmt(str, args);
        }
        static int fmtCnt = 0;
        public static QbFmt S(FormattableString fmtstr) {
            var fmt = fmtstr.Format.RegExReplace(@"\{([^\}]*)\}", () => $"@_qbs{fmtCnt++}");
            return new QbFmt(fmt, fmtstr.GetArguments());
        }

        public static QbIf If(bool condition) {
            return new QbIf(condition);
        }

        public static explicit operator Qb(string s) {
            return new Qb(s);
        }
    }
    
    public sealed class QbFmt : QueryBuilder {
        public QbFmt(String str, params object[] args) {
            Append(str, args);
        }

        public static explicit operator QbFmt(KeyValuePair<string, object> s) {
            return new QbFmt(s.Key, s.Value);
        }
    }
    public sealed class QbListOf<T> : QueryBuilder {
        public List<T> List { get; private set; }
        public Func<T, object> Selector { get; private set; }
        public QbListOf(List<T> list, Func<T, object> fn) {
            AppendListOf(list, fn);
        }
    }
    public class QueryBuilder : IQueryBuilder {

        private static long _qid = 0;

        public long Id { get; } = ++_qid;

        private bool _conditionalEngaged = false;

        private bool _elseEngaged = false;

        private bool _conditionalRetv = false;

        internal StringBuilder _queryString = new StringBuilder(2048);
        internal Dictionary<String, Object> _objParams = new Dictionary<String, Object>();

        public QueryBuilder() { }
        public QueryBuilder(params object[] args) {
            AppendQuery(args);
        }

        public void PrepareForQueryLength(int capacity) {
            _queryString.EnsureCapacity(capacity);
        }

        public IDbCommand ToCommand(IDbConnection connection) {
            var command = connection.CreateCommand();
            command.CommandText = this.GetCommandText();

            foreach (KeyValuePair<String, Object> param in this.GetParameters()) {
                var cmdParam = command.CreateParameter();
                cmdParam.ParameterName = param.Key;
                cmdParam.Value = param.Value;
                command.Parameters.Add(cmdParam);
            }
            return command;
        }

        public T ToCommand<T>(IDbConnection connection) where T: IDbCommand {
            return (T) ToCommand(connection);
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

        public bool IsEmpty {
            get {
                return _queryString.Length == 0;
            }
        }

        public QueryBuilder AppendListOf<T>(List<T> list, Func<T, object> fn) {
            for (int i = 0; i < list.Count; i++) {
                this.Append("@???", fn(list[i]));
                if (i < list.Count - 1) {
                    this.Append(",");
                }
            }
            return this;
        }

        public static QueryBuilder operator +(QueryBuilder a, QueryBuilder b) {
            return (QueryBuilder)a.Append(b);
        }

        public static QueryBuilder operator +(String a, QueryBuilder b) {
            return (QueryBuilder)new QbFmt(a).Append(b);
        }

        public static QueryBuilder operator +(QueryBuilder a, String b) {
            return (QueryBuilder)a.Append(b);
        }

        static long _l = 0;
        Random r = new Random();
        string l {
            get {
                String retv = "";

                lock ("___QUERYBUILDER_AUTOPARAM__") {
                    retv = (++_l).ToString("x");
                }

                return retv;
            }
        }

        public IQueryBuilder Append(String fragment, params object[] args) {
            if (!_conditionalEngaged || (_conditionalEngaged && _conditionalRetv)) {
                if(fragment.Contains("@???")) {
                    var pat = new Regex(Regex.Escape("@???"));
                    var s = false;

                    do {
                        s = pat.Match(fragment).Success;
                        fragment = pat.Replace(fragment, $"@{l}", 1);
                    } while (s);
                }
                if (_queryString.Length > 0 && !fragment.StartsWith(" "))
                    _queryString.Append(" ");
                var matchcol = Regex.Matches(fragment, "@(?<tagname>[a-zA-Z0-9_]*)");
                int iterator = -1;

                List<Match> matches = new List<Match>();
                foreach (Match match in matchcol) {
                    if (!matches.Where((m) => m.Groups["tagname"].Value == match.Groups["tagname"].Value).Any()) {
                        matches.Add(match);
                    }
                }

                var args2 = args.Where(a => !a?.GetType()?.Name.Contains("PythonFunction") ?? true).ToArray();
                if (args2.Length > 0 && args2.Length != matches.Count) {

                    throw new Exception($@"Parameter count mismatch on QueryBuilder.Append\r\n
                        Text Parameters: {string.Join(", ", matches.Select(x=> $"@{x.Groups["tagname"].Value}"))}\r\n
                        Parameters: {string.Join(", ", args)}\r\n
                        Text was: {fragment}\r\n");
                }

                foreach (Match match in matches) {
                    var pname = match.Groups["tagname"].Value;
                    if (++iterator > args.Length - 1)
                        break;
                    if (_objParams.ContainsKey(pname)) {
                        continue;
                    }
                    var item = args[iterator];
                    if (item is IQueryBuilder iqb) {
                        fragment = fragment.Replace($"@{match.Groups["tagname"].Value}", iqb.GetCommandText());
                        foreach (var param in iqb.GetParameters()) {
                            _objParams[param.Key] = param.Value;
                        }
                    } else {
                        _objParams.Add(pname, args[iterator]);
                    }
                }
                _queryString.Append(fragment);
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

        public Dictionary<String, Object> GetParameters() {
            return _objParams;
        }

        public string GetCommandText() {
            return _queryString.ToString();
        }
    }
}
