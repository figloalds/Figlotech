using System;
using System.Collections;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;

namespace MiniWhere {
    public static class WhereEngine {
        public static IEnumerable<T> Filter<T>(IEnumerable<T> items, string where) {
            var ast = new Parser(where).ParseExpression();
            foreach(var item in items.Where(x => EvalBool(ast, x!, null))) { 
                yield return item;
            }
        }

        public static bool EvalBool(Expr expr, object row, object? thisValue) {
            return expr switch {
                BinaryExpr b => b.Op switch {
                    BinaryOp.And => EvalBool(b.Left, row, thisValue) && EvalBool(b.Right, row, thisValue),
                    BinaryOp.Or => EvalBool(b.Left, row, thisValue) || EvalBool(b.Right, row, thisValue),
                    _ => throw new NotSupportedException()
                },
                NotExpr n => !EvalBool(n.Inner, row, thisValue),

                IsNullExpr isNull =>
                    (ResolveValue(isNull.Target, row, thisValue) is null) ^ isNull.Negated == true
                    ? true : false,
                InExpr inExpr => EvalIn(inExpr, row, thisValue),
                CompareExpr c => EvalCompare(c, row, thisValue),
                CollectionPredicateExpr cp => EvalCollectionPredicate(cp, row, thisValue),

                _ => throw new NotSupportedException($"Unknown Expr: {expr.GetType().Name}")
            };
        }

        private static bool EvalCollectionPredicate(CollectionPredicateExpr cp, object row, object? thisValue) {
            var collectionValue = ResolveValue(cp.Collection, row, thisValue);
            
            if (collectionValue is null) {
                // Null collection: ANY/EXISTS return false, ALL returns true (vacuous truth)
                return cp.Kind == CollectionPredicateKind.All;
            }

            if (collectionValue is not IEnumerable enumerable) {
                throw new NotSupportedException($"Collection predicate target must be IEnumerable, got {collectionValue.GetType().Name}");
            }

            // Handle string specially since it's IEnumerable<char> but that's not what we want
            if (collectionValue is string) {
                throw new NotSupportedException("Collection predicate cannot be used on string values");
            }

            return cp.Kind switch {
                CollectionPredicateKind.Any => EvalAny(enumerable, cp.Predicate, row),
                CollectionPredicateKind.All => EvalAll(enumerable, cp.Predicate, row),
                CollectionPredicateKind.Exists => EvalExists(enumerable, cp.Predicate, row),
                _ => throw new NotSupportedException($"Unknown collection predicate kind: {cp.Kind}")
            };
        }

        private static bool EvalAny(IEnumerable collection, Expr predicate, object row) {
            foreach (var item in collection) {
                if (item != null && EvalBool(predicate, row, item)) {
                    return true;
                }
            }
            return false;
        }

        private static bool EvalAll(IEnumerable collection, Expr predicate, object row) {
            bool hasItems = false;
            foreach (var item in collection) {
                hasItems = true;
                if (item == null || !EvalBool(predicate, row, item)) {
                    return false;
                }
            }
            // Empty collection: ALL returns true (vacuous truth)
            return true;
        }

        private static bool EvalExists(IEnumerable collection, Expr predicate, object row) {
            // EXISTS behaves like ANY - returns true if at least one item matches
            foreach (var item in collection) {
                if (item != null && EvalBool(predicate, row, item)) {
                    return true;
                }
            }
            return false;
        }

        private static bool EvalIn(InExpr e, object row, object? thisValue) {
            var left = ResolveValue(e.Target, row, thisValue);
            bool contains = e.Items.Any(it => ValuesEqual(left, ResolveValue(it, row, thisValue)));
            return e.Negated ? !contains : contains;
        }

        private static bool EvalCompare(CompareExpr c, object row, object? thisValue) {
            // Special-case LIKE encoded as FuncCallExpr("LIKE", ...)
            if (c.Right is FuncCallExpr f && f.Name.Equals("LIKE", StringComparison.OrdinalIgnoreCase)) {
                var leftVal = ResolveValue(f.Args[0], row, thisValue)?.ToString() ?? "";
                var pattern = ResolveValue(f.Args[1], row, thisValue)?.ToString() ?? "";
                return Like(leftVal, pattern);
            }

            var left = ResolveValue(c.Left, row, thisValue);
            var right = ResolveValue(c.Right, row, thisValue);

            int cmp = CompareLoose(left, right);
            return c.Op switch {
                CompareOp.Eq => ValuesEqual(left, right),
                CompareOp.Neq => !ValuesEqual(left, right),
                CompareOp.Lt => cmp < 0,
                CompareOp.Lte => cmp <= 0,
                CompareOp.Gt => cmp > 0,
                CompareOp.Gte => cmp >= 0,
                _ => throw new NotSupportedException()
            };
        }

        private static bool Like(string value, string pattern) {
            // SQL % wildcard
            var regex = "^" + Regex.Escape(pattern).Replace("%", ".*").Replace("_", ".") + "$";
            return Regex.IsMatch(value, regex, RegexOptions.IgnoreCase);
        }
        
        private static object? ResolveValue(ValueExpr v, object row, object? thisValue) {
            return v switch {
                LiteralExpr lit => lit.Value,
                IdentifierExpr id => GetByPath(row, id.Path),
                FuncCallExpr f => EvalFunc(f, row, thisValue),
                ThisExpr => thisValue,
                ThisPathExpr tp => thisValue != null ? GetByPath(thisValue, tp.Path) : null,
                _ => throw new NotSupportedException($"Unknown ValueExpr: {v.GetType().Name}")
            };
        }

        private static object? EvalFunc(FuncCallExpr f, object row, object? thisValue) {
            string name = f.Name.ToUpperInvariant();
            object? A(int i) => ResolveValue(f.Args[i], row, thisValue);

            return name switch {
                "CONTAINS" => (A(0)?.ToString() ?? "").IndexOf(A(1)?.ToString() ?? "", StringComparison.OrdinalIgnoreCase) >= 0,
                "STARTSWITH" => (A(0)?.ToString() ?? "").StartsWith(A(1)?.ToString() ?? "", StringComparison.OrdinalIgnoreCase),
                "ENDSWITH" => (A(0)?.ToString() ?? "").EndsWith(A(1)?.ToString() ?? "", StringComparison.OrdinalIgnoreCase),
                "LOWER" => (A(0)?.ToString() ?? "").ToLowerInvariant(),
                "UPPER" => (A(0)?.ToString() ?? "").ToUpperInvariant(),
                _ => throw new NotSupportedException($"Unknown function: {f.Name}")
            };
        }

        private static object? GetByPath(object obj, string path) {
            object? cur = obj;
            foreach (var seg in path.Split('.')) {
                if (cur is null) return null;

                var t = cur.GetType();
                var prop = t.GetProperty(seg, BindingFlags.Instance | BindingFlags.Public | BindingFlags.IgnoreCase);
                if (prop != null) { cur = prop.GetValue(cur); continue; }

                var field = t.GetField(seg, BindingFlags.Instance | BindingFlags.Public | BindingFlags.IgnoreCase);
                if (field != null) { cur = field.GetValue(cur); continue; }

                // IDictionary support
                if (cur is IDictionary dict && dict.Contains(seg)) { cur = dict[seg]; continue; }

                return null;
            }
            return cur;
        }

        private static bool ValuesEqual(object? a, object? b) {
            if (a is null && b is null) return true;
            if (a is null || b is null) return false;

            // numeric loose equality
            if (TryToDouble(a, out var da) && TryToDouble(b, out var db))
                return Math.Abs(da - db) < 1e-9;

            return string.Equals(a.ToString(), b.ToString(), StringComparison.OrdinalIgnoreCase);
        }

        private static int CompareLoose(object? a, object? b) {
            if (a is null && b is null) return 0;
            if (a is null) return -1;
            if (b is null) return 1;

            if (TryToDouble(a, out var da) && TryToDouble(b, out var db))
                return da.CompareTo(db);

            // DateTime parsing if right side is a string like "2025-01-01"
            if (TryToDateTime(a, out var ta) && TryToDateTime(b, out var tb))
                return ta.CompareTo(tb);

            return string.Compare(a.ToString(), b.ToString(), StringComparison.OrdinalIgnoreCase);
        }

        private static bool TryToDouble(object v, out double d) {
            if (v is double dd) { d = dd; return true; }
            if (v is float ff) { d = ff; return true; }
            if (v is int ii) { d = ii; return true; }
            if (v is long ll) { d = ll; return true; }
            return double.TryParse(v.ToString(), NumberStyles.Float, CultureInfo.InvariantCulture, out d);
        }

        private static bool TryToDateTime(object v, out DateTime dt) {
            if (v is DateTime dtt) { dt = dtt; return true; }
            return DateTime.TryParse(v.ToString(), CultureInfo.InvariantCulture, DateTimeStyles.AssumeLocal, out dt);
        }
    }
}
