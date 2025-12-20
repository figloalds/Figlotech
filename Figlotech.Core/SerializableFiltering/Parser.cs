using System;
using System.Collections.Generic;
using System.Globalization;

namespace MiniWhere {
    public sealed class Parser {
        private readonly List<Token> _t;
        private int _p;

        private static readonly HashSet<string> CollectionPredicateFunctions = new HashSet<string>(StringComparer.OrdinalIgnoreCase) {
            "ANY", "ALL", "EXISTS"
        };

        public Parser(string input) {
            _t = new Lexer(input).Lex();
        }

        public Expr ParseExpression() {
            var expr = ParseOr();
            Expect(TokenKind.EOF);
            return expr;
        }

        // OR has lowest precedence
        private Expr ParseOr() {
            var left = ParseAnd();
            while (Match(TokenKind.Or)) {
                var right = ParseAnd();
                left = new BinaryExpr(left, BinaryOp.Or, right);
            }
            return left;
        }

        private Expr ParseAnd() {
            var left = ParseNot();
            while (Match(TokenKind.And)) {
                var right = ParseNot();
                left = new BinaryExpr(left, BinaryOp.And, right);
            }
            return left;
        }

        private Expr ParseNot() {
            if (Match(TokenKind.Not)) {
                var inner = ParseNot();
                return new NotExpr(inner);
            }
            return ParsePrimary();
        }

        private Expr ParsePrimary() {
            if (Match(TokenKind.LParen)) {
                var e = ParseOr();
                Expect(TokenKind.RParen);
                return e;
            }

            // Check for collection predicate functions: ANY, ALL, EXISTS
            if (Check(TokenKind.Identifier) && PeekKind(1) == TokenKind.LParen) {
                string name = _t[_p].Text;
                if (CollectionPredicateFunctions.Contains(name)) {
                    return ParseCollectionPredicate();
                }
            }

            // must be a predicate starting with a ValueExpr (identifier or func call)
            var leftVal = ParseValue();

            // IS [NOT] NULL
            if (Match(TokenKind.Is)) {
                bool neg = Match(TokenKind.Not);
                if (Match(TokenKind.Null)) return new IsNullExpr(leftVal, neg);
                throw new Exception("Expected NULL after IS / IS NOT");
            }

            // [NOT] IN (...)
            if (Match(TokenKind.Not)) {
                if (Match(TokenKind.In)) {
                    var items = ParseInList();
                    return new InExpr(leftVal, items, true);
                }
                // otherwise treat NOT as unary already handled; reaching here means "x NOT = y" etc.
                throw new Exception("Unexpected NOT here");
            }

            if (Match(TokenKind.In)) {
                var items = ParseInList();
                return new InExpr(leftVal, items, false);
            }

            // LIKE
            if (Match(TokenKind.Like)) {
                var right = ParseValue();
                // represent LIKE as FuncCallExpr("LIKE", [left,right]) but we already have CompareExpr shape
                // We'll encode as CompareExpr with Eq and use evaluator special-case? Better: function:
                return new CompareExpr(leftVal, CompareOp.Eq, new FuncCallExpr("LIKE", new[] { leftVal, right }));
            }

            // compare operator
            var op = ParseCompareOp();
            var rightVal = ParseValue();
            return new CompareExpr(leftVal, op, rightVal);
        }

        private Expr ParseCollectionPredicate() {
            string name = Expect(TokenKind.Identifier).Text;
            Expect(TokenKind.LParen);
            
            // First argument: collection path (ValueExpr)
            var collection = ParseValue();
            
            Expect(TokenKind.Comma);
            
            // Second argument: predicate expression (full Expr, parsed recursively)
            var predicate = ParseOrInternal();
            
            Expect(TokenKind.RParen);
            
            var kind = name.ToUpperInvariant() switch {
                "ANY" => CollectionPredicateKind.Any,
                "ALL" => CollectionPredicateKind.All,
                "EXISTS" => CollectionPredicateKind.Exists,
                _ => throw new Exception($"Unknown collection predicate: {name}")
            };
            
            return new CollectionPredicateExpr(kind, collection, predicate);
        }

        // Internal version of ParseOr that doesn't expect EOF at the end
        private Expr ParseOrInternal() {
            var left = ParseAndInternal();
            while (Match(TokenKind.Or)) {
                var right = ParseAndInternal();
                left = new BinaryExpr(left, BinaryOp.Or, right);
            }
            return left;
        }

        private Expr ParseAndInternal() {
            var left = ParseNotInternal();
            while (Match(TokenKind.And)) {
                var right = ParseNotInternal();
                left = new BinaryExpr(left, BinaryOp.And, right);
            }
            return left;
        }

        private Expr ParseNotInternal() {
            if (Match(TokenKind.Not)) {
                var inner = ParseNotInternal();
                return new NotExpr(inner);
            }
            return ParsePrimaryInternal();
        }

        private Expr ParsePrimaryInternal() {
            if (Match(TokenKind.LParen)) {
                var e = ParseOrInternal();
                Expect(TokenKind.RParen);
                return e;
            }

            // Check for collection predicate functions: ANY, ALL, EXISTS
            if (Check(TokenKind.Identifier) && PeekKind(1) == TokenKind.LParen) {
                string name = _t[_p].Text;
                if (CollectionPredicateFunctions.Contains(name)) {
                    return ParseCollectionPredicate();
                }
            }

            // must be a predicate starting with a ValueExpr (identifier or func call)
            var leftVal = ParseValue();

            // IS [NOT] NULL
            if (Match(TokenKind.Is)) {
                bool neg = Match(TokenKind.Not);
                if (Match(TokenKind.Null)) return new IsNullExpr(leftVal, neg);
                throw new Exception("Expected NULL after IS / IS NOT");
            }

            // [NOT] IN (...)
            if (Match(TokenKind.Not)) {
                if (Match(TokenKind.In)) {
                    var items = ParseInList();
                    return new InExpr(leftVal, items, true);
                }
                throw new Exception("Unexpected NOT here");
            }

            if (Match(TokenKind.In)) {
                var items = ParseInList();
                return new InExpr(leftVal, items, false);
            }

            // LIKE
            if (Match(TokenKind.Like)) {
                var right = ParseValue();
                return new CompareExpr(leftVal, CompareOp.Eq, new FuncCallExpr("LIKE", new[] { leftVal, right }));
            }

            // compare operator
            var op = ParseCompareOp();
            var rightVal = ParseValue();
            return new CompareExpr(leftVal, op, rightVal);
        }

        private List<ValueExpr> ParseInList() {
            Expect(TokenKind.LParen);
            var items = new List<ValueExpr>();
            if (!Check(TokenKind.RParen)) {
                items.Add(ParseValue());
                while (Match(TokenKind.Comma))
                    items.Add(ParseValue());
            }
            Expect(TokenKind.RParen);
            return items;
        }

        private CompareOp ParseCompareOp() {
            if (Match(TokenKind.Eq)) return CompareOp.Eq;
            if (Match(TokenKind.Neq)) return CompareOp.Neq;
            if (Match(TokenKind.Lt)) return CompareOp.Lt;
            if (Match(TokenKind.Lte)) return CompareOp.Lte;
            if (Match(TokenKind.Gt)) return CompareOp.Gt;
            if (Match(TokenKind.Gte)) return CompareOp.Gte;
            throw new Exception("Expected comparison operator (=, !=, <, <=, >, >=)");
        }

        private ValueExpr ParseValue() {
            // THIS keyword
            if (Check(TokenKind.Identifier) && _t[_p].Text.Equals("THIS", StringComparison.OrdinalIgnoreCase)) {
                _p++;
                // Check for path continuation: THIS.Property
                if (Match(TokenKind.Dot)) {
                    var seg = Expect(TokenKind.Identifier).Text;
                    var path = seg;
                    while (Match(TokenKind.Dot)) {
                        seg = Expect(TokenKind.Identifier).Text;
                        path += "." + seg;
                    }
                    return new ThisPathExpr(path);
                }
                return new ThisExpr();
            }

            // Function call: IDENT '(' ...
            if (Check(TokenKind.Identifier) && PeekKind(1) == TokenKind.LParen) {
                string name = Expect(TokenKind.Identifier).Text;
                Expect(TokenKind.LParen);
                var args = new List<ValueExpr>();
                if (!Check(TokenKind.RParen)) {
                    args.Add(ParseValue());
                    while (Match(TokenKind.Comma)) args.Add(ParseValue());
                }
                Expect(TokenKind.RParen);
                return new FuncCallExpr(name, args);
            }

            // identifier path: ident ('.' ident)*
            if (Match(TokenKind.Identifier, out var tok)) {
                var path = tok.Text;
                while (Match(TokenKind.Dot)) {
                    var seg = Expect(TokenKind.Identifier).Text;
                    path += "." + seg;
                }
                return new IdentifierExpr(path);
            }

            // literals
            if (Match(TokenKind.String, out var s)) return new LiteralExpr(s.Text);
            if (Match(TokenKind.Number, out var n)) {
                if (!double.TryParse(n.Text, NumberStyles.Float, CultureInfo.InvariantCulture, out var d))
                    throw new Exception($"Invalid number: {n.Text}");
                return new LiteralExpr(d);
            }
            if (Match(TokenKind.True)) return new LiteralExpr(true);
            if (Match(TokenKind.False)) return new LiteralExpr(false);
            if (Match(TokenKind.Null)) return new LiteralExpr(null);

            throw new Exception($"Expected value at token {_t[_p].Kind}");
        }

        private bool Check(TokenKind k) {
            return _t[_p].Kind == k;
        }

        private TokenKind PeekKind(int offset) {
            return (_p + offset < _t.Count) ? _t[_p + offset].Kind : TokenKind.EOF;
        }

        private bool Match(TokenKind k) {
            if (Check(k)) { _p++; return true; }
            return false;
        }

        private bool Match(TokenKind k, out Token tok) {
            if (Check(k)) { tok = _t[_p++]; return true; }
            tok = _t[_p];
            return false;
        }

        private Token Expect(TokenKind k) {
            if (!Check(k)) throw new Exception($"Expected {k} but found {_t[_p].Kind}");
            return _t[_p++];
        }
    }
}
