using System.Collections.Generic;

namespace MiniWhere {
    public abstract class Expr {
    }

    public sealed class BinaryExpr : Expr {
        public BinaryExpr(Expr left, BinaryOp op, Expr right) {
            Left = left;
            Op = op;
            Right = right;
        }

        public Expr Left { get; }
        public BinaryOp Op { get; }
        public Expr Right { get; }
    }

    public enum BinaryOp {
        And,
        Or
    }

    public sealed class NotExpr : Expr {
        public NotExpr(Expr inner) {
            Inner = inner;
        }

        public Expr Inner { get; }
    }

    public sealed class CompareExpr : Expr {
        public CompareExpr(ValueExpr left, CompareOp op, ValueExpr right) {
            Left = left;
            Op = op;
            Right = right;
        }

        public ValueExpr Left { get; }
        public CompareOp Op { get; }
        public ValueExpr Right { get; }
    }

    public enum CompareOp {
        Eq,
        Neq,
        Lt,
        Lte,
        Gt,
        Gte
    }

    public sealed class IsNullExpr : Expr {
        public IsNullExpr(ValueExpr target, bool negated) {
            Target = target;
            Negated = negated;
        }

        public ValueExpr Target { get; }
        public bool Negated { get; }
    }

    public sealed class InExpr : Expr {
        public InExpr(ValueExpr target, IReadOnlyList<ValueExpr> items, bool negated) {
            Target = target;
            Items = items;
            Negated = negated;
        }

        public ValueExpr Target { get; }
        public IReadOnlyList<ValueExpr> Items { get; }
        public bool Negated { get; }
    }

    public abstract class ValueExpr {
    }

    public sealed class IdentifierExpr : ValueExpr {
        public IdentifierExpr(string path) {
            Path = path;
        }

        public string Path { get; }
    }

    public sealed class LiteralExpr : ValueExpr {
        public LiteralExpr(object? value) {
            Value = value;
        }

        public object? Value { get; }
    }

    public sealed class FuncCallExpr : ValueExpr {
        public FuncCallExpr(string name, IReadOnlyList<ValueExpr> args) {
            Name = name;
            Args = args;
        }

        public string Name { get; }
        public IReadOnlyList<ValueExpr> Args { get; }
    }

    public enum CollectionPredicateKind {
        Any,
        All,
        Exists
    }

    public sealed class CollectionPredicateExpr : Expr {
        public CollectionPredicateExpr(CollectionPredicateKind kind, ValueExpr collection, Expr predicate) {
            Kind = kind;
            Collection = collection;
            Predicate = predicate;
        }

        public CollectionPredicateKind Kind { get; }
        public ValueExpr Collection { get; }
        public Expr Predicate { get; }
    }

    public sealed class ThisExpr : ValueExpr {
    }

    public sealed class ThisPathExpr : ValueExpr {
        public ThisPathExpr(string path) {
            Path = path;
        }

        public string Path { get; }
    }
}
