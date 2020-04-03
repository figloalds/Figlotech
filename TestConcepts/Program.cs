using System;

namespace Figlotech.ECSEngine {
    public class Program {
        public static class V {
            public static IValueExpression<T> val<T>(T t) {
                return new ConstantValueExpression<T>(t);
            }
            public static IValueExpression<T> fn<T>(Func<T> t) {
                return new ComputedValueExpression<T>(t);
            }
        }

        public class V<T> : IValueExpression<T> {
            public T Value => throw new NotImplementedException();

            IValueExpression<T> wrappedExpression;

            public V(T t) {
                wrappedExpression = V.val(t);
            }
            public V(Func<T> t) {
                wrappedExpression = V.fn(t);
            }

        }

        public interface IValueExpression<T> {
            T Value { get; }
        }

        public class ComputedValueExpression<T> : IValueExpression<T> {
            public T Value => Expression != null ? Expression.Invoke() : default(T);
            public Func<T> Expression { get; private set; }
            public ComputedValueExpression(Func<T> expression) {
                Expression = expression;
            }
        }
        public class ConstantValueExpression<T> : IValueExpression<T> {
            public T Value { get; private set; }
            public ConstantValueExpression(T value) {
                Value = value;
            }
        }

        static void Main(string[] args) {

        }
    }
}
