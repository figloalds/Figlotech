using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Builders {
    public class Conditions<T> {
        internal Expression expression;
        public Conditions(Expression<Func<T, bool>> expr) {
            expression = expr.Body;
        }

        public void And(Expression<Func<T, bool>> expr) {
            expression = Expression.AndAlso(expression, expr.Body);
        }
        public void Or(Expression<Func<T, bool>> expr) {
            expression = Expression.OrElse(expression, expr.Body);
        }

        public static implicit operator Expression<Func<T, bool>>(Conditions<T> me) {
            return Expression.Lambda<Func<T, bool>>(me.expression, Expression.Parameter(typeof(T)));
        }
    }
}
