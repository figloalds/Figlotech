using Figlotech.BDados.Entity;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Helpers
{
    public class JoinConfigureHelper<T>
    {
        internal JoinDefinition _join;
        internal int _index;
        
        internal JoinConfigureHelper(JoinDefinition _join, int _index)
        {
            this._join = _join;
            this._index = _index;
        }
        public JoinConfigureHelper<T> As(String prefix) {
            _join.Joins[_index].Prefix = prefix;
            return this;
        }

        public delegate void SelectFields<T>(SelectFieldsHelper selector, T entity);

        public JoinConfigureHelper<T> RemoveFields(Expression<SelectFields<T>> colunas)
        {
            _join.Joins[_index].Excludes.Clear();
            if (colunas.Body.NodeType != ExpressionType.Call || (colunas.Body as MethodCallExpression).Method.Name != "Colunas")
                throw new BDadosException("Expecting a call to SelectFields in the first/only call of this lambda.");
            var args = ((MethodCallExpression)colunas.Body).Arguments;
            foreach (var arg in args) {
                var exp = (arg as NewArrayExpression);
                foreach (var expression in exp.Expressions) {
                    String exclude = null;
                    if(expression is MemberExpression)
                        exclude = (expression as MemberExpression).Member.Name;
                    if(expression is UnaryExpression)
                        exclude = ((expression as UnaryExpression).Operand as MemberExpression).Member.Name;
                    if (exclude == null) continue;
                    if (DataObject.GetFieldNames(_join.Joins[_index].ValueObject).Contains(exclude)) {
                        _join.Joins[_index].Excludes.Add(exclude);
                    }
                    continue;
                }
            }
            return this;
        }

        public JoinConfigureHelper<T> OnlyFields(params String[] fields) {
            _join.Joins[_index].Excludes.Clear();
            foreach(var a in typeof(T).GetMembers()) {
                if(a is FieldInfo || a is PropertyInfo) {
                    if (!fields.Contains(a.Name))
                        _join.Joins[_index].Excludes.Add(a.Name);
                }
            }
            return this;
        }

        public JoinConfigureHelper<T> On(String args)
        {
            _join.Joins[_index].Args = args;
            return this;
        }
    }
}
