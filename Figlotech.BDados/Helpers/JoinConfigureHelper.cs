using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Helpers
{
    public sealed class JoinConfigureHelper
    {
        internal JoinDefinition _join;
        internal int _index;
        
        internal JoinConfigureHelper(JoinDefinition _join, int _index)
        {
            this._join = _join;
            this._index = _index;
        }
        public JoinConfigureHelper As(String prefix) {
            _join.Joins[_index].Prefix = prefix;
            return this;
        }

        public JoinConfigureHelper OnlyFields(IEnumerable<string> fields) {
            _join.Joins[_index].Columns.AddRange(
                fields.Where(a => !_join.Joins[_index].Columns.Contains(a))
            );
            return this;
        }
        //public JoinConfigureHelper<T> OnlyFields(params String[] fields) {
        //    return OnlyFields(fields);
        //}

        public JoinConfigureHelper On(String args) {
            _join.Joins[_index].Args = args;
            return this;
        }
    }
}
