using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.DataAccessAbstractions {
    public interface IQueryBuildHelper {
        IQueryBuildHelper Cmp<T>(Expression<Func<T, bool>> expression);
        IQueryBuildHelper And();
        IQueryBuildHelper Or();
    }
}
