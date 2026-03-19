using System;
using System.Linq.Expressions;

namespace Figlotech.BDados.DataAccessAbstractions {
    public interface IQueryBuildHelper {
        IQueryBuildHelper Cmp<T>(Expression<Func<T, bool>> expression);
        IQueryBuildHelper And();
        IQueryBuildHelper Or();
    }
}
