using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Interfaces {
    public interface IQueryBuilder {

        IQueryBuilder Append(string Text, params object[] args);
        IQueryBuilder Append(IQueryBuilder other);

        List<DbParameter> GetParameters();
        string GetCommandText();

        bool IsEmpty { get; }

        IQueryBuilder If(bool condition);
        IQueryBuilder Then();
        IQueryBuilder EndIf();
        IQueryBuilder Else();
    }
}
