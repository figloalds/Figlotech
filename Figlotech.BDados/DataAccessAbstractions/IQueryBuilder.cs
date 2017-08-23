using System;
using System.Collections.Generic;
using System.Data.Common;

namespace Figlotech.BDados.DataAccessAbstractions {
    public interface IQueryBuilder {

        IQueryBuilder Append(string Text, params object[] args);
        IQueryBuilder Append(IQueryBuilder other);

        Dictionary<String, Object> GetParameters();
        string GetCommandText();

        bool IsEmpty { get; }

        IQueryBuilder If(bool condition);
        IQueryBuilder Then();
        IQueryBuilder EndIf();
        IQueryBuilder Else();
    }
}
