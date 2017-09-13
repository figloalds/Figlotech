using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace Figlotech.BDados.DataAccessAbstractions
{
    public interface IAdoImplementationProvider {
        IDbConnection GetValidConnection();

    }
}
