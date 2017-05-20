using Figlotech.BDados.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.DataAccessAbstractions {
    public interface IAggregateRoot : IDataObject {
        bool CascadingSave(Action fn = null, List<Type> alreadyTreatedTypes = null);
    }
}
