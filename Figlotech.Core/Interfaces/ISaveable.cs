using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core.Interfaces {
    public interface ISaveable {
        bool Save(Action fn = null);
        bool Load(Action fn = null);
    }
}
