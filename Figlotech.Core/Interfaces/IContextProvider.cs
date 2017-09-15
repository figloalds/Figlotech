using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core.Interfaces {

    public interface IContextProvider {
        void Set(String name, Object value);
        Object Get(String name);

        T Get<T>(string name);
    }
}
