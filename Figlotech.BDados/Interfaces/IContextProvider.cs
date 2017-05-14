using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.BDados.Interfaces {

    public interface IContextProvider {
        void Set(String name, Object value);
        Object Get(String name);

        Object Get(Type t);

        T Get<T>(string name);
    }
}
