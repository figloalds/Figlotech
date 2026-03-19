using System;

namespace Figlotech.Core.Interfaces {

    public interface IContextProvider {
        void Set(String name, Object value);
        Object Get(String name);

        T Get<T>(string name);
    }
}
