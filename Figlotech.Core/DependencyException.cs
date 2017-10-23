using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core {
    public class DependencyException : Exception {
        public Type DependedInterface;
        public Type DependantClass;

        public DependencyException(Type Dependant, Type Dependency) {
            DependedInterface = Dependency;
            DependantClass = Dependant;
        }
    }
}
