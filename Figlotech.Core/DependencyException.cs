using System;

namespace Figlotech.Core {
    public sealed class DependencyException : Exception {
        public Type DependedInterface;
        public Type DependantClass;

        public DependencyException(Type Dependant, Type Dependency) {
            DependedInterface = Dependency;
            DependantClass = Dependant;
        }
    }
}
