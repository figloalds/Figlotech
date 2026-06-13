using System;
using System.Collections.Generic;

namespace Figlotech.Core.DomainEvents {
    public interface IGenericDomainEventListener : IDomainEventListener {
        IEnumerable<Type> HandledTypes { get; }
    }
}
