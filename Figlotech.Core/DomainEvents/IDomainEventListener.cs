using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core.DomainEvents
{
    public interface IDomainEventListener {

    }
    public interface IDomainEventListener<T> : IDomainEventListener where T: IDomainEvent
    {
        void OnEventTriggered(T evt);
        void OnEventHandlingError(Exception x);
    }
}
