using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core.DomainEvents
{
    public class DataCreatedEvent<T> : DomainEvent
    {
        public T Value;
        public DataCreatedEvent(T data) {
            Value = data;
        }
    }
}
