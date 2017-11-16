using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core.DomainEvents
{
    public class DataChangedEvent<T> : DomainEvent {
        public T NewValue { get; set; }
        public T OldValue { get; set; }
        public DataChangedEvent(T oldValue, T newValue) {
            NewValue = oldValue;
            OldValue = newValue;
        }
    }
}
