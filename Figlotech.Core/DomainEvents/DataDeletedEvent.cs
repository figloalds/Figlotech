using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core.DomainEvents
{
    public sealed class DataDeletedEvent<T> : DomainEvent {
        public T Value { get; set; }
        public DataDeletedEvent(T data) {
            Value = data;
        }
    }
}
