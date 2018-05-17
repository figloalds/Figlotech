using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core.DomainEvents
{
    public interface IDomainEventListener {
        void OnEventTriggered(IDomainEvent evt);
        void OnEventHandlingError(IDomainEvent evt, Exception x);
    }

    public abstract class DomainEventListener<T> : IDomainEventListener where T: IDomainEvent {

        public abstract void OnEventTriggered(T evt);
        public abstract void OnEventHandlingError(T evt, Exception x);

        public void OnEventTriggered(IDomainEvent evt) {
            if(evt is T t) {
                OnEventTriggered(t);
            }
        }

        public void OnEventHandlingError(IDomainEvent evt, Exception x) {
            if (evt is T t) {
                OnEventHandlingError(t, x);
            }
        }
    }
}
