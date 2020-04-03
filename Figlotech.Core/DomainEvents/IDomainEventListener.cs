using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core.DomainEvents
{
    public interface IDomainEventListener {
        Task OnEventTriggered(IDomainEvent evt);
        Task OnEventHandlingError(IDomainEvent evt, Exception x);
    }

    public abstract class DomainEventListener<T> : IDomainEventListener where T: IDomainEvent {

        public abstract Task OnEventTriggered(T evt);
        public abstract Task OnEventHandlingError(T evt, Exception x);

        public async Task OnEventTriggered(IDomainEvent evt) {
            if(evt is T t) {
                await OnEventTriggered(t);
            }
        }

        public async Task OnEventHandlingError(IDomainEvent evt, Exception x) {
            if (evt is T t) {
                await OnEventHandlingError(t, x);
            }
        }
    }
}
