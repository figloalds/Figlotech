using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core.DomainEvents
{
    public class InlineLambdaListener<T> : IDomainEventListener<T> where T: IDomainEvent {
        public Action<T> OnRaise;
        public Action<Exception> OnHandle;
        public InlineLambdaListener(Action<T> action, Action<Exception> handler) {
            OnRaise = action;
            OnHandle = handler;
        }


        public void OnEventTriggered(T evt) {
            OnRaise?.Invoke(evt);
        }

        public void OnEventHandlingError(Exception x) {
            OnHandle?.Invoke(x);
        }

        public async Task AwaitEvent() {

        }

        private DomainEventsHub _registeredHub = null;
        public void RegisterSelf(DomainEventsHub hub = null) {
            if (hub == null)
                hub = DomainEventsHub.Global;
            _registeredHub = hub;
            _registeredHub.RegisterListener(this);
        }

        public void DeRegisterSelf() {
            _registeredHub.RemoveListener(this);
        }
    }
}
