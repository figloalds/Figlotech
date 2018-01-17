using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core.DomainEvents {
    public static class InlineLambdaListener {
        public static InlineLambdaListener<T> Create<T>(Action<T> action, Action<Exception> handler) where T : IDomainEvent {
            return new InlineLambdaListener<T>(action, handler);
        }
    }

    public class InlineLambdaListener<T> : IDomainEventListener<T> where T : IDomainEvent {
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

        private DomainEventsHub _registeredHub = null;
        public void Subscribe(DomainEventsHub hub = null) {
            if (hub == null)
                hub = DomainEventsHub.Global;
            _registeredHub = hub;
            _registeredHub.SubscribeListener(this);
        }

        public void DeRegisterSelf() {
            _registeredHub.SubscribeListener(this);
        }
    }
}
