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
        public Func<T, Task> OnRaise;
        public Func<Exception, Task> OnHandle;
        public InlineLambdaListener(Func<T, Task> action, Func<Exception, Task> handler) {
            OnRaise = action;
            OnHandle = handler;
        }
        public InlineLambdaListener(Action<T> action, Action<Exception> handler) {
            OnRaise = (a) => { action?.Invoke(a); return null; };
            OnHandle = (a) => { handler?.Invoke(a); return null; };
        }

        public Task OnEventTriggered(T evt) {
            return OnRaise?.Invoke(evt);
        }

        public Task OnEventHandlingError(Exception x) {
            return OnHandle?.Invoke(x);
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
