using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core.DomainEvents {
    public static class InlineLambdaListener {
        public static InlineLambdaListener<T> Create<T>(Action<T> action, Action<T, Exception> handler) where T : IDomainEvent {
            return new InlineLambdaListener<T>(action, handler);
        }
    }

    public class InlineLambdaListener<T> : DomainEventListener<T> where T : IDomainEvent {
        public Action<T> OnRaise;
        public Action<T, Exception> OnHandle;
        public DomainEventsHub EventsHub { get; set; }
        public InlineLambdaListener(Action<T> action, Action<T, Exception> handler = null) {
            OnRaise = action;
            OnHandle = handler;
        }

        public override void OnEventTriggered(T evt) {
            OnRaise?.Invoke(evt);
        }

        public override void OnEventHandlingError(T evt, Exception x) {
            if(OnHandle == null) {
                Fi.Tech.Throw(x);
            }
            OnHandle?.Invoke(evt, x);
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
