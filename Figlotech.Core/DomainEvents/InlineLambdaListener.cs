using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core.DomainEvents {
    public static class InlineLambdaListener {
        public static InlineLambdaListener<T> Create<T>(Func<T, Task> action, Func<T, Exception, Task> handler) where T : IDomainEvent {
            return new InlineLambdaListener<T>(action, handler);
        }
    }

    public sealed class InlineLambdaListener<T> : DomainEventListener<T> where T : IDomainEvent {
        public Func<T, Task> OnRaise;
        public Func<T, Exception, Task> OnHandle;
        public DomainEventsHub EventsHub { get; set; }
        public InlineLambdaListener(Func<T, Task> action, Func<T, Exception, Task> handler = null) {
            OnRaise = action;
            OnHandle = handler;
        }

        public override async Task OnEventTriggered(T evt) {
            await OnRaise?.Invoke(evt);
        }

        public override async Task OnEventHandlingError(T evt, Exception x) {
            if(OnHandle == null) {
                Fi.Tech.Throw(x);
            }
            await OnHandle?.Invoke(evt, x);
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
