using System;
using System.Threading.Tasks;

namespace Figlotech.Core.DomainEvents {
    public static class InlineLambdaListener {
        public static InlineLambdaListener<T> Create<T>(Func<T, ValueTask> action, Func<T, Exception, ValueTask> handler = null) where T : IDomainEvent {
            return new InlineLambdaListener<T>(action, handler);
        }
    }

    public sealed class InlineLambdaListener<T> : DomainEventListener<T> where T : IDomainEvent {
        public Func<T, ValueTask> OnRaise;
        public Func<T, Exception, ValueTask> OnHandle;
        public DomainEventsHub EventsHub { get; set; }

        public InlineLambdaListener(Func<T, ValueTask> action, Func<T, Exception, ValueTask> handler = null) {
            OnRaise = action;
            OnHandle = handler;
        }

        public override ValueTask OnEventTriggered(T evt) {
            var raise = OnRaise;
            if (raise != null) {
                return raise(evt);
            }
            return default;
        }

        public override ValueTask OnEventHandlingError(T evt, Exception x) {
            var handle = OnHandle;
            if (handle != null) {
                return handle(evt, x);
            }
            Fi.Tech.SwallowException(x);
            return default;
        }

        private DomainEventsHub _registeredHub = null;

        public void Subscribe(DomainEventsHub hub = null) {
            if (hub == null)
                hub = DomainEventsHub.Global;
            if(_registeredHub != null) {
                Unsubscribe();
            }
            _registeredHub = hub;
            _registeredHub.SubscribeListener(this);
        }

        public void Unsubscribe() {
            _registeredHub.RemoveListener(this);
            _registeredHub = null;
        }
    }
}
