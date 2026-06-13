using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Figlotech.Core.DomainEvents {
    public interface ICatchAllDomainEventListener : IDomainEventListener {
    }

    public interface IDomainEventListener {
        ValueTask OnEventTriggered(IDomainEvent evt);
        ValueTask OnEventHandlingError(IDomainEvent evt, Exception x);
        bool CanHandle(IDomainEvent evt);
    }

    public interface IGenericDomainEventListener : IDomainEventListener {
        IEnumerable<Type> HandledTypes { get; }
    }

    public abstract class DomainEventListener<T> : IDomainEventListener where T : IDomainEvent {
        public abstract ValueTask OnEventTriggered(T evt);
        public abstract ValueTask OnEventHandlingError(T evt, Exception x);

        public bool CanHandle(IDomainEvent evt) {
            return evt is T;
        }

        public ValueTask OnEventTriggered(IDomainEvent evt) {
            if (evt is T t) {
                return OnEventTriggered(t);
            }
            return default;
        }

        public ValueTask OnEventHandlingError(IDomainEvent evt, Exception x) {
            if (evt is T t) {
                return OnEventHandlingError(t, x);
            }
            return default;
        }
    }
}
