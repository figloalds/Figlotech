namespace Figlotech.Core.DomainEvents {
    public sealed class DataCreatedEvent<T> : DomainEvent {
        public T Value;
        public DataCreatedEvent(T data) {
            Value = data;
        }
    }
}
