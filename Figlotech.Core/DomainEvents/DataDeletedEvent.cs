namespace Figlotech.Core.DomainEvents {
    public sealed class DataDeletedEvent<T> : DomainEvent {
        public T Value { get; set; }
        public DataDeletedEvent(T data) {
            Value = data;
        }
    }
}
