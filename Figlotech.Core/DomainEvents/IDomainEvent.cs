using System;

namespace Figlotech.Core.DomainEvents {
    public interface IDomainEvent {
        DomainEventsHub EventsHub { get; set; }
        bool AllowPropagation { get; set; }
        string d_RaiseOrigin { get; set; }
        DateTime TimeStamp { get; set; }
        long Id { get; }
    }

    public interface IPreserializableDomainEvent {
        string GetSerializedData();
        void ClearSerializedData();
        void Serialize();
    }
}
