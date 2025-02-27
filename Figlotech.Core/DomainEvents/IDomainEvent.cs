using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core.DomainEvents
{
    public interface IDomainEvent {
        DomainEventsHub EventsHub { get; set; }
        bool AllowPropagation { get; set; }
        string d_RaiseOrigin { get; set; }
        DateTime TimeStamp { get; set; }
        string RID { get; }
        long Id { get; }
    }

    public interface IPreserializableDomainEvent {
        string GetSerializedData();
        void ClearSerializedData();
        void Serialize();
    }
}
