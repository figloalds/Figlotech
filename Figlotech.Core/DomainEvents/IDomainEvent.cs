using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core.DomainEvents
{
    public interface IDomainEvent {
        long Time { get; }
        long Id { get; }
    }
}
