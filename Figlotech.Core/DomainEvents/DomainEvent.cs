using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core.DomainEvents {
    public class DomainEvent : IDomainEvent {
        static short antiCollider = 0;
        long? created = null;
        static long ProgramStarted = DateTime.UtcNow.Ticks;

        public DateTime TimeStamp => new DateTime(Time);
        public long Time => (created ?? (created = generateTimeStamp())).Value;

        static long generateTimeStamp() {
            var retv = DateTime.UtcNow.Ticks;
            retv -= Int16.MaxValue;
            lock("anti_collider_access") {
                retv += antiCollider++;
            }
            return retv;
        }


    }
}
