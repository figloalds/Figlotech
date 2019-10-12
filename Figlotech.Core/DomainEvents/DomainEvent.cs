using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Timers;

namespace Figlotech.Core.DomainEvents {


    public class DomainEvent : IDomainEvent {
        static short antiCollider = 0;
        static long _idGen = 0;
        long? created = null;
        static long ProgramStarted = DateTime.UtcNow.Ticks;

        public DateTime TimeStamp => new DateTime(Time);
        public long Time => (created ?? (created = generateTimeStamp())).Value;
        public long Id { get; private set; } = ++_idGen;
        public bool AllowPropagation { get; set; } = true;

        public string RID { get; set; } = new RID().AsBase36;
        
        [JsonIgnore]
        public string d_RaiseOrigin { get; set; }
        
        [JsonIgnore]
        public DomainEventsHub EventsHub { get; set; }

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
