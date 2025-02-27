using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Timers;

namespace Figlotech.Core.DomainEvents {
    public class PreserializableDomainEvent : DomainEvent, IPreserializableDomainEvent {
        private string _cachedSerialization;
        public string GetSerializedData() {
            if (_cachedSerialization != null) {
                return _cachedSerialization;
            }
            lock (this) {
                if (_cachedSerialization == null) {
                    _cachedSerialization = JsonConvert.SerializeObject(this);
                }
                return _cachedSerialization;
            }
        }
        public void ClearSerializedData() {
            lock (this) {
                _cachedSerialization = null;
            }
        }
        public void Serialize() {
            lock (this) {
                if (_cachedSerialization == null) {
                    _cachedSerialization = JsonConvert.SerializeObject(this);
                }
            }
        }
    }

    public class DomainEvent : IDomainEvent {
        static short antiCollider = 0;
        static long _idGen = 0;
        long? created = null;
        static long ProgramStarted = DateTime.UtcNow.Ticks;

        public DateTime TimeStamp { get; set; }
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
                retv += antiCollider++;
            return retv;
        }


    }
}
