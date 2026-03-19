using Newtonsoft.Json;
using System;

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
        static readonly short antiCollider = 0;
        static long _idGen = 0;
        readonly long? created = null;
        static readonly long ProgramStarted = DateTime.UtcNow.Ticks;

        public DateTime TimeStamp { get; set; } = DateTime.UtcNow;
        public long Id { get; private set; } = ++_idGen;
        public bool AllowPropagation { get; set; } = true;

        [JsonIgnore]
        public string d_RaiseOrigin { get; set; }

        [JsonIgnore]
        public DomainEventsHub EventsHub { get; set; }

    }
}
