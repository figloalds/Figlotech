using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core.DomainEvents
{
    public class DomainEventsHub
    {
        public static DomainEventsHub Global = new DomainEventsHub();

        public TimeSpan EventCacheDuration { get; set; } = TimeSpan.FromMinutes(30);
        
        private List<IDomainEvent> _eventCache = new List<IDomainEvent>();
        public List<IDomainEvent> EventCache => _eventCache;

        private List<IDomainEventListener> _listeners = new List<IDomainEventListener>();
        private List<IDomainEventListener> Listeners => _listeners;

        // Wanna grab hold of tasks so that GC won't kill them.
        List<Task> EventTasks = new List<Task>();

        public void Raise<T>(T domainEvent) where T: IDomainEvent {
            // Cache event
            EventCache.Add(domainEvent);
            EventCache.RemoveAll(e => DateTime.UtcNow.Ticks - e.Time > EventCacheDuration.Ticks);

            // Raise event on all listeners.
            Listeners.RemoveAll(l => l == null);
            foreach(var listener in Listeners) {
                if(listener is IDomainEventListener<T> correctListener) {
                    EventTasks.Add(Task.Run(() => {
                        try {
                            correctListener.OnEventTriggered(domainEvent);
                        } catch(Exception x) {
                            correctListener.OnEventHandlingError(x);
                        }
                    }));
                }
            }

            // Clear "Ran to completion" tasks;
            EventTasks.RemoveAll(t => t.IsCompleted || t.IsFaulted || t.IsCanceled);
        }
        
        public bool IsTimeInDomainCacheDuration(DateTime dt) {
            return DateTime.UtcNow.Subtract(dt) > EventCacheDuration;
        }

        public IEnumerable<IDomainEvent> GetEventsSince(DateTime dt) {
            return EventCache.Where(e => e.Time > dt.Ticks);
        }

        public async Task<List<IDomainEvent>> PollForEventsSince<T>(TimeSpan maximumPollTime, DateTime dt, Predicate<T> filter) {
            return await PollForEventsSince(maximumPollTime, dt, e => e is T evt && filter(evt));
        }

        public async Task<List<IDomainEvent>> PollForEventsSince(TimeSpan maximumPollTime, DateTime dt, Predicate<IDomainEvent> filter) {
            DateTime pollStart = DateTime.UtcNow;
            return await Task.Run(async () => {
                List<IDomainEvent> retv = new List<IDomainEvent>();
                do {
                    var events = GetEventsSince(dt);
                    retv.AddRange(events.Where(e=> filter(e)));
                    if (events.Any()) dt = new DateTime(events.Max(e => e.Time));
                    if(!retv.Any()) {
                        await Task.Delay(400);
                    }
                } while (!retv.Any() && DateTime.UtcNow.Subtract(pollStart) < maximumPollTime);

                return retv;
            });
        }

        public void RegisterListener<T>(IDomainEventListener<T> listener) where T: IDomainEvent{
            _listeners.Add(listener);
        }

        public void RemoveListener(IDomainEventListener listener) {
            _listeners.Remove(listener);
        }
    }
}
