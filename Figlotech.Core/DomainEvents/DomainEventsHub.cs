using Figlotech.Core.BusinessModel;
using Figlotech.Core.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.Core.DomainEvents {

    public sealed class ScheduledDomainEvent {
        public IDomainEvent Event;
        public DateTime ScheduledTime;
        public string Identifier;
        internal Timer timer;
    }

    public sealed class CustomDomainValidation  {
        public CustomDomainValidation(IValidationRule validator, int? phase) {
            this.Validator = validator;
            this.ValidationPhase = phase;
        }
        public IValidationRule Validator { get; set; }
        public int? ValidationPhase { get; set; } = null;
    }

    public sealed class CustomFlushOrderToken {
        public bool IsFlushIssued { get; set; } = false;
        public bool IsReleased { get; set; } = false;
    }

    public static class DomainEventsHubExtensions {
        public static void SubscribeInline<T>(this DomainEventsHub self, Func<T, Task> fn, Func<T, Exception, Task> handler = null) where T: IDomainEvent {
            self.SubscribeListener(InlineLambdaListener.Create<T>(fn, handler));
        }
    }

    public sealed class DomainEventsHub {

        public static DomainEventsHub Global = new DomainEventsHub(FiTechCoreExtensions.GlobalQueuer);
        private readonly DomainEventsHub parentHub;
        private CancellationTokenSource CancelationTokenSource { get; set; } = new CancellationTokenSource();

        private bool IsTerminationIssued = false;

        public TimeSpan EventCacheDuration { get; set; } = TimeSpan.FromMinutes(2);
        public DateTime LastEventDateTime { get; set; }
        List<CustomFlushOrderToken> FlushOrderTokens { get; set; } = new List<CustomFlushOrderToken>();
        private List<ScheduledDomainEvent> ScheduledEvents { get; set; } = new List<ScheduledDomainEvent>();

        private WorkQueuer MainQueuer { get; set; }

        public DomainEventsHub(WorkQueuer queuer = null, DomainEventsHub parentHub = null) {
            this.parentHub = parentHub;
            MainQueuer = queuer ?? FiTechCoreExtensions.GlobalQueuer;
        }

        public bool EnableEventCache { get; set; } = true;
        private List<IDomainEvent> EventCache { get; set; } = new List<IDomainEvent>();
        private List<IDomainEventListener> Listeners { get; set; } = new List<IDomainEventListener>();
        public Dictionary<String, Object> Scope { get; private set; } = new Dictionary<string, object>();
        //public List<CustomDomainValidation> CustomValidators { get; private set; } = new List<CustomDomainValidation>();

        public void Schedule(IDomainEvent evt, DateTime when, string identifier) {
            var due = when.ToUniversalTime() - DateTime.UtcNow;
            var sched = ScheduledEvents.FirstOrDefault(s=> s.Identifier == identifier) ??
                new ScheduledDomainEvent() {
                    Identifier = identifier ?? new RID().AsBase36,
                };
            sched.Event = evt;
            sched.ScheduledTime = when;
            if(sched.timer != null) {
                sched.timer.Dispose();
                sched.timer = null;
            }
            
            sched.timer = new Timer((s) => {
                this.Raise((s as ScheduledDomainEvent).Event);
                this.Unschedule(sched);
            }, sched, (long)due.TotalMilliseconds, Timeout.Infinite);

            ScheduledEvents.Add(sched);
        }

        public void Unschedule(ScheduledDomainEvent sched) {
            sched.timer.Dispose();
            ScheduledEvents.Remove(sched);
        }

        public void Unschedule(string identifier) {
            ScheduledEvents.ForEachReverse(i => {
                if (i.Identifier == identifier) {
                    this.Unschedule(i);
                }
            });
        }

        public void Raise<T>(IEnumerable<T> domainEvents) where T : IDomainEvent {
            var tempLi = domainEvents.ToList();
            tempLi.ForEach(evt => Raise(evt));
        }

        object LockFlushSwitch = new object();
        public void FlushAllInlineListeners() {
            lock(LockFlushSwitch) {
                foreach (var a in FlushOrderTokens) {
                    a.IsFlushIssued = true;
                }
            }
        }

        private void WriteLog(string log) {
            Fi.Tech.WriteLine("FTH:EventHub", log);
        }

        List<IExtension> Extensions = new List<IExtension>();

        public void RegisterExtension(IExtension Extension) {
            if(Extension == null) {
                throw new ArgumentNullException("Trying to register null extension");
            }
            if(!Extensions.Contains(Extension)) {
                Extensions.Add(Extension);
            }
        }

        public void RegisterExtension<TOpCode, T>(Extension<TOpCode, T> Extension) {
            RegisterExtension(Extension);
        }

        public async Task ExecuteExtensionsAsync<TOpCode, T>(TOpCode opcode, T input) {

            foreach(var Extension in Extensions) {
                if(Extension is Extension<TOpCode, T> ext) {
                    try {
                        await ext.Execute(opcode, input);
                    } catch (Exception x) {
                        await ext.OnError(opcode, input, x);
                    }
                }
            }
        }

        public void Raise(IDomainEvent domainEvent) {
            WriteLog($"Raising Event {domainEvent.GetType()}");
            // Cache event
            if(FiTechCoreExtensions.StdoutEventHubLogs) {
                domainEvent.d_RaiseOrigin = Environment.StackTrace;
            }
            domainEvent.EventsHub = this;
            lock (EventCache) {
                if(EventCache.Any(x=> x.RID == domainEvent.RID)) {
                    return;
                }
                EventCache.RemoveAll(e => (DateTime.UtcNow - e.TimeStamp) > EventCacheDuration);
            }

            // Raise event on all listeners.
            List<IDomainEventListener> listeners;
            lock(Listeners) {
                Listeners.RemoveAll(l => l == null);
                listeners = Listeners.ToList();
            }
            foreach (var listener in listeners) {
                MainQueuer.Enqueue(async () => {
                    await listener.OnEventTriggered(domainEvent).ConfigureAwait(false);
                    if (domainEvent.AllowPropagation) {
                        parentHub?.Raise(domainEvent);
                    }
                }, async x=> {
                    try {
                        await listener.OnEventHandlingError(domainEvent, x).ConfigureAwait(false);
                    } catch (Exception y) {
                        Fi.Tech.Throw(x);
                    }
                }, (b)=> {
                    return Fi.Result();
                });
            }

            if(EnableEventCache) {
                lock (EventCache) {
                    domainEvent.TimeStamp = DateTime.UtcNow;
                    EventCache.Add(domainEvent);
                }
                LastEventDateTime = Fi.Tech.GetUtcTime();
                CancelationTokenSource.Cancel();
                CancelationTokenSource = new CancellationTokenSource();
            }
        }

        public bool IsTimeInDomainCacheDuration(DateTime dt) {
            return DateTime.UtcNow.Subtract(dt) > EventCacheDuration;
        }

        public IEnumerable<IDomainEvent> GetEventsSince(long Id) {
            lock (EventCache) {
                return EventCache.Where(e => e.Id > Id).ToArray();
            }
        }

        public IEnumerable<IDomainEvent> GetEventsSince(DateTime Stamp) {
            lock (EventCache) {
                return EventCache.Where(e => e.TimeStamp >= Stamp).ToArray();
            }
        }

        public Task<IDomainEvent[]> PollForEventsSince(TimeSpan maximumPollTime, long Id, Predicate<IDomainEvent> filter) {
            return PollForEventsSince(maximumPollTime, ()=> GetEventsSince(Id), e => filter(e));
        }

        public Task<IDomainEvent[]> PollForEventsSince(TimeSpan maximumPollTime, DateTime dt, Predicate<IDomainEvent> filter) {
            return PollForEventsSince(maximumPollTime, () => GetEventsSince(dt), e => filter(e));
        }

        private Task<IDomainEvent[]> PollForEventsSince(TimeSpan maximumPollTime, Func<IEnumerable<IDomainEvent>> getEvents, Predicate<IDomainEvent> filter) {
            DateTime pollStart = DateTime.UtcNow;
            return Task.Run<IDomainEvent[]>(async () => {
                IDomainEvent[] events;
                var flushOrder = new CustomFlushOrderToken();
                lock ("FLUSH_SWITCH") {
                    FlushOrderTokens.Add(flushOrder);
                }
                do {
                    lock (EventCache) {
                        events = getEvents().ToArray();
                    }
                    if (events.Length > 0) {
                        return events;
                    }
                    if (flushOrder.IsFlushIssued) {
                        return events;
                    }

                    var wh = CancelationTokenSource;
                    try {
                        await Task.Delay(maximumPollTime, CancelationTokenSource.Token).ConfigureAwait(false);
                    } catch(Exception x) {

                    }
                } while (DateTime.UtcNow.Subtract(pollStart) < maximumPollTime);
                flushOrder.IsReleased = true;
                lock ("FLUSH_SWITCH") {
                    FlushOrderTokens.Remove(flushOrder);
                }
                return new IDomainEvent[0];
            });
        }

        public void SubscribeListener(IDomainEventListener listener) {
            lock(Listeners) {
                if(!Listeners.Contains(listener)) {
                    Listeners.Add(listener);
                }
            }
        }

        public void RemoveListener(IDomainEventListener listener) {
            lock(Listeners) {
                if (Listeners.Contains(listener)) {
                    Listeners.Remove(listener);
                }
            }
        }
    }
}
