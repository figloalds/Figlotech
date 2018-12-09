using Figlotech.Core.BusinessModel;
using Figlotech.Core.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.Core.DomainEvents {
    public class CustomDomainValidation {
        public CustomDomainValidation(IValidationRule validator, int? phase) {
            this.Validator = validator;
            this.ValidationPhase = phase;
        }
        public IValidationRule Validator { get; set; }
        public int? ValidationPhase { get; set; } = null;
    }

    public class CustomFlushOrderToken {
        public bool IsFlushIssued { get; set; } = false;
        public bool IsReleased { get; set; } = false;
    }

    public static class DomainEventsHubExtensions {
        public static void SubscribeInline<T>(this DomainEventsHub self, Action<T> fn, Action<T, Exception> handler = null) where T: IDomainEvent {
            self.SubscribeListener(InlineLambdaListener.Create<T>(fn, handler));
        }
    }

    public class DomainEventsHub {
        public static DomainEventsHub Global = new DomainEventsHub();
        private readonly DomainEventsHub parentHub;

        private bool IsTerminationIssued = false;

        List<CustomFlushOrderToken> FlushOrderTokens { get; set; } = new List<CustomFlushOrderToken>();

        public DomainEventsHub(DomainEventsHub parentHub = null) {
            this.parentHub = parentHub;
        }

        public TimeSpan EventCacheDuration { get; set; } = TimeSpan.FromMinutes(30);

        public DateTime LastEventDateTime {
            get {
                lock(EventCache) {
                    return EventCache.Count == 0 ? DateTime.MinValue : EventCache[EventCache.Count - 1]?.TimeStamp ?? DateTime.MinValue;
                }
            }
        }

        public List<IDomainEvent> EventCache { get; private set; } = new List<IDomainEvent>();
        private List<IDomainEventListener> Listeners { get; set; } = new List<IDomainEventListener>();
        public Dictionary<String, Object> Scope { get; private set; } = new Dictionary<string, object>();
        public List<CustomDomainValidation> CustomValidators { get; private set; } = new List<CustomDomainValidation>();

        // Wanna grab hold of tasks so that GC won't kill them.
        List<Task> EventTasks = new List<Task>();

        public void Raise<T>(IEnumerable<T> domainEvents) where T : IDomainEvent {
            var tempLi = domainEvents.ToList();
            tempLi.ForEach(evt => Raise(evt));
        }

        public void FlushAllInlineListeners() {
            lock("FLUSH_SWITCH") {
                foreach (var a in FlushOrderTokens) {
                    a.IsFlushIssued = true;
                }
            }
        }

        private void WriteLog(string log) {
            Fi.Tech.WriteLine("FTH:EventHub", log);
        }

        public void Raise(IDomainEvent domainEvent) {
            WriteLog($"Raising Event {domainEvent.GetType()}");
            // Cache event
            if(FiTechCoreExtensions.StdoutEventHubLogs) {
                domainEvent.d_RaiseOrigin = Environment.StackTrace;
            }
            domainEvent.EventsHub = this;
            lock (EventCache) {
                EventCache.RemoveAll(e => DateTime.UtcNow.Ticks - e.Time > EventCacheDuration.Ticks);
            }

            // Raise event on all listeners.
            Listeners.RemoveAll(l => l == null);
            var eventTask = new List<Task>();
            foreach (var listener in Listeners) {
                eventTask.Add(Fi.Tech.FireTask(() => {
                    try {
                        listener.OnEventTriggered(domainEvent);
                        if (domainEvent.AllowPropagation) {
                            parentHub?.Raise(domainEvent);
                        }
                    } catch (Exception x) {
                        try {
                            listener.OnEventHandlingError(domainEvent, x);
                        } catch (Exception y) {
                            Fi.Tech.Throw(x);
                        }
                    }
                }));
            }
            lock(EventTasks) {
                EventTasks.AddRange(eventTask);
                EventTasks.Add(Fi.Tech.FireTask(async () => {
                    await Task.WhenAll(eventTask.ToArray());
                    lock(EventCache)
                        EventCache.Add(domainEvent);
                }));
            }

            // Clear "Ran to completion" tasks;
            lock(EventTasks) {
                EventTasks.RemoveAll(t => t.IsCompleted || t.IsFaulted || t.IsCanceled);
            }
        }

        /// <summary>
        /// Invokes previously registered domain hub validators within the same specified phase.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="validator">The object to run validations against</param>
        /// <param name="phase">The validation phase to apply, only validation rules in this phase will be invoked.</param>
        public ValidationErrors RunValidators<T>(T validationTarget, int? phase = null) where T : IBusinessObject, new() {
            ValidationErrors errs = new ValidationErrors();

            foreach (var validation in CustomValidators) {
                if (validation.ValidationPhase == phase) {
                    try {
                        validation.Validator.Validate(validationTarget as IBusinessObject, errs);
                    } catch (Exception x) {
                        errs.Add("Application", $"Validation has throw an Exception");
                        this.WriteLog(x.Message);
                        this.WriteLog(x.StackTrace);
                    }
                }
            }

            return errs;
        }

        /// <summary>
        /// Adds a custom validator to this domain hub, with the option to specify a custom validation phase.
        /// When Running validations from this same hub, only validations within the same phase will be invoked.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="validator">The validator function to invoke</param>
        /// <param name="validationPhase">The validation phase in which this validation rule should apply, only validations runs invoking this phase will effectively invoke this rule..</param>
        public void SubscribeValidator<T>(IValidationRule<T> validator, int? validationPhase = null) where T : IBusinessObject, new() {
            CustomValidators.Add(new CustomDomainValidation(validator, validationPhase));
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
                return EventCache.Where(e => e.Time > Stamp.Ticks).ToArray();
            }
        }

        public async Task<IDomainEvent[]> PollForEventsSince(TimeSpan maximumPollTime, long Id, Predicate<IDomainEvent> filter) {
            return await PollForEventsSince(maximumPollTime, ()=> GetEventsSince(Id), e => filter(e));
        }

        private async Task<IDomainEvent[]> PollForEventsSince(TimeSpan maximumPollTime, Func<IEnumerable<IDomainEvent>> getEvents, Predicate<IDomainEvent> filter) {
            DateTime pollStart = DateTime.UtcNow;
            return await Task.Run<IDomainEvent[]>(async () => {
                IDomainEvent[] events;
                var flushOrder = new CustomFlushOrderToken();
                FlushOrderTokens.Add(flushOrder);
                do {
                    lock (EventCache) {
                        events = getEvents().ToArray();
                    }
                    if (events.Length > 0) {
                        WriteLog($"Event pooling returned  {events.Length} {String.Join(", ", events.Select(e=> e.GetType().Name))}");
                        return events;
                    }
                    if(flushOrder.IsFlushIssued) {
                        WriteLog($"Event flushing issued {events.Length} {String.Join(", ", events.Select(e => e.GetType().Name))}");
                        return events;
                    }
                    await Task.Delay(500);
                } while (DateTime.UtcNow.Subtract(pollStart) < maximumPollTime);
                flushOrder.IsReleased = true;
                FlushOrderTokens.Remove(flushOrder);
                WriteLog($"Event pooling returned no events");
                return new IDomainEvent[0];
            });
        }

        public async Task<IDomainEvent[]> PollForEventsSince(TimeSpan maximumPollTime, DateTime dt, Predicate<IDomainEvent> filter) {
            return await PollForEventsSince(maximumPollTime, ()=> GetEventsSince(dt), e => filter(e));
        }


        public void SubscribeListener(IDomainEventListener listener) {
            Listeners.Add(listener);
        }

        public void RemoveListener(IDomainEventListener listener) {
            Listeners.Remove(listener);
        }
    }
}
