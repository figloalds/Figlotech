using Figlotech.Core.BusinessModel;
using Figlotech.Core.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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

    public class DomainEventsHub {
        public static DomainEventsHub Global = new DomainEventsHub();
        private readonly DomainEventsHub parentHub;

        public DomainEventsHub(DomainEventsHub parentHub = null) {
            this.parentHub = parentHub;
        }

        public TimeSpan EventCacheDuration { get; set; } = TimeSpan.FromMinutes(30);

        private List<IDomainEvent> _eventCache = new List<IDomainEvent>();
        public List<IDomainEvent> EventCache => _eventCache;

        private List<IDomainEventListener> _listeners = new List<IDomainEventListener>();
        public List<IDomainEventListener> Listeners => _listeners;

        private List<CustomDomainValidation> _customValidators = new List<CustomDomainValidation>();
        public List<CustomDomainValidation> CustomValidators => _customValidators;
        
        // Wanna grab hold of tasks so that GC won't kill them.
        List<Task> EventTasks = new List<Task>();
        
        public void Raise<T>(IEnumerable<T> domainEvents) where T : IDomainEvent {
            domainEvents.Iterate(evt => Raise(evt));
        }
        public void Raise<T>(T domainEvent) where T : IDomainEvent {
            // Cache event
            lock(EventCache) {
                EventCache.Add(domainEvent);
                EventCache.RemoveAll(e => DateTime.UtcNow.Ticks - e.Time > EventCacheDuration.Ticks);
            }

            // Raise event on all listeners.
            Listeners.RemoveAll(l => l == null);
            foreach (var listener in Listeners) {
                if (listener is IDomainEventListener<T> correctListener) {
                    EventTasks.Add(Fi.Tech.FireTask(async () => {
                        try {
                            var t = correctListener.OnEventTriggered(domainEvent);
                            if(t != null) {
                                await t;
                            }
                            if (domainEvent.AllowPropagation) {
                                parentHub?.Raise(domainEvent);
                            }
                        } catch (Exception x) {
                            var t = correctListener.OnEventHandlingError(x);
                            if (t != null) {
                                await t;
                            }
                        }
                    }));
                }
            }

            // Clear "Ran to completion" tasks;
            EventTasks.RemoveAll(t => t.IsCompleted || t.IsFaulted || t.IsCanceled);
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
                if(validation.ValidationPhase == phase) {
                    try {
                        validation.Validator.Validate(validationTarget as IBusinessObject, errs);
                    } catch(Exception x) {
                        errs.Add("Application", $"Validation has throw an Exception");
                        Fi.Tech.WriteLine(x.Message);
                        Fi.Tech.WriteLine(x.StackTrace);
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

        public async Task<List<IDomainEvent>> PollForEventsSince<T>(TimeSpan maximumPollTime, long Id, Predicate<T> filter) {
            return await PollForEventsSince(maximumPollTime, Id, e => e is T evt && filter(evt));
        }
        public async Task<List<IDomainEvent>> PollForEventsSince(TimeSpan maximumPollTime, long Id, Predicate<IDomainEvent> filter) {
            DateTime pollStart = DateTime.UtcNow;
            return await await Fi.Tech.FireTask(async () => {
                List<IDomainEvent> retv = new List<IDomainEvent>();
                do {
                    var events = GetEventsSince(Id).ToList();
                    retv.AddRange(events.Where(e => filter(e)));
                    if (events.Any())
                        Id = events.Max(e => e.Id);
                    if (!retv.Any()) {
                        await Task.Delay(500);
                    }
                } while (!retv.Any() && DateTime.UtcNow.Subtract(pollStart) < maximumPollTime);

                return retv;
            });
        }

        public async Task<List<IDomainEvent>> PollForEventsSince<T>(TimeSpan maximumPollTime, DateTime dt, Predicate<T> filter) {
            return await PollForEventsSince(maximumPollTime, dt, e => e is T evt && filter(evt));
        }

        public async Task<List<IDomainEvent>> PollForEventsSince(TimeSpan maximumPollTime, DateTime dt, Predicate<IDomainEvent> filter) {
            DateTime pollStart = DateTime.UtcNow;
            return await await Fi.Tech.FireTask(async () => {
                List<IDomainEvent> retv = new List<IDomainEvent>();
                do {
                    var events = GetEventsSince(dt).ToList();
                    retv.AddRange(events.Where(e => filter(e)));
                    if (events.Any()) dt = new DateTime(events.Max(e => e.Time));
                    if (!retv.Any()) {
                        await Task.Delay(400);
                    }
                } while (!retv.Any() && DateTime.UtcNow.Subtract(pollStart) < maximumPollTime);

                return retv;
            });
        }

        public void SubscribeListener<T>(IDomainEventListener<T> listener) where T : IDomainEvent {
            _listeners.Add(listener);
        }

        public void RemoveListener(IDomainEventListener listener) {
            _listeners.Remove(listener);
        }
    }
}
