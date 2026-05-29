using Figlotech.Core.Helpers;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.Core.DomainEvents {
    public static class DomainEventsHubExtensions {
        public static void SubscribeInline<T>(this DomainEventsHub self, Func<T, Task> fn, Func<T, Exception, Task> handler = null) where T : IDomainEvent {
            self.SubscribeListener(InlineLambdaListener.Create<T>(fn, handler));
        }
    }

    public sealed class EventRaiseResponse {
        public List<TaskCompletionSource<int>> CompletionSources { get; internal set; }
    }

    public sealed class DomainEventsHub {

        public static DomainEventsHub Global = new DomainEventsHub(FiTechCoreExtensions.GlobalQueuer);
        private readonly DomainEventsHub parentHub;

        private WorkQueuer MainQueuer { get; set; }

        public DomainEventsHub(WorkQueuer queuer = null, DomainEventsHub parentHub = null) {
            this.parentHub = parentHub;
            MainQueuer = queuer ?? FiTechCoreExtensions.GlobalQueuer;
        }

        private ImmutableArray<IDomainEventListener> _listeners = ImmutableArray<IDomainEventListener>.Empty;

        private readonly List<IExtension> Extensions = new List<IExtension>();


        public void Raise<T>(IEnumerable<T> domainEvents) where T : IDomainEvent {
            foreach (var domainEvent in domainEvents) {
                Raise(domainEvent);
            }
        }

        private void WriteLog(string log) {
            Fi.Tech.WriteLine("FTH:EventHub", log);
        }

        public void RegisterExtension(IExtension Extension) {
            if (Extension == null) {
                throw new ArgumentNullException("Trying to register null extension");
            }
            if (!Extensions.Contains(Extension)) {
                Extensions.Add(Extension);
            }
        }

        public void RegisterExtension<TOpCode, T>(Extension<TOpCode, T> Extension) {
            if (Extension == null) {
                throw new ArgumentNullException("Trying to register null extension");
            }
            RegisterExtension((IExtension)Extension);
        }

        public async Task ExecuteExtensionsAsync<TOpCode, T>(TOpCode opcode, T input) {

            foreach (var Extension in Extensions) {
                if (Extension is Extension<TOpCode, T> ext) {
                    try {
                        await ext.Execute(opcode, input);
                    } catch (Exception x) {
                        await ext.OnError(opcode, input, x);
                    }
                }
            }
        }

        public bool AllowTelemetry { get; set; } = false;

        public EventRaiseResponse Raise(IDomainEvent domainEvent) {
            WriteLog($"Raising Event {domainEvent.GetType()}");
            
            if (FiTechCoreExtensions.StdoutEventHubLogs) {
                domainEvent.d_RaiseOrigin = Environment.StackTrace;
            }
            
            // Atomic snapshot of listeners - no locks, no allocation
            var listeners = _listeners;
            
            var tcsList = new List<TaskCompletionSource<int>>();
            for (int i = 0; i < listeners.Length; i++) {
                var listener = listeners[i];
                if (!listener.CanHandle(domainEvent)) {
                    continue;
                }
                if (MainQueuer != null) {
                    var req = MainQueuer.Enqueue(new WorkJob(async () => {
                        await listener.OnEventTriggered(domainEvent).ConfigureAwait(false);
                    }, async x => {
                        try {
                            await listener.OnEventHandlingError(domainEvent, x).ConfigureAwait(false);
                        } catch {
                            Fi.Tech.SwallowException(x);
                        }
                    }, (b) => {
                        return Fi.Result();
                    }) {
                        Name = $"Raising Event {domainEvent.GetType().Name} on {listener.GetType().Name}",
                        AllowTelemetry = AllowTelemetry,
                    });
                    tcsList.Add(req.TaskCompletionSource);
                }
            }
            if (domainEvent.AllowPropagation) {
                parentHub?.Raise(domainEvent);
            }

            return new EventRaiseResponse() {
                CompletionSources = tcsList,
            };
        }

        public void SubscribeListener(IDomainEventListener listener) {
            if(listener == null) throw new ArgumentNullException(nameof(listener));
            
            ImmutableInterlocked.Update(ref _listeners, (current, l) => {
                if (current.Contains(l)) {
                    return current;
                }
                return current.Add(l);
            }, listener);
        }

        public void RemoveListener(IDomainEventListener listener) {
            if(listener == null) throw new ArgumentNullException(nameof(listener));
            
            ImmutableInterlocked.Update(ref _listeners, (current, l) => current.Remove(l), listener);
        }
    }
}
