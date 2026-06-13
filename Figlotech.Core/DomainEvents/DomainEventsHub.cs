using Figlotech.Core.Helpers;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.Core.DomainEvents {
    public static class DomainEventsHubExtensions {
        public static void SubscribeInline<T>(this DomainEventsHub self, Func<T, ValueTask> fn, Func<T, Exception, ValueTask> handler = null) where T : IDomainEvent {
            self.SubscribeListener(InlineLambdaListener.Create<T>(fn, handler));
        }
    }

    public sealed class DomainEventsHub {

        public static DomainEventsHub Global = new DomainEventsHub(FiTechCoreExtensions.GlobalQueuer);
        private readonly DomainEventsHub parentHub;

        private WorkQueuer MainQueuer { get; set; }

        public DomainEventsHub(WorkQueuer queuer = null, DomainEventsHub parentHub = null) {
            this.parentHub = parentHub;
            MainQueuer = queuer ?? FiTechCoreExtensions.GlobalQueuer;
        }

        private sealed class ListenerEntry {
            public IDomainEventListener Listener { get; }
            public ListenerEntry(IDomainEventListener listener) => Listener = listener;
        }

        private ImmutableDictionary<Type, ImmutableArray<ListenerEntry>> _listenersByType
            = ImmutableDictionary<Type, ImmutableArray<ListenerEntry>>.Empty;

        private ImmutableList<IExtension> _extensions = ImmutableList<IExtension>.Empty;


        public void Raise<T>(IEnumerable<T> domainEvents) where T : IDomainEvent {
            if (domainEvents == null) throw new ArgumentNullException(nameof(domainEvents));
            foreach (var domainEvent in domainEvents) {
                Raise(domainEvent);
            }
        }

        private void WriteLog(string log) {
            Fi.Tech.WriteLine("FTH:EventHub", log);
        }

        public void RegisterExtension(IExtension Extension) {
            if (Extension == null) throw new ArgumentNullException(nameof(Extension));
            ImmutableInterlocked.Update(ref _extensions, (current, e) => current.Contains(e) ? current : current.Add(e), Extension);
        }

        public void RegisterExtension<TOpCode, T>(Extension<TOpCode, T> Extension) {
            if (Extension == null) throw new ArgumentNullException(nameof(Extension));
            RegisterExtension((IExtension)Extension);
        }

        public async ValueTask ExecuteExtensionsAsync<TOpCode, T>(TOpCode opcode, T input) {
            var extensions = _extensions;
            foreach (var Extension in extensions) {
                if (Extension is Extension<TOpCode, T> ext) {
                    try {
                        await ext.Execute(opcode, input).ConfigureAwait(false);
                    } catch (Exception x) {
                        await ext.OnError(opcode, input, x).ConfigureAwait(false);
                    }
                }
            }
        }

        public bool AllowTelemetry { get; set; } = false;

        public void Raise(IDomainEvent domainEvent) {
            WriteLog($"Raising Event {domainEvent.GetType()}");

            if (FiTechCoreExtensions.StdoutEventHubLogs) {
                domainEvent.d_RaiseOrigin = Environment.StackTrace;
            }

            var listeners = _listenersByType;
            Dispatch(domainEvent, listeners);

            if (domainEvent.AllowPropagation) {
                parentHub?.Raise(domainEvent);
            }
        }

        private void Dispatch(IDomainEvent domainEvent, ImmutableDictionary<Type, ImmutableArray<ListenerEntry>> listeners) {
            if (listeners == null || listeners.IsEmpty) {
                return;
            }

            var eventType = domainEvent.GetType();
            if (!listeners.TryGetValue(eventType, out var entries)) {
                return;
            }

            for (int i = 0; i < entries.Length; i++) {
                var listener = entries[i].Listener;
                EnqueueListenerWork(domainEvent, listener);
            }
        }

        private void EnqueueListenerWork(IDomainEvent domainEvent, IDomainEventListener listener) {
            if (MainQueuer != null) {
                MainQueuer.Enqueue(new WorkJob(async () => {
                    try {
                        await listener.OnEventTriggered(domainEvent).ConfigureAwait(false);
                    } catch (Exception x) {
                        try {
                            await listener.OnEventHandlingError(domainEvent, x).ConfigureAwait(false);
                        } catch {
                            Fi.Tech.SwallowException(x);
                        }
                    }
                }, (b) => Fi.Result()) {
                    Name = $"Raising Event {domainEvent.GetType().Name} on {listener.GetType().Name}",
                    AllowTelemetry = AllowTelemetry,
                });
            }
        }

        public ValueTask RaiseAndWaitForHandlers(IDomainEvent domainEvent) {
            WriteLog($"Raising Event {domainEvent.GetType()}");

            if (FiTechCoreExtensions.StdoutEventHubLogs) {
                domainEvent.d_RaiseOrigin = Environment.StackTrace;
            }

            var listeners = _listenersByType;
            var task = DispatchAndWait(domainEvent, listeners);

            if (domainEvent.AllowPropagation) {
                return AwaitWithPropagation(task, domainEvent);
            }

            return task;
        }

        private async ValueTask AwaitWithPropagation(ValueTask current, IDomainEvent domainEvent) {
            await current.ConfigureAwait(false);
            if (parentHub != null) {
                await parentHub.RaiseAndWaitForHandlers(domainEvent).ConfigureAwait(false);
            }
        }

        private ValueTask DispatchAndWait(IDomainEvent domainEvent, ImmutableDictionary<Type, ImmutableArray<ListenerEntry>> listeners) {
            if (listeners == null || listeners.IsEmpty || MainQueuer == null) {
                return default;
            }

            var eventType = domainEvent.GetType();
            if (!listeners.TryGetValue(eventType, out var entries) || entries.IsEmpty) {
                return default;
            }

            if (entries.Length == 1) {
                var request = EnqueueListenerWorkWithRequest(domainEvent, entries[0].Listener);
                return request != null ? new ValueTask(request.TaskCompletionSource.Task) : default;
            }

            var tasks = new Task[entries.Length];
            for (int i = 0; i < entries.Length; i++) {
                var request = EnqueueListenerWorkWithRequest(domainEvent, entries[i].Listener);
                tasks[i] = request?.TaskCompletionSource.Task ?? Task.CompletedTask;
            }

            return new ValueTask(Task.WhenAll(tasks));
        }

        private WorkJobExecutionRequest EnqueueListenerWorkWithRequest(IDomainEvent domainEvent, IDomainEventListener listener) {
            return MainQueuer?.Enqueue(new WorkJob(async () => {
                try {
                    await listener.OnEventTriggered(domainEvent).ConfigureAwait(false);
                } catch (Exception x) {
                    try {
                        await listener.OnEventHandlingError(domainEvent, x).ConfigureAwait(false);
                    } catch {
                        Fi.Tech.SwallowException(x);
                    }
                }
            }, (b) => Fi.Result()) {
                Name = $"Raising Event {domainEvent.GetType().Name} on {listener.GetType().Name}",
                AllowTelemetry = AllowTelemetry,
            });
        }

        public IDisposable SubscribeListener(IDomainEventListener listener) {
            if (listener == null) throw new ArgumentNullException(nameof(listener));

            ImmutableInterlocked.Update(ref _listenersByType, (current, l) => {
                var handledTypes = GetHandledTypes(l);
                var next = current;
                foreach (var type in handledTypes) {
                    var entries = next.GetValueOrDefault(type, ImmutableArray<ListenerEntry>.Empty);
                    if (entries.Any(e => ReferenceEquals(e.Listener, l))) {
                        continue;
                    }
                    next = next.SetItem(type, entries.Add(new ListenerEntry(l)));
                }
                return next;
            }, listener);

            return new DomainEventListenerSubscription(this, listener);
        }

        private IEnumerable<Type> GetHandledTypes(IDomainEventListener listener) {
            if (listener is IGenericDomainEventListener generic) {
                return generic.HandledTypes;
            }
            return listener.GetType().GetInterfaces()
                .Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(DomainEventListener<>))
                .Select(i => i.GetGenericArguments()[0]);
        }

        public void RemoveListener(IDomainEventListener listener) {
            if (listener == null) throw new ArgumentNullException(nameof(listener));

            ImmutableInterlocked.Update(ref _listenersByType, (current, l) => {
                var next = current;
                foreach (var type in current.Keys.ToList()) {
                    var entries = current[type];
                    var filtered = entries.RemoveAll(e => ReferenceEquals(e.Listener, l));
                    if (filtered.IsEmpty) {
                        next = next.Remove(type);
                    } else if (filtered.Length != entries.Length) {
                        next = next.SetItem(type, filtered);
                    }
                }
                return next;
            }, listener);
        }

        public void ClearListeners() {
            _listenersByType = ImmutableDictionary<Type, ImmutableArray<ListenerEntry>>.Empty;
        }
    }
}
