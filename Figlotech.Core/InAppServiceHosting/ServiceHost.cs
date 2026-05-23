using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.Core.InAppServiceHosting {
    public sealed class ServiceHost {
        public static ServiceHost Default { get; set; } = new ServiceHost();

        private List<IFthService> Services { get; set; } = new List<IFthService>();
        readonly ConcurrentDictionary<IFthService, Task> ServiceTasks = new ConcurrentDictionary<IFthService, Task>();
        readonly ConcurrentDictionary<IFthService, FthServiceInfo> ServiceInfos = new ConcurrentDictionary<IFthService, FthServiceInfo>();
        readonly ConcurrentDictionary<IFthService, CancellationTokenSource> CyclicServiceIterationResets = new ConcurrentDictionary<IFthService, CancellationTokenSource>();

        public IFthService InitService<T>(params object[] args) {
            return InitService(typeof(T), args);
        }
        public IFthService InitService(Type svcType, params object[] args) {
            var svc = Services.FirstOrDefault(s => s.GetType() == svcType);
            if (svc != null)
                return svc;
            if (svcType != null) {
                IFthService instance = (IFthService)Activator.CreateInstance(svcType, new object[] { });
                instance.Init(args);
                Services.Add(instance);
                return instance;
            }
            return null;
        }

        public void Start(IFthService service) {
            if (service == null) {
                throw new ArgumentNullException(nameof(service));
            }
            if (!ServiceTasks.ContainsKey(service)) {
                var t = Task.Run(async () => {
                    try {
                        var rt = service.Run();
                        if (rt != null) {
                            await rt.ConfigureAwait(false);
                        }
                        if (service is IFthCyclicService cServ) {
                            var rt2 = cServ.MainLoopInit();
                            if (rt2 != null) {
                                await rt2.ConfigureAwait(false);
                            }

                            cServ.BreakMainLoop = false;
                            cServ.InterruptIssued = false;
                            Task[] lt = new Task[2];
                            int l = 0;
                            while (!cServ.BreakMainLoop && !cServ.InterruptIssued) {
                                var cts = new CancellationTokenSource();
                                CyclicServiceIterationResets[cServ] = cts;
                                try {
                                    var iterationTask = cServ.MainLoopIteration();
                                    if (iterationTask != null) {
                                        lt[l] = iterationTask;
                                    }
                                    var prev = (l - 1) >= 0 ? l - 1 : lt.Length - 1;
                                    if (lt[prev] != null && !lt[prev].IsCompleted) {
                                        await lt[prev].ConfigureAwait(false);
                                    }
                                    l = (l + 1) % lt.Length;
                                    await Task.Delay(cServ.IterationDelay, cts.Token).ConfigureAwait(false);
                                } catch (TaskCanceledException) {
                                } catch (Exception x) {
                                    Fi.Tech.WriteLine($"Error in {service.GetType().Name} iteration: {x.Message}");
                                    OnServiceError?.Invoke(service, x);
                                } finally {
                                    CyclicServiceIterationResets.TryRemove(cServ, out _);
                                    cts.Dispose();
                                }
                            }
                        }
                    } catch (Exception x) {
                        Fi.Tech.WriteLine($"Error Executing Service {service.GetType().Name} {x.Message}");
                        OnServiceError?.Invoke(service, x);
                    }
                });
                if (!Services.Contains(service)) {
                    Services.Add(service);
                }
                ServiceTasks[service] = t;
                ServiceInfos[service] = new FthServiceInfo(service);
            }
        }

        public event Action<IFthService, Exception> OnServiceError;

        public async Task WaitForAllToExit() {
            await Task.WhenAll(ServiceTasks.Values).ConfigureAwait(false);
        }

        public async Task Stop(IFthService service) {
            if (ServiceTasks.ContainsKey(service)) {
                service.InterruptIssued = true;
                if (service is IFthCyclicService cServ) {
                    if (CyclicServiceIterationResets.TryGetValue(cServ, out var cts)) {
                        try {
                            cts.Cancel();
                        } catch (ObjectDisposedException) {
                        }
                    }
                }
                await Fi.Tech.Timesout(ServiceTasks[service], TimeSpan.FromSeconds(12)).ConfigureAwait(false);
                if (!service.IsCritical) {
                    ServiceTasks.TryRemove(service, out _);
                    ServiceInfos.TryRemove(service, out _);
                    CyclicServiceIterationResets.TryRemove(service, out _);
                }
            }
        }

        public void ResetIterationDelay<T>() where T : IFthCyclicService, new() {
            var instance = GetInstance<T>();
            if (instance == null) {
                return;
            }
            if (CyclicServiceIterationResets.TryGetValue(instance, out var cts)) {
                try {
                    cts.Cancel();
                } catch (ObjectDisposedException) {
                }
            }
        }

        public void Run<T>(params object[] args) where T : IFthService, new() {
            Run(typeof(T), args);
        }

        public void Run(Type t, params object[] args) {
            var svc = InitService(t, args);
            Start(svc);
        }

        public IEnumerable<FthServiceInfo> GetServiceInfos() {
            return ServiceInfos.Values;
        }
        public T GetInstance<T>() where T : IFthService {
            return (T)Services
                .FirstOrDefault(s =>
                    s.GetType() == typeof(T)
                );
        }

        public async Task<string> Exec(string service, string Commands) {
            var svc = Services.FirstOrDefault(s => s.GetType().Name == service);
            return svc != null ? await svc.Exec(Commands).ConfigureAwait(false) : null;
        }
    }
}
