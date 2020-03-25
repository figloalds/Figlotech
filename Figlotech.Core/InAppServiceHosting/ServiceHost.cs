using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.Core.InAppServiceHosting
{
    public class ServiceHost
    {
        public static ServiceHost Default { get; set; } = new ServiceHost();
        
        private List<IFthService> Services { get; set; } = new List<IFthService>();
        Dictionary<IFthService, Thread> ServiceThreads = new Dictionary<IFthService, Thread>();
        Dictionary<IFthService, FthServiceInfo> ServiceInfos = new Dictionary<IFthService, FthServiceInfo>();

        public IFthService InitService<T>(params object[] args) {
            return InitService(typeof(T), args);
        }
        public IFthService InitService(Type svcType, params object[] args) {
            var svc = Services.FirstOrDefault(s => s.GetType() == svcType);
            if (svc != null)
                return svc;
            if (svcType != null) {
                IFthService instance = (IFthService)Activator.CreateInstance(svcType);
                instance.Init(args);
                Services.Add(instance);
                return instance;
            }
            return null;
        }

        int idgen = 0;
        public void Start(IFthService service) {
            if(!ServiceThreads.ContainsKey(service)) {
                var t = Fi.Tech.SafeCreateThread(()=> {
                    try {
                        var rt = service.Run();
                        if (rt != null) {
                            rt?.Wait();
                        }
                        if(service is IFthCyclicService cServ) {
                            var rt2 = cServ.MainLoopInit();
                            if (rt2 != null) {
                                rt2?.Wait();
                            }

                            cServ.BreakMainLoop = false;
                            cServ.InterruptIssued = false;
                            Task[] lt = new Task[1];
                            int l = 0;
                            while (!cServ.BreakMainLoop && !cServ.InterruptIssued) {
                                lt[l] = cServ.MainLoopIteration();
                                var prev = (l - 1) >= 0 ? l - 1 : lt.Length - 1;
                                if (lt[prev] != null && !lt[prev].IsCompleted) {
                                    lt[prev].Wait();
                                }
                                l = (l + 1) % lt.Length;
                                Task.Delay(cServ.IterationDelay).Wait();
                            }
                        }
                    } catch(Exception x) {

                        Fi.Tech.WriteLine($"Error Executing Service {service.GetType().Name} {x.Message}");
                        OnServiceError?.Invoke(service, x);
                    }
                });
                t.IsBackground = !service.IsCritical;
                t.Name = $"fthservice_{idgen++}_{service.GetType().Name}";
                t.Start();
                Services.Add(service);
                ServiceThreads[service] = t;
                ServiceInfos[service] = new FthServiceInfo(service);
            }
        }

        public event Action<IFthService, Exception> OnServiceError;

        public void Stop(IFthService service) {
            if (ServiceThreads.ContainsKey(service)) {
                service.InterruptIssued = true;
                ServiceThreads[service].Join(TimeSpan.FromSeconds(12));
                if(!service.IsCritical) {
                    if(ServiceThreads[service].IsAlive) {
                        ServiceThreads[service].Interrupt();
                    }
                    ServiceThreads.Remove(service);
                    ServiceInfos.Remove(service);
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
        public T GetInstance<T>() where T: IFthService {
            return (T) Services
                .FirstOrDefault(s => 
                    s.GetType() == typeof(T)
                );
        }

        public async Task<string> Exec(string service, string Commands) {
            var svc = Services.FirstOrDefault(s => s.GetType().Name == service);
            return svc != null ? await svc?.Exec(Commands) : null;
        }
    }
}
