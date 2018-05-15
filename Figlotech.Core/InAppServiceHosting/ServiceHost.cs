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
        private List<Type> _declaredMicroServices = new List<Type>();

        private List<IFthService> Services { get; set; } = new List<IFthService>();
        Dictionary<IFthService, Thread> ServiceThreads = new Dictionary<IFthService, Thread>();
        Dictionary<IFthService, FthServiceInfo> ServiceInfos = new Dictionary<IFthService, FthServiceInfo>();

        public void AddDeclaredMicroService(Type serviceClass) {
            if (serviceClass.GetInterfaces().FirstOrDefault(i => i == typeof(IFthService)) == null) {
                throw new Exception($"{serviceClass.Name} was expected to implement IFthService interface.");
            }
            if(_declaredMicroServices.FirstOrDefault(t=> t==serviceClass) == null)
                _declaredMicroServices.Add(serviceClass);
        }

        public void AddDeclaredMicroService<T>() where T: IFthService, new() {
            AddDeclaredMicroService(typeof(T));
        }

        public void UseDeclaredMicroservicesFrom(ServiceHost other) {
            _declaredMicroServices.Clear();
            _declaredMicroServices.AddRange(other._declaredMicroServices);
        }

        /// <summary>
        /// Instantiates a service that has been declared earlier and initializes it. This function does not start said service.
        /// </summary>
        /// <param name="serviceName"></param>
        /// <param name="args"></param>
        /// <returns></returns>
        public IFthService InitService(String serviceName, params object[] args) {
            var svcType = _declaredMicroServices.FirstOrDefault(t => t.Name == serviceName);
            return InitService(svcType, args);
        }
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
                var t = Fi.Tech.SafeCreateThread(async ()=> {
                    try {
                        var rt = service.Run();
                        if (rt != null) {
                            await rt;
                        }
                        if(service is IFthCyclicService cServ) {
                            var rt2 = cServ.MainLoopInit();
                            if (rt2 != null) {
                                await rt2;
                            }

                            cServ.BreakMainLoop = false;
                            cServ.InterruptIssued = false;
                            while (!cServ.BreakMainLoop && !cServ.InterruptIssued) {
                                var lt = cServ.MainLoopIteration();
                                if (lt != null) {
                                    await lt;
                                }
                            }
                        }
                    } catch(Exception x) {
                        Fi.Tech.WriteLine();
                        Fi.Tech.WriteLine();
                        Fi.Tech.WriteLine();
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
            AddDeclaredMicroService(t);
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
