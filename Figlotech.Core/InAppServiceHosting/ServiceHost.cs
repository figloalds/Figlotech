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
                var t = new Thread(()=> {
                    try {
                        service.Run();
                    } catch(Exception x) {
                        OnServiceError?.Invoke(service, x);
                    }
                });
                t.IsBackground = !service.IsCritical;
                t.Name = $"fthservice_{idgen++}_{service.GetType().Name}";
                t.Start();
                ServiceThreads[service] = t;
            }
        }

        public event Action<object, Exception> OnServiceError;

        public void Stop(IFthService service) {
            if (ServiceThreads.ContainsKey(service)) {
                service.InterruptIssued = true;
                ServiceThreads[service].Join(TimeSpan.FromSeconds(12));
                if(!service.IsCritical) {
                    if(ServiceThreads[service].IsAlive) {
                        ServiceThreads[service].Interrupt();
                    }
                    ServiceThreads.Remove(service);
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


        public string Exec(string service, string Commands) {
            var svc = Services.FirstOrDefault(s => s.GetType().Name == service);
            return svc?.Exec(Commands);
        }
    }
}
