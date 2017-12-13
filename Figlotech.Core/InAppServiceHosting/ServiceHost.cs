using Figlotech.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.Core.InAppServiceHosting {
    public class ServiceHost
    {
        List<IFthService> Services = new List<IFthService>();
        public static ServiceHost Default = new ServiceHost();


        public IFthService Run<T>(params string[] args) where T : IFthService, new() {
            return Run(typeof(T), args);
        }
        public IFthService Run(Type serviceType, params string[] args) {
            if(!typeof(IFthService).IsAssignableFrom(serviceType)) {
                throw new Exception($"Specified service class {serviceType.Name} is not an implementation of IFthService interface.");
            }
            IFthService service = (IFthService) Activator.CreateInstance(serviceType);
            service.Init(args);
            service.Thread = new Thread(service.Run);
            Services.Add(service);

            service.Thread.Start();
            return service;
        }
    }
}
