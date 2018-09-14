using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Figlotech.Core.InAppServiceHosting
{
    public class FthServiceInfo
    {
        DateTime _started = DateTime.UtcNow;
        IFthService service;

        internal FthServiceInfo(IFthService service) {
            this.Name = service.GetType().Name;
        }

        public IFthService Instance => service;
        public string Name { get; internal set; }
        public DateTime Started => _started;
        public TimeSpan ExecutingTime => DateTime.UtcNow.Subtract(Started);
    }
}
