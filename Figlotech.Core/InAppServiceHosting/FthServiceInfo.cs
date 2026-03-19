using System;

namespace Figlotech.Core.InAppServiceHosting {
    public sealed class FthServiceInfo {
        readonly DateTime _started = DateTime.UtcNow;
        readonly IFthService service;

        internal FthServiceInfo(IFthService service) {
            this.Name = service.GetType().Name;
        }

        public IFthService Instance => service;
        public string Name { get; internal set; }
        public DateTime Started => _started;
        public TimeSpan ExecutingTime => DateTime.UtcNow.Subtract(Started);
    }
}
