using System;
using System.Threading;

namespace Figlotech.Core.DomainEvents {
    internal sealed class DomainEventListenerSubscription : IDisposable {
        private readonly DomainEventsHub _hub;
        private readonly IDomainEventListener _listener;
        private int _disposed;

        public DomainEventListenerSubscription(DomainEventsHub hub, IDomainEventListener listener) {
            _hub = hub ?? throw new ArgumentNullException(nameof(hub));
            _listener = listener ?? throw new ArgumentNullException(nameof(listener));
        }

        public void Dispose() {
            if (Interlocked.Exchange(ref _disposed, 1) == 0) {
                _hub.RemoveListener(_listener);
            }
        }
    }
}
