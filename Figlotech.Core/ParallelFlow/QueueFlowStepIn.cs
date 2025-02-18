using System.Collections.Generic;
using System.Threading.Tasks;

namespace Figlotech.Core {

    public static partial class FiTechCoreExtensions {
        public sealed class QueueFlowStepIn<T> : IParallelFlowStepIn<T> {
            Queue<T> Host { get; set; }
            public QueueFlowStepIn(Queue<T> host) {
                this.Host = host;
            }

            public void Put(T input) {
                this.Host.Enqueue(input);
            }

            public Task NotifyDoneQueueing() {
                return Task.FromResult(0);
            }
        }
    }
}