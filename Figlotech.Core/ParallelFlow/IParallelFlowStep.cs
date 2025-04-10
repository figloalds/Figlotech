using static Figlotech.Core.FiTechCoreExtensions;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using System.Threading;

namespace Figlotech.Core {

    public interface IParallelFlowStep {
    }
    public interface IParallelFlowStepOut<TOut> : IParallelFlowStep, IAsyncEnumerable<TOut> {
        TaskAwaiter<List<TOut>> GetAwaiter();
        IAsyncEnumerator<TOut> GetAsyncEnumerator(CancellationToken cancellation);
        Task<List<TOut>> TaskObj { get; }
    }
    public interface IParallelFlowStepIn<TIn> : IParallelFlowStep {
        void Put(TIn input);
        Task NotifyDoneQueueing();
    }
}