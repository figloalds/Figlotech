using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Figlotech.Core.ParallelFlow {

    /// <summary>
    /// Base interface for all flow blocks that can produce output
    /// </summary>
    public interface IFlowBlock<TOut> {
        /// <summary>
        /// Gets the output channel from which consumers can read data
        /// </summary>
        ChannelReader<TOut> Output { get; }

        /// <summary>
        /// Signals that the block has completed processing
        /// </summary>
        Task Completion { get; }
    }

    /// <summary>
    /// Base interface for blocks that receive input
    /// </summary>
    public interface IFlowReceiver<TIn> {
        /// <summary>
        /// Gets the input channel to which producers can write data
        /// </summary>
        ChannelWriter<TIn> Input { get; }

        /// <summary>
        /// Signals completion to the block (no more input will arrive)
        /// </summary>
        void Complete();
    }

    /// <summary>
    /// Configuration options for flow block execution
    /// </summary>
    public class FlowExecutionOptions {
        /// <summary>
        /// Maximum degree of parallelism. -1 uses Environment.ProcessorCount
        /// </summary>
        public int MaxDegreeOfParallelism { get; set; } = -1;

        /// <summary>
        /// Capacity for bounded channels. -1 for unbounded
        /// </summary>
        public int BoundedCapacity { get; set; } = -1;

        /// <summary>
        /// Whether to propagate exceptions or capture them
        /// </summary>
        public bool PropagateExceptions { get; set; } = true;

        /// <summary>
        /// Cancellation token for the flow execution
        /// </summary>
        public CancellationToken CancellationToken { get; set; } = CancellationToken.None;

        /// <summary>
        /// Optional error handler for exceptions during processing
        /// </summary>
        public Func<Exception, Task>? ErrorHandler { get; set; }

        /// <summary>
        /// Whether to ensure ordered output (may reduce parallelism)
        /// </summary>
        public bool EnsureOrdered { get; set; } = false;

        public static FlowExecutionOptions Default => new FlowExecutionOptions();

        internal int GetEffectiveParallelism() =>
            MaxDegreeOfParallelism <= 0 ? Environment.ProcessorCount : MaxDegreeOfParallelism;

        internal BoundedChannelOptions CreateBoundedChannelOptions() =>
            new BoundedChannelOptions(BoundedCapacity) {
                FullMode = BoundedChannelFullMode.Wait
            };

        internal UnboundedChannelOptions CreateUnboundedChannelOptions() =>
            new UnboundedChannelOptions();
    }

    /// <summary>
    /// Base class for flow blocks with common functionality
    /// </summary>
    public abstract class FlowBlockBase<TOut> : IFlowBlock<TOut> {
        protected readonly Channel<TOut> OutputChannel;
        protected readonly TaskCompletionSource<bool> CompletionSource;
        protected readonly FlowExecutionOptions Options;

        protected FlowBlockBase(FlowExecutionOptions? options = null) {
            Options = options ?? FlowExecutionOptions.Default;

            OutputChannel = Options.BoundedCapacity > 0
                ? Channel.CreateBounded<TOut>(Options.CreateBoundedChannelOptions())
                : Channel.CreateUnbounded<TOut>(Options.CreateUnboundedChannelOptions());

            CompletionSource = new TaskCompletionSource<bool>();
        }

        public ChannelReader<TOut> Output => OutputChannel.Reader;
        public Task Completion => CompletionSource.Task;

        protected async Task HandleError(Exception ex) {
            if (Options.ErrorHandler != null) {
                try {
                    await Options.ErrorHandler(ex).ConfigureAwait(false);
                } catch {
                    // Error handler itself failed, just propagate original
                }
            }
        }

        protected void CompleteOutput(Exception? error = null) {
            OutputChannel.Writer.Complete(error);
            if (error != null && Options.PropagateExceptions) {
                CompletionSource.TrySetException(error);
            } else {
                CompletionSource.TrySetResult(true);
            }
        }
    }
}
