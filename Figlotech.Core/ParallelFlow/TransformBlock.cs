using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Figlotech.Core.ParallelFlow {

    /// <summary>
    /// Transform block that applies a transformation function to input items
    /// and produces output items
    /// </summary>
    public sealed class TransformBlock<TIn, TOut> : FlowBlockBase<TOut>, IFlowReceiver<TIn> {
        private readonly Channel<TIn> _inputChannel;
        private readonly Func<TIn, Task<TOut>> _transformFunc;
        private readonly Func<TIn, IAsyncEnumerable<TOut>>? _transformManyFunc;

        private TransformBlock(
            Func<TIn, Task<TOut>>? transformFunc,
            Func<TIn, IAsyncEnumerable<TOut>>? transformManyFunc,
            FlowExecutionOptions? options = null)
            : base(options) {

            _transformFunc = transformFunc!;
            _transformManyFunc = transformManyFunc;

            _inputChannel = Options.BoundedCapacity > 0
                ? Channel.CreateBounded<TIn>(Options.CreateBoundedChannelOptions())
                : Channel.CreateUnbounded<TIn>(Options.CreateUnboundedChannelOptions());

            _ = RunAsync();
        }

        /// <summary>
        /// Creates a transform block with 1-to-1 transformation
        /// </summary>
        public static TransformBlock<TIn, TOut> Create(
            Func<TIn, Task<TOut>> transform,
            FlowExecutionOptions? options = null) {

            return new TransformBlock<TIn, TOut>(transform, null, options);
        }

        /// <summary>
        /// Creates a transform block with 1-to-1 synchronous transformation
        /// </summary>
        public static TransformBlock<TIn, TOut> Create(
            Func<TIn, TOut> transform,
            FlowExecutionOptions? options = null) {

            return new TransformBlock<TIn, TOut>(
                item => Task.FromResult(transform(item)),
                null,
                options);
        }

        /// <summary>
        /// Creates a transform block with 1-to-many transformation
        /// </summary>
        public static TransformBlock<TIn, TOut> CreateMany(
            Func<TIn, IAsyncEnumerable<TOut>> transform,
            FlowExecutionOptions? options = null) {

            return new TransformBlock<TIn, TOut>(null, transform, options);
        }

        /// <summary>
        /// Creates a transform block with 1-to-many synchronous transformation
        /// </summary>
        public static TransformBlock<TIn, TOut> CreateMany(
            Func<TIn, IEnumerable<TOut>> transform,
            FlowExecutionOptions? options = null) {

            return new TransformBlock<TIn, TOut>(
                null,
                item => transform(item).ToAsyncEnumerable(),
                options);
        }

        public ChannelWriter<TIn> Input => _inputChannel.Writer;

        public void Complete() {
            _inputChannel.Writer.Complete();
        }

        private async Task RunAsync() {
            try {
                var parallelism = Options.GetEffectiveParallelism();

                if (parallelism == 1 || Options.EnsureOrdered) {
                    await RunSequentialAsync().ConfigureAwait(false);
                } else {
                    await RunParallelAsync(parallelism).ConfigureAwait(false);
                }

                CompleteOutput();
            } catch (Exception ex) {
                await HandleError(ex).ConfigureAwait(false);
                CompleteOutput(Options.PropagateExceptions ? ex : null);
            }
        }

        private async Task RunSequentialAsync() {
            await foreach (var item in _inputChannel.Reader.ReadAllAsync(Options.CancellationToken).ConfigureAwait(false)) {
                try {
                    if (_transformManyFunc != null) {
                        await foreach (var output in _transformManyFunc(item).WithCancellation(Options.CancellationToken).ConfigureAwait(false)) {
                            await OutputChannel.Writer.WriteAsync(output, Options.CancellationToken).ConfigureAwait(false);
                        }
                    } else {
                        var output = await _transformFunc(item).ConfigureAwait(false);
                        await OutputChannel.Writer.WriteAsync(output, Options.CancellationToken).ConfigureAwait(false);
                    }
                } catch (Exception ex) {
                    await HandleError(ex).ConfigureAwait(false);
                    if (Options.PropagateExceptions) throw;
                }
            }
        }

        private async Task RunParallelAsync(int parallelism) {
            var workers = new List<Task>(parallelism);

            for (int i = 0; i < parallelism; i++) {
                workers.Add(WorkerAsync());
            }

            await Task.WhenAll(workers).ConfigureAwait(false);
        }

        private async Task WorkerAsync() {
            await foreach (var item in _inputChannel.Reader.ReadAllAsync(Options.CancellationToken).ConfigureAwait(false)) {
                try {
                    if (_transformManyFunc != null) {
                        await foreach (var output in _transformManyFunc(item).WithCancellation(Options.CancellationToken).ConfigureAwait(false)) {
                            await OutputChannel.Writer.WriteAsync(output, Options.CancellationToken).ConfigureAwait(false);
                        }
                    } else {
                        var output = await _transformFunc(item).ConfigureAwait(false);
                        await OutputChannel.Writer.WriteAsync(output, Options.CancellationToken).ConfigureAwait(false);
                    }
                } catch (Exception ex) {
                    await HandleError(ex).ConfigureAwait(false);
                    if (Options.PropagateExceptions) throw;
                }
            }
        }
    }
}
