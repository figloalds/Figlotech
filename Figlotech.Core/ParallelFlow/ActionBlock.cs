using System;
using System.Collections.Generic;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Figlotech.Core.ParallelFlow {

    /// <summary>
    /// Action block that consumes input items and performs side effects
    /// Terminal block in a flow pipeline
    /// </summary>
    public sealed class ActionBlock<TIn> : IFlowReceiver<TIn> {
        private readonly Channel<TIn> _inputChannel;
        private readonly Func<TIn, Task> _action;
        private readonly TaskCompletionSource<bool> _completionSource;
        private readonly FlowExecutionOptions _options;

        private ActionBlock(Func<TIn, Task> action, FlowExecutionOptions? options = null) {
            _action = action;
            _options = options ?? FlowExecutionOptions.Default;

            _inputChannel = _options.BoundedCapacity > 0
                ? Channel.CreateBounded<TIn>(_options.CreateBoundedChannelOptions())
                : Channel.CreateUnbounded<TIn>(_options.CreateUnboundedChannelOptions());

            _completionSource = new TaskCompletionSource<bool>();

            _ = RunAsync();
        }

        /// <summary>
        /// Creates an action block that performs an async action on each item
        /// </summary>
        public static ActionBlock<TIn> Create(
            Func<TIn, Task> action,
            FlowExecutionOptions? options = null) {

            return new ActionBlock<TIn>(action, options);
        }

        /// <summary>
        /// Creates an action block that performs a synchronous action on each item
        /// </summary>
        public static ActionBlock<TIn> Create(
            Action<TIn> action,
            FlowExecutionOptions? options = null) {

            return new ActionBlock<TIn>(
                item => {
                    action(item);
                    return Task.CompletedTask;
                },
                options);
        }

        public ChannelWriter<TIn> Input => _inputChannel.Writer;

        public void Complete() {
            _inputChannel.Writer.Complete();
        }

        public Task Completion => _completionSource.Task;

        private async Task RunAsync() {
            try {
                var parallelism = _options.GetEffectiveParallelism();

                if (parallelism == 1 || _options.EnsureOrdered) {
                    await RunSequentialAsync().ConfigureAwait(false);
                } else {
                    await RunParallelAsync(parallelism).ConfigureAwait(false);
                }

                _completionSource.TrySetResult(true);
            } catch (Exception ex) {
                if (_options.ErrorHandler != null) {
                    try {
                        await _options.ErrorHandler(ex).ConfigureAwait(false);
                    } catch {
                        // Error handler failed
                    }
                }

                if (_options.PropagateExceptions) {
                    _completionSource.TrySetException(ex);
                } else {
                    _completionSource.TrySetResult(false);
                }
            }
        }

        private async Task RunSequentialAsync() {
            await foreach (var item in _inputChannel.Reader.ReadAllAsync(_options.CancellationToken).ConfigureAwait(false)) {
                try {
                    await _action(item).ConfigureAwait(false);
                } catch (Exception ex) {
                    if (_options.ErrorHandler != null) {
                        await _options.ErrorHandler(ex).ConfigureAwait(false);
                    }
                    if (_options.PropagateExceptions) throw;
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
            await foreach (var item in _inputChannel.Reader.ReadAllAsync(_options.CancellationToken).ConfigureAwait(false)) {
                try {
                    await _action(item).ConfigureAwait(false);
                } catch (Exception ex) {
                    if (_options.ErrorHandler != null) {
                        await _options.ErrorHandler(ex).ConfigureAwait(false);
                    }
                    if (_options.PropagateExceptions) throw;
                }
            }
        }
    }
}
