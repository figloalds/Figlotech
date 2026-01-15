using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Figlotech.Core.ParallelFlow {

    /// <summary>
    /// Source block that produces data from an enumerable or generator function
    /// </summary>
    public sealed class SourceBlock<TOut> : FlowBlockBase<TOut> {
        private Func<ValueTask> _produceFunc;

        private SourceBlock(FlowExecutionOptions? options = null)
            : base(options) {
            _produceFunc = () => default;
        }

        /// <summary>
        /// Creates a source block from an enumerable
        /// </summary>
        public static SourceBlock<TOut> FromEnumerable(
            IEnumerable<TOut> source,
            FlowExecutionOptions? options = null) {

            var block = new SourceBlock<TOut>(options);
            block._produceFunc = async () => {
                foreach (var item in source) {
                    await block.OutputChannel.Writer.WriteAsync(item, block.Options.CancellationToken).ConfigureAwait(false);
                }
            };
            _ = block.RunAsync();
            return block;
        }

        /// <summary>
        /// Creates a source block from an async enumerable
        /// </summary>
        public static SourceBlock<TOut> FromAsyncEnumerable(
            IAsyncEnumerable<TOut> source,
            FlowExecutionOptions? options = null) {

            var block = new SourceBlock<TOut>(options);
            block._produceFunc = async () => {
                await foreach (var item in source.WithCancellation(block.Options.CancellationToken).ConfigureAwait(false)) {
                    await block.OutputChannel.Writer.WriteAsync(item, block.Options.CancellationToken).ConfigureAwait(false);
                }
            };
            _ = block.RunAsync();
            return block;
        }

        /// <summary>
        /// Creates a source block from a generator function with yield capability
        /// </summary>
        public static SourceBlock<TOut> FromGenerator(
            Func<Action<TOut>, Task> generator,
            FlowExecutionOptions? options = null) {

            var block = new SourceBlock<TOut>(options);
            block._produceFunc = async () => {
                await generator(item => {
                    block.OutputChannel.Writer.TryWrite(item);
                }).ConfigureAwait(false);
            };
            _ = block.RunAsync();
            return block;
        }

        /// <summary>
        /// Creates a source block from a single item
        /// </summary>
        public static SourceBlock<TOut> FromValue(TOut value, FlowExecutionOptions? options = null) {
            return FromEnumerable(new[] { value }, options);
        }

        /// <summary>
        /// Creates an empty source block
        /// </summary>
        public static SourceBlock<TOut> Empty(FlowExecutionOptions? options = null) {
            return FromEnumerable(Array.Empty<TOut>(), options);
        }

        private async Task RunAsync() {
            try {
                await _produceFunc().ConfigureAwait(false);
                CompleteOutput();
            } catch (Exception ex) {
                await HandleError(ex).ConfigureAwait(false);
                CompleteOutput(Options.PropagateExceptions ? ex : null);
            }
        }
    }
}
