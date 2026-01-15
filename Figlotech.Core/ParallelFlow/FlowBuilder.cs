using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace Figlotech.Core.ParallelFlow {

    /// <summary>
    /// Fluent API builder for creating data flow pipelines
    /// </summary>
    public sealed class FlowBuilder<T> : IFlowBlock<T> {
        private readonly IFlowBlock<T> _source;

        internal FlowBuilder(IFlowBlock<T> source) {
            _source = source;
        }

        public ChannelReader<T> Output => _source.Output;
        public Task Completion => _source.Completion;

        /// <summary>
        /// Transforms each item using the specified function
        /// </summary>
        public FlowBuilder<TResult> Transform<TResult>(
            Func<T, TResult> transform,
            int maxParallelism = -1,
            int boundedCapacity = -1) {

            return Transform(
                item => Task.FromResult(transform(item)),
                maxParallelism,
                boundedCapacity);
        }

        /// <summary>
        /// Transforms each item using the specified async function
        /// </summary>
        public FlowBuilder<TResult> Transform<TResult>(
            Func<T, Task<TResult>> transform,
            int maxParallelism = -1,
            int boundedCapacity = -1) {

            var options = new FlowExecutionOptions {
                MaxDegreeOfParallelism = maxParallelism,
                BoundedCapacity = boundedCapacity
            };

            var transformBlock = TransformBlock<T, TResult>.Create(transform, options);

            _ = LinkToAsync(transformBlock);

            return new FlowBuilder<TResult>(transformBlock);
        }

        /// <summary>
        /// Transforms each item into multiple items (SelectMany)
        /// </summary>
        public FlowBuilder<TResult> SelectMany<TResult>(
            Func<T, IEnumerable<TResult>> transform,
            int maxParallelism = -1,
            int boundedCapacity = -1) {

            return SelectMany(
                item => transform(item).ToAsyncEnumerable(),
                maxParallelism,
                boundedCapacity);
        }

        /// <summary>
        /// Transforms each item into multiple items async (SelectMany)
        /// </summary>
        public FlowBuilder<TResult> SelectMany<TResult>(
            Func<T, IAsyncEnumerable<TResult>> transform,
            int maxParallelism = -1,
            int boundedCapacity = -1) {

            var options = new FlowExecutionOptions {
                MaxDegreeOfParallelism = maxParallelism,
                BoundedCapacity = boundedCapacity
            };

            var transformBlock = TransformBlock<T, TResult>.CreateMany(transform, options);

            _ = LinkToAsync(transformBlock);

            return new FlowBuilder<TResult>(transformBlock);
        }

        /// <summary>
        /// Transforms each item into multiple items async with enumerable result (SelectMany)
        /// </summary>
        public FlowBuilder<TResult> SelectMany<TResult>(
            Func<T, Task<IEnumerable<TResult>>> transform,
            int maxParallelism = -1,
            int boundedCapacity = -1) {

            var options = new FlowExecutionOptions {
                MaxDegreeOfParallelism = maxParallelism,
                BoundedCapacity = boundedCapacity
            };

            // Create transform block with helper function
            var transformBlock = TransformBlock<T, TResult>.CreateMany(
                item => ConvertToAsyncEnumerable(transform(item)),
                options);

            _ = LinkToAsync(transformBlock);

            return new FlowBuilder<TResult>(transformBlock);
        }

        private static async IAsyncEnumerable<TResult> ConvertToAsyncEnumerable<TResult>(Task<IEnumerable<TResult>> task) {
            var results = await task;
            foreach (var item in results) {
                yield return item;
            }
        }

        /// <summary>
        /// Filters items using the specified predicate
        /// </summary>
        public FlowBuilder<T> Where(
            Func<T, bool> predicate,
            int maxParallelism = -1) {

            return SelectMany<T>(
                item => predicate(item) ? new[] { item } : Array.Empty<T>(),
                maxParallelism);
        }

        /// <summary>
        /// Filters items using the specified async predicate
        /// </summary>
        public FlowBuilder<T> Where(
            Func<T, Task<bool>> predicate,
            int maxParallelism = -1) {

            return SelectMany<T>(
                async item => await predicate(item) ? new[] { item } : Array.Empty<T>(),
                maxParallelism);
        }

        /// <summary>
        /// Batches items into lists of specified size
        /// </summary>
        public FlowBuilder<List<T>> Buffer(int batchSize) {
            return Transform(async _ => {
                var batch = new List<T>(batchSize);
                await foreach (var item in Output.ReadAllAsync()) {
                    batch.Add(item);
                    if (batch.Count >= batchSize) {
                        return batch;
                    }
                }
                return batch.Count > 0 ? batch : null!;
            }).Where(b => b != null);
        }

        /// <summary>
        /// Performs an action on each item without transforming it
        /// </summary>
        public FlowBuilder<T> Do(
            Action<T> action,
            int maxParallelism = -1) {

            return Transform(item => {
                action(item);
                return item;
            }, maxParallelism);
        }

        /// <summary>
        /// Performs an async action on each item without transforming it
        /// </summary>
        public FlowBuilder<T> Do(
            Func<T, Task> action,
            int maxParallelism = -1) {

            return Transform(async item => {
                await action(item);
                return item;
            }, maxParallelism);
        }

        /// <summary>
        /// Consumes all items with the specified action
        /// </summary>
        public async Task ForEachAsync(
            Action<T> action,
            CancellationToken cancellationToken = default) {

            await ForEachAsync(item => {
                action(item);
                return Task.CompletedTask;
            }, cancellationToken);
        }

        /// <summary>
        /// Consumes all items with the specified async action
        /// </summary>
        public async Task ForEachAsync(
            Func<T, Task> action,
            CancellationToken cancellationToken = default) {

            await foreach (var item in Output.ReadAllAsync(cancellationToken)) {
                await action(item);
            }

            await Completion;
        }

        /// <summary>
        /// Collects all items into a list
        /// </summary>
        public async Task<List<T>> ToListAsync(CancellationToken cancellationToken = default) {
            var result = new List<T>();
            await foreach (var item in Output.ReadAllAsync(cancellationToken)) {
                result.Add(item);
            }
            await Completion;
            return result;
        }

        /// <summary>
        /// Collects all items into an array
        /// </summary>
        public async Task<T[]> ToArrayAsync(CancellationToken cancellationToken = default) {
            var list = await ToListAsync(cancellationToken);
            return list.ToArray();
        }

        /// <summary>
        /// Gets the flow as an async enumerable for custom consumption
        /// </summary>
        public IAsyncEnumerable<T> AsAsyncEnumerable() {
            return Output.ReadAllAsync();
        }

        /// <summary>
        /// Gets an awaiter to materialize all results
        /// </summary>
        public TaskAwaiter<List<T>> GetAwaiter() {
            return ToListAsync().GetAwaiter();
        }

        private async Task LinkToAsync<TIn>(IFlowReceiver<TIn> target)
            where TIn : T {

            try {
                await foreach (var item in Output.ReadAllAsync()) {
                    await target.Input.WriteAsync((TIn)item);
                }
                target.Complete();
            } catch (Exception) {
                target.Complete();
                throw;
            }
        }
    }

    /// <summary>
    /// Static entry point for creating flow pipelines
    /// </summary>
    public static class Flow {
        /// <summary>
        /// Creates a flow from an enumerable
        /// </summary>
        public static FlowBuilder<T> FromEnumerable<T>(
            IEnumerable<T> source,
            int boundedCapacity = -1) {

            var options = new FlowExecutionOptions {
                BoundedCapacity = boundedCapacity
            };

            var sourceBlock = SourceBlock<T>.FromEnumerable(source, options);
            return new FlowBuilder<T>(sourceBlock);
        }

        /// <summary>
        /// Creates a flow from an async enumerable
        /// </summary>
        public static FlowBuilder<T> FromAsyncEnumerable<T>(
            IAsyncEnumerable<T> source,
            int boundedCapacity = -1) {

            var options = new FlowExecutionOptions {
                BoundedCapacity = boundedCapacity
            };

            var sourceBlock = SourceBlock<T>.FromAsyncEnumerable(source, options);
            return new FlowBuilder<T>(sourceBlock);
        }

        /// <summary>
        /// Creates a flow from a generator function
        /// </summary>
        public static FlowBuilder<T> FromGenerator<T>(
            Func<Action<T>, Task> generator,
            int boundedCapacity = -1) {

            var options = new FlowExecutionOptions {
                BoundedCapacity = boundedCapacity
            };

            var sourceBlock = SourceBlock<T>.FromGenerator(generator, options);
            return new FlowBuilder<T>(sourceBlock);
        }

        /// <summary>
        /// Creates a flow from a single value
        /// </summary>
        public static FlowBuilder<T> FromValue<T>(T value) {
            var sourceBlock = SourceBlock<T>.FromValue(value);
            return new FlowBuilder<T>(sourceBlock);
        }

        /// <summary>
        /// Creates an empty flow
        /// </summary>
        public static FlowBuilder<T> Empty<T>() {
            var sourceBlock = SourceBlock<T>.Empty();
            return new FlowBuilder<T>(sourceBlock);
        }
    }
}
