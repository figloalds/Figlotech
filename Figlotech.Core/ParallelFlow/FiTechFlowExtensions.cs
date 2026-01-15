using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.Core.ParallelFlow {

    /// <summary>
    /// Extension methods to integrate Flow API with Fi.Tech pattern
    /// </summary>
    public static class FiTechFlowExtensions {

        /// <summary>
        /// Entry point for Flow API via Fi.Tech pattern
        /// Usage: Fi.Tech.Flow().FromEnumerable(items)
        /// </summary>
        public static FlowStarter Flow(this Fi _selfie) {
            return new FlowStarter();
        }

        /// <summary>
        /// Convenience method: Fi.Tech.FlowFrom(items)
        /// </summary>
        public static FlowBuilder<T> FlowFrom<T>(
            this Fi _selfie,
            IEnumerable<T> source,
            int maxParallelism = -1,
            int boundedCapacity = -1) {

            return ParallelFlow.Flow.FromEnumerable(source, boundedCapacity);
        }

        /// <summary>
        /// Convenience method: Fi.Tech.FlowFrom(asyncItems)
        /// </summary>
        public static FlowBuilder<T> FlowFrom<T>(
            this Fi _selfie,
            IAsyncEnumerable<T> source,
            int boundedCapacity = -1) {

            return ParallelFlow.Flow.FromAsyncEnumerable(source, boundedCapacity);
        }

        /// <summary>
        /// Convenience method: Fi.Tech.FlowGenerate(generator)
        /// </summary>
        public static FlowBuilder<T> FlowGenerate<T>(
            this Fi _selfie,
            Func<Action<T>, Task> generator,
            int boundedCapacity = -1) {

            return ParallelFlow.Flow.FromGenerator(generator, boundedCapacity);
        }

        /// <summary>
        /// Convenience method: Fi.Tech.FlowGenerate(syncGenerator)
        /// </summary>
        public static FlowBuilder<T> FlowGenerate<T>(
            this Fi _selfie,
            Action<Action<T>> generator,
            int boundedCapacity = -1) {

            return ParallelFlow.Flow.FromGenerator<T>(
                yield => {
                    generator(yield);
                    return Task.CompletedTask;
                },
                boundedCapacity);
        }
    }

    /// <summary>
    /// Flow starter for fluent API from Fi.Tech
    /// Allows: Fi.Tech.Flow().FromEnumerable(items)
    /// </summary>
    public sealed class FlowStarter {
        /// <summary>
        /// Creates a flow from an enumerable
        /// </summary>
        public FlowBuilder<T> FromEnumerable<T>(
            IEnumerable<T> source,
            int boundedCapacity = -1) {

            return ParallelFlow.Flow.FromEnumerable(source, boundedCapacity);
        }

        /// <summary>
        /// Creates a flow from an async enumerable
        /// </summary>
        public FlowBuilder<T> FromAsyncEnumerable<T>(
            IAsyncEnumerable<T> source,
            int boundedCapacity = -1) {

            return ParallelFlow.Flow.FromAsyncEnumerable(source, boundedCapacity);
        }

        /// <summary>
        /// Creates a flow from a generator function
        /// </summary>
        public FlowBuilder<T> FromGenerator<T>(
            Func<Action<T>, Task> generator,
            int boundedCapacity = -1) {

            return ParallelFlow.Flow.FromGenerator(generator, boundedCapacity);
        }

        /// <summary>
        /// Creates a flow from a synchronous generator function
        /// </summary>
        public FlowBuilder<T> FromGenerator<T>(
            Action<Action<T>> generator,
            int boundedCapacity = -1) {

            return ParallelFlow.Flow.FromGenerator<T>(
                yield => {
                    generator(yield);
                    return Task.CompletedTask;
                },
                boundedCapacity);
        }

        /// <summary>
        /// Creates a flow from a single value
        /// </summary>
        public FlowBuilder<T> FromValue<T>(T value) {
            return ParallelFlow.Flow.FromValue(value);
        }

        /// <summary>
        /// Creates an empty flow
        /// </summary>
        public FlowBuilder<T> Empty<T>() {
            return ParallelFlow.Flow.Empty<T>();
        }
    }
}
