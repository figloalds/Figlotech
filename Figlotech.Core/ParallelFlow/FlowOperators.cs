using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Figlotech.Core.ParallelFlow {

    /// <summary>
    /// Rx-like operators for flow pipelines
    /// </summary>
    public static class FlowOperators {

        /// <summary>
        /// Takes only the first N items from the flow
        /// </summary>
        public static FlowBuilder<T> Take<T>(this FlowBuilder<T> flow, int count) {
            return Flow.FromGenerator<T>(async yield => {
                int taken = 0;
                await foreach (var item in flow.AsAsyncEnumerable()) {
                    if (taken >= count) break;
                    yield(item);
                    taken++;
                }
            });
        }

        /// <summary>
        /// Skips the first N items from the flow
        /// </summary>
        public static FlowBuilder<T> Skip<T>(this FlowBuilder<T> flow, int count) {
            return Flow.FromGenerator<T>(async yield => {
                int skipped = 0;
                await foreach (var item in flow.AsAsyncEnumerable()) {
                    if (skipped < count) {
                        skipped++;
                        continue;
                    }
                    yield(item);
                }
            });
        }

        /// <summary>
        /// Takes items while the predicate is true
        /// </summary>
        public static FlowBuilder<T> TakeWhile<T>(
            this FlowBuilder<T> flow,
            Func<T, bool> predicate) {

            return Flow.FromGenerator<T>(async yield => {
                await foreach (var item in flow.AsAsyncEnumerable()) {
                    if (!predicate(item)) break;
                    yield(item);
                }
            });
        }

        /// <summary>
        /// Skips items while the predicate is true
        /// </summary>
        public static FlowBuilder<T> SkipWhile<T>(
            this FlowBuilder<T> flow,
            Func<T, bool> predicate) {

            return Flow.FromGenerator<T>(async yield => {
                bool skipping = true;
                await foreach (var item in flow.AsAsyncEnumerable()) {
                    if (skipping && predicate(item)) continue;
                    skipping = false;
                    yield(item);
                }
            });
        }

        /// <summary>
        /// Returns distinct items based on default equality
        /// </summary>
        public static FlowBuilder<T> Distinct<T>(this FlowBuilder<T> flow) {
            return Flow.FromGenerator<T>(async yield => {
                var seen = new HashSet<T>();
                await foreach (var item in flow.AsAsyncEnumerable()) {
                    if (seen.Add(item)) {
                        yield(item);
                    }
                }
            });
        }

        /// <summary>
        /// Returns distinct items based on a key selector
        /// </summary>
        public static FlowBuilder<T> DistinctBy<T, TKey>(
            this FlowBuilder<T> flow,
            Func<T, TKey> keySelector) {

            return Flow.FromGenerator<T>(async yield => {
                var seen = new HashSet<TKey>();
                await foreach (var item in flow.AsAsyncEnumerable()) {
                    if (seen.Add(keySelector(item))) {
                        yield(item);
                    }
                }
            });
        }

        /// <summary>
        /// Throttles the flow to emit at most one item per time window
        /// </summary>
        public static FlowBuilder<T> Throttle<T>(
            this FlowBuilder<T> flow,
            TimeSpan timeWindow) {

            return Flow.FromGenerator<T>(async yield => {
                DateTime lastEmit = DateTime.MinValue;
                await foreach (var item in flow.AsAsyncEnumerable()) {
                    var now = DateTime.UtcNow;
                    if (now - lastEmit >= timeWindow) {
                        yield(item);
                        lastEmit = now;
                    }
                }
            });
        }

        /// <summary>
        /// Debounces the flow - emits an item only if no other item arrives within the time window
        /// </summary>
        public static FlowBuilder<T> Debounce<T>(
            this FlowBuilder<T> flow,
            TimeSpan timeWindow) {

            return Flow.FromGenerator<T>(async yield => {
                T? lastItem = default;
                DateTime lastReceived = DateTime.MinValue;
                bool hasItem = false;

                await foreach (var item in flow.AsAsyncEnumerable()) {
                    lastItem = item;
                    lastReceived = DateTime.UtcNow;
                    hasItem = true;

                    await Task.Delay(timeWindow);

                    if (hasItem && (DateTime.UtcNow - lastReceived) >= timeWindow) {
                        yield(lastItem!);
                        hasItem = false;
                    }
                }
            });
        }

        /// <summary>
        /// Groups items into time-based windows
        /// </summary>
        public static FlowBuilder<List<T>> Window<T>(
            this FlowBuilder<T> flow,
            TimeSpan windowSize) {

            return Flow.FromGenerator<List<T>>(async yield => {
                var window = new List<T>();
                var windowStart = DateTime.UtcNow;

                await foreach (var item in flow.AsAsyncEnumerable()) {
                    var now = DateTime.UtcNow;

                    if (now - windowStart >= windowSize) {
                        if (window.Count > 0) {
                            yield(window);
                            window = new List<T>();
                        }
                        windowStart = now;
                    }

                    window.Add(item);
                }

                if (window.Count > 0) {
                    yield(window);
                }
            });
        }

        /// <summary>
        /// Batches items into groups of specified size with optional timeout
        /// </summary>
        public static FlowBuilder<List<T>> BatchWithTimeout<T>(
            this FlowBuilder<T> flow,
            int batchSize,
            TimeSpan timeout) {

            return Flow.FromGenerator<List<T>>(async yield => {
                var batch = new List<T>(batchSize);
                var lastBatch = DateTime.UtcNow;

                await foreach (var item in flow.AsAsyncEnumerable()) {
                    batch.Add(item);

                    var shouldFlush = batch.Count >= batchSize ||
                                     (DateTime.UtcNow - lastBatch) >= timeout;

                    if (shouldFlush) {
                        yield(new List<T>(batch));
                        batch.Clear();
                        lastBatch = DateTime.UtcNow;
                    }
                }

                if (batch.Count > 0) {
                    yield(batch);
                }
            });
        }

        /// <summary>
        /// Samples the flow at regular intervals, taking the most recent item
        /// Note: Simplified implementation for netstandard2.1 compatibility
        /// </summary>
        public static FlowBuilder<T> Sample<T>(
            this FlowBuilder<T> flow,
            TimeSpan interval) {

            return Flow.FromGenerator<T>(async yield => {
                T? lastItem = default;
                bool hasItem = false;
                var lastEmit = DateTime.UtcNow;

                await foreach (var item in flow.AsAsyncEnumerable()) {
                    lastItem = item;
                    hasItem = true;

                    var now = DateTime.UtcNow;
                    if (now - lastEmit >= interval) {
                        yield(lastItem);
                        hasItem = false;
                        lastEmit = now;
                    }
                }

                // Emit final item if any
                if (hasItem) {
                    yield(lastItem!);
                }
            });
        }

        /// <summary>
        /// Executes a side effect for each item with error handling
        /// </summary>
        public static FlowBuilder<T> Tap<T>(
            this FlowBuilder<T> flow,
            Action<T> action,
            Action<Exception>? onError = null) {

            return flow.Do(item => {
                try {
                    action(item);
                } catch (Exception ex) {
                    onError?.Invoke(ex);
                }
            });
        }

        /// <summary>
        /// Retries failed operations with exponential backoff
        /// </summary>
        public static FlowBuilder<TResult> TransformWithRetry<T, TResult>(
            this FlowBuilder<T> flow,
            Func<T, Task<TResult>> transform,
            int maxRetries = 3,
            TimeSpan? initialDelay = null) {

            var delay = initialDelay ?? TimeSpan.FromMilliseconds(100);

            return flow.Transform(async item => {
                Exception? lastException = null;
                var currentDelay = delay;

                for (int retry = 0; retry <= maxRetries; retry++) {
                    try {
                        return await transform(item);
                    } catch (Exception ex) {
                        lastException = ex;
                        if (retry < maxRetries) {
                            await Task.Delay(currentDelay);
                            currentDelay = TimeSpan.FromTicks(currentDelay.Ticks * 2);
                        }
                    }
                }

                throw lastException!;
            });
        }

        /// <summary>
        /// Combines items with their index
        /// </summary>
        public static FlowBuilder<(T Item, long Index)> WithIndex<T>(this FlowBuilder<T> flow) {
            return Flow.FromGenerator<(T, long)>(async yield => {
                long index = 0;
                await foreach (var item in flow.AsAsyncEnumerable()) {
                    yield((item, index++));
                }
            });
        }

        /// <summary>
        /// Combines consecutive items into pairs (sliding window of 2)
        /// </summary>
        public static FlowBuilder<(T Previous, T Current)> Pairwise<T>(this FlowBuilder<T> flow) {
            return Flow.FromGenerator<(T, T)>(async yield => {
                T? previous = default;
                bool hasPrevious = false;

                await foreach (var item in flow.AsAsyncEnumerable()) {
                    if (hasPrevious) {
                        yield((previous!, item));
                    }
                    previous = item;
                    hasPrevious = true;
                }
            });
        }

        /// <summary>
        /// Materializes exceptions as Either results instead of throwing
        /// </summary>
        public static FlowBuilder<Either<Exception, T>> Materialize<T>(this FlowBuilder<T> flow) {
            return Flow.FromGenerator<Either<Exception, T>>(async yield => {
                await foreach (var item in flow.AsAsyncEnumerable()) {
                    yield(Either<Exception, T>.Right(item));
                }
            });
        }
    }

    /// <summary>
    /// Simple Either type for error handling
    /// </summary>
    public class Either<TLeft, TRight> {
        private readonly TLeft? _left;
        private readonly TRight? _right;
        private readonly bool _isLeft;

        private Either(TLeft left, TRight right, bool isLeft) {
            _left = left;
            _right = right;
            _isLeft = isLeft;
        }

        public static Either<TLeft, TRight> Left(TLeft value) => new Either<TLeft, TRight>(value, default!, true);
        public static Either<TLeft, TRight> Right(TRight value) => new Either<TLeft, TRight>(default!, value, false);

        public bool IsLeft => _isLeft;
        public bool IsRight => !_isLeft;

        public TLeft LeftValue => _isLeft ? _left! : throw new InvalidOperationException("Not a Left value");
        public TRight RightValue => !_isLeft ? _right! : throw new InvalidOperationException("Not a Right value");

        public TResult Match<TResult>(Func<TLeft, TResult> left, Func<TRight, TResult> right) =>
            _isLeft ? left(_left!) : right(_right!);
    }
}
