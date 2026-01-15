using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Figlotech.Core.ParallelFlow {

    /// <summary>
    /// Example usage patterns for Figlotech Flow
    /// </summary>
    public static class FlowExamples {

        /// <summary>
        /// Basic LINQ-style transformation
        /// </summary>
        public static async Task<List<int>> BasicTransformation() {
            return await Fi.Tech
                .FlowFrom(Enumerable.Range(1, 100))
                .Transform(x => x * 2, maxParallelism: 4)
                .Where(x => x > 50)
                .Transform(x => x + 10)
                .ToListAsync();
        }

        /// <summary>
        /// Async I/O operations with parallelism
        /// </summary>
        public static async Task AsyncIOExample() {
            var urls = new[] {
                "https://api.example.com/data/1",
                "https://api.example.com/data/2",
                "https://api.example.com/data/3"
            };

            await Fi.Tech
                .FlowFrom(urls)
                .Transform(async url => {
                    // Simulated HTTP call
                    await Task.Delay(100);
                    return $"Response from {url}";
                }, maxParallelism: 10)
                .ForEachAsync(response => Console.WriteLine(response));
        }

        /// <summary>
        /// Batching for efficient database operations
        /// </summary>
        public static async Task BatchingExample() {
            var records = Enumerable.Range(1, 10000)
                .Select(i => new { Id = i, Name = $"Record {i}" });

            await Fi.Tech
                .FlowFrom(records)
                .Buffer(batchSize: 100)
                .Transform(async batch => {
                    // Simulated batch insert
                    await Task.Delay(10);
                    Console.WriteLine($"Inserted batch of {batch.Count} records");
                    return batch.Count;
                })
                .ToListAsync();
        }

        /// <summary>
        /// Error handling with retry
        /// </summary>
        public static async Task ErrorHandlingExample() {
            var items = Enumerable.Range(1, 10);

            await Fi.Tech
                .FlowFrom(items)
                .TransformWithRetry(
                    async item => {
                        // Simulated unreliable operation
                        if (new Random().Next(10) < 3) {
                            throw new Exception($"Transient error for item {item}");
                        }
                        await Task.Delay(10);
                        return item * 2;
                    },
                    maxRetries: 3,
                    initialDelay: TimeSpan.FromMilliseconds(100)
                )
                .Tap(
                    item => Console.WriteLine($"Processed: {item}"),
                    error => Console.WriteLine($"Error: {error.Message}")
                )
                .ToListAsync();
        }

        /// <summary>
        /// Rate-limited API calls
        /// </summary>
        public static async Task ThrottlingExample() {
            var requests = Enumerable.Range(1, 50);

            await Fi.Tech
                .FlowFrom(requests)
                .Throttle(TimeSpan.FromMilliseconds(100)) // Max 10 per second
                .Transform(async req => {
                    Console.WriteLine($"Processing request {req} at {DateTime.Now:HH:mm:ss.fff}");
                    await Task.Delay(10);
                    return req;
                })
                .ToListAsync();
        }

        /// <summary>
        /// Time-windowed aggregation
        /// </summary>
        public static async Task WindowAggregationExample() {
            var events = Enumerable.Range(1, 100)
                .Select(i => new { Timestamp = DateTime.UtcNow, Value = i });

            await Fi.Tech
                .FlowFrom(events)
                .Window(TimeSpan.FromSeconds(5))
                .Transform(window => new {
                    Count = window.Count,
                    Sum = window.Sum(e => e.Value),
                    Average = window.Average(e => e.Value)
                })
                .ForEachAsync(stats => {
                    Console.WriteLine($"Window: Count={stats.Count}, Sum={stats.Sum}, Avg={stats.Average}");
                });
        }

        /// <summary>
        /// Generator-based source
        /// </summary>
        public static async Task GeneratorExample() {
            await Fi.Tech
                .FlowGenerate<int>(yield => {
                    for (int i = 1; i <= 10; i++) {
                        yield(i);
                        yield(i * 10);
                        yield(i * 100);
                    }
                })
                .Where(x => x > 50)
                .Transform(x => x.ToString())
                .ForEachAsync(x => Console.WriteLine(x));
        }

        /// <summary>
        /// Complex ETL pipeline
        /// </summary>
        public static async Task ETLExample() {
            var sourceData = Enumerable.Range(1, 1000)
                .Select(i => new SourceRecord {
                    Id = i,
                    Name = $"Record {i}",
                    Value = i * 10,
                    IsActive = i % 2 == 0
                });

            var results = await Fi.Tech
                .FlowFrom(sourceData)
                // Extract & Validate
                .Transform(record => {
                    record.IsValid = record.Value > 0 && !string.IsNullOrEmpty(record.Name);
                    return record;
                }, maxParallelism: 4)
                // Filter invalid
                .Where(record => record.IsValid && record.IsActive)
                // Transform
                .Transform(source => new TargetRecord {
                    Id = source.Id,
                    DisplayName = source.Name.ToUpper(),
                    Amount = source.Value * 1.1m
                })
                // Add index for logging
                .WithIndex()
                // Batch for efficient loading
                .Buffer(100)
                // Load
                .Transform(async batch => {
                    // Simulated database insert
                    await Task.Delay(50);
                    var firstIndex = batch.First().Index;
                    Console.WriteLine($"Loaded batch starting at index {firstIndex} with {batch.Count} records");
                    return batch.Count;
                }, maxParallelism: 2)
                .ToListAsync();

            Console.WriteLine($"Total records processed: {results.Sum()}");
        }

        /// <summary>
        /// Distinct and deduplication
        /// </summary>
        public static async Task DistinctExample() {
            var items = new[] { 1, 2, 3, 2, 4, 3, 5, 1, 6 };

            var distinct = await Fi.Tech
                .FlowFrom(items)
                .Distinct()
                .ToListAsync();

            Console.WriteLine($"Original: {string.Join(", ", items)}");
            Console.WriteLine($"Distinct: {string.Join(", ", distinct)}");
        }

        /// <summary>
        /// Pairwise comparison
        /// </summary>
        public static async Task PairwiseExample() {
            var values = new[] { 1, 3, 2, 5, 4, 8 };

            await Fi.Tech
                .FlowFrom(values)
                .Pairwise()
                .Transform(pair => new {
                    Previous = pair.Previous,
                    Current = pair.Current,
                    Difference = pair.Current - pair.Previous
                })
                .ForEachAsync(result => {
                    Console.WriteLine($"{result.Previous} -> {result.Current} (diff: {result.Difference})");
                });
        }

        /// <summary>
        /// SelectMany for flattening
        /// </summary>
        public static async Task SelectManyExample() {
            var groups = new[] {
                new { GroupId = 1, Items = new[] { "A", "B", "C" } },
                new { GroupId = 2, Items = new[] { "D", "E" } },
                new { GroupId = 3, Items = new[] { "F", "G", "H", "I" } }
            };

            await Fi.Tech
                .FlowFrom(groups)
                .SelectMany(group => group.Items.Select(item => $"{group.GroupId}-{item}"))
                .ForEachAsync(result => Console.WriteLine(result));
        }

        // Example domain objects
        private class SourceRecord {
            public int Id { get; set; }
            public string Name { get; set; } = string.Empty;
            public int Value { get; set; }
            public bool IsActive { get; set; }
            public bool IsValid { get; set; }
        }

        private class TargetRecord {
            public int Id { get; set; }
            public string DisplayName { get; set; } = string.Empty;
            public decimal Amount { get; set; }
        }
    }
}
