# Figlotech Flow Guide

## Overview

Figlotech Flow is a lightweight, channel-based data flow library for building multi-stage processing pipelines with parallelism, backpressure, and composability.

**Key Features:**
- ✅ Channel-based communication (no manual locking)
- ✅ Built-in backpressure via bounded channels
- ✅ LINQ-like and Rx-like operators
- ✅ Configurable parallelism per stage
- ✅ Proper async/await patterns
- ✅ Cancellation token support
- ✅ Memory efficient streaming

## Quick Start

### Basic Usage

```csharp
using Figlotech.Core.ParallelFlow;

// Simple transformation pipeline
var results = await Fi.Tech
    .FlowFrom(Enumerable.Range(1, 100))
    .Transform(x => x * 2, maxParallelism: 4)
    .Where(x => x > 50)
    .ToListAsync();
```

### Using Fi.Tech.Flow() Entry Point

```csharp
// Longer syntax with Flow() method
var results = await Fi.Tech
    .Flow()
    .FromEnumerable(items)
    .Transform(async x => await ProcessAsync(x))
    .ToListAsync();
```

## Core Concepts

### Source Blocks

Source blocks produce data:

```csharp
// From enumerable
var flow = Fi.Tech.FlowFrom(new[] { 1, 2, 3, 4, 5 });

// From async enumerable
var flow = Fi.Tech.FlowFrom(GetItemsAsync());

// From generator
var flow = Fi.Tech.FlowGenerate<int>(yield => {
    for (int i = 0; i < 100; i++) {
        yield(i);
    }
});

// From async generator
var flow = Fi.Tech.FlowGenerate<int>(async yield => {
    for (int i = 0; i < 100; i++) {
        yield(await GetItemAsync(i));
    }
});
```

### Transform Blocks

Transform blocks process data with configurable parallelism:

```csharp
// 1-to-1 transformation
flow.Transform(x => x * 2, maxParallelism: 4)

// Async transformation
flow.Transform(async x => await ProcessAsync(x), maxParallelism: 8)

// 1-to-many transformation (SelectMany)
flow.SelectMany(x => Enumerable.Range(0, x))

// Filtering
flow.Where(x => x > 10)
```

### Terminal Operations

Terminal operations consume the pipeline:

```csharp
// Collect to list
var list = await flow.ToListAsync();

// Collect to array
var array = await flow.ToArrayAsync();

// Process each item
await flow.ForEachAsync(x => Console.WriteLine(x));

// Async processing
await flow.ForEachAsync(async x => await SaveAsync(x));

// Custom consumption
await foreach (var item in flow.AsAsyncEnumerable()) {
    // Custom logic
}
```

## Advanced Patterns

### Backpressure Control

```csharp
// Bounded channel with capacity limit
var flow = Fi.Tech
    .FlowFrom(largeDataset)
    .Transform(
        async x => await ExpensiveOperation(x),
        maxParallelism: 4,
        boundedCapacity: 100  // Max 100 items buffered
    )
    .ToListAsync();
```

### Error Handling

```csharp
// With error handler
var options = new FlowExecutionOptions {
    MaxDegreeOfParallelism = 4,
    PropagateExceptions = false,
    ErrorHandler = async ex => {
        await LogErrorAsync(ex);
    }
};

// Custom error handling per stage
await Fi.Tech
    .FlowFrom(items)
    .TransformWithRetry(
        async x => await UnreliableOperationAsync(x),
        maxRetries: 3,
        initialDelay: TimeSpan.FromMilliseconds(100)
    )
    .ToListAsync();
```

### Cancellation

```csharp
using var cts = new CancellationTokenSource();
cts.CancelAfter(TimeSpan.FromSeconds(30));

try {
    var results = await Fi.Tech
        .FlowFrom(items)
        .Transform(async x => await ProcessAsync(x))
        .ToListAsync(cts.Token);
} catch (OperationCanceledException) {
    // Handle cancellation
}
```

### Batching

```csharp
// Simple batching
await Fi.Tech
    .FlowFrom(items)
    .Buffer(batchSize: 100)
    .Transform(async batch => await SaveBatchAsync(batch))
    .ToListAsync();

// Batching with timeout
await Fi.Tech
    .FlowFrom(items)
    .BatchWithTimeout(batchSize: 100, timeout: TimeSpan.FromSeconds(5))
    .Transform(async batch => await SaveBatchAsync(batch))
    .ToListAsync();
```

### Time-Based Operations

```csharp
// Throttle (rate limiting)
await Fi.Tech
    .FlowFrom(events)
    .Throttle(TimeSpan.FromSeconds(1))  // Max 1 per second
    .ForEachAsync(e => ProcessEvent(e));

// Debounce (ignore rapid changes)
await Fi.Tech
    .FlowFrom(userInputs)
    .Debounce(TimeSpan.FromMilliseconds(300))
    .ForEachAsync(input => HandleInput(input));

// Time windows
await Fi.Tech
    .FlowFrom(metrics)
    .Window(TimeSpan.FromMinutes(1))
    .Transform(window => CalculateStats(window))
    .ForEachAsync(stats => LogStats(stats));
```

### Rx-Style Operators

```csharp
// Take/Skip
flow.Take(10).Skip(5)

// TakeWhile/SkipWhile
flow.TakeWhile(x => x < 100)
    .SkipWhile(x => x < 10)

// Distinct
flow.Distinct()
flow.DistinctBy(x => x.Id)

// Pairwise (sliding window)
flow.Pairwise()  // Returns (previous, current) tuples

// WithIndex
flow.WithIndex()  // Returns (item, index) tuples

// Tap (side effects)
flow.Tap(
    x => Console.WriteLine($"Processing: {x}"),
    ex => Console.WriteLine($"Error: {ex}")
)
```

## Real-World Examples

### ETL Pipeline

```csharp
await Fi.Tech
    .FlowFrom(GetSourceRecords())
    .Transform(async record => await ValidateAsync(record), maxParallelism: 4)
    .Where(record => record.IsValid)
    .Transform(record => TransformToTarget(record))
    .Buffer(1000)
    .Transform(async batch => {
        await BulkInsertAsync(batch);
        return batch.Count;
    })
    .ForEachAsync(count => Console.WriteLine($"Inserted {count} records"));
```

### API Request Processing

```csharp
await Fi.Tech
    .FlowFrom(apiRequests)
    .Throttle(TimeSpan.FromMilliseconds(100))  // Rate limit
    .TransformWithRetry(
        async req => await CallExternalApiAsync(req),
        maxRetries: 3
    )
    .Transform(response => ParseResponse(response), maxParallelism: 8)
    .ForEachAsync(async data => await SaveAsync(data));
```

### File Processing

```csharp
await Fi.Tech
    .FlowFrom(Directory.EnumerateFiles(inputPath))
    .Transform(async file => await File.ReadAllLinesAsync(file), maxParallelism: 4)
    .SelectMany(lines => lines)
    .Where(line => !string.IsNullOrWhiteSpace(line))
    .Transform(line => ProcessLine(line), maxParallelism: 16)
    .Buffer(10000)
    .Transform(async batch => {
        await File.AppendAllLinesAsync(outputPath, batch);
        return batch.Count;
    })
    .ToListAsync();
```

### Real-Time Event Processing

```csharp
await Fi.Tech
    .FlowFrom(eventStream)
    .Window(TimeSpan.FromSeconds(10))
    .Transform(window => AggregateEvents(window))
    .Where(aggregated => aggregated.Count > threshold)
    .Transform(async agg => await NotifyAsync(agg))
    .ForEachAsync(result => LogResult(result));
```

## Migration from Old ParallelFlow

### Old API

```csharp
var result = await Fi.Tech.ParallelFlow(items)
    .Then(async x => await ProcessAsync(x))
    .Then(x => x * 2)
    .Then(async x => await SaveAsync(x));
```

### New API

```csharp
var result = await Fi.Tech
    .FlowFrom(items)
    .Transform(async x => await ProcessAsync(x))
    .Transform(x => x * 2)
    .Do(async x => await SaveAsync(x))
    .ToListAsync();
```

### Key Differences

| Old API | New API | Notes |
|---------|---------|-------|
| `ParallelFlow(items)` | `FlowFrom(items)` | Clearer intent |
| `Then(fn)` | `Transform(fn)` | LINQ-style naming |
| `Then(action)` | `Do(action)` | Distinguish side effects |
| `FlowYield` | `SelectMany` | Standard LINQ pattern |
| Manual `NotifyDoneQueueing()` | Automatic | Simpler lifecycle |
| Queue-based | Channel-based | Better performance |
| Manual locking | Lock-free | Thread-safe by design |

## Performance Tips

1. **Set appropriate parallelism**: Don't use more workers than needed
   ```csharp
   .Transform(fn, maxParallelism: 4)  // Good for CPU-bound
   .Transform(fn, maxParallelism: 32) // Good for I/O-bound
   ```

2. **Use bounded channels for large datasets**: Prevent memory bloat
   ```csharp
   .FlowFrom(items, boundedCapacity: 1000)
   ```

3. **Batch operations when possible**: Reduce overhead
   ```csharp
   .Buffer(100).Transform(batch => ProcessBatch(batch))
   ```

4. **Avoid ordered processing unless needed**: It limits parallelism
   ```csharp
   // Don't set EnsureOrdered = true unless order matters
   ```

5. **Use appropriate operators**: `Do()` for side effects, `Transform()` for transformations

## Best Practices

1. **Always handle errors**: Use error handlers or try-catch
2. **Use cancellation tokens**: For long-running operations
3. **Profile before optimizing**: Measure actual bottlenecks
4. **Keep transforms small**: One responsibility per stage
5. **Use descriptive variable names**: Pipelines can get complex
6. **Test with small datasets first**: Debug before scaling

## Troubleshooting

### Pipeline Not Starting

**Problem**: Nothing happens when creating a flow

**Solution**: Flows are lazy - they only execute when consumed:
```csharp
// This does nothing:
var flow = Fi.Tech.FlowFrom(items).Transform(x => x * 2);

// This executes:
await flow.ToListAsync();
```

### Memory Issues

**Problem**: OutOfMemoryException with large datasets

**Solution**: Use bounded channels:
```csharp
.FlowFrom(items, boundedCapacity: 1000)
.Transform(fn, boundedCapacity: 100)
```

### Slow Performance

**Problem**: Pipeline is slower than expected

**Solution**: Profile and adjust parallelism:
```csharp
// Too low - not using available resources
.Transform(fn, maxParallelism: 1)

// Too high - context switching overhead
.Transform(fn, maxParallelism: 1000)

// Just right - match workload type
.Transform(fn, maxParallelism: Environment.ProcessorCount)
```

## API Reference

See inline XML documentation in the code for complete API reference.

## Contributing

When adding new operators:
1. Follow LINQ/Rx naming conventions
2. Support both sync and async variants
3. Add XML documentation
4. Include examples in this guide
5. Consider backpressure implications
