# Figlotech ParallelFlow - Channel-Based Redesign

## Overview

The ParallelFlow library has been completely redesigned to use **System.Threading.Channels** for inter-block communication, providing a modern, efficient, and thread-safe data flow pipeline framework.

## What Changed

### Architecture
- **Old**: Queue-based with manual locking
- **New**: Channel-based, lock-free design

### Key Improvements
1. ✅ **Thread-Safe by Design**: No more manual locks
2. ✅ **Built-in Backpressure**: Bounded channels prevent memory bloat
3. ✅ **Cancellation Support**: CancellationToken throughout
4. ✅ **Better Performance**: Optimized async patterns
5. ✅ **LINQ + Rx Style API**: Familiar, composable operators
6. ✅ **Proper Error Handling**: Configurable error propagation

## New Files

### Core Infrastructure
- `IFlowBlock.cs` - Core interfaces and execution options
- `SourceBlock.cs` - Data source blocks
- `TransformBlock.cs` - Transformation blocks (1-to-1 and 1-to-many)
- `ActionBlock.cs` - Terminal consumer blocks

### API & Extensions
- `FlowBuilder.cs` - Fluent LINQ-style API
- `FiTechFlowExtensions.cs` - Fi.Tech integration
- `FlowOperators.cs` - Rx-like operators (Throttle, Debounce, Buffer, etc.)
- `AsyncEnumerableExtensions.cs` - Helper extensions

### Documentation & Examples
- `FLOW_GUIDE.md` - Comprehensive user guide
- `Examples.cs` - Practical code examples
- `README.md` - This file

## Quick Start

```csharp
using Figlotech.Core.ParallelFlow;

// Simple transformation pipeline
var results = await Fi.Tech
    .FlowFrom(Enumerable.Range(1, 100))
    .Transform(x => x * 2, maxParallelism: 4)
    .Where(x => x > 50)
    .ToListAsync();

// With backpressure control
var results = await Fi.Tech
    .FlowFrom(largeDataset, boundedCapacity: 1000)
    .Transform(async x => await ProcessAsync(x),
               maxParallelism: 8,
               boundedCapacity: 100)
    .Buffer(100)
    .ForEachAsync(batch => SaveBatchAsync(batch));
```

## API Patterns

### Entry Points

```csharp
// Via Fi.Tech extension (recommended)
Fi.Tech.FlowFrom(items)
Fi.Tech.Flow().FromEnumerable(items)
Fi.Tech.FlowGenerate(yield => { ... })

// Direct static access
Flow.FromEnumerable(items)
Flow.FromAsyncEnumerable(asyncItems)
Flow.FromGenerator(async yield => { ... })
```

### Transformation Operators

```csharp
.Transform(x => x * 2)                    // 1-to-1 transformation
.SelectMany(x => ExpandToMany(x))         // 1-to-many transformation
.Where(x => x > 10)                       // Filtering
.Do(x => Console.WriteLine(x))            // Side effects
```

### Rx-Style Operators

```csharp
.Take(10)                                 // Take first N
.Skip(5)                                  // Skip first N
.Distinct()                               // Remove duplicates
.Buffer(100)                              // Batch into lists
.Throttle(TimeSpan.FromSeconds(1))       // Rate limiting
.Debounce(TimeSpan.FromMilliseconds(300))// Debouncing
.Window(TimeSpan.FromMinutes(1))         // Time windows
.WithIndex()                              // Add index
.Pairwise()                               // Sliding window of 2
```

### Terminal Operations

```csharp
.ToListAsync()                            // Collect to list
.ToArrayAsync()                           // Collect to array
.ForEachAsync(x => Process(x))           // Process each item
.AsAsyncEnumerable()                     // Get IAsyncEnumerable
await flow                                // Implicit ToListAsync()
```

## Configuration

### Parallelism

```csharp
.Transform(fn, maxParallelism: 4)         // 4 concurrent workers
.Transform(fn, maxParallelism: -1)        // Auto (CPU count)
```

### Backpressure

```csharp
FlowFrom(items, boundedCapacity: 1000)    // Max 1000 items buffered
.Transform(fn, boundedCapacity: 100)      // Max 100 between stages
```

### Error Handling

```csharp
var options = new FlowExecutionOptions {
    PropagateExceptions = false,
    ErrorHandler = async ex => await LogAsync(ex)
};

// Or use retry operator
.TransformWithRetry(fn, maxRetries: 3)
```

## Migration from Old API

| Old API | New API |
|---------|---------|
| `Fi.Tech.ParallelFlow(items)` | `Fi.Tech.FlowFrom(items)` |
| `.Then(fn)` | `.Transform(fn)` |
| `.Then(action)` | `.Do(action)` |
| `FlowYield` | `.SelectMany(...)` |
| Manual `NotifyDoneQueueing()` | Automatic |

## Performance Tips

1. **Set appropriate parallelism** for your workload type
   - CPU-bound: `maxParallelism: 4-8`
   - I/O-bound: `maxParallelism: 16-32`

2. **Use bounded channels** for large datasets
   - Prevents OutOfMemoryException
   - `boundedCapacity: 1000` is a good default

3. **Batch operations** where possible
   - `.Buffer(100)` reduces overhead

4. **Profile before optimizing**
   - Measure actual bottlenecks

## Integration with Existing Code

The old `ParallelFlowStepInOut` API remains functional for backward compatibility. New code should use the channel-based API:

```csharp
// Old (still works)
var result = await Fi.Tech.ParallelFlow(items)
    .Then(async x => await ProcessAsync(x));

// New (recommended)
var result = await Fi.Tech
    .FlowFrom(items)
    .Transform(async x => await ProcessAsync(x))
    .ToListAsync();
```

## See Also

- `FLOW_GUIDE.md` - Detailed user guide with many examples
- `Examples.cs` - Runnable code examples
- `AsyncEnumerableWeaver.cs` - Utility for merging sorted streams

## Future Enhancements

Potential additions:
- Broadcast/multicast blocks
- Join/merge operations
- Conditional branching
- Circuit breaker pattern
- Metrics and telemetry hooks
- Custom block implementations

## Credits

Redesigned using modern .NET async patterns and TPL Dataflow concepts, adapted for the Fi.Tech ecosystem.
