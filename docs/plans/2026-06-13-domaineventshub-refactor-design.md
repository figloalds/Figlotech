# DomainEventsHub Refactor Design

## Objective
Improve performance, memory efficiency, and lifecycle cleanliness of `Figlotech.Core.DomainEvents.DomainEventsHub` while simplifying its public API.

## Motivation
Current implementation has several hotspots:

- `Raise` allocates `EventRaiseResponse`, `List<TaskCompletionSource<int>>`, `WorkJob`, and multiple closures for every event.
- Listener dispatch scans the entire flat `_listeners` array and calls `CanHandle` for every listener on every event (O(total listeners) per event).
- `Environment.StackTrace` is captured when `StdoutEventHubLogs` is enabled, which is extremely expensive.
- `InlineLambdaListener` and `DomainEventListener<T>` use `async`/`await` wrappers that allocate state machines even for synchronous handlers.
- `Extensions` uses a non-thread-safe `List<IExtension>` that can corrupt under concurrent registration.
- `EventRaiseResponse` exposes a mutable list and does not provide an ergonomic wait primitive.

## Proposed Public API

```csharp
public sealed class DomainEventsHub {
    public static DomainEventsHub Global { get; }
    public DomainEventsHub(WorkQueuer queuer = null, DomainEventsHub parentHub = null);

    // Fire-and-forget. Returns synchronously after enqueueing.
    public void Raise(IDomainEvent domainEvent);
    public void Raise<T>(IEnumerable<T> domainEvents) where T : IDomainEvent;

    // Waits for all matching handlers to complete.
    public ValueTask RaiseAndWaitForHandlers(IDomainEvent domainEvent);

    public IDisposable SubscribeListener(IDomainEventListener listener);
    public void ClearListeners();
    public void RegisterExtension(IExtension extension);
    public ValueTask ExecuteExtensionsAsync<TOpCode, T>(TOpCode opcode, T input);
    public bool AllowTelemetry { get; set; }
}
```

- Remove `EventRaiseResponse` entirely.
- `SubscribeListener` returns an `IDisposable` handle. Disposing it unsubscribes that exact registration.

## Listener Interface Changes

```csharp
public interface IDomainEventListener {
    ValueTask OnEventTriggered(IDomainEvent evt);
    ValueTask OnEventHandlingError(IDomainEvent evt, Exception x);
    bool CanHandle(IDomainEvent evt);
}
```

`ValueTask` removes allocations when handlers complete synchronously. `DomainEventListener<T>` and `InlineLambdaListener<T>` are updated accordingly.

## Internal State

- Replace flat `ImmutableArray<IDomainEventListener>` with `ImmutableDictionary<Type, ImmutableArray<ListenerEntry>>` keyed by the concrete event type.
- Each `ListenerEntry` contains the listener and a stable identity used by the subscription handle.
- Dispatch iterates only listeners that can handle the concrete event type.
- Subscription updates use `ImmutableInterlocked.Update` to keep reads lock-free.

## Allocation Reductions

- `Raise` returns `void`; no `EventRaiseResponse`, no `List<T>`, no TCS list.
- `RaiseAndWaitForHandlers` gathers the `Task`/`ValueTask` instances returned by `WorkQueuer.Enqueue` and awaits `Task.WhenAll` over a rented array.
- Avoid `Environment.StackTrace` unless `StdoutEventHubLogs` is enabled; lazily format diagnostic strings.
- Avoid async state machines in listener wrappers when delegating to typed overrides.

## Thread Safety

- `Extensions` becomes `ImmutableList<IExtension>` updated with `ImmutableInterlocked.Update`.
- `SubscribeListener`, `RemoveListener`, and `ClearListeners` are lock-free on the read path.
- `Raise` and `RaiseAndWaitForHandlers` take a single snapshot of the type-indexed dictionary per event.

## Error Handling

- Handler exceptions inside queued work are routed through `OnEventHandlingError`, matching current behavior.
- `RaiseAndWaitForHandlers` surfaces handler failures through the awaited `Task`; if multiple handlers fail, callers receive an `AggregateException` unless each listener handles its own errors.

## Propagation

- `AllowPropagation` still bubbles events to `parentHub`.
- Parent dispatch uses its own type-indexed snapshot, so the recursion remains cheap.

## Open Questions Resolved

- **Breaking changes allowed?** Yes.
- **Keep `TaskCompletionSource` in hub?** No. Rely on `WorkQueuer` job tasks and `Task.WhenAll`.
- **Subscription lifecycle?** Return `IDisposable` handles.
