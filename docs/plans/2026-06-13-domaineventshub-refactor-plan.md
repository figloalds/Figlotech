# DomainEventsHub Refactor Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Refactor `DomainEventsHub` and related domain-event types to use disposable subscription handles, type-indexed immutable dispatch, `ValueTask` handlers, and eliminate `EventRaiseResponse` / `TaskCompletionSource` usage from the hub API.

**Architecture:** Replace the flat `ImmutableArray<IDomainEventListener>` with an `ImmutableDictionary<Type, ImmutableArray<ListenerEntry>>` so each event is dispatched only to listeners that can handle its concrete type. Subscription handles implement `IDisposable` for exact unregistration. `Raise` becomes fire-and-forget `void`; `RaiseAndWaitForHandlers` returns `ValueTask` and awaits the `WorkQueuer` job tasks directly via `Task.WhenAll`.

**Tech Stack:** C# (.NET Standard 2.1 / .NET 6 for tests), `System.Collections.Immutable`, `System.Threading.Channels` (via `WorkQueuer`).

---

## Pre-Flight: Verify Baseline Build

**Files:**
- `Figlotech.sln`

**Step 1: Build the solution**

Run: `dotnet build Figlotech.sln`
Expected: BUILD SUCCEEDED with no errors.

---

## Task 1: Update `IDomainEventListener` to `ValueTask`

**Files:**
- Modify: `Figlotech.Core/DomainEvents/IDomainEventListener.cs`

**Step 1: Update the interface and base class**

Change `IDomainEventListener` methods to return `ValueTask` and remove unnecessary `async` state machines in `DomainEventListener<T>`.

```csharp
using System;
using System.Threading.Tasks;

namespace Figlotech.Core.DomainEvents {
    public interface IDomainEventListener {
        ValueTask OnEventTriggered(IDomainEvent evt);
        ValueTask OnEventHandlingError(IDomainEvent evt, Exception x);
        bool CanHandle(IDomainEvent evt);
    }

    public abstract class DomainEventListener<T> : IDomainEventListener where T : IDomainEvent {
        public abstract ValueTask OnEventTriggered(T evt);
        public abstract ValueTask OnEventHandlingError(T evt, Exception x);

        public bool CanHandle(IDomainEvent evt) {
            return evt is T;
        }

        public ValueTask OnEventTriggered(IDomainEvent evt) {
            if (evt is T t) {
                return OnEventTriggered(t);
            }
            return default;
        }

        public ValueTask OnEventHandlingError(IDomainEvent evt, Exception x) {
            if (evt is T t) {
                return OnEventHandlingError(t, x);
            }
            return default;
        }
    }
}
```

**Step 2: Build**

Run: `dotnet build Figlotech.Core/Figlotech.Core.csproj`
Expected: BUILD SUCCEEDED (other callers will be broken until later tasks).

**Step 3: Commit**

```bash
git add Figlotech.Core/DomainEvents/IDomainEventListener.cs
git commit -m "refactor(domainevents): switch IDomainEventListener to ValueTask"
```

---

## Task 2: Update `InlineLambdaListener`

**Files:**
- Modify: `Figlotech.Core/DomainEvents/InlineLambdaListener.cs`

**Step 1: Replace `Task` with `ValueTask` and remove `async` wrappers**

```csharp
using System;
using System.Threading.Tasks;

namespace Figlotech.Core.DomainEvents {
    public static class InlineLambdaListener {
        public static InlineLambdaListener<T> Create<T>(Func<T, ValueTask> action, Func<T, Exception, ValueTask> handler = null) where T : IDomainEvent {
            return new InlineLambdaListener<T>(action, handler);
        }
    }

    public sealed class InlineLambdaListener<T> : DomainEventListener<T> where T : IDomainEvent {
        public Func<T, ValueTask> OnRaise;
        public Func<T, Exception, ValueTask> OnHandle;
        public DomainEventsHub EventsHub { get; set; }

        public InlineLambdaListener(Func<T, ValueTask> action, Func<T, Exception, ValueTask> handler = null) {
            OnRaise = action;
            OnHandle = handler;
        }

        public override ValueTask OnEventTriggered(T evt) {
            var raise = OnRaise;
            if (raise != null) {
                return raise(evt);
            }
            return default;
        }

        public override ValueTask OnEventHandlingError(T evt, Exception x) {
            var handle = OnHandle;
            if (handle != null) {
                return handle(evt, x);
            }
            Fi.Tech.SwallowException(x);
            return default;
        }

        private DomainEventsHub _registeredHub = null;

        public void Subscribe(DomainEventsHub hub = null) {
            if (hub == null)
                hub = DomainEventsHub.Global;
            if (_registeredHub != null) {
                Unsubscribe();
            }
            _registeredHub = hub;
            _registeredHub.SubscribeListener(this);
        }

        public void Unsubscribe() {
            _registeredHub.RemoveListener(this);
            _registeredHub = null;
        }
    }
}
```

**Step 2: Build**

Run: `dotnet build Figlotech.Core/Figlotech.Core.csproj`
Expected: BUILD SUCCEEDED.

**Step 3: Commit**

```bash
git add Figlotech.Core/DomainEvents/InlineLambdaListener.cs
git commit -m "refactor(domainevents): use ValueTask in InlineLambdaListener"
```

---

## Task 3: Add Disposable Subscription Handle Type

**Files:**
- Create: `Figlotech.Core/DomainEvents/DomainEventListenerSubscription.cs`

**Step 1: Create the handle type**

```csharp
using System;

namespace Figlotech.Core.DomainEvents {
    internal sealed class DomainEventListenerSubscription : IDisposable {
        private readonly DomainEventsHub _hub;
        private readonly IDomainEventListener _listener;
        private int _disposed;

        public DomainEventListenerSubscription(DomainEventsHub hub, IDomainEventListener listener) {
            _hub = hub ?? throw new ArgumentNullException(nameof(hub));
            _listener = listener ?? throw new ArgumentNullException(nameof(listener));
        }

        public void Dispose() {
            if (Interlocked.Exchange(ref _disposed, 1) == 0) {
                _hub.RemoveListener(_listener);
            }
        }
    }
}
```

**Step 2: Build**

Run: `dotnet build Figlotech.Core/Figlotech.Core.csproj`
Expected: BUILD SUCCEEDED.

**Step 3: Commit**

```bash
git add Figlotech.Core/DomainEvents/DomainEventListenerSubscription.cs
git commit -m "feat(domainevents): add disposable subscription handle"
```

---

## Task 4: Refactor `DomainEventsHub` Core

**Files:**
- Modify: `Figlotech.Core/DomainEvents/DomainEventsHub.cs`

**Step 1: Replace flat listener storage with type-indexed immutable dictionary**

Introduce:

```csharp
private sealed class ListenerEntry {
    public IDomainEventListener Listener { get; }
    public ListenerEntry(IDomainEventListener listener) => Listener = listener;
}

private ImmutableDictionary<Type, ImmutableArray<ListenerEntry>> _listenersByType
    = ImmutableDictionary<Type, ImmutableArray<ListenerEntry>>.Empty;
```

**Step 2: Update `Raise` to return `void` and use type-indexed dispatch**

```csharp
public void Raise(IDomainEvent domainEvent) {
    WriteLog($"Raising Event {domainEvent.GetType()}");

    if (FiTechCoreExtensions.StdoutEventHubLogs) {
        domainEvent.d_RaiseOrigin = Environment.StackTrace;
    }

    var listeners = _listenersByType;
    Dispatch(domainEvent, listeners);

    if (domainEvent.AllowPropagation) {
        parentHub?.Raise(domainEvent);
    }
}

private void Dispatch(IDomainEvent domainEvent, ImmutableDictionary<Type, ImmutableArray<ListenerEntry>> listeners) {
    if (listeners == null || listeners.IsEmpty) {
        return;
    }

    var eventType = domainEvent.GetType();
    if (!listeners.TryGetValue(eventType, out var entries)) {
        return;
    }

    for (int i = 0; i < entries.Length; i++) {
        var listener = entries[i].Listener;
        EnqueueListenerWork(domainEvent, listener);
    }
}

private void EnqueueListenerWork(IDomainEvent domainEvent, IDomainEventListener listener) {
    if (MainQueuer != null) {
        MainQueuer.Enqueue(new WorkJob(async () => {
            try {
                await listener.OnEventTriggered(domainEvent).ConfigureAwait(false);
            } catch (Exception x) {
                try {
                    await listener.OnEventHandlingError(domainEvent, x).ConfigureAwait(false);
                } catch {
                    Fi.Tech.SwallowException(x);
                }
            }
        }, (b) => Fi.Result()) {
            Name = $"Raising Event {domainEvent.GetType().Name} on {listener.GetType().Name}",
            AllowTelemetry = AllowTelemetry,
        });
    }
}
```

> Note: `WorkJob` constructor accepts `Func<ValueTask>`; using `async () => { ... }` here is acceptable because the inner body calls `ValueTask`-returning methods. If a synchronous-fast-path overload is added later, revisit.

**Step 3: Add `RaiseAndWaitForHandlers` returning `ValueTask`**

```csharp
public ValueTask RaiseAndWaitForHandlers(IDomainEvent domainEvent) {
    WriteLog($"Raising Event {domainEvent.GetType()}");

    if (FiTechCoreExtensions.StdoutEventHubLogs) {
        domainEvent.d_RaiseOrigin = Environment.StackTrace;
    }

    var listeners = _listenersByType;
    var task = DispatchAndWait(domainEvent, listeners);

    if (domainEvent.AllowPropagation) {
        return AwaitWithPropagation(task, domainEvent);
    }

    return task;
}

private async ValueTask AwaitWithPropagation(ValueTask current, IDomainEvent domainEvent) {
    await current.ConfigureAwait(false);
    if (parentHub != null) {
        await parentHub.RaiseAndWaitForHandlers(domainEvent).ConfigureAwait(false);
    }
}

private ValueTask DispatchAndWait(IDomainEvent domainEvent, ImmutableDictionary<Type, ImmutableArray<ListenerEntry>> listeners) {
    if (listeners == null || listeners.IsEmpty || MainQueuer == null) {
        return default;
    }

    var eventType = domainEvent.GetType();
    if (!listeners.TryGetValue(eventType, out var entries) || entries.IsEmpty) {
        return default;
    }

    if (entries.Length == 1) {
        var request = EnqueueListenerWorkWithRequest(domainEvent, entries[0].Listener);
        return request != null ? new ValueTask(request.TaskCompletionSource.Task) : default;
    }

    var tasks = new Task[entries.Length];
    for (int i = 0; i < entries.Length; i++) {
        var request = EnqueueListenerWorkWithRequest(domainEvent, entries[i].Listener);
        tasks[i] = request?.TaskCompletionSource.Task ?? Task.CompletedTask;
    }

    return new ValueTask(Task.WhenAll(tasks));
}

private WorkJobExecutionRequest EnqueueListenerWorkWithRequest(IDomainEvent domainEvent, IDomainEventListener listener) {
    return MainQueuer?.Enqueue(new WorkJob(async () => {
        try {
            await listener.OnEventTriggered(domainEvent).ConfigureAwait(false);
        } catch (Exception x) {
            try {
                await listener.OnEventHandlingError(domainEvent, x).ConfigureAwait(false);
            } catch {
                Fi.Tech.SwallowException(x);
            }
        }
    }, (b) => Fi.Result()) {
        Name = $"Raising Event {domainEvent.GetType().Name} on {listener.GetType().Name}",
        AllowTelemetry = AllowTelemetry,
    });
}
```

**Step 4: Update subscription API and internal state rebuild**

```csharp
public IDisposable SubscribeListener(IDomainEventListener listener) {
    if (listener == null) throw new ArgumentNullException(nameof(listener));

    ImmutableInterlocked.Update(ref _listenersByType, (current, l) => {
        var handledTypes = GetHandledTypes(l);
        var next = current;
        foreach (var type in handledTypes) {
            var entries = next.GetValueOrDefault(type, ImmutableArray<ListenerEntry>.Empty);
            if (entries.Any(e => ReferenceEquals(e.Listener, l))) {
                continue;
            }
            next = next.SetItem(type, entries.Add(new ListenerEntry(l)));
        }
        return next;
    }, listener);

    return new DomainEventListenerSubscription(this, listener);
}

private IEnumerable<Type> GetHandledTypes(IDomainEventListener listener) {
    if (listener is IGenericDomainEventListener generic) {
        return generic.HandledTypes;
    }
    return listener.GetType().GetInterfaces()
        .Where(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(DomainEventListener<>))
        .Select(i => i.GetGenericArguments()[0]);
}

public void RemoveListener(IDomainEventListener listener) {
    if (listener == null) throw new ArgumentNullException(nameof(listener));

    ImmutableInterlocked.Update(ref _listenersByType, (current, l) => {
        var next = current;
        foreach (var type in current.Keys.ToList()) {
            var entries = current[type];
            var filtered = entries.RemoveAll(e => ReferenceEquals(e.Listener, l));
            if (filtered.IsEmpty) {
                next = next.Remove(type);
            } else if (filtered.Length != entries.Length) {
                next = next.SetItem(type, filtered);
            }
        }
        return next;
    }, listener);
}

public void ClearListeners() {
    _listenersByType = ImmutableDictionary<Type, ImmutableArray<ListenerEntry>>.Empty;
}
```

> Optional: introduce `IGenericDomainEventListener` if a listener wants to advertise handled types explicitly, avoiding reflection in `GetHandledTypes`.

**Step 5: Update `Raise(IEnumerable<T>)` and remove `EventRaiseResponse`**

```csharp
public void Raise<T>(IEnumerable<T> domainEvents) where T : IDomainEvent {
    if (domainEvents == null) throw new ArgumentNullException(nameof(domainEvents));
    foreach (var domainEvent in domainEvents) {
        Raise(domainEvent);
    }
}
```

Remove the `EventRaiseResponse` class entirely.

**Step 6: Update extension storage to be thread-safe**

```csharp
private ImmutableList<IExtension> _extensions = ImmutableList<IExtension>.Empty;

public void RegisterExtension(IExtension Extension) {
    if (Extension == null) throw new ArgumentNullException(nameof(Extension));
    ImmutableInterlocked.Update(ref _extensions, (current, e) => current.Contains(e) ? current : current.Add(e), Extension);
}

public void RegisterExtension<TOpCode, T>(Extension<TOpCode, T> Extension) {
    if (Extension == null) throw new ArgumentNullException(nameof(Extension));
    RegisterExtension((IExtension)Extension);
}

public async ValueTask ExecuteExtensionsAsync<TOpCode, T>(TOpCode opcode, T input) {
    var extensions = _extensions;
    foreach (var Extension in extensions) {
        if (Extension is Extension<TOpCode, T> ext) {
            try {
                await ext.Execute(opcode, input).ConfigureAwait(false);
            } catch (Exception x) {
                await ext.OnError(opcode, input, x).ConfigureAwait(false);
            }
        }
    }
}
```

**Step 7: Build**

Run: `dotnet build Figlotech.Core/Figlotech.Core.csproj`
Expected: BUILD SUCCEEDED.

**Step 8: Commit**

```bash
git add Figlotech.Core/DomainEvents/DomainEventsHub.cs
git commit -m "refactor(domainevents): type-indexed dispatch, disposable handles, ValueTask"
```

---

## Task 5: Update Extension / Extensible Helpers for `ValueTask`

**Files:**
- Modify: `Figlotech.Core/DomainEvents/IExtension.cs`

**Step 1: Change extension methods to `ValueTask` where appropriate**

`ExecuteExtensionsAsync` should become `ValueTask` to match the hub change.

```csharp
public static async ValueTask ExecuteExtensionsAsync<TOpCode, T>(this T self, TOpCode opcode) where T : IExtensible<TOpCode> {
    if (self == null || self.EventsHub == null) {
        throw new NullReferenceException($"EventHub was null when trying to execute opcode {opcode} for item of type {typeof(T)}");
    }
    await self.EventsHub.ExecuteExtensionsAsync<TOpCode, T>(opcode, self).ConfigureAwait(false);
}

public static async ValueTask ExecuteExtensionsAsync<TOpCode, T, TOther>(this T self, TOpCode opcode, TOther other) where T : IExtensible<TOpCode> {
    if (self == null || self.EventsHub == null) {
        throw new NullReferenceException($"EventHub was null when trying to execute opcode {opcode} for item of type {typeof(T)}");
    }
    await self.EventsHub.ExecuteExtensionsAsync<TOpCode, TOther>(opcode, other).ConfigureAwait(false);
}
```

**Step 2: Build**

Run: `dotnet build Figlotech.Core/Figlotech.Core.csproj`
Expected: BUILD SUCCEEDED.

**Step 3: Commit**

```bash
git add Figlotech.Core/DomainEvents/IExtension.cs
git commit -m "refactor(domainevents): align extension helpers with ValueTask hub"
```

---

## Task 6: Add `IGenericDomainEventListener` (optional optimization)

**Files:**
- Create: `Figlotech.Core/DomainEvents/IGenericDomainEventListener.cs`

**Step 1: Add interface for explicit handled-type advertisement**

```csharp
using System;
using System.Collections.Generic;

namespace Figlotech.Core.DomainEvents {
    public interface IGenericDomainEventListener : IDomainEventListener {
        IEnumerable<Type> HandledTypes { get; }
    }
}
```

**Step 2: Use it in `GetHandledTypes`**

Already referenced in Task 4. Ensure the file is added to the project (it will be picked up automatically by SDK-style projects).

**Step 3: Build**

Run: `dotnet build Figlotech.Core/Figlotech.Core.csproj`
Expected: BUILD SUCCEEDED.

**Step 4: Commit**

```bash
git add Figlotech.Core/DomainEvents/IGenericDomainEventListener.cs
git commit -m "feat(domainevents): add IGenericDomainEventListener for explicit handled types"
```

---

## Task 7: Update All Callers / Tests

**Files:**
- Search for: `EventRaiseResponse`, `.SubscribeListener(`, `OnEventTriggered` overrides, `OnEventHandlingError` overrides, `Raise` return value usage.

**Step 1: Find broken callers**

Run: `dotnet build Figlotech.sln`
Expected: BUILD FAILS with compile errors pointing to callers expecting `EventRaiseResponse`, `Task`, or `void SubscribeListener`.

**Step 2: Fix each caller**

For each error:
- If code does `var response = hub.Raise(...);` → change to `hub.Raise(...);` or `await hub.RaiseAndWaitForHandlers(...);` as appropriate.
- If code overrides `Task OnEventTriggered(...)` → change to `ValueTask`.
- If code calls `hub.SubscribeListener(listener)` and later `hub.RemoveListener(listener)` → either keep `RemoveListener` or switch to `using var handle = hub.SubscribeListener(listener);`.

Common locations to check:
- `Figlotech.Core` for any custom `IDomainEventListener` implementations.
- `Figlotech.BDados.*` adapter projects.
- `test/` console project / benchmarks.

**Step 3: Build**

Run: `dotnet build Figlotech.sln`
Expected: BUILD SUCCEEDED.

**Step 4: Commit**

```bash
git add -A
git commit -m "chore: update callers to new DomainEventsHub API"
```

---

## Task 8: Add / Update Unit Tests

**Files:**
- Create or modify tests in `test/` or a new test project if one is added.

**Step 1: Add a test for basic fire-and-forget raise**

```csharp
[Test]
public async Task Raise_InvokesMatchingListener() {
    var hub = new DomainEventsHub(FiTechCoreExtensions.GlobalQueuer);
    var tcs = new TaskCompletionSource<TestEvent>();
    using (hub.SubscribeListener(InlineLambdaListener.Create<TestEvent>(async e => {
        tcs.SetResult(e);
        await default(ValueTask);
    }))) {
        var evt = new TestEvent();
        hub.Raise(evt);
        var result = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.AreSame(evt, result);
    }
}
```

**Step 2: Add a test for RaiseAndWaitForHandlers**

```csharp
[Test]
public async Task RaiseAndWaitForHandlers_AwaitsAllListeners() {
    var hub = new DomainEventsHub(FiTechCoreExtensions.GlobalQueuer);
    int counter = 0;
    using (hub.SubscribeListener(InlineLambdaListener.Create<TestEvent>(e => {
        Interlocked.Increment(ref counter);
        return default(ValueTask);
    })))
    using (hub.SubscribeListener(InlineLambdaListener.Create<TestEvent>(e => {
        Interlocked.Increment(ref counter);
        return default(ValueTask);
    }))) {
        await hub.RaiseAndWaitForHandlers(new TestEvent());
        Assert.AreEqual(2, counter);
    }
}
```

**Step 3: Add a test for disposable unsubscription**

```csharp
[Test]
public void SubscriptionHandle_Dispose_UnsubscribesListener() {
    var hub = new DomainEventsHub(FiTechCoreExtensions.GlobalQueuer);
    int counter = 0;
    var handle = hub.SubscribeListener(InlineLambdaListener.Create<TestEvent>(e => {
        Interlocked.Increment(ref counter);
        return default(ValueTask);
    }));
    handle.Dispose();
    hub.Raise(new TestEvent());
    Thread.Sleep(50);
    Assert.AreEqual(0, counter);
}
```

**Step 4: Add a test for type-indexed dispatch**

```csharp
[Test]
public void Raise_DoesNotInvokeUnrelatedListener() {
    var hub = new DomainEventsHub(FiTechCoreExtensions.GlobalQueuer);
    bool unrelatedCalled = false;
    using (hub.SubscribeListener(InlineLambdaListener.Create<OtherEvent>(e => {
        unrelatedCalled = true;
        return default(ValueTask);
    })))
    using (hub.SubscribeListener(InlineLambdaListener.Create<TestEvent>(e => default(ValueTask)))) {
        hub.Raise(new TestEvent());
        Thread.Sleep(50);
        Assert.IsFalse(unrelatedCalled);
    }
}
```

**Step 5: Run tests**

Run: `dotnet test <path-to-test-csproj>`
Expected: All new tests PASS.

**Step 6: Commit**

```bash
git add <test-files>
git commit -m "test(domainevents): add tests for new hub API"
```

---

## Task 9: Benchmark / Smoke Test

**Files:**
- Modify: `test/Program.cs` or existing benchmarks if convenient.

**Step 1: Add a minimal benchmark**

```csharp
[MemoryDiagnoser]
public class DomainEventsHubBenchmark {
    private DomainEventsHub _hub;
    private TestEvent _evt;

    [GlobalSetup]
    public void Setup() {
        _hub = new DomainEventsHub(FiTechCoreExtensions.GlobalQueuer);
        for (int i = 0; i < 10; i++) {
            _hub.SubscribeListener(InlineLambdaListener.Create<TestEvent>(e => default(ValueTask)));
        }
        _evt = new TestEvent();
    }

    [Benchmark]
    public void Raise() {
        _hub.Raise(_evt);
    }
}
```

**Step 2: Run benchmark**

Run: `dotnet run --project test/test.csproj -c Release -- --filter *DomainEventsHubBenchmark*`
Expected: Benchmark completes and reports allocation data.

**Step 3: Commit**

```bash
git add test/
git commit -m "chore(test): add DomainEventsHub benchmark"
```

---

## Task 10: Final Solution Build and Verification

**Step 1: Full build**

Run: `dotnet build Figlotech.sln`
Expected: BUILD SUCCEEDED.

**Step 2: Run all tests**

Run: `dotnet test <path-to-test-csproj>`
Expected: PASS.

**Step 3: Final commit / summary**

```bash
git status
git diff --stat
```

Expected: Changes limited to `Figlotech.Core/DomainEvents/`, callers, and tests/benchmarks.

---

## Notes

- Keep public API changes localized to `DomainEventsHub` and listener interfaces.
- If `ValueTask` causes problems with `WorkJob` expectations, add a `WorkJob` constructor overload accepting `Func<CancellationToken, ValueTask>` and reuse it.
- Monitor for `await ValueTask` anti-patterns; always consume a `ValueTask` exactly once.
- Do not commit until explicitly requested by the user.
