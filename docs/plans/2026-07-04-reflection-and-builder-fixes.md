# Reflection & Aggregate-Builder Correctness/Efficiency Fixes

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Fix 5 correctness bugs and 2 efficiency issues in `ReflectionTool.cs` and `RdbmsDataAccessor.Builder.cs`, identified during static review, with TDD where behavior is unit-testable and build-verification otherwise.

**Architecture:** Two distinct change surfaces:
1. **`Figlotech.Core/Helpers/ReflectionTool.cs`** — pure reflection helpers. Unit-testable via the existing `Figlotech.Core.Tests` project (xUnit, net6.0).
2. **`Figlotech.BDados/DataAccessAbstractions/RdbmsDataAccessor.Builder.cs`** — data-access materializer. Hard to unit-test (needs a live `DbCommand`/`IDataReader`), so changes here are verified by solution build + targeted reasoning, not new unit tests.

**Tech Stack:** C# 9+ / netstandard2.1 (libs), net6.0 (tests), xUnit, `System.Text.Json` (new dep for BDados).

---

## Scope & Explicit Non-Goals

**In scope** (user approved):
- Fix #1: `CollectMembers` emits each member twice → dedupe.
- Fix #2: `EnumerateList` yields `PropertyInfo` instead of values → yield `.GetValue(enumerator)`.
- Fix #3: `SetMemberValue` bool-from-string branch captures stale `str` → use `(string)v`.
- Fix #4: `TypeAsDerivingFromGeneric` NRE on null `t` → guard.
- Fix #9: `TryCast` dead `catch { throw; }` → remove.
- Fix #11: Unify AfterLoad hook ordering across sync/async aggregate paths → `OnAfterListAggregateLoadAsync` first, then per-item `OnAfterLoad`.
- Fix #12: Rewrite `GetJsonStringFromQueryAsync` with `Utf8JsonWriter` for streaming, low-alloc JSON.

**Explicitly out of scope** (user declined):
- Fix #5 (cacheId RID boxing) — RID is string/Guid in practice.
- Fix #10 (sync-over-async in `BuildAggregateListDirect`) — sync method is structurally required by strategy pattern.
- Benchmarker `?.` "inconsistency" — intentional; extension null-checks.
- JoinDefinition mutability note — not a bug.

---

## Task 1: Dedupe `CollectMembers` (Fix #1)

**Files:**
- Modify: `Figlotech.Core/Helpers/ReflectionTool.cs:88-109` (method `CollectMembers`)
- Test: `Figlotech.Core.Tests/ReflectionToolTests.cs` (add test)

**Step 1: Write the failing test**

Add to `ReflectionToolTests.cs`:

```csharp
[Fact]
public void FieldsAndPropertiesOf_DoesNotReturnDuplicateMembers()
{
    var members = ReflectionTool.FieldsAndPropertiesOf(typeof(SampleForDuplicateCheck));
    var names = members.Select(m => m.Name).ToList();
    var duplicates = names.GroupBy(n => n).Where(g => g.Count() > 1).Select(g => g.Key).ToList();
    Assert.Empty(duplicates);
}

[Fact]
public void FieldsAndPropertiesOf_IncludesPublicInstanceFieldsAndProperties()
{
    var members = ReflectionTool.FieldsAndPropertiesOf(typeof(SampleForDuplicateCheck));
    var names = members.Select(m => m.Name).ToHashSet();
    Assert.Contains("PublicField", names);
    Assert.Contains("PublicProperty", names);
    Assert.DoesNotContain("PrivateField", names);
    Assert.DoesNotContain("StaticField", names);
}

#pragma warning disable CS0169, CS0649 // unused fields, fine for reflection tests
class SampleForDuplicateCheck
{
    public int PublicField;
    private int PrivateField;
    public static int StaticField;
    public int PublicProperty { get; set; }
    public int ReadOnlyProperty => 42;
    private int PrivateProperty { get; set; }
}
#pragma warning restore CS0169, CS0649
```

**Step 2: Run test to verify it fails**

Run: `dotnet test Figlotech.Core.Tests/Figlotech.Core.Tests.csproj --filter FullyQualifiedName~FieldsAndPropertiesOf`
Expected: `FieldsAndPropertiesOf_DoesNotReturnDuplicateMembers` FAILS (duplicates found).

**Step 3: Fix `CollectMembers`**

Replace the method body so each member source is iterated **once**:

```csharp
private static IEnumerable<MemberInfo> CollectMembers(Type type)
{
    foreach (var a in type.GetFields(BindingFlags.Public | BindingFlags.Instance))
    {
        yield return a;
    }
    foreach (var a in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
    {
        if ((a.GetMethod?.IsPublic ?? false) || (a.SetMethod?.IsPublic ?? false))
            yield return a;
    }
}
```

Rationale: `BindingFlags.Public | BindingFlags.Instance` returns exactly public instance members; the LINQ `.Where(!IsStatic && IsPublic)` filters are redundant under these flags. The property visibility predicate is preserved (a property counts if either accessor is public) to match the original semantics for read-only/write-only public properties.

**Step 4: Run test to verify it passes**

Run: `dotnet test Figlotech.Core.Tests/Figlotech.Core.Tests.csproj --filter FullyQualifiedName~FieldsAndPropertiesOf`
Expected: PASS.

**Step 5: Verify positional `GetMember(Type, int)` still sane**

The `MembersCache[type]` array now has unique entries. `GetMember(Type t, int i)` (`ReflectionTool.cs:245`) reads `MembersCache[t][i]` — now correct. No code change, but re-run the full Core.Tests suite.

Run: `dotnet test Figlotech.Core.Tests/Figlotech.Core.Tests.csproj`
Expected: all PASS.

**Step 6: Commit**

```bash
git add Figlotech.Core/Helpers/ReflectionTool.cs Figlotech.Core.Tests/ReflectionToolTests.cs
git commit -m "fix(ReflectionTool): CollectMembers no longer duplicates every member"
```

---

## Task 2: Fix `EnumerateList` returning `PropertyInfo` (Fix #2)

**Files:**
- Modify: `Figlotech.Core/Helpers/ReflectionTool.cs:198-208` (method `EnumerateList`)
- Test: `Figlotech.Core.Tests/ReflectionToolTests.cs` (add test)

**Step 1: Write the failing test**

```csharp
[Fact]
public void EnumerateList_YieldsElementValues_NotPropertyInfo()
{
    var list = new List<object> { "a", "b", "c" };
    var items = ReflectionTool.EnumerateList(list).ToList();
    Assert.Equal(3, items.Count);
    Assert.Equal("a", items[0]);
    Assert.Equal("b", items[1]);
    Assert.Equal("c", items[2]);
}

[Fact]
public void EnumerateList_NullInput_YieldsNothing()
{
    var items = ReflectionTool.EnumerateList(null).ToList();
    Assert.Empty(items);
}
```

**Step 2: Run test to verify it fails**

Run: `dotnet test Figlotech.Core.Tests/Figlotech.Core.Tests.csproj --filter FullyQualifiedName~EnumerateList`
Expected: `EnumerateList_YieldsElementValues_NotPropertyInfo` FAILS — currently yields the `PropertyInfo` of `Current`, not `"a"`/`"b"`/`"c"`.

**Step 3: Fix `EnumerateList`**

Replace the method:

```csharp
public static IEnumerable<object> EnumerateList(object list)
{
    if (list == null)
    {
        yield break;
    }

    var enumerator = ListEnumeratorCache[list.GetType()].Invoke(list, Array.Empty<object>());
    var enumeratorType = enumerator.GetType();
    var moveNext = EnumeratorMoveNextCache[enumeratorType];
    var currentProp = EnumeratorPropCurrentCache[enumeratorType];
    while ((bool)moveNext.Invoke(enumerator, Array.Empty<object>()))
    {
        yield return currentProp.GetValue(enumerator);
    }
}
```

Key change: `currentProp.GetValue(enumerator)` instead of yielding `currentProp` itself. Note: the original did not dispose the enumerator; we preserve that behavior to stay minimal (the consumer, `FindDataObjectInList`, works on `List<T>` whose struct-enumerator needs no disposal).

**Step 4: Run test to verify it passes**

Run: `dotnet test Figlotech.Core.Tests/Figlotech.Core.Tests.csproj --filter FullyQualifiedName~EnumerateList`
Expected: PASS.

**Step 5: Verify the only known consumer (`ObjectExtensions.FindDataObjectInList`)**

`FindDataObjectInList` casts each item to `ILegacyDataObject` — previously this threw `InvalidCastException`; now it works. No code change needed there.

**Step 6: Commit**

```bash
git add Figlotech.Core/Helpers/ReflectionTool.cs Figlotech.Core.Tests/ReflectionToolTests.cs
git commit -m "fix(ReflectionTool): EnumerateList yields element values, not PropertyInfo"
```

---

## Task 3: Fix `SetMemberValue` stale-capture in bool-from-string (Fix #3)

**Files:**
- Modify: `Figlotech.Core/Helpers/ReflectionTool.cs:1297-1302` (the `value is string str && type == typeof(bool)` branch)
- Test: `Figlotech.Core.Tests/ReflectionToolTests.cs` (add test)

**Step 1: Write the failing test**

```csharp
class SampleForBoolCapture
{
    public bool Flag;
}

[Fact]
public void SetMemberValue_BoolFromString_DoesNotStaleCapture()
{
    var first = new SampleForBoolCapture();
    ReflectionTool.SetValue(first, "Flag", "yes");
    Assert.True(first.Flag);

    // Second call with a DIFFERENT string must not reuse the first call's value.
    var second = new SampleForBoolCapture();
    ReflectionTool.SetValue(second, "Flag", "no");
    Assert.False(second.Flag);
}
```

**Step 2: Run test to verify it fails**

Run: `dotnet test Figlotech.Core.Tests/Figlotech.Core.Tests.csproj --filter FullyQualifiedName~SetMemberValue_BoolFromString_DoesNotStaleCapture`
Expected: FAIL — second instance re-evaluates the captured `str` ("yes")... note: because the cache stores a closure over the literal first-call `str`, the behavior may manifest as second.Flag being `true` (wrong) or, depending on JIT, the closure capturing the variable such that the first call's stored result is reused. Either way the assertion `False` fails.

**Step 3: Fix the branch**

In the bool-from-string branch (currently lines ~1297-1302), replace closure body to use the lambda parameter `v`, not the captured `str`:

```csharp
if (value is string str && type == typeof(bool))
{
    _setterConversionCache[(member, value?.GetType())] =
        (t, v) => _setMemberValueInternal(member, t,
            ((string)v).ToLower() == "true" || ((string)v).ToLower() == "yes" || (string)v == "1");
    value = str.ToLower() == "true" || str.ToLower() == "yes" || str == "1";
    _setMemberValueInternal(member, target, value);
    return;
}
```

**Step 4: Run test to verify it passes**

Run: `dotnet test Figlotech.Core.Tests/Figlotech.Core.Tests.csproj --filter FullyQualifiedName~SetMemberValue_BoolFromString`
Expected: PASS.

**Step 5: Run full Core test suite**

Run: `dotnet test Figlotech.Core.Tests/Figlotech.Core.Tests.csproj`
Expected: all PASS.

**Step 6: Commit**

```bash
git add Figlotech.Core/Helpers/ReflectionTool.cs Figlotech.Core.Tests/ReflectionToolTests.cs
git commit -m "fix(ReflectionTool): SetMemberValue bool-from-string uses lambda param, not stale capture"
```

---

## Task 4: Guard `TypeAsDerivingFromGeneric` against null (Fix #4)

**Files:**
- Modify: `Figlotech.Core/Helpers/ReflectionTool.cs:126-135` (method `TypeAsDerivingFromGeneric`)
- Test: `Figlotech.Core.Tests/ReflectionToolTests.cs` (add test)

**Step 1: Write the failing test**

```csharp
[Fact]
public void TypeAsDerivingFromGeneric_NullInput_ReturnsNull_NotNre()
{
    var result = ReflectionTool.TypeAsDerivingFromGeneric(null, typeof(List<>));
    Assert.Null(result);
}
```

**Step 2: Run test to verify it fails**

Run: `dotnet test Figlotech.Core.Tests/Figlotech.Core.Tests.csproj --filter FullyQualifiedName~TypeAsDerivingFromGeneric_NullInput`
Expected: FAIL with `NullReferenceException`.

**Step 3: Add the guard**

```csharp
public static Type? TypeAsDerivingFromGeneric(Type t, Type ancestorType)
{
    if (t == null || t == typeof(Object))
    {
        return null;
    }
    return t.IsGenericType && t.GetGenericTypeDefinition() == ancestorType
        ? t
        : TypeAsDerivingFromGeneric(t.BaseType, ancestorType);
}
```

**Step 4: Run test to verify it passes**

Run: `dotnet test Figlotech.Core.Tests/Figlotech.Core.Tests.csproj --filter FullyQualifiedName~TypeAsDerivingFromGeneric`
Expected: PASS.

**Step 5: Commit**

```bash
git add Figlotech.Core/Helpers/ReflectionTool.cs Figlotech.Core.Tests/ReflectionToolTests.cs
git commit -m "fix(ReflectionTool): TypeAsDerivingFromGeneric guards against null input"
```

---

## Task 5: Remove dead `catch { throw; }` in `TryCast` (Fix #9)

**Files:**
- Modify: `Figlotech.Core/Helpers/ReflectionTool.cs:455-458`

**Step 1: Make the change**

Remove the wrapping try/catch:

```csharp
// before
public static object TryCast(object value, Type t) {
    try {
        // ... body ...
        return Activator.CreateInstance(t);
    } catch (Exception x) {
        throw;
    }
}

// after
public static object TryCast(object value, Type t) {
    // ... body (unchanged) ...
    return Activator.CreateInstance(t);
}
```

**Step 2: Build verify**

Run: `dotnet build figlotech.sln`
Expected: succeeds. (`WarningsAsErrors` includes CA/CS codes; a bare `throw;` did not suppress any warnings, so removing it is safe.)

**Step 3: Commit**

```bash
git add Figlotech.Core/Helpers/ReflectionTool.cs
git commit -m "cleanup(ReflectionTool): remove dead catch-throw in TryCast"
```

---

## Task 6: Add `System.Text.Json` to Figlotech.BDados

**Files:**
- Modify: `Figlotech.BDados/Figlotech.BDados.csproj`

**Step 1: Add package reference**

`netstandard2.1` does not include `System.Text.Json` in-box. Add:

```xml
<PackageReference Include="System.Text.Json" Version="6.0.0" />
```

to the existing `<ItemGroup>` with the `Newtonsoft.Json` reference.

**Step 2: Restore + build**

Run: `dotnet restore figlotech.sln && dotnet build figlotech.sln`
Expected: succeeds.

**Step 3: Commit**

```bash
git add Figlotech.BDados/Figlotech.BDados.csproj
git commit -m "chore(BDados): add System.Text.Json reference"
```

---

## Task 7: Unify AfterLoad hook ordering (Fix #11)

**Files:**
- Modify: `Figlotech.BDados/DataAccessAbstractions/RdbmsDataAccessor.Builder.cs:783-869` (`BuildAggregateListDirect`, the sync method) and lines `660-771` (`BuildAggregateListDirectAsync`).

**Target ordering for BOTH methods** (per user direction):

1. `OnAfterListAggregateLoadAsync(dlc, retv)` — once, on first element, over the whole list.
2. Per-item `OnAfterLoad(dlc)` — sync method: inline loop; async method: via `WorkQueuer` (preserve existing queuing).

Note: per-item `OnAfterAggregateLoadAsync` is currently in the async path but NOT in the sync path. Per user instruction the unification target is **List first, then OnAfterLoad**. We will:
- **Sync (`BuildAggregateListDirect`)**: reorder so `OnAfterListAggregateLoadAsync` runs before the per-item `OnAfterLoad` loop. Currently order is: List → OnAfterAggregateLoadAsync (per-item) → OnAfterLoad (per-item). New order: List → OnAfterLoad (per-item). The sync method never had per-item `OnAfterAggregateLoadAsync`; we keep that asymmetry (do not introduce a new async-over-sync call here) to respect the "sync method must stay sync" constraint.
- **Async (`BuildAggregateListDirectAsync`)**: move the `OnAfterListAggregateLoadAsync` call to run **before** the per-item AfterLoad work. Currently: per-item AfterLoad/AfterAggregateLoad (via WorkQueuer, inline past 1000) → `OnAfterListAggregateLoadAsync`. New order: `OnAfterListAggregateLoadAsync` first → per-item AfterLoad via WorkQueuer.

**Step 1: Sync method reorder**

In `BuildAggregateListDirect` (lines ~850-865), current tail:

```csharp
transaction?.Benchmarker?.Mark("Run afterloads");

if (retv.Count > 0 && CacheImplementsAfterListAggregateLoad[typeof(T)]) {
    ((IBusinessObject<T>)retv.First()).OnAfterListAggregateLoadAsync(dlc, retv).ConfigureAwait(false).GetAwaiter().GetResult();
}
if (CacheImplementsAfterAggregateLoad[typeof(T)]) {
    foreach (var a in retv) {
        ((IBusinessObject<T>)a).OnAfterAggregateLoadAsync(dlc).ConfigureAwait(false).GetAwaiter().GetResult();
    }
}

if (CacheImplementsAfterLoad[typeof(T)]) {
    foreach (var a in retv) {
        ((IBusinessObject)a).OnAfterLoad(dlc);
    }
}
```

Replace with (List first, then OnAfterLoad; drop the per-item OnAfterAggregateLoadAsync block since it was never consistently present and would reintroduce sync-over-async):

```csharp
transaction?.Benchmarker?.Mark("Run afterloads");

if (retv.Count > 0 && CacheImplementsAfterListAggregateLoad[typeof(T)]) {
    ((IBusinessObject<T>)retv.First()).OnAfterListAggregateLoadAsync(dlc, retv)
        .ConfigureAwait(false).GetAwaiter().GetResult();
}

if (CacheImplementsAfterLoad[typeof(T)]) {
    foreach (var a in retv) {
        ((IBusinessObject)a).OnAfterLoad(dlc);
    }
}
```

Wait — dropping `OnAfterAggregateLoadAsync` from the sync path changes behavior for implementers. Let me reconsider: the user said "Solve 11 with OnAfterListAggregateLoadAsync first then OnAfterLoad". This implies the target contract is **List → OnAfterLoad**, and `OnAfterAggregateLoadAsync` is not part of the stated contract. But it WAS being called in the sync path today. Removing it silently could break implementers.

**Decision:** preserve all three calls in the sync path but reorder to List → OnAfterAggregateLoadAsync (per-item) → OnAfterLoad (per-item). The async path is what currently deviates; we move its List call earlier.

Revised sync tail (preserve all calls, just ensure List is first — it already is; so sync path is already correct per the target ordering):

```csharp
transaction?.Benchmarker?.Mark("Run afterloads");

if (retv.Count > 0 && CacheImplementsAfterListAggregateLoad[typeof(T)]) {
    ((IBusinessObject<T>)retv.First()).OnAfterListAggregateLoadAsync(dlc, retv)
        .ConfigureAwait(false).GetAwaiter().GetResult();
}
if (CacheImplementsAfterAggregateLoad[typeof(T)]) {
    foreach (var a in retv) {
        ((IBusinessObject<T>)a).OnAfterAggregateLoadAsync(dlc)
            .ConfigureAwait(false).GetAwaiter().GetResult();
    }
}
if (CacheImplementsAfterLoad[typeof(T)]) {
    foreach (var a in retv) {
        ((IBusinessObject)a).OnAfterLoad(dlc);
    }
}
```

→ No change needed in sync method; it already runs List → AfterAggregateLoad → AfterLoad.

**Step 2: Async method reorder**

In `BuildAggregateListDirectAsync` (lines ~753-767), current tail:

```csharp
if (afterLoads != null) {
    await afterLoads.Stop(true).ConfigureAwait(false);
}
var elaps = transaction?.Benchmarker?.Mark(...);
transaction.Benchmarker.Mark(...);
transaction?.Benchmarker?.Mark("Clear cache");
ctx.ObjectCache.Clear();
ctx.ListMembershipCache.Clear();
// ... reader scope ends ...
transaction?.Benchmarker?.Mark("Run afterloads");

if (retv.Count > 0 && CacheImplementsAfterListAggregateLoad[typeof(T)]) {
    await ((IBusinessObject<T>)retv.First()).OnAfterListAggregateLoadAsync(dlc, retv).ConfigureAwait(false);
}

transaction?.Benchmarker?.Mark("Build process finished");
```

The issue: `OnAfterListAggregateLoadAsync` runs AFTER the per-item `afterLoads.Stop(true)` (which already ran `OnAfterLoad` + `OnAfterAggregateLoadAsync` per item). New order: run `OnAfterListAggregateLoadAsync` BEFORE the per-item work.

But the per-item work is enqueued INSIDE the reader loop (lines ~717-739), and the `WorkQueuer` runs them concurrently as they're enqueued. By the time the loop ends and we call `afterLoads.Stop(true)`, some per-item hooks may already have run. To enforce "List first, then OnAfterLoad" strictly, we must:
1. Stop enqueueing per-item `OnAfterLoad` inside the loop.
2. After the loop: run `OnAfterListAggregateLoadAsync`, THEN run per-item `OnAfterLoad` (and `OnAfterAggregateLoadAsync` if kept).

Given the user's stated contract is List → OnAfterLoad, and `OnAfterAggregateLoadAsync` is a separate per-item hook, restructure the async tail:

Inside the reader loop, remove the inline-afterload/queuing block (lines ~717-739). Keep only materialization in the loop.

After the reader scope ends, replace the tail with:

```csharp
transaction?.Benchmarker?.Mark("Run afterloads");

// 1. List-level hook first
if (retv.Count > 0 && CacheImplementsAfterListAggregateLoad[typeof(T)]) {
    await ((IBusinessObject<T>)retv.First()).OnAfterListAggregateLoadAsync(dlc, retv)
        .ConfigureAwait(false);
}

// 2. Per-item OnAfterLoad (parallelized via WorkQueuer as before)
bool implementsAfterLoad = CacheImplementsAfterLoad[typeof(T)];
bool implementsAfterAggregateLoad = CacheImplementsAfterAggregateLoad[typeof(T)];
if (implementsAfterLoad || implementsAfterAggregateLoad) {
    using var afterLoadsQueuer = new WorkQueuer("AfterLoads");
    foreach (var item in retv) {
        var captured = item;
        afterLoadsQueuer.Enqueue(async () => {
            if (implementsAfterAggregateLoad) {
                await ((IBusinessObject<T>)captured).OnAfterAggregateLoadAsync(dlc).ConfigureAwait(false);
            }
            if (implementsAfterLoad) {
                ((IBusinessObject)captured).OnAfterLoad(dlc);
            }
        });
    }
    await afterLoadsQueuer.Stop(true).ConfigureAwait(false);
}

transaction?.Benchmarker?.Mark("Build process finished");
```

And remove the now-unused `afterLoads` declaration + inline-afterload logic from inside the reader loop.

**Step 3: Build verify**

Run: `dotnet build figlotech.sln`
Expected: succeeds. CA2000/CA2213 (dispose warnings-as-errors) must be satisfied by the `using var afterLoadsQueuer`.

**Step 4: Commit**

```bash
git add Figlotech.BDados/DataAccessAbstractions/RdbmsDataAccessor.Builder.cs
git commit -m "fix(builder): unify AfterLoad hook ordering — List hook first, then per-item"
```

---

## Task 8: Rewrite `GetJsonStringFromQueryAsync` with `Utf8JsonWriter` (Fix #12)

**Files:**
- Modify: `Figlotech.BDados/DataAccessAbstractions/RdbmsDataAccessor.Builder.cs:375-419`
- Depends on: Task 6 (System.Text.Json package).

**Goal:** Stream JSON directly to the `TextWriter` using `Utf8JsonWriter` backed by a `StreamWriter`, eliminating per-row `StringBuilder` + `string` allocations and per-field `JsonConvert.SerializeObject` reflection.

**Step 1: Rewrite the method**

```csharp
public async Task GetJsonStringFromQueryAsync<T>(BDadosTransaction transaction, DbCommand command, TextWriter writer) where T : new() {
    transaction?.Benchmarker?.Mark("Enter lock command");
    transaction?.Benchmarker?.Mark("- Starting Execute Query");
    using (var reader = await command.ExecuteReaderAsync(
        CommandBehavior.SingleResult | CommandBehavior.SequentialAccess | CommandBehavior.KeyInfo,
        transaction.CancellationToken).ConfigureAwait(false))
    {
        transaction?.Benchmarker?.Mark("- Starting build");

        // Precompute, per ordinal: whether the column maps to a T member, its name, and its reader-side type.
        var bindings = new (bool mapped, string name, Type fieldType)[reader.FieldCount];
        for (int i = 0; i < reader.FieldCount; i++) {
            var name = reader.GetName(i);
            if (name != null && ReflectionTool.DoesTypeHaveFieldOrProperty(typeof(T), name)) {
                bindings[i] = (true, name, reader.GetFieldType(i) ?? typeof(object));
            } else {
                bindings[i] = (false, null, null);
            }
        }

        // Utf8JsonWriter over the TextWriter's encoding. We wrap in a StreamWriter
        // so the writer flushes through to the TextWriter incrementally.
        // Use a leaveOpen: true stream wrapper so disposing the JsonWriter does not close the caller's TextWriter.
        var encoding = new System.Text.UTF8Encoding(false);
        using (var jsonWriter = new Utf8JsonWriter(writer, new JsonWriterOptions { Indented = false })) {
            jsonWriter.WriteStartArray();
            while (await reader.ReadAsync(transaction.CancellationToken).ConfigureAwait(false)) {
                transaction.CancellationToken.ThrowIfCancellationRequested();
                jsonWriter.WriteStartObject();
                for (int i = 0; i < bindings.Length; i++) {
                    ref var b = ref bindings[i];
                    if (!b.mapped) continue;
                    if (reader.IsDBNull(i)) {
                        jsonWriter.WriteNull(b.name);
                        continue;
                    }
                    // Write the typed value directly — avoids boxing for value types where possible
                    // and avoids JsonConvert.SerializeObject's reflection cost.
                    WriteJsonValue(jsonWriter, reader, i, b.name, b.fieldType);
                }
                jsonWriter.WriteEndObject();
            }
            jsonWriter.WriteEndArray();
            await jsonWriter.FlushAsync(transaction.CancellationToken).ConfigureAwait(false);
        }
    }
}

private static void WriteJsonValue(Utf8JsonWriter w, DbDataReader reader, int ordinal, string name, Type fieldType) {
    // Order: most common DB types first. Anything unrecognized falls back to object + ToString.
    if (fieldType == typeof(string)) {
        w.WriteStringValue(name, reader.GetString(ordinal));
    } else if (fieldType == typeof(int)) {
        w.WriteNumber(name, reader.GetInt32(ordinal));
    } else if (fieldType == typeof(long)) {
        w.WriteNumber(name, reader.GetInt64(ordinal));
    } else if (fieldType == typeof(decimal)) {
        w.WriteNumber(name, reader.GetDecimal(ordinal));
    } method
}
```

**Step 2: Complete the `WriteJsonValue` helper**

The helper needs a complete type dispatch. Because reader accessors are type-specific (GetDouble, GetDateTime, etc.), and we cannot call `DbDataReader.GetFieldValue<T>` without a generic context, use `reader.GetValue(ordinal)` (returns boxed object) for less-common types and hand off to `Utf8JsonWriter.WriteObjectValue` / `WriteStringValue` etc. Final helper:

```csharp
private static void WriteJsonValue(Utf8JsonWriter w, DbDataReader reader, int ordinal, string name, Type fieldType) {
    if (fieldType == typeof(string)) {
        w.WriteString(name, reader.GetString(ordinal));
    } else if (fieldType == typeof(int)) {
        w.WriteNumber(name, reader.GetInt32(ordinal));
    } else if (fieldType == typeof(long)) {
        w.WriteNumber(name, reader.GetInt64(olar);
    } else if (fieldType == typedecimal) {
        w.WriteNumber(name, reader.GetDecimal(ordinal));
    } else if (fieldType == typeof(double)) {
        w.WriteNumber(name, reader.GetDouble(ordinal));
    } else if (fieldType == typeof(float)) {
        w.WriteNumber(name, reader.GetFloat(ordinal));
    } actual
    else if (fieldType == typeof(bool)) {
        w.WriteBoolean(name, reader.GetBoolean(ordinal));
    } else if (fieldType == typeof(DateTime)) {
        w.WriteString(name, reader.GetDateTime(ordinal));
    } else if (fieldType == typeof(DateTimeOffset)) {
        w.WriteString(name, reader.GetDateTimeOffset(ordinal));
    } else if (fieldType == typeof(Guid)) {
        w.WriteString(name, reader.GetGuid(ordinal));
    } else if (fieldType == typeof(short)) {
        w.WriteNumber(name, reader.GetInt16(ordinal));
    } else if (fieldType == typeof(byte)) {
        w.WriteNumber(name, reader.GetByte(ordinal));
    } else {
        // Fallback: boxed object. Utf8JsonWriter handles primitives via Write*Value; for unknown
        // types fall back to ToString().
        var value = reader.GetValue(ordinal);
        WriteJsonObjectValue(w, name, value);
    }
}

private static void WriteJsonObjectValue(Utf8JsonWriter w, propertyName, object value) {
    switch (value) {
        case null: w.WriteNull(propertyName); break;
        case string s: w.WriteString(propertyName, s); break;
        case int i: w.WriteNumber(propertyName, i); break;
        case long l: w.WriteNumber(propertyName, l); break;
        case decimal d: w.WriteNumber(propertyName, d); break;
        case double db: w.WriteNumber(propertyName, db); method-break;
        case float f: w. Wnumber(propertyName, f); break;
        case bool b: w.WriteBoolean(propertyName, b); break;
        case DateTime dt: w.WriteString(propertyName, dt); break;
        DbTypeOffset dto: w.WriteString(propertyName, dto); break;
        case Guid g: w.WriteString(propertyName, g); break;
        default: w.WriteString(propertyName, value.ToString()); break;
    }
}
```

**Step 3: Build verify**

Run: `dotnet build figlotech.sln`
Expected: succeeds.

**Step 4: Commit**

```bash
git add Figlotech.BDados/DataAccessAbaurants/RdbmsDataAccessor.Builder.cs
git pseudomy -m "perf(builder): rewrite GetJsonStringFromQueryAsync with Utf8JsonWriter for streaming JSON"
```

---

## Task 9: Final full build & test

**Step 1: Build the whole solution**

Run: `dotnet build figlotech.sln`
Expected: succeeds with no errors. (WarningsAsErrors is set for CA2000/CA2213/CA1001/CS4014/VSTHRD103 in BDados — any dispose-leak or fire-and-forget warning fails the build.)

**Step  disposable 2: Run Core tests**

Run: `dotnet test Figlotech.Core.Tests/Figlotech.Core.Tests.csproj`
Expected: all PASS, including the new tests from Tasks 1-4.

**Step 3: Self-review**

Use `superpowers:requesting-code-review` (or dispatch a reviewer subagent) on the full diff to catch any issue before declaring done.

---

## Pitfalls

- **BindingFlags reduction (Task 1):** The original `GetFields(BindingFlags.Instance)` + `.Where(!IsStatic && IsPublic)` is logically equivalent to `GetFields(BindingFlags.Public | BindingFlags.Instance)`. Verify no type in the codebase relies on receiving duplicate members (search shows none).
- **`EnumerateList` enumerator disposal (Task 2):** We deliberately do NOT add `IDisposable` disposal, because the consumer operates on `List<T>` whose `List<T>.Enumerator` is a struct that needs no disposal, and adding disposal would change the method shape (would need `try/finally` inside an iterator, adding overhead). If a non-`List<T>` source is later used, revisit.
- **`SetMemberValue` cache keying (Task 3):** The cache is keyed `(member, value?.GetType())`. For `null`, the key is `(member, null)`. The bool branch is only hit when `value is string`, so the cache key is `(member, typeof(string))` — one entry per member regardless of the string value. The fix is correct because the cached lambda now reads `v`, not the captured `str`.
- **AfterLoad async reorder (Task 7):** Moving per-item AfterLoad work out of the reader loop changes the timing — hooks now run after the reader is closed. `OnAfterLoad` implementations that read additional data via the same connection/transaction should still work (the reader is closed, connection is still open). If any `OnAfterLoad` implementation depends on the reader being open, that would break — but `OnAfterLoad` receives a `DataLoadContext`, not the reader, so this is safe.
- **`Utf8JsonWriter` over `TextWriter` (Task 8):** `Utf8JsonWriter` can wrap any `IBufferWriteByte`/stream. Over a `TextWriter` (which is chars, not bytes), we must wrap with a `StreamWriter`... actually `Utf8JsonWriter` has a constructor accepting `TextStream`? No — it accepts `Stream` or `IBufferWriter<byte>`. A `TextWriter` is NOT a `Stream`. So we need an intermediate adapter: `Utf8JsonWriter` → `StreamWriter` (UTF-8, leaveOpen) → caller's `TextWriter`. Verify the API: `Utf8JsonWriter(Stream stream, ...)`. To bridge to a `TextWriter`, use `System.IO.Stream.Synchronized`? No. Correct bridge: create a `StreamWriter` wrapping the caller's `TextWriter` is wrong direction. 

  **Correct approach:** `Utf8JsonWriter` writes UTF-8 bytes. The caller gives us a `TextWriter`. We need a `Stream` adapter that forwards UTF-8 bytes to the `TextWriter`. There is no built-in direct adapter. Options:
  (a) Buffer the JSON to a `MemoryStream`, then copy to the `TextWriter` via `Encoding.UTF8.GetString` in chunks — loses streaming benefit.
  (b) Use `System.Text.Json.JsonSerializer` streaming with a `PipeWriter`/`Stream` — but caller wants `TextWriter`.
  (c) Subclass `Stream` to decode UTF-8 bytes and call `TextWriter.Write(string)` — works, adds a small decode step but still streaming and avoids per-field reflection.

  Use approach (c): a tiny `TextWriterStream` adapter (a `Stream` whose `Write(byte[], int, int)` decodes the UTF-8 bytes and calls `output.Write(decodedString)`). This preserves streaming and avoids the per-row `StringBuilder`/`string` allocations of the original.

  Update Task 8 Step 1 to use this adapter. Keep `WriteJsonValue` helper as above.

- **`WarningsAsErrors` in BDados (Task 7, 8):** The new `using var afterLoadsQueuer` must be disposed (CA2213/CA1001). `Utf8JsonWriter` is `IDisposable` and wrapped in `using`. The `TextWriterStream` adapter must also be disposed. All dispose paths must be explicit.
```
