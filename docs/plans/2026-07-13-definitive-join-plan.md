# Definitive Join Plan Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace mutable executable `JoinDefinition` graphs with one deeply immutable `DefinitiveJoinPlan` produced by a single compiler and consumed by all query generators and aggregate materializers.

**Architecture:** Automatic joins compile once from `(RootType, AggregateJoinShape, FormatVersion)`; custom mutable builders must call `Freeze()` before execution. The frozen plan owns deterministic aliases, projection order, T-typed identifier metadata, aggregate relations, root-ordering requirements, and an immutable compiled materializer, so provider query generation and readers cannot mutate or redefine the plan.

**Tech Stack:** C# 10, .NET Standard 2.1 libraries, .NET 6 xUnit tests, `System.Collections.Immutable`, expression trees, ADO.NET providers.

---

## Constraints and execution rules

- Follow strict RED-GREEN-REFACTOR for every task.
- Keep the tree green before starting the next task.
- Do not commit automatically; the repository instructions require explicit user approval before commits.
- Preserve existing public APIs through obsolete adapter overloads until all internal callers migrate.
- Preserve current provider behavior that emits `LEFT JOIN`, even when legacy `JoinType` fields request another type. Correcting join kinds is a separate change.
- Treat `RID` as the entity's opaque T-typed identifier column. Never coerce or assume `string`/`long`.
- Do not combine unrelated ReflectionTool, lifecycle-hook, or provider cleanup with this refactor.

## Grounding

The refactor crosses approximately 7,972 lines in the main affected source files:

| File | Lines |
|---|---:|
| `RdbmsDataAccessor.cs` | 3,964 |
| `RdbmsDataAccessor.Builder.cs` | 985 |
| `JoinDefinition.cs` | 160 |
| `ConditionParser.cs` | 560 |
| `MySqlQueryGenerator.cs` | 851 |
| `PgSQLQueryGenerator.cs` | 776 |
| `SqliteQueryGenerator.cs` | 676 |

Current mutation/caching anchors:
- Automatic mutable caches: `RdbmsDataAccessor.cs:3311-3398`.
- Parallel query/build traversals: `RdbmsDataAccessor.cs:1780-2067`.
- Mutable aggregate plan caches: `RdbmsDataAccessor.Builder.cs:53-64`, `:494`, `:638-646`.
- Provider projection mutation: MySQL `:341-353`, PostgreSQL `:311-320`, SQLite `:251-263`.
- Runtime alias mutation: `PrefixMaker.cs:7-38`, `ConditionParser.cs:70-105`.

---

### Task 1: Add a dedicated BDados unit-test project and fixtures

**Files:**
- Create: `Figlotech.BDados.Tests/Figlotech.BDados.Tests.csproj`
- Create: `Figlotech.BDados.Tests/PlanTestModels.cs`
- Create: `Figlotech.BDados.Tests/SmokeTests.cs`
- Modify: `figlotech.sln`

**Step 1: Create the test project file**

Use the same test package versions as `Figlotech.Core.Tests` and reference the core BDados project:

```xml
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFramework>net6.0</TargetFramework>
    <ImplicitUsings>enable</ImplicitUsings>
    <Nullable>enable</Nullable>
    <IsPackable>false</IsPackable>
  </PropertyGroup>
  <ItemGroup>
    <PackageReference Include="Microsoft.NET.Test.Sdk" Version="17.3.2" />
    <PackageReference Include="xunit" Version="2.4.2" />
    <PackageReference Include="xunit.runner.visualstudio" Version="2.4.5">
      <IncludeAssets>runtime; build; native; contentfiles; analyzers; buildtransitive</IncludeAssets>
      <PrivateAssets>all</PrivateAssets>
    </PackageReference>
    <PackageReference Include="coverlet.collector" Version="3.2.0">
      <IncludeAssets>runtime; build; native; contentfiles; analyzers; buildtransitive</IncludeAssets>
      <PrivateAssets>all</PrivateAssets>
    </PackageReference>
  </ItemGroup>
  <ItemGroup>
    <ProjectReference Include="..\Figlotech.BDados\Figlotech.BDados.csproj" />
  </ItemGroup>
</Project>
```

**Step 2: Add representative test models**

Create models covering:
- roots with at least two non-string identifier CLR types,
- `AggregateField`,
- `AggregateFarField`,
- `AggregateObject`,
- `AggregateList`,
- a deliberately cyclic graph for validation tests.

The identifier fixture must prove that the plan records `MemberInfo` and the member's actual CLR type rather than coercing RID.

**Step 3: Add a smoke test**

```csharp
[Fact]
public void TestProjectLoadsBDadosAssembly() {
    Assert.NotNull(typeof(RdbmsDataAccessor).Assembly);
}
```

**Step 4: Add the project to the solution**

Run:

```bash
dotnet sln figlotech.sln add Figlotech.BDados.Tests/Figlotech.BDados.Tests.csproj
```

**Step 5: Verify the new test project**

Run:

```bash
dotnet test Figlotech.BDados.Tests/Figlotech.BDados.Tests.csproj
```

Expected: the smoke test passes.

---

### Task 2: Lock down portable conversion semantics required by precompiled plans

**Files:**
- Modify: `Figlotech.Core.Tests/ReflectionToolTests.cs`
- Modify: `Figlotech.Core/Helpers/ReflectionTool.cs:643-878`

**Step 1: Write failing conversion tests**

Add tests proving:
- `BuildObjectToTargetConversionExpression(object, object)` converts `DBNull.Value` to `null`.
- `byte[]` to `string` follows the established UTF-8 semantics rather than returning `"System.Byte[]"`.
- null into a non-nullable destination preserves the materializer's skip-assignment contract at the assignment layer.

**Step 2: Run focused tests and verify RED**

```bash
dotnet test Figlotech.Core.Tests/Figlotech.Core.Tests.csproj --filter FullyQualifiedName~ReflectionToolTests
```

Expected: the new `DBNull`/binary tests fail against the current identity/`ToString()` branches.

**Step 3: Implement the minimal conversion fixes**

- Move null/`DBNull` normalization ahead of the `object -> object` identity return.
- Add an explicit `byte[] -> string` UTF-8 branch consistent with `TryCast`.
- Do not change numeric checked/unchecked behavior in this task.

**Step 4: Re-run focused tests**

Expected: all `ReflectionToolTests` pass.

**Step 5: Build Core**

```bash
dotnet build Figlotech.Core/Figlotech.Core.csproj -c Release --no-restore
```

Expected: 0 errors.

---

### Task 3: Introduce deeply immutable definitive-plan value types

**Files:**
- Create: `Figlotech.BDados/DataAccessAbstractions/DefinitiveJoinPlan.cs`
- Modify: `Figlotech.BDados/Figlotech.BDados.csproj`
- Create: `Figlotech.BDados.Tests/DefinitiveJoinPlanTests.cs`

**Step 1: Write failing construction and immutability tests**

Test the intended public model:

```csharp
[Fact]
public void PlanPreservesTypedIdentifierMetadata() {
    var plan = DefinitiveJoinPlanTestFactory.CreateFor(typeof(GuidRoot));
    Assert.Equal(typeof(Guid), plan.Tables[plan.RootTableIndex].Identifier.ClrType);
}

[Fact]
public void NestedPlanCollectionsAreImmutableSnapshots() {
    var sourceColumns = new List<string> { "RID", "Name" };
    var plan = DefinitiveJoinPlanTestFactory.CreateWithColumns(sourceColumns);
    sourceColumns.Add("LateMutation");
    Assert.DoesNotContain(plan.Projection, x => x.SourceColumn == "LateMutation");
}
```

Also test unique projection ordinals, unique result aliases, and constructor validation.

**Step 2: Run tests and verify RED**

Expected: compilation fails because the plan types do not exist.

**Step 3: Add immutable plan types**

Implement constructor-only/get-only classes using `ImmutableArray<T>` and `ImmutableDictionary<TKey,TValue>`:

- `AggregateJoinShape`
- `AutoJoinPlanKey`
- `DefinitiveJoinPlan`
- `DefinitiveJoinTable`
- `DefinitiveIdentifier`
- `DefinitiveJoinRelation`
- `DefinitiveProjectionColumn`
- `AggregatePath`
- `RootOrderingRequirement`

Add a direct `System.Collections.Immutable` package reference matching the repository's existing 6.0.0 dependency.

Do not expose mutable arrays or delegate collections. Compute and store `FormatVersion` and `StructuralSignature` once in the constructor/factory.

**Step 4: Re-run definitive-plan tests**

Expected: all construction, identifier-type, and snapshot tests pass.

**Step 5: Build BDados**

```bash
dotnet build Figlotech.BDados/Figlotech.BDados.csproj -c Release --no-restore
```

Expected: 0 errors, 0 warnings in the new files.

---

### Task 4: Add a universal freeze boundary for existing custom joins

**Files:**
- Create: `Figlotech.BDados/DataAccessAbstractions/JoinDefinitionFreezer.cs`
- Create: `Figlotech.BDados/DataAccessAbstractions/IDefinitiveJoinBuilder.cs` or an equivalent extension-only seam
- Modify: `Figlotech.BDados/DataAccessAbstractions/JoinDefinition.cs`
- Modify: `Figlotech.BDados/Builders/JoinObjectBuilder.cs:60-65`
- Create: `Figlotech.BDados.Tests/JoinDefinitionFreezerTests.cs`

**Step 1: Write failing custom-freeze tests**

Cover:
- `Freeze(rootType, shape)` creates immutable table/relation/projection snapshots.
- Mutating `JoinDefinition`, `JoiningTable.Columns`, or `Relation.Fields` after freeze does not alter the plan.
- Missing aliases, duplicate result aliases, invalid table indices, missing relation keys, and missing identifier projections fail before execution.
- A T-typed identifier remains opaque and unchanged.
- Calling `JoinObjectBuilder.GenerateQuery` freezes before calling the generator.

**Step 2: Run tests and verify RED**

Expected: compilation fails because `Freeze` and `BuildPlan` do not exist.

**Step 3: Implement the freezer and compatibility API**

Add:

```csharp
public DefinitiveJoinPlan Freeze(Type rootType, AggregateJoinShape shape)
```

Add this through a companion interface or extension:

```csharp
DefinitiveJoinPlan BuildPlan(Type rootType, AggregateJoinShape shape);
```

Do not add a required member to `IJoinBuilder`, because external implementations must continue to compile. Keep `GetJoin()` temporarily for source compatibility, mark concrete built-in access paths obsolete where appropriate, and prevent executable internal paths from accepting its result once migration completes.

The freezer must add required identifier/relation-key projections before assigning ordinals. It must not call a provider to finish the plan.

**Step 4: Re-run custom-freeze tests**

Expected: all custom-plan snapshot and validation tests pass.

**Step 5: Run BDados tests and build**

```bash
dotnet test Figlotech.BDados.Tests/Figlotech.BDados.Tests.csproj
dotnet build Figlotech.BDados/Figlotech.BDados.csproj -c Release --no-restore
```

Expected: green.

---

### Task 5: Build the single-pass automatic plan compiler

**Files:**
- Create: `Figlotech.BDados/DataAccessAbstractions/DefinitiveJoinPlanCompiler.cs`
- Create: `Figlotech.BDados/DataAccessAbstractions/DefinitiveAliasAllocator.cs`
- Create: `Figlotech.BDados.Tests/DefinitiveJoinPlanCompilerTests.cs`
- Read/compare: `RdbmsDataAccessor.cs:1780-2067`

**Step 1: Write failing compiler tests**

Build expected topology tests for the fixtures:
- root table and identifier,
- aggregate scalar join and target member,
- aggregate far-field two-hop path,
- aggregate object relation,
- aggregate list relation,
- deterministic projection order and aliases,
- cycle rejection with aggregate-path diagnostics.

Test shape semantics explicitly:

```csharp
[Fact]
public void ScalarShapeKeepsScalarAndFarFieldsButOmitsObjectsAndLists() { ... }

[Fact]
public void FullGraphIncludesObjectAndListRelations() { ... }
```

**Step 2: Run tests and verify RED**

Expected: compiler type is missing.

**Step 3: Implement one metadata traversal**

Implement:

```csharp
public DefinitiveJoinPlan Compile(Type rootType, AggregateJoinShape shape)
```

The traversal must create query tables and materialization relations in the same branch for each attribute. Use a local alias allocator; never use the cached `PrefixMaker`.

Maintain a recursion-path set keyed by `(Type, aggregate path/member)` and throw an actionable cycle exception. Do not use an arbitrary depth limit.

**Step 4: Add differential tests against the legacy builders**

For acyclic fixtures, compare the new plan with the current `MakeQueryAggregations`/`MakeBuildAggregations` observable topology:
- table types,
- aliases,
- projected field names,
- parent/child relation indices,
- aggregate build kinds.

Do not compare mutable object identity.

**Step 5: Re-run compiler tests and full BDados tests**

Expected: green.

---

### Task 6: Add the canonical automatic plan cache and immutable alias resolver

**Files:**
- Create: `Figlotech.BDados/DataAccessAbstractions/DefinitiveJoinPlanCache.cs`
- Modify: `Figlotech.BDados/Helpers/ConditionParser.cs:14-105`
- Create: `Figlotech.BDados.Tests/DefinitiveJoinPlanCacheTests.cs`
- Create: `Figlotech.BDados.Tests/ConditionParserPlanTests.cs`

**Step 1: Write failing cache concurrency tests**

Verify:
- concurrent requests for the same `(Type, Shape, FormatVersion)` return the same plan instance,
- different types compile independently,
- scalar and full shapes return different plans,
- compiler invocation count is exactly one per key.

**Step 2: Write failing alias-resolution tests**

Construct `ConditionParser` from a plan and verify:
- known aggregate paths resolve to frozen aliases,
- absent paths in scalar-only shape throw instead of allocating aliases,
- parsing does not mutate the plan or any alias collection.

**Step 3: Implement the cache**

Use a per-key publication pattern such as:

```csharp
ConcurrentDictionary<AutoJoinPlanKey, Lazy<DefinitiveJoinPlan>>
```

Do not use `GetOrAddWithLocking`, whose lock is factory-name scoped rather than plan-key scoped.

**Step 4: Implement plan-backed condition alias resolution**

Add a `ConditionParser(DefinitiveJoinPlan plan)` constructor and replace runtime `PrefixMaker.GetAliasFor` calls in the automatic path with frozen `AliasByPath` lookup.

Keep the old constructor temporarily for non-migrated simple-query paths.

**Step 5: Run cache and parser tests**

Expected: green and deterministic under concurrency.

---

### Task 7: Make provider query generation consume frozen projections

**Files:**
- Create: `Figlotech.BDados/DataAccessAbstractions/IDefinitiveJoinQueryGenerator.cs`
- Modify: `Figlotech.BDados/DataAccessAbstractions/IQueryGenerator.cs:19` only where source-compatible documentation or obsolete annotations are appropriate
- Modify: `Figlotech.BDados.MySqlDataAccessor/MySqlQueryGenerator.cs:322-405`
- Modify: `Figlotech.BDados.PostgreSQLDataAccessor/PgSQLQueryGenerator.cs:295-373`
- Modify: `Figlotech.BDados.SQLiteDataAccessor/SqliteQueryGenerator.cs:235-312`
- Modify: `Figlotech.BDados.Tests/Figlotech.BDados.Tests.csproj`
- Create: `Figlotech.BDados.Tests/ProviderJoinQueryTests.cs`

**Step 1: Add provider project references to the test project**

Reference MySQL, PostgreSQL, and SQLite provider projects so SQL generation can be tested without opening connections.

**Step 2: Write failing provider SQL tests**

For one frozen custom plan and one automatic plan per shape, verify each provider:
- emits `plan.Projection` in exact ordinal order,
- preserves every `ResultAlias`,
- does not alter plan projections after generation,
- includes identifiers already present in the plan without appending them,
- emits an outer root-identifier ordering required by coroutine aggregation,
- preserves current `LEFT JOIN` output during migration.

Capture the plan before and after query generation and assert structural signature and immutable collection references/content are unchanged.

**Step 3: Add the frozen-plan companion contract**

Add a companion interface implemented by built-in providers:

```csharp
public interface IDefinitiveJoinQueryGenerator {
    IQueryBuilder GenerateJoinQuery(
        DefinitiveJoinPlan plan,
        IQueryBuilder conditions,
        int? skip = 0,
        int? limit = 100,
        MemberInfo orderingMember = null,
        OrderingType ordering = OrderingType.Asc,
        IQueryBuilder rootConditions = null);
}
```

Do not add a required method to `IQueryGenerator`, because external provider implementations would break. Retain the old public method. Add an extension/dispatcher that uses `IDefinitiveJoinQueryGenerator` when available. For a legacy external provider, create an ephemeral mutable `JoinDefinition` copy from the frozen plan and call the old method; never expose the canonical plan to mutation, and validate the returned schema. Do not keep two independent SQL implementations inside built-in providers.

**Step 4: Refactor each provider**

Generate joins and projection only from immutable plan members. Remove `Columns.Add`/`Columns.AddRange` from query generation. Keep provider-specific pagination syntax outside the plan.

Do not begin honoring legacy `JoinType` values in this task.

**Step 5: Run provider SQL tests and build provider projects**

```bash
dotnet test Figlotech.BDados.Tests/Figlotech.BDados.Tests.csproj --filter FullyQualifiedName~ProviderJoinQueryTests
dotnet build Figlotech.BDados.MySqlDataAccessor/Figlotech.BDados.MySqlDataAccessor.csproj -c Release --no-restore
dotnet build Figlotech.BDados.PostgreSQLDataAccessor/Figlotech.BDados.PgSQLDataAccessor.csproj -c Release --no-restore
dotnet build Figlotech.BDados.SQLiteDataAccessor/Figlotech.BDados.SqliteDataAccessor.csproj -c Release --no-restore
```

Expected: all green.

---

### Task 8: Replace mutable aggregate materializer plans with an immutable compiled plan

**Files:**
- Create: `Figlotech.BDados/DataAccessAbstractions/CompiledAggregateMaterializerPlan.cs`
- Modify: `Figlotech.BDados/DataAccessAbstractions/RdbmsDataAccessor.Builder.cs:23-333`
- Create: `Figlotech.BDados.Tests/CompiledAggregateMaterializerPlanTests.cs`

**Step 1: Write failing materializer-plan tests**

Create a frozen plan and an `object[]` matching its projection. Verify:
- all root fields populate by fixed plan ordinal,
- aggregate scalar fields convert correctly,
- object/list relations use T-typed identifiers as opaque dictionary keys,
- null/`DBNull` semantics match the conversion contract,
- delegate/relation arrays cannot be obtained and mutated externally.

**Step 2: Run tests and verify RED**

Expected: the immutable compiled plan does not exist.

**Step 3: Implement the compiled plan**

Move expression compilation from `AggregateMaterializerPlan.Build` into a constructor/factory that accepts `DefinitiveJoinPlan` only. Store private readonly immutable arrays or immutable collections. Expose execution methods rather than public delegate arrays.

The compiled plan must not accept reader field types or reader-derived names/ordinals.

**Step 4: Replace relation execution plumbing**

Update `BuildAggregateObject` to invoke the immutable compiled plan APIs. Preserve object/list identity semantics and relation ordering.

**Step 5: Run focused and full tests**

Expected: green.

---

### Task 9: Validate reader schemas and migrate aggregate builders to frozen plans

**Files:**
- Modify: `Figlotech.BDados/DataAccessAbstractions/RdbmsDataAccessor.Builder.cs:623-901`, `:954-983`
- Create: `Figlotech.BDados/DataAccessAbstractions/ReaderSchemaValidator.cs`
- Create: `Figlotech.BDados.Tests/ReaderSchemaValidatorTests.cs`
- Create: `Figlotech.BDados.Tests/AggregateBuilderPlanTests.cs`

**Step 1: Write failing schema validation tests**

Use a fake `DbDataReader`/`IDataRecord` and verify:
- exact ordered aliases pass,
- reordered, missing, duplicated, and unexpected aliases fail with expected/actual diagnostics,
- provider field types do not affect plan identity,
- custom raw commands cannot silently materialize against a mismatched plan.

**Step 2: Implement schema validation**

Validate the reader once after execution and before reading rows. Validation compares ordered result aliases to `plan.Projection`; it does not build or cache a new ordinal map.

Allow an explicit opt-out/uncached custom materialization path only if an existing public use case requires arbitrary command shapes. Do not silently fall back.

**Step 3: Add frozen-plan overloads to aggregate builders**

Change sync, async-list, and coroutine internal paths to consume:

```csharp
DefinitiveJoinPlan plan
CompiledAggregateMaterializerPlan materializer
```

Retain obsolete `JoinDefinition` overloads only as freeze-and-forward adapters.

**Step 4: Remove reader-derived plan lookup from migrated paths**

Stop calling `CreateFieldNamesDict`, `_autoAggregateCache`, and `AggregateMaterializerPlan.GetOrCreate` in migrated methods.

**Step 5: Run aggregate builder tests**

Verify equivalent object graphs across sync, async-list, and coroutine methods.

---

### Task 10: Wire automatic aggregate loading to the canonical plan

**Files:**
- Modify: `Figlotech.BDados/DataAccessAbstractions/RdbmsDataAccessor.cs:3311-3398`
- Modify: `Figlotech.BDados/DataAccessAbstractions/RdbmsDataAccessor.cs:3400-3570`
- Modify: `Figlotech.BDados/DataAccessAbstractions/RdbmsDataAccessor.cs:1780-2067`
- Create: `Figlotech.BDados.Tests/AutomaticAggregateLoadPlanTests.cs`

**Step 1: Write failing wiring tests**

Expose/test the internal plan selection seam and verify:
- `Linear=true` maps to `ScalarAggregatesOnly`,
- `Linear=false` maps to `FullGraph`,
- query generation and materialization receive the same plan instance,
- automatic loads do not access cached mutable `JoinDefinition` or `PrefixMaker`,
- plan count is at most one per requested `(Type, Shape, FormatVersion)`.

**Step 2: Replace automatic mutable caches**

Remove internal use of:
- `CacheAutoPrefixer`
- `CacheAutoPrefixerLinear`
- `CacheAutoJoin`
- `CacheAutoJoinLinear`
- `CacheAutomaticJoinBuilder`
- `CacheAutomaticJoinBuilderLinear`

Resolve the canonical plan through `DefinitiveJoinPlanCache`.

**Step 3: Replace automatic query/materializer calls**

Construct `ConditionParser` from the plan, generate provider SQL from the plan, and pass the same plan/materializer into aggregate building.

**Step 4: Retire parallel automatic traversals**

After differential tests are green and no caller remains, remove or obsolete automatic use of `MakeQueryAggregations` and `MakeBuildAggregations`. Keep only compatibility code needed by custom mutable builders until Task 11.

**Step 5: Run all BDados tests and provider builds**

Expected: green.

---

### Task 11: Remove stale caches, close mutable execution entry points, and verify compatibility

**Files:**
- Modify: `Figlotech.BDados/DataAccessAbstractions/RdbmsDataAccessor.Builder.cs:53-64`, `:494`, `:954-983`
- Modify: `Figlotech.BDados/Builders/JoinObjectBuilder.cs`
- Modify: `Figlotech.BDados/DataAccessAbstractions/IDefinitiveJoinBuilder.cs` or the chosen extension seam
- Modify: provider query generators
- Modify: `docs/plans/2026-07-13-definitive-join-plan-design.md` only if implementation deviations were approved

**Step 1: Add static regression searches/tests**

Verify no executable internal path:
- keys a cache by mutable `JoinDefinition`,
- passes `JoinDefinition` directly to a provider generator,
- appends columns during provider query generation,
- uses cached mutable `PrefixMaker` for aggregate conditions,
- exposes mutable aggregate materializer delegate arrays.

**Step 2: Remove obsolete internal caches**

Delete:
- `_autoAggregateCache`
- mutable `_planCache`
- migrated reader-schema cache creation
- MySQL mutable-join `AutoJoinCache` or replace it with a bounded/canonical-plan skeleton cache if measurement justifies it.

**Step 3: Seal the execution boundary**

All current execution entry points must freeze and forward or accept `DefinitiveJoinPlan`. Mutable types remain builders only. Add `[Obsolete]` messages that point consumers to `BuildPlan`/`Freeze` without immediately removing public members.

**Step 4: Run focused regression suites**

```bash
dotnet test Figlotech.Core.Tests/Figlotech.Core.Tests.csproj -c Release --no-restore
dotnet test Figlotech.BDados.Tests/Figlotech.BDados.Tests.csproj -c Release --no-restore
dotnet test Figlotech.BDados.Analyzers.Tests/Figlotech.BDados.Analyzers.Tests.csproj -c Release --no-restore
```

Expected: all tests pass.

**Step 5: Build the full solution**

```bash
dotnet build figlotech.sln -c Release --no-restore
```

Expected: 0 errors; document any pre-existing warnings separately.

**Step 6: Verify repository hygiene**

```bash
git diff --check
git status --short
git diff --stat
```

Expected: only intended source, tests, solution, and plan/design files are modified.

---

### Task 12: Benchmark and scale-attack the finalized architecture

**Files:**
- Create: `test/DefinitiveJoinPlanBenchmark.cs` or add an equivalent runnable benchmark project if the existing `test` host cannot execute BenchmarkDotNet safely
- Modify: `test/Program.cs` only if necessary to expose an explicit benchmark runner

**Step 1: Add cold-plan benchmarks**

Measure compilation for scalar/full plans with representative graph sizes and verify one build per cache key.

**Step 2: Add warm materialization benchmarks**

Compare:
- current baseline captured before removal,
- frozen-plan compiled materializer,
- 1, 8, 32, and 128 columns,
- null densities of 0%, 10%, 50%, and 100%,
- identifiers of multiple CLR types.

**Step 3: Run on .NET 6**

Report:
- cold compile latency,
- warm ns/row,
- bytes/row,
- Gen0 collections,
- plan-cache hit rate.

**Step 4: Run provider integration probes**

Use deterministic local MySQL, PostgreSQL, and SQLite data where available. Report materialization-only and end-to-end query timings separately.

**Step 5: Attack scale assumptions**

Verify:
- automatic cache cardinality follows requested `(Type, Shape)` pairs,
- custom plan execution does not create an unbounded global cache,
- collectible/plugin type behavior is documented,
- outer root ordering prevents split aggregates,
- no reader schema can mutate or replace a frozen plan.

---

## Final acceptance criteria

- Every executable join is represented by `DefinitiveJoinPlan`.
- Automatic plans are canonical per `(RootType, AggregateJoinShape, FormatVersion)`.
- Custom mutable definitions freeze before execution.
- RID/identifier CLR types remain opaque and T-typed throughout planning and identity caching.
- Query generators are read-only consumers of fixed projections.
- Condition parsing cannot allocate aliases after plan compilation.
- Aggregate materializers use plan-fixed ordinals and expose no mutable internals.
- Reader schema mismatches fail clearly instead of poisoning/reusing a cache.
- Mutable-join aggregate schema/materializer caches are removed.
- Sync, async-list, and coroutine behavior remains equivalent.
- All tests and the full solution build pass.
