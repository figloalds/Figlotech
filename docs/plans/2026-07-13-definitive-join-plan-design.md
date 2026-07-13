# Definitive Join Plan Design

## Date
2026-07-13

## Goal
Replace the shared mutable `JoinDefinition` execution lifecycle with a single compilation boundary that produces one deeply immutable `DefinitiveJoinPlan`. Query generation, condition alias resolution, aggregate materialization, and downstream caching must consume only the frozen plan.

## Scope
- Automatic aggregate joins for both current load shapes.
- Public/custom joins built through `IJoinBuilder`.
- Provider query generators for MySQL, PostgreSQL, and SQLite.
- Aggregate materializer planning and reader-schema validation.
- Alias allocation and aggregate relation validation.

## Non-goals
- Redefining the legacy identifier model.
- Changing identifier values to strings or longs.
- Correcting every existing join semantic during the same refactor.
- Changing public query condition, pagination, ordering, or lifecycle-hook behavior.
- Immediately honoring configured `JoinType` values where providers currently emit `LEFT JOIN`; compatibility comes first.

## Identifier Semantics
`RID` is treated semantically as the entity type's identifier column. Its CLR type is the type declared by the entity member and must remain opaque to join planning and identity caches.

The definitive plan records identifier metadata for every joined table:
- identifier member,
- database column name,
- CLR type,
- projected ordinal/result alias.

No materializer or aggregate identity cache may assume that RID is a `string`, `long`, or any other fixed CLR type.

## Current Failure Mode
The current lifecycle has no finalization boundary:

1. A mutable `JoinDefinition` is built and cached by root `Type` and the linear/full split.
2. `GenerateRelations()` clears and rebuilds relations.
3. `MakeQueryAggregations` mutates tables and projected columns.
4. `MakeBuildAggregations` mutates/reclassifies relations after the join is cached.
5. Provider query generators append identifier columns during query generation.
6. A mutable cached `PrefixMaker` can allocate more aliases while conditions are parsed.
7. The first reader schema becomes the effective projection and supplies ordinals.
8. Mutable aggregate materializer plans are cached by the mutable join's reference identity.

This makes the reader the first point where the result shape is finally known and allows stale query, ordinal, and materializer caches.

## Approved Architecture

### 1. Explicit aggregate load shape
Replace the internal boolean distinction with:

```csharp
public enum AggregateJoinShape {
    ScalarAggregatesOnly,
    FullGraph
}
```

`ScalarAggregatesOnly` preserves aggregate scalar and far-field joins but omits aggregate object/list graph construction. `FullGraph` includes aggregate objects and lists recursively.

Public `Linear` options can remain temporarily and map to this enum at the API boundary.

### 2. Mutable construction is isolated
Mutable state remains useful while defining a join, but it must be confined to a builder:

```text
Automatic: (RootType, AggregateJoinShape) -> DefinitiveJoinPlanCompiler
Custom:    JoinDefinition builder -> Freeze() -> DefinitiveJoinPlan
```

`JoinDefinition`, `JoiningTable`, `Relation`, `JoinConfigureHelper`, and `BuildParametersHelper` may remain as compatibility/building types during migration. They are not valid query-generator or materializer inputs after the freeze boundary.

### 3. Single-pass definitive plan compiler
Automatic plans are compiled in one traversal of aggregate metadata. The traversal must produce query topology and materialization relations together instead of maintaining separate `MakeQueryAggregations` and `MakeBuildAggregations` passes.

The compiler performs:
1. Root table and identifier discovery.
2. Deterministic alias allocation.
3. Aggregate scalar/far/object/list traversal according to `AggregateJoinShape`.
4. Join predicate and relation construction.
5. Required identifier and relation-key projection.
6. Stable projection ordering and result-alias generation.
7. Ordinal assignment.
8. Aggregate graph/cycle validation.
9. Root ordering requirement construction.
10. Immutable materializer-plan construction.
11. Deep freeze.

### 4. Deeply immutable plan
`DefinitiveJoinPlan` and every nested value use constructor-only/get-only state and immutable collections. No mutable list, array, dictionary, field, or settable delegate array is exposed.

Conceptual shape:

```text
DefinitiveJoinPlan
  RootType
  Shape
  RootTableIndex
  Tables: ImmutableArray<DefinitiveJoinTable>
  Relations: ImmutableArray<DefinitiveJoinRelation>
  Projection: ImmutableArray<DefinitiveProjectionColumn>
  AliasByPath: ImmutableDictionary<AggregatePath, string>
  TableIndexByAlias: ImmutableDictionary<string, int>
  RootOrdering
  Materializer
  StructuralSignature
  FormatVersion

DefinitiveJoinTable
  EntityType
  TableName
  Alias
  Prefix
  JoinKind
  JoinPredicate
  Identifier
  ProjectedColumns

DefinitiveIdentifier
  Member
  ColumnName
  ClrType
  ProjectionOrdinal
  ResultAlias

DefinitiveJoinRelation
  ParentTableIndex
  ChildTableIndex
  ParentKey
  ChildKey
  BuildKind
  TargetMember
  SourceFields

DefinitiveProjectionColumn
  Ordinal
  TableIndex
  SourceColumn
  ResultAlias
  DestinationMember
  DestinationType
```

`AggregateMaterializerPlan` and `AggregateRelationPlan` are replaced or encapsulated so their delegate arrays and relation details are private readonly immutable snapshots.

### 5. Canonical automatic plan cache
Automatic plans use one cache:

```text
AutoJoinPlanKey(RootType, AggregateJoinShape, FormatVersion)
    -> Lazy<DefinitiveJoinPlan>
```

The expected upper bound is two plans per aggregate root type for the current format version. `Lazy` or equivalent per-key publication prevents duplicate compilation without serializing unrelated root types.

The plan is the canonical cache value and runtime contract. Downstream aggregate schema and materializer caches should be removed where the finalized plan already contains that information.

### 6. Custom joins
Every custom join must freeze before execution. Built-in query generators and aggregate builders accept `DefinitiveJoinPlan`, never a mutable `JoinDefinition`.

Do not add required members directly to the public `IJoinBuilder` or `IQueryGenerator` interfaces during migration because that would break external implementations. Introduce companion interfaces/extensions. A legacy external query generator may receive an ephemeral mutable projection adapter copied from the frozen plan; it must never receive or mutate the canonical plan, and its result schema remains subject to validation.

Equivalent custom plans are not automatically placed in the global `(Type, Shape)` cache. A custom plan may own its compiled execution artifacts directly. Structural interning, if later needed, must use a bounded cache keyed by a canonical structural signature.

### 7. Plan identity and equality
Deep immutability does not by itself provide structural equality. Canonical automatic plans may use reference identity downstream because one instance is published per automatic cache key.

`StructuralSignature` is computed once from all execution-relevant fields for diagnostics, custom-plan comparison, provider skeleton caching, and format migration. Hash collisions must not be treated as structural equality without confirmation.

### 8. Immutable alias resolution
Alias allocation occurs only during plan compilation. The frozen plan stores path-to-alias mappings.

`ConditionParser` resolves aliases from the plan. It must not allocate a new alias at query time. A condition that references an aggregate path absent from the chosen plan shape fails with an actionable exception.

The cached mutable `PrefixMaker` is removed from the runtime automatic-load path.

### 9. Provider query generation
The built-in provider implementations consume `DefinitiveJoinPlan` through a companion query-generator contract plus dynamic invocation values:
- conditions,
- root conditions,
- pagination,
- ordering.

Providers must:
- emit projection columns in exactly `plan.Projection` order,
- preserve result aliases,
- never append identifier columns,
- never mutate plan tables, relations, or projections,
- enforce the plan's outer root-ordering requirement for coroutine materialization.

Initial migration preserves the current de-facto `LEFT JOIN` emission even when legacy mutable definitions contain another `JoinType`. Correct join-kind behavior is a separate compatibility change.

### 10. Materialization
The aggregate materializer consumes the immutable plan's fixed ordinals. The reader supplies row values only and no longer defines the plan.

The portable conversion path uses runtime `object` values and compiled direct assignments. Reader-side provider types are not part of automatic plan identity.

For custom raw commands, the returned schema must match the frozen projection. A mismatch fails validation or uses an explicit uncached/custom materialization path; it must never silently reuse stale ordinals.

## Validation Rules
A plan cannot freeze unless:
- aliases and result aliases are unique,
- root and child identifiers are known and projected,
- identifier CLR types are retained without coercion assumptions,
- all relation table indices and keys are valid,
- all aggregate destination members exist and are writable where required,
- every materializer ordinal maps to the frozen projection,
- recursive aggregate graph traversal is cycle-safe,
- selected shape semantics are respected,
- root ordering required by streaming aggregation is representable,
- no provider mutation is needed to make the projection executable.

## Error Handling
Plan compilation errors include root type, aggregate path, member, and relation context. Schema mismatch errors include expected and actual ordered result aliases. Unsupported provider conversion errors identify source runtime type and destination member/type.

Failures occur before row materialization whenever possible. The system does not silently rebuild a different plan under the same key.

## Compatibility and Migration
1. Introduce immutable plan types and freeze validation without changing public execution paths.
2. Add adapters that freeze existing custom builders.
3. Build the single-pass automatic compiler and differential-test it against current definitions.
4. Move provider generators to frozen projections.
5. Move condition parsing to immutable alias maps.
6. Move aggregate builders to the immutable materializer.
7. Remove reader-derived aggregate schema caching.
8. Remove mutable automatic join and prefix caches.
9. Obsolete mutable execution overloads; retain builder compatibility for a deprecation window.

## Testing Strategy

### Unit tests
- Same `(RootType, Shape)` returns the same automatic plan instance under concurrency.
- Full and scalar-only plans are distinct and deterministic.
- Scalar-only plans retain scalar/far fields and omit object/list graph relations.
- Every nested collection is immutable and prior frozen plans are unaffected by later builder mutations.
- Identifier metadata preserves `T`-typed RID values.
- Alias and projection ordering are deterministic.
- Cycles and invalid relations fail at freeze time.
- Custom joins always freeze before execution.

### Differential tests
- Compare old and new table topology, aliases, projection, and relation behavior for representative models.
- Snapshot SQL for MySQL, PostgreSQL, and SQLite.
- Preserve current `LEFT JOIN` behavior during the architecture migration.
- Compare sync, async-list, and coroutine aggregate results.

### Materializer tests
- Reordered/missing/unexpected columns fail schema validation.
- Null/`DBNull`, nullable members, enums, numeric conversions, `DateTimeOffset`, `Guid`, `byte[]`, `ValueBox<T>`, and provider-specific values.
- Multiple RID CLR types participate correctly in identity caches.
- Repeated root rows are contiguous or rejected according to ordering validation.

### Performance tests
- Cold plan compilation per `(RootType, Shape)`.
- Warm SQL generation and materialization.
- Allocation per row and per plan.
- 1, 8, 32, and 128 projected columns.
- Real MySQL, PostgreSQL, and SQLite readers.

## Risks
- Public API breakage because mutable join models are currently exposed.
- Attribute graphs may contain cycles previously hidden by uncontrolled recursion.
- Provider-specific lazy identity-column additions may differ today.
- Enforcing outer root ordering can affect database execution plans.
- Preserving versus correcting ignored `JoinType` values must remain explicit.
- Static `Type` caches retain collectible plugin assemblies; plugin-host scenarios may require scoped or weak caches.

## Verification
- Build `Figlotech.BDados` and all provider projects.
- Run `Figlotech.Core.Tests` and new BDados plan/materializer tests.
- Run provider SQL snapshot tests.
- Confirm `git diff --check` and a clean source tree outside intended files.

## References
- `Figlotech.BDados/DataAccessAbstractions/JoinDefinition.cs`
- `Figlotech.BDados/DataAccessAbstractions/JoiningTable.cs`
- `Figlotech.BDados/DataAccessAbstractions/Relation.cs`
- `Figlotech.BDados/Builders/JoinObjectBuilder.cs`
- `Figlotech.BDados/Helpers/JoinConfigureHelper.cs`
- `Figlotech.BDados/Helpers/BuildParametersHelper.cs`
- `Figlotech.BDados/Helpers/PrefixMaker.cs`
- `Figlotech.BDados/Helpers/ConditionParser.cs`
- `Figlotech.BDados/DataAccessAbstractions/RdbmsDataAccessor.cs`
- `Figlotech.BDados/DataAccessAbstractions/RdbmsDataAccessor.Builder.cs`
- Provider query generators for MySQL, PostgreSQL, and SQLite
