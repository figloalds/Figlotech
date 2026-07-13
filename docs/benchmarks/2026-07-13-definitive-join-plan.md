# Definitive join plan benchmark and scale attack

Recorded 2026-07-13T15:35:36-03:00 from branch `refactor/definitive-join-plan`.

## Scope

This report measures the finalized immutable join architecture in four layers:

1. direct cold `DefinitiveJoinPlanCompiler.Compile(Type, shape)` cost;
2. warm `CompiledAggregateMaterializerPlan.Materialize<T>` cost normalized per row;
3. warm `AutomaticJoinPlanCache.GetOrAdd(Type, shape)` cost and canonical hit behavior;
4. a deterministic SQLite end-to-end probe through frozen SQL generation, reader schema validation, and compiled aggregate materialization.

It does not claim a comparison with the removed mutable implementation. No reliable pre-removal benchmark was captured before Task 11, so a baseline comparison is **unavailable** rather than reconstructed or fabricated.

Raw compact datasets:

- [`data/definitive-join-plan-cold.csv`](data/definitive-join-plan-cold.csv)
- [`data/definitive-join-plan-warm.csv`](data/definitive-join-plan-warm.csv)
- [`data/definitive-join-plan-cache.csv`](data/definitive-join-plan-cache.csv)

## Environment and configuration

- BenchmarkDotNet 0.13.1
- .NET 6.0.36, x64 RyuJIT
- concurrent server GC
- Windows 10.0.26200 as reported by BenchmarkDotNet
- .NET SDK 10.0.301
- `Job.ShortRun`: one launch, three warmups, three measured iterations
- `MemoryDiagnoser` enabled
- warm materialization uses 256 uniquely identified rows per invocation and `OperationsPerInvoke = 256`; reported time and allocation are therefore per row

The .NET SDK emits `NETSDK1138` because .NET 6 is out of support. The benchmark intentionally targets .NET 6 because that is the repository's existing runtime constraint.

## Verification results

The non-timed verification mode completed successfully:

```text
WARM_SCENARIOS_VERIFIED=64
COLD_PLANS_VERIFIED=6
PLAN_CACHE_KEYS=6
PLAN_CACHE_HIT_RATE=100.00%
CUSTOM_PLAN_GLOBAL_CACHE_KEYS=0
BENCHMARK_VERIFICATION_PASS
```

The verifier checks:

- every identifier/column/null-density scenario materializes all 256 unique roots;
- scalar/full compilation returns the requested root and shape;
- concurrent automatic requests publish one canonical instance for each `(Type, Shape, FormatVersion)` key;
- 1,200 subsequent automatic requests are reference-equal cache hits;
- a reader-schema mismatch throws without mutating signature or projection;
- provider SQL contains deterministic root-identifier ordering;
- no static cache is keyed by mutable `JoinDefinition`.

## Cold plan compilation

| Graph size | Shape | Mean | Allocated |
|---|---|---:|---:|
| Small | Scalar aggregates | 33.02 µs | 63 KB |
| Small | Full graph | 32.76 µs | 63 KB |
| Medium | Scalar aggregates | 88.90 µs | 194 KB |
| Medium | Full graph | 91.53 µs | 194 KB |
| Large | Scalar aggregates | 101.60 µs | 211 KB |
| Large | Full graph | 233.46 µs | 480 KB |

The large full graph is the scale outlier at about 2.3× the large scalar latency and 2.3× its allocation. This cost is paid once per automatic cache key; it is not on the warm materialization path.

## Warm materialization

The full 64-case matrix is in the warm CSV. The table below shows 0% null density, which exercises every projected setter.

| Identifier | 1 column | 8 columns | 32 columns | 128 columns |
|---|---:|---:|---:|---:|
| `int` | 77.39 ns / 234 B | 667.68 ns / 626 B | 2.885 µs / 1,970 B | 11.604 µs / 7,346 B |
| `long` | 109.04 ns / 234 B | 654.21 ns / 626 B | 3.108 µs / 1,970 B | 12.633 µs / 7,346 B |
| `Guid` | 87.74 ns / 250 B | 727.03 ns / 642 B | 2.691 µs / 1,986 B | 10.695 µs / 7,362 B |
| `string` | 76.41 ns / 210 B | 710.76 ns / 602 B | 2.384 µs / 1,946 B | 11.459 µs / 7,322 B |

Observed scale behavior:

- latency and allocation grow approximately linearly with projection width;
- identifier CLR type is a secondary factor compared with column count;
- high null density generally reduces setter/conversion work and allocation, although three-iteration ShortRun noise produces individual non-monotonic means;
- BenchmarkDotNet reported Gen0 values of `0.0001` collections per 1,000 rows for several one-column cases and no displayable Gen0 rate (`-`) for the remaining short samples. Exact values are preserved in the CSV.

At 128 columns and 0% null density, the measured envelope is 10.695–12.633 µs/row and 7,322–7,362 B/row.

## Automatic plan cache

| Shape | Warm lookup mean | Allocated |
|---|---:|---:|
| Scalar aggregates | 84.69 ns | 24 B |
| Full graph | 82.39 ns | 24 B |

The executable verifier observed 100% reference-equal hits after initial population. Cache cardinality followed the six requested `(Type, Shape)` pairs exactly.

`AutomaticJoinPlanCache` strongly retains its `Type` keys. Consequently, automatically cached collectible/plugin types cannot unload after first use. This is a known lifecycle trade-off of the canonical process-wide cache. Custom definitive plans are not inserted into that dictionary; compiled materializers are weakly keyed by plan identity.

## SQLite provider probe

A temporary file-backed SQLite database was seeded outside the timed sections with 256 rows and eight projected columns.

| Probe | Result |
|---|---:|
| Materialization-only stopwatch probe | 959.70 ns/row |
| Frozen generated SQL + reader + validation + materialization | 3,590.35 ns/row |

The provider probe uses the production `SqliteQueryGenerator`, `RdbmsDataAccessor`, frozen plan, schema validator, and compiled builder. It uses a benchmark-local connection adapter because the repository's `SqlitePluginConfiguration` emits `Max Pool Size`, which `Microsoft.Data.Sqlite` 7.0.12 rejects. That pre-existing provider configuration incompatibility is outside this architecture task.

MySQL and PostgreSQL end-to-end probes are reported unavailable because no deterministic local servers are configured. No synthetic provider result is substituted.

## Correctness and methodology constraints

- Every input row has a unique, non-null identifier so identity deduplication cannot undercount roots.
- Null density applies only to non-identifier columns; null identifiers would measure row rejection rather than materialization.
- One-column scenarios contain only the identifier, so their 0/10/50/100% null variants are semantically identical. Differences between those points are measurement noise.
- Row arrays are immutable and reused between invocations. Each `Materialize` call creates fresh identity contexts and output objects.
- Wide plans use unique source columns/result aliases but intentionally target the same nullable `Value` destination member. This attacks projection dispatch, null handling, conversion, and setter execution without introducing 128 artificial CLR properties. It does not model instruction/data-cache effects of 128 distinct destination members.
- `ShortRun` is suitable for scale reconnaissance, not regression gating. Several 99.9% confidence intervals are wider than the means. A stable CI gate should use more launches/iterations on controlled hardware.
- The provider stopwatch numbers are deterministic smoke measurements, not BenchmarkDotNet-quality provider baselines.

## Reproduction

```bash
dotnet build Figlotech.BDados.Benchmarks/Figlotech.BDados.Benchmarks.csproj -c Release
dotnet run --project Figlotech.BDados.Benchmarks/Figlotech.BDados.Benchmarks.csproj -c Release --no-build -- --verify
dotnet run --project Figlotech.BDados.Benchmarks/Figlotech.BDados.Benchmarks.csproj -c Release --no-build -- --providers
dotnet run --project Figlotech.BDados.Benchmarks/Figlotech.BDados.Benchmarks.csproj -c Release --no-build -- --filter '*ColdPlanCompilationBenchmarks*'
dotnet run --project Figlotech.BDados.Benchmarks/Figlotech.BDados.Benchmarks.csproj -c Release --no-build -- --filter '*WarmMaterializationBenchmarks*'
dotnet run --project Figlotech.BDados.Benchmarks/Figlotech.BDados.Benchmarks.csproj -c Release --no-build -- --filter '*AutomaticPlanCacheBenchmarks*'
```

## Architectural conclusion

The frozen architecture's warm path scales predictably with projected column count, automatic cache lookup is sub-100 ns in this run, and automatic cache cardinality is bounded by requested root/shape keys. The main quantified costs are cold full-graph compilation/allocation and per-row allocations in very wide projections. The main lifecycle limitation is strong retention of automatically cached `Type` keys. No evidence from this scale attack requires reopening the finalized architecture, but a future performance gate should establish a committed baseline on controlled hardware before assigning regression thresholds.
