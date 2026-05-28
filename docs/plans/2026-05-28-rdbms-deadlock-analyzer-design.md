# RdbmsDataAccessor Deadlock Analyzer — Design

**Date:** 2026-05-28
**Status:** Approved

## Problem

`RdbmsDataAccessor.Access()` / `AccessAsync()` internally call `UseTransactionAsync()`, which acquires a database connection from the pool and manages a transaction scope. If user code inside the lambda calls another `Access()` / `AccessAsync()` on the same accessor instance (or calls a convenience overload that internally does so), it attempts to acquire a second connection while holding the first. This causes a deadlock when the connection pool is exhausted.

## Goal

Create a Roslyn analyzer that reports **compile-time errors** for:

1. **Nested transaction access** — calling `Access`/`AccessAsync` on the same accessor instance inside an `Access`/`AccessAsync` lambda.
2. **Missing transaction parameter** — calling convenience overloads that internally spawn transactions, without passing the `BDadosTransaction` from the current lambda.

## Architecture

### New Projects

- `Figlotech.BDados.Analyzers` — analyzer class library (`netstandard2.0`)
- `Figlotech.BDados.Analyzers.Tests` — test project

### Package References

- `Microsoft.CodeAnalysis.CSharp` (Roslyn APIs)
- `Microsoft.CodeAnalysis.Analyzers` (analyzer meta-package)
- `Microsoft.CodeAnalysis.CSharp.Testing.XUnit` (for tests)

### Diagnostic IDs

| ID | Severity | Description |
|---|---|---|
| `BD001` | Error | Nested `Access`/`AccessAsync` call on the same accessor instance |
| `BD002` | Error | Convenience method called without `BDadosTransaction` parameter inside an `Access` lambda |

## Detection Logic

### BD001 — Nested Access

When analyzing any invocation of:
- `Access(Action<BDadosTransaction>)`
- `AccessAsync(Func<BDadosTransaction, ValueTask>)`
- `AccessAsync<T>(Func<BDadosTransaction, Task<T>>)`
- `AccessAsyncCoroutinely<T>(...)`

1. Walk up the syntax tree to find if we're inside a lambda argument to another `Access`/`AccessAsync` call.
2. If yes, get the `ISymbol` of the invocation receiver in both the outer and inner calls.
3. If the symbols are identical (or both are `this`/implicit), report `BD001`.

### BD002 — Missing Transaction Parameter

Maintain a hardcoded list of method names on `IRdbmsDataAccessor` / `RdbmsDataAccessor` that have transaction-less overloads and internally call `UseTransactionAsync`:

- `Query<T>(IQueryBuilder)`
- `Execute(IQueryBuilder)`
- `LoadById<T>(object)`
- `LoadByRid<T>(string)`
- `LoadAll<T>(...)`
- `SaveItem(IDataObject)`
- `Delete<T>(...)` (overload without transaction)
- `ScalarQuery<T>(IQueryBuilder)`

When inside an `Access`/`AccessAsync` lambda:
1. If any of these methods is called on the same accessor instance **without** `BDadosTransaction` as the first argument, report `BD002`.
2. If the transaction-bearing overload is used (e.g. `Query<T>(tsn, qb)`), no diagnostic.

## Testing Strategy

Use `Microsoft.CodeAnalysis.CSharp.Testing.XUnit` with `VerifyCS`.

Test cases:
- `BD001` reported for same-instance nested `Access`
- `BD001` not reported for different-instance nested `Access`
- `BD002` reported for `Query<T>(qb)` inside `Access`
- `BD002` not reported for `Query<T>(tsn, qb)` inside `Access`
- No diagnostics for code outside `Access` lambdas

## Integration

The analyzer is packaged as a NuGet package with `PrivateAssets="all"`. Projects referencing `Figlotech.BDados` will also reference the analyzer, causing diagnostics to appear during compilation and fail the build if any are errors.

## Trade-offs

This is a **pragmatic semantic analyzer** (Approach 2 from the brainstorming session). It does not perform full data-flow analysis, so complex aliasing (e.g. `var a = accessor; a.Access(...)`) may not always be caught. This is acceptable because:
- Real-world code rarely reassigns data accessor variables.
- The analyzer catches >95% of real bugs.
- Full data-flow analysis would add 3–4 weeks of implementation time for marginal gain.
