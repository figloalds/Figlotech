# Root/Child Aggregate Cycle Validation Design

## Problem

Automatic `DefinitiveJoinPlan` compilation records ancestry as entity types. It therefore rejects a root entity type when that type reappears below an aggregate edge, even when its root-only structural aggregate members are inapplicable in child context and traversal terminates naturally.

The concrete shape is:

```text
FinanceiroParcelas(root)
  -> ObjPagamento: FinanceiroTitulos(child)
    -> Parcelas: FinanceiroParcelas(child)
```

`FinanceiroParcelas.ObjPagamento` has `Flags = "root"`, so the final child occurrence must not traverse back to `FinanceiroTitulos`.

## Design

Represent automatic-compilation ancestry as traversal states containing the entity `Type` and whether that occurrence is the root. Initialize ancestry with `(rootType, true)`. Every structural aggregate child is visited as `(childType, false)`.

A cycle exists only when the exact traversal state repeats in the active ancestry. This permits `(T, true)` followed by `(T, false)`, but still rejects a second `(T, false)` occurrence. Existing flag evaluation remains the authority for deciding which aggregate members apply at each visited state.

No public attribute, cache-key, provider, plan, or materializer API changes are required.

## Failure behavior

Cycle exceptions remain eager and retain root type, ancestry, repeated type, aggregate path, and member diagnostics. The ancestry display continues to show type names so existing diagnostic expectations remain stable.

## Verification

Add a representative object/list model where a root-only object edge returns through a list to the root type. Assert that full-graph compilation succeeds, contains exactly the expected three table occurrences and paths, and does not include the root-only edge beneath the repeated child. Retain the existing unrestricted `A -> B -> A` fixture and assert that it still throws.

Run focused compiler tests, all `Figlotech.BDados.Tests`, the solution build, and `git diff --check`.
