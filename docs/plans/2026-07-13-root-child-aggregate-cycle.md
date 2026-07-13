# Root/Child Aggregate Cycle Validation Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make automatic aggregate cycle validation distinguish root and child traversal contexts while continuing to reject genuine child-context cycles.

**Architecture:** Replace bare-type ancestry entries inside automatic compilation with immutable traversal-state values `(Type, IsRoot)`. Structural children always enter with `IsRoot = false`; cycle detection compares complete states, while diagnostics continue to print type names.

**Tech Stack:** C#, .NET, xUnit, immutable `DefinitiveJoinPlan` compilation.

---

### Task 1: Add the root-bounded regression fixture and test

**Files:**
- Modify: `Figlotech.BDados.Tests/PlanTestModels.cs:88`
- Modify: `Figlotech.BDados.Tests/AutomaticDefinitiveJoinPlanCompilerTests.cs:125`

**Step 1: Add fixture types**

Create a root type with a root-only `AggregateObject`, a payment/title type with an `AggregateList` returning to the root type, and foreign-key fields needed by both joins.

**Step 2: Add a failing compiler test**

Compile the root as `FullGraph` and assert:

- entity types are root, title, root;
- paths contain `ObjPagamento` and `ObjPagamento.Parcelas`;
- no path exists below `ObjPagamento.Parcelas` for another `ObjPagamento`;
- aggregate relations contain exactly one object edge and one list edge.

**Step 3: Run the focused test and capture RED**

Run:

```bash
dotnet test Figlotech.BDados.Tests/Figlotech.BDados.Tests.csproj --filter FullyQualifiedName~CompileAutomaticAllowsRootTypeToRepeatAsChildWhenRootOnlyEdgeStopsTraversal
```

Expected: FAIL with the existing `Automatic aggregate cycle` exception.

### Task 2: Make ancestry context-aware

**Files:**
- Modify: `Figlotech.BDados/DataAccessAbstractions/DefinitiveJoinPlanCompiler.cs:138-271`

**Step 1: Add an immutable internal traversal-state value**

Add a private value type containing `Type EntityType` and `bool IsRoot`, with equality and hash code based on both values.

**Step 2: Thread traversal states through automatic compilation**

- Initialize ancestry with `(rootType, true)`.
- Change `Visit`, `EmitAggregateObject`, `EmitAggregateList`, and `VisitChild` ancestry parameters to the traversal-state type.
- In `VisitChild`, compare `(childType, false)` against ancestry.
- Append that state and visit with `isRoot: false`.
- Preserve exception text by projecting ancestry entries to `EntityType.Name`.

**Step 3: Run focused cycle tests and capture GREEN**

Run:

```bash
dotnet test Figlotech.BDados.Tests/Figlotech.BDados.Tests.csproj --filter FullyQualifiedName~CompileAutomaticAllowsRootTypeToRepeatAsChildWhenRootOnlyEdgeStopsTraversal

dotnet test Figlotech.BDados.Tests/Figlotech.BDados.Tests.csproj --filter FullyQualifiedName~CompileAutomaticFullGraphRejectsCyclesButScalarGraphDoesNotTraverseThem
```

Expected: both PASS.

### Task 3: Verify regression safety

**Files:** none unless a failure reveals an in-scope defect.

**Step 1: Run all BDados tests**

```bash
dotnet test Figlotech.BDados.Tests/Figlotech.BDados.Tests.csproj
```

Expected: PASS.

**Step 2: Build the solution**

```bash
dotnet build figlotech.sln --no-restore
```

Expected: successful build, subject to documented external test-project dependencies.

**Step 3: Inspect worktree integrity**

```bash
git diff --check
git status --short
git diff -- Figlotech.BDados/DataAccessAbstractions/DefinitiveJoinPlanCompiler.cs Figlotech.BDados.Tests/PlanTestModels.cs Figlotech.BDados.Tests/AutomaticDefinitiveJoinPlanCompilerTests.cs
```

Confirm the pre-existing `Figlotech.BDados.MySqlDataAccessor/MySqlQueryGenerator.cs` modification is untouched and no unrelated files changed.

No commit is included because the user did not request one.
