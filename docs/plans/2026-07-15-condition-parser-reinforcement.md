# ConditionParser Reinforcement Implementation Plan

> **For Hermes controller:** REQUIRED SUB-SKILL: Use `subagent-driven-development` to dispatch this plan task-by-task; do not implement production code in the controller context.

**Goal:** Reinforce `ConditionParser` with regression-focused automated tests, correct proven SQL-semantic defects, eliminate unsafe expression/method handling, and support parameterized collection membership equivalent to `IN`.

**Architecture:** Keep the existing public constructors, frozen `DefinitiveJoinPlan`/`DefinitiveAliasResolver` boundaries, `PrefixMaker` root-only compatibility behavior, and `QueryBuilder` output contract. Harden the existing recursive translator with small exact-classification helpers rather than rewriting the entire parser. Compile captured/local values through a weakly held cache, classify method families by declaring type and signature rather than name alone, and translate evaluable local `IEnumerable` membership to parameterized SQL while rejecting query-dependent collection sources and unsupported overloads actionably.

**Tech Stack:** C# / .NET Standard 2.1 library, .NET 6 xUnit tests, expression trees, `QueryBuilder`, frozen aggregate join plans.

**Mandatory Coding Standard:** `C:/Users/felyp/Desktop/Projetos/AGENTS_CODING_STYLE.md` (every implementer and code-quality/fix agent must read it before editing or reviewing)

---

## Classification and baseline

- **Workflow:** NON-TRIVIAL.
- **Why:** This changes observable SQL semantics, expression-tree dispatch, aggregate predicate behavior, cache retention, and adds a new supported expression family.
- **Isolated worktree:** `C:/Users/felyp/Desktop/Projetos/Figlotech/.worktrees/condition-parser-reinforcement`
- **Baseline:** `dotnet test Figlotech.BDados.Tests/Figlotech.BDados.Tests.csproj --no-restore -v:minimal` passes 229/229; `ConditionParserFrozenAliasTests` passes 25/25.
- **Known unrelated verifier blocker:** `dotnet restore figlotech.sln` fails because `figlotech.sln` references missing `test/test.csproj`. Do not repair that unrelated solution defect in this task; use the authoritative project-level restore/test/build commands below.
- **EOL constraints:** `ConditionParser.cs` and existing parser tests are LF-only/no BOM. `Qh.cs` is UTF-8 BOM + CRLF and must retain that exact convention without whole-file churn.

## Audit evidence that must be locked down

1. `72e991df` migrated parser aliases and collection lambdas to frozen plans but enlarged the semantic risk surface.
2. `163827d2` repaired root-condition projection after joined predicates could produce malformed or over-restrictive root SQL.
3. `29458e58` repaired case-sensitive root-alias stripping.
4. `17c5fd96` repaired ordinary root predicates so they do not compile unrelated invalid aggregate metadata.
5. Current `String.StartsWith`/`EndsWith` wildcard placement is incorrect.
6. Captured nulls and null-on-left comparisons are not normalized to `IS NULL`/`IS NOT NULL`.
7. Method dispatch uses method names broadly enough to misclassify collection `Contains` as string `Contains`, and other unsupported same-name methods as supported SQL operations.
8. `Where(predicate).Any()` omits the collection identifier guard that direct `Any(predicate)` emits.
9. `First(predicate)` silently ignores its predicate.
10. `_compiledExpressionCache` strongly retains expression trees and their captured closure objects indefinitely.
11. CLR-side `Qh.In` compares boxed selector/member values with `object ==`, which is reference equality for boxed value types, and `Qh.NotIn` returns `Any(...)` rather than its negation.
12. Documented `QueryComparisonAttribute(IgnoreCase)` falls through to ordinary case-sensitive equality.
13. `ConditionParser` stores the active root type, resolver, and parameter counter as mutable instance state without a same-instance concurrency boundary.
14. String LIKE translations do not escape `%`, `_`, or the escape marker, so user values are treated as SQL patterns instead of CLR literal substrings/prefixes/suffixes.

## Global acceptance criteria

- Preserve every existing frozen alias, root projection, constructor, provider, and ordinary-root boundary test.
- SQL fragments never contain an empty binary operand or silently omit an unsupported method/predicate.
- All values originating outside mapped members remain parameters; never inline collection values.
- Existing `Qh.In`/`Qh.NotIn` parser behavior remains compatible, including empty/null list SQL behavior already defined by `Qb`.
- Standard captured collection membership supports instance `Contains` and `Enumerable.Contains` for local/evaluable enumerables, including arrays, lists, sets, and local LINQ projections.
- Collection membership rejects comparer overloads and query-root-dependent sources with an actionable wrapped exception.
- Empty or null local membership sources produce `FALSE`; a null element is represented with `IS NULL` so nullable-member semantics match CLR `Contains`.
- Negated membership preserves the logical negation of the complete membership fragment.
- Root-only membership works through default/`PrefixMaker` parsers without aggregate-plan compilation; aggregate-member membership uses only aliases published by the selected frozen plan; `fullConditions:false` preserves root membership and drops joined membership according to the existing implication-safe projection rules.
- Captured expression caching must not strongly root transient expression trees/closures.
- No public API is removed or widened solely for tests.
- No new package dependency is added.

### Task 1: Characterize and repair existing ConditionParser semantics

**Files:**
- Create: `Figlotech.BDados.Tests/ConditionParserBehaviorTests.cs`
- Modify: `Figlotech.BDados/Helpers/ConditionParser.cs`
- Modify: `Figlotech.BDados/Builders/Qh.cs`
- Modify only if a fixture is genuinely needed: `Figlotech.BDados.Tests/PlanTestModels.cs`

**Step 1: Read required context before editing**

Read completely:
- `C:/Users/felyp/Desktop/Projetos/AGENTS_CODING_STYLE.md`
- `AGENTS.md`
- `Figlotech.BDados/Helpers/ConditionParser.cs`
- `Figlotech.BDados/Builders/Qh.cs`
- `Figlotech.BDados.Tests/ConditionParserFrozenAliasTests.cs`
- `Figlotech.Core/Data/QueryBuilder.cs`

Explicitly confirm the shared guide was read. Stop if it is inaccessible or materially conflicts with the repository/requirements.

**Step 2: RED — add one vertical regression slice at a time**

Add focused xUnit tests, running each new behavior before production edits:

1. Captured-null equality and inequality emit `IS NULL` / `IS NOT NULL`, bind no null parameter, and work with null on either side.
2. `String.Contains`, `StartsWith`, and `EndsWith` emit exact wildcard shapes:
   - contains: `LIKE CONCAT('%', @value, '%')`
   - starts-with: `LIKE CONCAT(@value, '%')`
   - ends-with: `LIKE CONCAT('%', @value)`
3. Instance string `Equals` parses the actual mapped member expression and parameter; unsupported/static overloads either receive an explicitly tested correct translation or fail actionably—never null-reference or emit partial SQL.
4. `AggregateList.Where(item => predicate).Any()` emits the same typed identifier non-null guard as direct `Any(item => predicate)` and preserves the predicate once.
5. `First(predicate)` fails with an actionable unsupported-expression diagnostic rather than silently discarding its predicate. Preserve existing `First().Member` compatibility.
6. A deliberately unsupported method and unsupported binary node fail actionably rather than producing empty/partial SQL.
7. Passing a non-null public `strBuilder` appends/returns the parsed condition without self-appending, duplicate SQL, or parameter loss.
8. CLR-side `Qh.In` uses value equality for selector/member results, and `Qh.NotIn` is its logical complement for null, empty, matching, and nonmatching lists, including boxed value types.

For each slice run:

```bash
dotnet test Figlotech.BDados.Tests/Figlotech.BDados.Tests.csproj --no-restore --filter "FullyQualifiedName~ConditionParserBehaviorTests.<ExactTestName>" -v:minimal
```

Expected RED: an assertion failure or actionable unsupported-behavior mismatch proving the current defect. A typo/harness failure is not valid RED.

**Step 3: GREEN — implement only the required semantic repair**

Use small helpers with exact responsibilities:

- Normalize null comparison operands only when an operand is safely evaluable and evaluates to null; do not execute expressions containing the query parameter.
- Map supported binary node types explicitly and throw `NotSupportedException` for unknown nodes.
- Classify string operations by declaring type/signature, not `Method.Name` alone.
- Share aggregate collection context/identifier logic so `Any(predicate)` and `Where(predicate).Any()` have identical existence guards.
- Reject `First(predicate)` explicitly until true first-row semantics are designed.
- Honor the public destination builder only at the top-level boundary; recursive child builders must remain independent to avoid self-append duplication.
- Replace the strong `ConcurrentDictionary<Expression, Func<object>>` with weak-key retention (for example `ConditionalWeakTable<Expression, Func<object>>`) or an equally scoped design that cannot retain transient closure graphs forever. Preserve evaluation behavior and original exception context instead of swallowing unrelated failures.
- Correct `Qh.In` to use value equality and correct `Qh.NotIn` as its negation while preserving the file's UTF-8 BOM + CRLF bytes.
- Remove only dead code/comments directly displaced by the new helper structure; no broad unrelated cleanup.

**Step 4: GREEN verification for Task 1**

Run:

```bash
dotnet test Figlotech.BDados.Tests/Figlotech.BDados.Tests.csproj --no-restore --filter "FullyQualifiedName~ConditionParserBehaviorTests|FullyQualifiedName~ConditionParserFrozenAliasTests" -v:minimal
dotnet build Figlotech.BDados/Figlotech.BDados.csproj --no-restore --no-incremental -v:minimal
git diff --check
git -c core.whitespace=cr-at-eol diff --check
```

Expected: all focused tests pass; build exits 0; no new warnings in modified files; ordinary and CRLF-aware diff checks are clean.

**Step 5: Self-review and handoff evidence**

Report:
- exact RED commands and representative expected failures;
- exact GREEN commands and pass counts;
- changed files;
- EOL/BOM before/after for all changed existing files;
- style-guide compliance and any justified deviation;
- assumptions and unresolved risks.

Do not commit.

### Task 2: Add standard collection membership (`Contains` → parameterized `IN`)

**Files:**
- Modify: `Figlotech.BDados.Tests/ConditionParserBehaviorTests.cs`
- Modify: `Figlotech.BDados/Helpers/ConditionParser.cs`
- Modify only if fixture data is needed: `Figlotech.BDados.Tests/PlanTestModels.cs`

**Step 1: RED — implement the membership matrix one vertical slice at a time**

Add tests for:

1. Captured `List<Guid>.Contains(x.ScalarAggregateId)` emits root-column `IN` with one parameter per value.
2. `Enumerable.Contains(array, x.ScalarAggregateId)` and `HashSet<Guid>.Contains(...)` produce equivalent SQL/parameters.
3. A local projection such as `items.Select(item => item.Id).Contains(x.ScalarAggregateId)` is evaluated locally and translated to the projected values; the selector/source must not reference the query root.
4. Empty and null sources emit exactly logical `FALSE` with no parameters.
5. A source containing null emits `(member IN (...) OR member IS NULL)` (or the equivalent exact boolean shape); null-only source emits `member IS NULL`.
6. `!values.Contains(member)` negates the complete membership fragment without dropping the null branch.
7. Root membership works through default and `PrefixMaker` constructors without compiling deliberately invalid unrelated aggregate metadata.
8. Aggregate-field membership uses the selected frozen alias and cannot introduce an alias absent from `plan.AliasByPath`.
9. `fullConditions:false` preserves root-member membership with root alias stripped; joined-member membership projects to implication-safe `TRUE`.
10. String `Contains` remains a LIKE operation and is not misclassified.
11. `Enumerable.Contains` comparer overload and a collection source that depends on the query parameter throw actionable wrapped exceptions.

Run each exact test and observe valid RED before production code:

```bash
dotnet test Figlotech.BDados.Tests/Figlotech.BDados.Tests.csproj --no-restore --filter "FullyQualifiedName~ConditionParserBehaviorTests.<ExactMembershipTest>" -v:minimal
```

**Step 2: GREEN — add exact membership translation**

Implement an exact recognizer for:

- instance collection `Contains(value)` where the receiver is not `string` and is safely evaluable without the query root;
- two-argument `System.Linq.Enumerable.Contains(source, value)`.

Required implementation properties:

- Determine source/value positions from the actual method signature.
- Reject comparer overloads and any source with a free reference to the query root.
- Evaluate/materialize the source once per parse into a stable local sequence.
- Parse the tested member/value expression through the existing member/alias pipeline.
- Parameterize every non-null element using parser/query-builder facilities.
- Produce deterministic `FALSE`, `IS NULL`, `IN`, or combined `IN OR IS NULL` output according to source contents.
- Do not route standard collection membership through reflection-based `Qh`/`Qb` method matching.
- Keep `Qh.In`/`Qh.NotIn` as their existing compatibility surface, but replace arity-only reflection dispatch with exact/direct dispatch if touched.

**Step 3: GREEN verification for Task 2**

Run:

```bash
dotnet test Figlotech.BDados.Tests/Figlotech.BDados.Tests.csproj --no-restore --filter "FullyQualifiedName~ConditionParserBehaviorTests" -v:minimal
dotnet test Figlotech.BDados.Tests/Figlotech.BDados.Tests.csproj --no-restore --filter "FullyQualifiedName~ConditionParserFrozenAliasTests|FullyQualifiedName~ProviderDefinitiveJoinQueryTests" -v:minimal
dotnet build Figlotech.BDados.Tests/Figlotech.BDados.Tests.csproj --no-restore --no-incremental -v:minimal
git diff --check
git -c core.whitespace=cr-at-eol diff --check
```

Expected: all membership and prior parser/provider tests pass; build exits 0; no modified-file warning regressions or whitespace/EOL churn.

**Step 4: Self-review and handoff evidence**

Report the same evidence required by Task 1. Do not commit.

### Task 3: Complete supported-syntax characterization and concurrency hardening

**Files:**
- Modify: `Figlotech.BDados.Tests/ConditionParserBehaviorTests.cs`
- Modify: `Figlotech.BDados/Helpers/ConditionParser.cs`
- Modify only for focused fixtures: `Figlotech.BDados.Tests/PlanTestModels.cs`

**Step 1: RED — characterize every retained supported branch**

Add focused tests for:

1. Every `QueryComparisonAttribute` mode (`ExactValue`, `Containing`, `StartingWith`, `EndingWith`, `IgnoreCase`) with exact SQL shape and one bound value. `IgnoreCase` must compare both sides case-insensitively, for example `LOWER(column)=LOWER(@value)`, without inlining.
2. `ToUpper`, `ToLower`, `Trim`, `Replace`, `StringExtensions.RegExReplace`, `Int32.Parse`, and `Int64.Parse` each emit one nonduplicated SQL function/cast with complete parameter preservation.
3. `Contains`, `StartsWith`, `EndsWith`, and the corresponding `QueryComparisonAttribute` LIKE modes escape literal `%`, `_`, and the chosen escape marker in bound values and emit a provider-neutral single-character `ESCAPE` clause (for example `ESCAPE '!'`). Tests must prove ordinary text and all three special characters preserve CLR literal semantics.
4. `ParseExpression<T>(Conditions<T>)` preserves SQL/parameters equivalently to the lambda overload for the same supported predicate and wraps invalid input actionably.
5. Negated aggregate `Any()` and negated `Any(predicate)` emit valid complete SQL under `fullConditions:true`; under `fullConditions:false` they retain the existing implication-safe `TRUE` projection.
6. Reusing the same parser concurrently for many predicates of the same configured root never corrupts aliases, SQL, parameter values, or parameter dictionaries. Use deterministic tasks and assert every individual output, not only absence of exceptions.
7. Existing combined `Qh.In` + root-predicate projection preserves every parameter and never emits an empty operand. Add a stronger exact-parameter assertion if current coverage is insufficient.

Run each exact test before implementation and preserve valid RED evidence.

**Step 2: GREEN — minimal hardening**

- Implement documented case-insensitive comparison with parameterized SQL.
- Escape LIKE values with one explicit provider-neutral escape marker; never inline the original or escaped value.
- Make same-instance public parse entrypoints safe as one parse transaction. A simple private synchronization boundary is acceptable because current callers construct parsers per operation; do not falsely make only the parameter counter atomic while leaving root/resolver state racy.
- Ensure unary negation consumes a complete operand and unsupported collection chains cannot return whitespace fragments.
- Remove duplicate/unreachable string `Replace` handling and only dead commented parser code directly displaced by the helpers.
- Do not expand the public method-support surface beyond the explicitly tested retained branches.

**Step 3: GREEN verification for Task 3**

```bash
dotnet test Figlotech.BDados.Tests/Figlotech.BDados.Tests.csproj --no-restore --filter "FullyQualifiedName~ConditionParserBehaviorTests|FullyQualifiedName~ConditionParserFrozenAliasTests" -v:minimal
dotnet test Figlotech.BDados.Tests/Figlotech.BDados.Tests.csproj --no-restore -v:minimal
dotnet build Figlotech.BDados.Tests/Figlotech.BDados.Tests.csproj --no-restore --no-incremental -v:minimal
git diff --check
git -c core.whitespace=cr-at-eol diff --check
```

Report per-slice RED/GREEN evidence, exact pass counts, changed files, EOL/BOM audit, style compliance, and unresolved risks. Do not commit.

## Controller review gates after each task

1. Inspect `git status`, `git diff --stat`, `git diff --numstat`, and the complete scoped diff.
2. Independently rerun the task's focused test command and build command.
3. Snapshot hashes of every in-scope tracked/untracked file.
4. Dispatch a read-only specification reviewer against the exact task requirements. Any mutation invalidates the verdict.
5. Verify hashes/status are unchanged.
6. After specification approval, run the canonical independent code-quality review. Review cached-expression retention, exact method classification, SQL parameterization/injection safety, null/three-valued SQL semantics, aggregate alias authority, root-projection implication safety, and adversarial test adequacy.
7. Delegate Critical/Important fixes to a fresh focused implementer under strict TDD, then repeat the affected review gate.

## Final integration verification

After both tasks and reviews:

```bash
dotnet test Figlotech.BDados.Tests/Figlotech.BDados.Tests.csproj --no-restore -v:minimal
dotnet build Figlotech.BDados.Tests/Figlotech.BDados.Tests.csproj --no-restore --no-incremental -v:minimal
git diff --check
git -c core.whitespace=cr-at-eol diff --check
git status --short
git diff --stat
git diff --numstat
git diff -- Figlotech.BDados/Helpers/ConditionParser.cs Figlotech.BDados/Builders/Qh.cs Figlotech.BDados.Tests/ConditionParserBehaviorTests.cs Figlotech.BDados.Tests/PlanTestModels.cs docs/plans/2026-07-15-condition-parser-reinforcement.md
```

Also compare BOM/CRLF/LF counts of every modified existing file against `HEAD`. The final report must separate the unrelated solution-level missing-project blocker from the authoritative project-level green evidence.
