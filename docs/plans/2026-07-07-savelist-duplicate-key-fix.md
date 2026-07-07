# SaveListAsync Duplicate-Key Crash Fix

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Stop `SaveListAsync` from throwing `ArgumentException: An item with the same key has already been added` when the input list contains duplicate `Id` or `RID` values.

**Architecture:** The `ridMap`/`idMap` built at `RdbmsDataAccessor.cs:2197-2198` are *fallback lookups* used to match DB-returned IDs back to in-memory objects. They are built with `ToDictionary`, which throws on duplicate keys. Replace with the existing `ToDictionaryIgnoreDuplicates` extension (last-wins semantics), which is already in `Figlotech.Core.Extensions` and already imported by the file. Add a regression test for that extension method since it currently has none.

**Tech Stack:** C# / netstandard2.1, xunit (Figlotech.Core.Tests)

---

### Task 1: Regression test for ToDictionaryIgnoreDuplicates

**Files:**
- Create: `Figlotech.Core.Tests/IEnumerableExtensionsTests.cs`

**Step 1: Write the failing test**

```csharp
using Figlotech.Core.Extensions;
using Xunit;
using System.Collections.Generic;
using System.Linq;

namespace Figlotech.Core.Tests {
    public class IEnumerableExtensionsTests {
        [Fact]
        public void ToDictionaryIgnoreDuplicates_LastKeyWinsOnDuplicate() {
            var items = new List<(int Key, string Val)> {
                (1, "first"), (2, "a"), (1, "second"), (3, "b"), (2, "c")
            };
            var dict = items.ToDictionaryIgnoreDuplicates(x => x.Key, x => x.Val);
            Assert.Equal("second", dict[1]);
            Assert.Equal("c", dict[2]);
            Assert.Equal("b", dict[3]);
            Assert.Equal(3, dict.Count);
        }

        [Fact]
        public void ToDictionaryIgnoreDuplicates_EmptySource_ReturnsEmpty() {
            var dict = new List<(int, string)>()
                .ToDictionaryIgnoreDuplicates(x => x.Item1, x => x.Item2);
            Assert.Empty(dict);
        }
    }
}
```

**Step 2: Run test**

Run: `dotnet test Figlotech.Core.Tests/Figlotech.Core.Tests.csproj --filter *IEnumerableExtensions*`
Expected: PASS (the method already exists and works — this is a characterization/regression guard, not RED).

**Step 3: Commit**

```bash
git add Figlotech.Core.Tests/IEnumerableExtensionsTests.cs
git commit -m "test: add regression tests for ToDictionaryIgnoreDuplicates"
```

---

### Task 2: Swap ToDictionary → ToDictionaryIgnoreDuplicates in SaveListAsync

**Files:**
- Modify: `Figlotech.BDados/DataAccessAbstractions/RdbmsDataAccessor.cs:2197-2198`

**Step 1: Apply the edit**

Replace:
```csharp
                var ridMap = legacyChunk.ToDictionary(item => item.RID, item => item);
                var idMap = legacyChunk.Where(item => item.Id > 0).ToDictionary(item => item.Id, item => item);
```
With:
```csharp
                // Last-wins: duplicate RID/Id in the input list must not crash the save.
                // These maps are fallback lookups for matching DB-returned IDs back to objects.
                var ridMap = legacyChunk.ToDictionaryIgnoreDuplicates(item => item.RID, item => item);
                var idMap = legacyChunk.Where(item => item.Id > 0).ToDictionaryIgnoreDuplicates(item => item.Id, item => item);
```

**Step 2: Build**

Run: `dotnet build figlotech.sln`
Expected: success, no new warnings.

**Step 3: Run tests**

Run: `dotnet test Figlotech.Core.Tests/Figlotech.Core.Tests.csproj`
Expected: all PASS.

**Step 4: Commit**

```bash
git add Figlotech.BDados/DataAccessAbstractions/RdbmsDataAccessor.cs
git commit -m "fix: use duplicate-tolerant lookup in SaveListAsync to avoid ArgumentException on duplicate Id/RID"
```
