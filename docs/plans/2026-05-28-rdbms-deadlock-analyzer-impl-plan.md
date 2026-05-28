# RdbmsDataAccessor Deadlock Analyzer Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Create a Roslyn analyzer that reports compile-time errors (BD001, BD002) for nested Access/AccessAsync calls and missing transaction parameters within RdbmsDataAccessor lambdas.

**Architecture:** Add a new `netstandard2.0` analyzer project `Figlotech.BDados.Analyzers` that registers a `DiagnosticAnalyzer` inspecting `InvocationExpressionSyntax` nodes. It walks up parent lambdas to detect if we're inside an `Access`/`AccessAsync` call, then uses semantic model symbol comparison to flag same-instance nested calls (BD001) or convenience overload calls missing the transaction parameter (BD002).

**Tech Stack:** C# Roslyn (`Microsoft.CodeAnalysis.CSharp`), `Microsoft.CodeAnalysis.Analyzers`, `Microsoft.CodeAnalysis.CSharp.Testing.XUnit`

---

### Task 1: Create Analyzer Project

**Files:**
- Create: `Figlotech.BDados.Analyzers/Figlotech.BDados.Analyzers.csproj`
- Modify: `figlotech.sln`

**Step 1: Create project file**

Create `Figlotech.BDados.Analyzers/Figlotech.BDados.Analyzers.csproj`:

```xml
<Project Sdk="Microsoft.NET.Sdk">

  <PropertyGroup>
    <TargetFramework>netstandard2.0</TargetFramework>
    <IncludeBuildOutput>false</IncludeBuildOutput>
    <SuppressDependenciesWhenPacking>true</SuppressDependenciesWhenPacking>
    <GeneratePackageOnBuild>true</GeneratePackageOnBuild>
  </PropertyGroup>

  <ItemGroup>
    <PackageReference Include="Microsoft.CodeAnalysis.CSharp" Version="4.9.2" PrivateAssets="all" />
    <PackageReference Include="Microsoft.CodeAnalysis.Analyzers" Version="3.3.4" PrivateAssets="all" />
  </ItemGroup>

  <ItemGroup>
    <None Update="tools\*.ps1" CopyToOutputDirectory="Always" Pack="true" PackagePath="" />
    <None Include="$(OutputPath)\$(AssemblyName).dll" Pack="true" PackagePath="analyzers/dotnet/cs" Visible="false" />
  </ItemGroup>

</Project>
```

**Step 2: Add project to solution**

Run:
```bash
dotnet sln figlotech.sln add Figlotech.BDados.Analyzers/Figlotech.BDados.Analyzers.csproj
```

Expected: `Project added successfully.`

**Step 3: Commit**

```bash
git add Figlotech.BDados.Analyzers/ figlotech.sln
git commit -m "chore: add BDados analyzer project"
```

---

### Task 2: Define Diagnostic Descriptors

**Files:**
- Create: `Figlotech.BDados.Analyzers/AnalyzerReleases.Shipped.md`
- Create: `Figlotech.BDados.Analyzers/AnalyzerReleases.Unshipped.md`
- Create: `Figlotech.BDados.Analyzers/RdbmsDataAccessorDiagnostics.cs`

**Step 1: Create shipped releases file**

Create `Figlotech.BDados.Analyzers/AnalyzerReleases.Shipped.md`:

```markdown
## Release 1.0

### New Rules
| Rule ID | Category | Severity | Notes |
|---------|----------|----------|-------|
| BD001   | Reliability | Error | Nested transaction access detected |
| BD002   | Reliability | Error | Missing BDadosTransaction parameter |
```

**Step 2: Create unshipped releases file**

Create `Figlotech.BDados.Analyzers/AnalyzerReleases.Unshipped.md` with empty content for now.

**Step 3: Create diagnostic descriptors**

Create `Figlotech.BDados.Analyzers/RdbmsDataAccessorDiagnostics.cs`:

```csharp
using Microsoft.CodeAnalysis;

namespace Figlotech.BDados.Analyzers {
    public static class RdbmsDataAccessorDiagnostics {
        public const string NestedAccessDiagnosticId = "BD001";
        public const string MissingTransactionDiagnosticId = "BD002";

        public static readonly DiagnosticDescriptor NestedAccess = new DiagnosticDescriptor(
            id: NestedAccessDiagnosticId,
            title: "Nested transaction access",
            messageFormat: "Calling '{0}' on the same accessor instance inside another '{1}' call causes deadlock. Use the BDadosTransaction parameter instead.",
            category: "Reliability",
            defaultSeverity: DiagnosticSeverity.Error,
            isEnabledByDefault: true,
            description: "Nested Access/AccessAsync calls on the same RdbmsDataAccessor instance can deadlock when the connection pool is exhausted."
        );

        public static readonly DiagnosticDescriptor MissingTransaction = new DiagnosticDescriptor(
            id: MissingTransactionDiagnosticId,
            title: "Missing BDadosTransaction parameter",
            messageFormat: "Method '{0}' should be called with the BDadosTransaction parameter inside an Access/AccessAsync lambda to avoid nested transactions.",
            category: "Reliability",
            defaultSeverity: DiagnosticSeverity.Error,
            isEnabledByDefault: true,
            description: "Calling transaction-spawning convenience methods without passing the transaction parameter causes nested transactions and potential deadlocks."
        );
    }
}
```

**Step 4: Commit**

```bash
git add Figlotech.BDados.Analyzers/
git commit -m "feat: add BD001/BD002 diagnostic descriptors"
```

---

### Task 3: Implement the Analyzer

**Files:**
- Create: `Figlotech.BDados.Analyzers/RdbmsDataAccessorAnalyzer.cs`

**Step 1: Write analyzer implementation**

Create `Figlotech.BDados.Analyzers/RdbmsDataAccessorAnalyzer.cs`:

```csharp
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Diagnostics;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

namespace Figlotech.BDados.Analyzers {
    [DiagnosticAnalyzer(LanguageNames.CSharp)]
    public class RdbmsDataAccessorAnalyzer : DiagnosticAnalyzer {
        private static readonly ImmutableArray<string> AccessMethodNames = ImmutableArray.Create(
            "Access",
            "AccessAsync",
            "AccessAsyncCoroutinely"
        );

        private static readonly ImmutableArray<string> ConvenienceMethodNames = ImmutableArray.Create(
            "Query",
            "Execute",
            "LoadById",
            "LoadByRid",
            "LoadAll",
            "SaveItem",
            "Delete",
            "ScalarQuery",
            "ForceExist",
            "Fetch",
            "FetchAsync",
            "LoadFirstOrDefault",
            "LoadFirstOrDefaultAsync",
            "AggregateLoad",
            "AggregateLoadAsync",
            "AggregateLoadAsyncCoroutinely",
            "ExistsByRIDAsync",
            "ExistsByIdAsync",
            "DeleteAsync",
            "DeleteWhereRidNotIn",
            "DeleteWhereRidNotInAsync",
            "SaveList",
            "SaveListAsync",
            "Update",
            "UpdateAndMutate",
            "UpdateAndMutateIfSuccess",
            "UpdateAsync",
            "UpdateAndMutateAsync",
            "UpdateAndMutateIfSuccessAsync"
        );

        public override ImmutableArray<DiagnosticDescriptor> SupportedDiagnostics => ImmutableArray.Create(
            RdbmsDataAccessorDiagnostics.NestedAccess,
            RdbmsDataAccessorDiagnostics.MissingTransaction
        );

        public override void Initialize(AnalysisContext context) {
            context.ConfigureGeneratedCodeAnalysis(GeneratedCodeAnalysisFlags.None);
            context.EnableConcurrentExecution();
            context.RegisterSyntaxNodeAction(AnalyzeInvocation, SyntaxKind.InvocationExpression);
        }

        private void AnalyzeInvocation(SyntaxNodeAnalysisContext context) {
            var invocation = (InvocationExpressionSyntax)context.Node;
            var methodSymbol = context.SemanticModel.GetSymbolInfo(invocation).Symbol as IMethodSymbol;
            if (methodSymbol == null) return;

            var receiver = GetReceiverSymbol(invocation, context.SemanticModel);
            if (receiver == null) return;

            // Check if this invocation is itself an Access/AccessAsync call (BD001)
            if (AccessMethodNames.Contains(methodSymbol.Name)) {
                var enclosingAccess = FindEnclosingAccessLambda(invocation, context.SemanticModel);
                if (enclosingAccess != null) {
                    var outerReceiver = GetReceiverSymbol(enclosingAccess.Invocation, context.SemanticModel);
                    if (outerReceiver != null && SymbolEqualityComparer.Default.Equals(receiver, outerReceiver)) {
                        var diagnostic = Diagnostic.Create(
                            RdbmsDataAccessorDiagnostics.NestedAccess,
                            invocation.GetLocation(),
                            methodSymbol.Name,
                            enclosingAccess.MethodName
                        );
                        context.ReportDiagnostic(diagnostic);
                    }
                }
            }

            // Check for missing transaction parameter (BD002)
            if (ConvenienceMethodNames.Contains(methodSymbol.Name)) {
                var enclosingAccess = FindEnclosingAccessLambda(invocation, context.SemanticModel);
                if (enclosingAccess != null) {
                    var outerReceiver = GetReceiverSymbol(enclosingAccess.Invocation, context.SemanticModel);
                    if (outerReceiver != null && SymbolEqualityComparer.Default.Equals(receiver, outerReceiver)) {
                        // Check if first argument is BDadosTransaction
                        var args = invocation.ArgumentList.Arguments;
                        if (args.Count == 0 || !IsBDadosTransaction(args[0], context.SemanticModel)) {
                            var diagnostic = Diagnostic.Create(
                                RdbmsDataAccessorDiagnostics.MissingTransaction,
                                invocation.GetLocation(),
                                methodSymbol.Name
                            );
                            context.ReportDiagnostic(diagnostic);
                        }
                    }
                }
            }
        }

        private ISymbol GetReceiverSymbol(InvocationExpressionSyntax invocation, SemanticModel semanticModel) {
            if (invocation.Expression is MemberAccessExpressionSyntax memberAccess) {
                return semanticModel.GetSymbolInfo(memberAccess.Expression).Symbol;
            }
            // Implicit this access
            if (invocation.Expression is IdentifierNameSyntax) {
                // Try to get containing type's instance
                var methodDecl = invocation.Ancestors().OfType<MethodDeclarationSyntax>().FirstOrDefault();
                if (methodDecl != null) {
                    var methodSymbol = semanticModel.GetDeclaredSymbol(methodDecl);
                    if (methodSymbol != null) {
                        return methodSymbol.ContainingType;
                    }
                }
            }
            return null;
        }

        private (InvocationExpressionSyntax Invocation, string MethodName) FindEnclosingAccessLambda(
            SyntaxNode node,
            SemanticModel semanticModel) {
            var current = node.Parent;
            while (current != null) {
                if (current is LambdaExpressionSyntax lambda) {
                    // Check if this lambda is an argument to an Access/AccessAsync call
                    var parent = lambda.Parent;
                    if (parent is ArgumentSyntax arg) {
                        var invocation = arg.Ancestors().OfType<InvocationExpressionSyntax>().FirstOrDefault();
                        if (invocation != null) {
                            var symbol = semanticModel.GetSymbolInfo(invocation).Symbol as IMethodSymbol;
                            if (symbol != null && AccessMethodNames.Contains(symbol.Name)) {
                                return (invocation, symbol.Name);
                            }
                        }
                    }
                }
                current = current.Parent;
            }
            return (null, null);
        }

        private bool IsBDadosTransaction(ArgumentSyntax argument, SemanticModel semanticModel) {
            var typeInfo = semanticModel.GetTypeInfo(argument.Expression);
            if (typeInfo.Type == null) return false;
            return typeInfo.Type.ToDisplayString() == "Figlotech.BDados.DataAccessAbstractions.BDadosTransaction";
        }
    }
}
```

**Step 2: Commit**

```bash
git add Figlotech.BDados.Analyzers/RdbmsDataAccessorAnalyzer.cs
git commit -m "feat: implement BD001/BD002 analyzer logic"
```

---

### Task 4: Create Test Project

**Files:**
- Create: `Figlotech.BDados.Analyzers.Tests/Figlotech.BDados.Analyzers.Tests.csproj`
- Create: `Figlotech.BDados.Analyzers.Tests/RdbmsDataAccessorAnalyzerTests.cs`
- Modify: `figlotech.sln`

**Step 1: Create test project file**

Create `Figlotech.BDados.Analyzers.Tests/Figlotech.BDados.Analyzers.Tests.csproj`:

```xml
<Project Sdk="Microsoft.NET.Sdk">

  <PropertyGroup>
    <TargetFramework>net6.0</TargetFramework>
    <IsPackable>false</IsPackable>
  </PropertyGroup>

  <ItemGroup>
    <PackageReference Include="Microsoft.NET.Test.Sdk" Version="17.9.0" />
    <PackageReference Include="Microsoft.CodeAnalysis.CSharp.Testing.XUnit" Version="1.1.1" />
    <PackageReference Include="xunit" Version="2.7.0" />
    <PackageReference Include="xunit.runner.visualstudio" Version="2.5.7">
      <PrivateAssets>all</PrivateAssets>
      <IncludeAssets>runtime; build; native; contentfiles; analyzers</IncludeAssets>
    </PackageReference>
  </ItemGroup>

  <ItemGroup>
    <ProjectReference Include="..\Figlotech.BDados.Analyzers\Figlotech.BDados.Analyzers.csproj" />
  </ItemGroup>

</Project>
```

**Step 2: Add test project to solution**

Run:
```bash
dotnet sln figlotech.sln add Figlotech.BDados.Analyzers.Tests/Figlotech.BDados.Analyzers.Tests.csproj
```

**Step 3: Commit**

```bash
git add Figlotech.BDados.Analyzers.Tests/ figlotech.sln
git commit -m "chore: add analyzer test project"
```

---

### Task 5: Write Tests for BD001 (Nested Access)

**Files:**
- Create: `Figlotech.BDados.Analyzers.Tests/RdbmsDataAccessorAnalyzerTests.cs`

**Step 1: Write BD001 tests**

Create `Figlotech.BDados.Analyzers.Tests/RdbmsDataAccessorAnalyzerTests.cs` with the test class and BD001 tests:

```csharp
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Testing;
using Microsoft.CodeAnalysis.Testing;
using Microsoft.CodeAnalysis.Testing.Verifiers;
using System.Threading.Tasks;
using Xunit;

namespace Figlotech.BDados.Analyzers.Tests {
    public class RdbmsDataAccessorAnalyzerTests {
        private static CSharpAnalyzerTest<RdbmsDataAccessorAnalyzer, XUnitVerifier> CreateTest(string source, params DiagnosticResult[] expectedDiagnostics) {
            var test = new CSharpAnalyzerTest<RdbmsDataAccessorAnalyzer, XUnitVerifier> {
                TestCode = source,
                ReferenceAssemblies = ReferenceAssemblies.Net.Net60,
            };
            test.TestState.AdditionalReferences.Add(typeof(RdbmsDataAccessor).Assembly);
            foreach (var diag in expectedDiagnostics) {
                test.ExpectedDiagnostics.Add(diag);
            }
            return test;
        }

        [Fact]
        public async Task BD001_ReportsNestedAccessOnSameInstance() {
            var source = @"
using Figlotech.BDados.DataAccessAbstractions;
using System;

public class TestClass {
    private IRdbmsDataAccessor _accessor;

    public void TestMethod() {
        _accessor.Access(tsn => {
            _accessor.Access(innerTsn => {
                Console.WriteLine(""nested"");
            });
        });
    }
}
";
            var expected = new DiagnosticResult(RdbmsDataAccessorDiagnostics.NestedAccess)
                .WithLocation(10, 13)
                .WithArguments("Access", "Access");

            await CreateTest(source, expected).RunAsync();
        }

        [Fact]
        public async Task BD001_DoesNotReportDifferentInstance() {
            var source = @"
using Figlotech.BDados.DataAccessAbstractions;
using System;

public class TestClass {
    private IRdbmsDataAccessor _accessor1;
    private IRdbmsDataAccessor _accessor2;

    public void TestMethod() {
        _accessor1.Access(tsn => {
            _accessor2.Access(innerTsn => {
                Console.WriteLine(""ok"");
            });
        });
    }
}
";
            await CreateTest(source).RunAsync();
        }

        [Fact]
        public async Task BD001_ReportsNestedAccessAsync() {
            var source = @"
using Figlotech.BDados.DataAccessAbstractions;
using System;
using System.Threading.Tasks;

public class TestClass {
    private IRdbmsDataAccessor _accessor;

    public async Task TestMethod() {
        await _accessor.AccessAsync(async tsn => {
            await _accessor.AccessAsync(async innerTsn => {
                Console.WriteLine(""nested"");
            }, default);
        }, default);
    }
}
";
            var expected = new DiagnosticResult(RdbmsDataAccessorDiagnostics.NestedAccess)
                .WithLocation(11, 19)
                .WithArguments("AccessAsync", "AccessAsync");

            await CreateTest(source, expected).RunAsync();
        }
    }
}
```

**Step 2: Commit**

```bash
git add Figlotech.BDados.Analyzers.Tests/RdbmsDataAccessorAnalyzerTests.cs
git commit -m "test: add BD001 nested access tests"
```

---

### Task 6: Write Tests for BD002 (Missing Transaction)

**Files:**
- Modify: `Figlotech.BDados.Analyzers.Tests/RdbmsDataAccessorAnalyzerTests.cs`

**Step 1: Add BD002 tests**

Append to `Figlotech.BDados.Analyzers.Tests/RdbmsDataAccessorAnalyzerTests.cs`:

```csharp
        [Fact]
        public async Task BD002_ReportsMissingTransactionParameter() {
            var source = @"
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.Data;
using System;

public class TestClass {
    private IRdbmsDataAccessor _accessor;

    public void TestMethod() {
        _accessor.Access(tsn => {
            var results = _accessor.Query<MyDataObject>(null);
        });
    }
}

public class MyDataObject : IDataObject {
    public long Id { get; set; }
    public string RID { get; set; }
    public DateTime CreatedTime { get; set; }
    public DateTime UpdatedTime { get; set; }
}
";
            var expected = new DiagnosticResult(RdbmsDataAccessorDiagnostics.MissingTransaction)
                .WithLocation(10, 27)
                .WithArguments("Query");

            await CreateTest(source, expected).RunAsync();
        }

        [Fact]
        public async Task BD002_DoesNotReportWhenTransactionPassed() {
            var source = @"
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.Data;
using System;

public class TestClass {
    private IRdbmsDataAccessor _accessor;

    public void TestMethod() {
        _accessor.Access(tsn => {
            var results = _accessor.Query<MyDataObject>(tsn, null);
        });
    }
}

public class MyDataObject : IDataObject {
    public long Id { get; set; }
    public string RID { get; set; }
    public DateTime CreatedTime { get; set; }
    public DateTime UpdatedTime { get; set; }
}
";
            await CreateTest(source).RunAsync();
        }

        [Fact]
        public async Task BD002_DoesNotReportOutsideAccessLambda() {
            var source = @"
using Figlotech.BDados.DataAccessAbstractions;
using Figlotech.Data;
using System;

public class TestClass {
    private IRdbmsDataAccessor _accessor;

    public void TestMethod() {
        var results = _accessor.Query<MyDataObject>(null);
    }
}

public class MyDataObject : IDataObject {
    public long Id { get; set; }
    public string RID { get; set; }
    public DateTime CreatedTime { get; set; }
    public DateTime UpdatedTime { get; set; }
}
";
            await CreateTest(source).RunAsync();
        }
```

**Step 2: Commit**

```bash
git add Figlotech.BDados.Analyzers.Tests/RdbmsDataAccessorAnalyzerTests.cs
git commit -m "test: add BD002 missing transaction tests"
```

---

### Task 7: Build and Run Tests

**Step 1: Build solution**

Run:
```bash
dotnet build figlotech.sln
```

Expected: Build succeeds with no errors.

**Step 2: Run analyzer tests**

Run:
```bash
dotnet test Figlotech.BDados.Analyzers.Tests/Figlotech.BDados.Analyzers.Tests.csproj --no-build
```

Expected: All 6 tests pass (2 BD001 positive, 1 BD001 negative, 1 BD002 positive, 2 BD002 negative).

**Step 3: Commit**

```bash
git add .
git commit -m "test: verify analyzer passes all tests"
```

---

### Task 8: Fix Any Build Issues in Existing Code

**Step 1: Build full solution to check for new analyzer errors**

Run:
```bash
dotnet build figlotech.sln
```

**Step 2: If BD001/BD002 errors appear in existing Figlotech code, fix them**

Look for errors in `Figlotech.BDados`, `Figlotech.BDados.*`, `Figlotech.Core`, etc. Common fixes:
- Replace `accessor.Query<T>(qb)` with `accessor.Query<T>(tsn, qb)` inside Access lambdas
- Replace `accessor.SaveItem(obj)` with `accessor.SaveItem(tsn, obj)` inside Access lambdas
- Remove nested `Access` calls by using the transaction parameter directly

**Step 3: Commit fixes**

```bash
git add .
git commit -m "fix: resolve BD001/BD002 analyzer errors in existing code"
```

---

### Task 9: Final Verification

**Step 1: Full build**

Run:
```bash
dotnet build figlotech.sln
```

Expected: Clean build with 0 errors.

**Step 2: Run tests**

Run:
```bash
dotnet test Figlotech.BDados.Analyzers.Tests/Figlotech.BDados.Analyzers.Tests.csproj
```

Expected: All tests pass.

**Step 3: Commit**

```bash
git add .
git commit -m "feat: complete RdbmsDataAccessor deadlock analyzer"
```
