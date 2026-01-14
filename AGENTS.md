# AGENTS.md

This guide helps agentic coding tools work safely in this repository.
It summarizes build/test commands and local coding conventions.

## Scope
- Applies to the entire repository.
- No other AGENTS.md files were found.

## Repo Overview
- Solution file: `figlotech.sln`.
- Primary libraries: `Figlotech.Core`, `Figlotech.BDados`, adapters in `Figlotech.BDados.*`.
- Utility projects: `Figlotech.ExcelUtil`, `Figlotech.DataFlow`, `Figlotech.Scripting`.
- Test/bench project: `test/test.csproj` (console app, BenchmarkDotNet).
- Packaging output: `_nuget/` and `_dist/` folders.

## Build / Pack / Publish Commands
Use these from the repo root:

- Build everything (solution):
  - `dotnet build figlotech.sln`
- Restore (if needed):
  - `dotnet restore figlotech.sln`
- Pack (netstandard2.1) via script:
  - `./build.sh` (Linux/macOS)
  - `build-nuget.bat` (Windows)
- Publish netstandard2.1 artifacts:
  - `build-netstandard2.0.cmd`
- Generate local nupkgs:
  - `generate-nupkg.cmd`
  - `genpkg.sh`
- Pack + push to GitHub packages:
  - `pack-to-github.sh`
  - `pack-to-github.bat`
- Pack + copy to local NuGet cache:
  - `pack-to-local-nuget.bat`

## Test / Benchmark Commands
There is no dedicated unit-test framework configured.
The `test/` project is a console app that runs BenchmarkDotNet benchmarks.

- Run the test/bench console app:
  - `dotnet run --project test/test.csproj`
- Run benchmarks in Release (recommended by BenchmarkDotNet):
  - `dotnet run --project test/test.csproj -c Release`
- Run a single benchmark (BenchmarkDotNet filter):
  - `dotnet run --project test/test.csproj -c Release -- --filter *PersonLoadBenchmark*`

If you add a unit-test project later, use the standard filter:
- `dotnet test <path-to-test-csproj> --filter FullyQualifiedName~Namespace.ClassName`

## Lint / Formatting
- No linting or formatting commands are configured in this repo.
- The only `.editorconfig` rule disables CA1012 for C# files.
- Avoid introducing new analyzers/formatters unless requested.

## Cursor / Copilot Rules
- No Cursor rules found in `.cursor/rules/` or `.cursorrules`.
- No Copilot rules found in `.github/copilot-instructions.md`.

## C# Code Style Guidelines
These are inferred from existing code.

### Layout and Formatting
- Indentation: 4 spaces; no tabs.
- Braces: opening brace on the same line (K&R style).
- Namespace style: block namespaces (no file-scoped namespaces).
- Blank lines used sparingly between logical blocks.
- Keep files small and focused when adding new types.

### Using Directives
- `using` directives appear at the top of the file.
- System namespaces generally appear first.
- Third-party and Figlotech namespaces come after System.
- Avoid unused `using` directives.

### Naming Conventions
- Types, methods, and public properties: `PascalCase`.
- Local variables and parameters: `camelCase`.
- Private fields: `_camelCase` (underscore prefix).
- Interfaces: `I` prefix (e.g., `IFileSystem`).
- Async methods: suffix with `Async` when returning `Task`/`ValueTask`.
- Constants: `PascalCase` (consistent with existing style).

### Types and Var Usage
- Use explicit types for public APIs and clarity.
- `var` is acceptable for obvious types or LINQ queries.
- Prefer `ValueTask` where the project already does (e.g., `Fi.Result`).

### Nullability and Guards
- Add null checks for public method arguments.
- Existing patterns include `Fi.NullCheck` and custom validation exceptions.
- Prefer throwing `ArgumentNullException` for simple guards, unless the
  surrounding code uses `BusinessValidationException` for domain validation.

### Error Handling
- Catch exceptions only when adding context or fallback behavior.
- Keep exception messages actionable and concise.
- Avoid swallowing exceptions; rethrow or wrap with context.

### Collections and LINQ
- LINQ is used heavily; keep expressions readable.
- Avoid complex nested LINQ chains; split into intermediate variables.

### Async and Concurrency
- Use async/await consistently; avoid blocking calls in async paths.
- Prefer `ConfigureAwait(false)` only if a specific need is shown nearby.

### Logging / Diagnostics
- No centralized logging framework found.
- When adding diagnostics, use existing patterns (Console output in `test/`).

### Public API Stability
- These libraries appear to be consumed externally; avoid breaking public APIs
  unless explicitly requested.

### Performance Notes
- Benchmarks exist in `test/` using BenchmarkDotNet.
- Avoid unnecessary allocations in hot paths (core utils, BDados).

## Project-Specific Notes
- Target frameworks include `netstandard2.1` for libraries and `net6.0` for
  the `test/` console project.
- Packaging scripts compute version numbers via `git rev-list --count` and
  store intermediate values in `fitech.version` and `rev`.
- The test project references external DLLs from sibling repos; running it
  may require those dependencies to exist.

## When Editing
- Prefer minimal, localized changes.
- Keep file structure and naming consistent with surrounding code.
- Update documentation only when behavior or usage changes.

## Quick Command Index
- Build: `dotnet build figlotech.sln`
- Pack: `./build.sh` or `build-nuget.bat`
- Publish: `build-netstandard2.0.cmd`
- Run benchmarks: `dotnet run --project test/test.csproj -c Release`
- Single benchmark: `dotnet run --project test/test.csproj -c Release -- --filter *PersonLoadBenchmark*`
