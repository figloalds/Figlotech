# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Figlotech is a personal utility framework built around a distinctive extension method pattern. The codebase provides infrastructure for database abstraction (BDados), file system abstraction, data flow processing, and numerous utility functions. It's designed for rapid development with minimal boilerplate code.

**Philosophy:** Pragmatic over purist. Focus on developer productivity, willing to break conventions for convenience. This is experimental/learning code, not production-grade.

## Build Commands

### Build Solution
```bash
dotnet build figlotech.sln
```

### Build NuGet Packages
```bash
# Windows
build-nuget.bat

# Linux/Mac
./build.sh
```

Packages are output to `_nuget/` directory with version format `1.0.<git-rev-count>.<rev>`.

### Pack to Local NuGet Feed
```bash
pack-to-local-nuget.bat
```
Builds packages and copies them to `../NugetLocal/`.

### Pack to GitHub Packages
```bash
# Windows
pack-to-github.bat

# Linux/Mac
./pack-to-github.sh
```

### Run Test Project
```bash
dotnet run --project test/test.csproj
```
Note: The test project uses BenchmarkDotNet for performance testing and requires external database credentials.

## Project Structure

### Main Assemblies

- **Figlotech.Core** - Foundation library with utilities, extensions, and abstractions
- **Figlotech.BDados** - Database abstraction layer (like Dapper but custom)
- **Figlotech.BDados.MySqlDataAccessor** - MySQL implementation
- **Figlotech.BDados.PostgreSQLDataAccessor** - PostgreSQL implementation
- **Figlotech.BDados.SQLiteDataAccessor** - SQLite implementation
- **Figlotech.Core.FileAcessAbstractions.AzureBlobsFileAccessor** - Azure Blob Storage adapter
- **Figlotech.DataFlow** - ETL-style data transformation pipelines
- **Figlotech.ExcelUtil** - Excel file utilities
- **Figlotech.Scripting** - Scripting capabilities
- **TestConcepts** (Figlotech.ECSEngine) - Experimental ECS engine

### Target Framework
All libraries target `netstandard2.1` for broad compatibility.

## Core Architecture

### The Fi.Tech Pattern

The entire framework revolves around `Fi.Tech` - an empty singleton class extended via extension methods. This provides a centralized, discoverable API surface.

**Location:** `Figlotech.Core/Fi.Tech.cs` and `Figlotech.Core/FiTechCoreExtensions.cs`

**Usage pattern:**
```csharp
// Time utilities
Fi.Tech.GetUtcTime()

// Scheduling
Fi.Tech.ScheduleTask(identifier, when, job)

// DTO mapping
Fi.Tech.MapIntoDTO<TOutput>(sourceObject)

// Hashing
Fi.Tech.ComputeHash(string)
```

Instead of scattering utility methods across multiple static classes, everything extends from `Fi.Tech`.

### Dependency Injection - DependencyResolver

**Location:** `Figlotech.Core/DependencyResolver.cs`

Custom IoC container with hierarchical contexts:
```csharp
// Global singleton
DependencyResolver.Default

// Named contexts (hierarchical)
DependencyResolver.Default["ContextName"]

// Registration
.AddAbstract<IService, ServiceImpl>()    // Interface binding
.AddInstance<IService>(instance)          // Singleton
.AddFactory<IService>(() => new Service()) // Factory

// Resolution
.Resolve<IService>()
.SmartResolve(object)  // Auto-inject interface properties
```

### File System Abstraction

**Interface:** `IFileSystem` (Figlotech.Core/FileAccessAbstractions/)

Enables swapping between local filesystem, Azure Blobs, or custom implementations:
```csharp
public interface IFileSystem {
    Task Write(string relative, Func<Stream, Task> func);
    Task<bool> Read(string relative, Func<Stream, Task> func);
    IEnumerable<string> GetFilesIn(string relative);
    Task<bool> ExistsAsync(string relative);
    IFileSystem Fork(string relative);  // Sub-context
}
```

**Implementations:**
- `FileAccessor` - Local filesystem
- `MixedFileAccessor` - Combines multiple IFileSystem instances
- `AzureBlobsFileAccessor` - Azure Blob Storage

**SmartCopy utility:** Synchronizes directories between any two IFileSystem implementations (like Robocopy but abstracted).

### Database Abstraction (Figlotech.BDados)

**Core interface:** `IRdbmsDataAccessor` (Figlotech.BDados/DataAccessAbstractions/)

Custom ORM-like layer wrapping ADO.NET:

#### Transaction Pattern
```csharp
await dataAccessor.AccessAsync(async (transaction) => {
    var user = await dataAccessor.LoadFirstOrDefaultAsync<User>(
        transaction,
        u => u.Email == email
    );
    user.LastLogin = DateTime.UtcNow;
    await dataAccessor.SaveItemAsync(transaction, user);
}, cancellationToken);
```

#### DataObject Pattern
```csharp
public class User : DataObject<User> {
    // Inherited: Id, RID, CreatedTime, UpdatedTime

    [Field("email")]
    public string Email { get; set; }

    [ForeignKey("company_id")]
    public long CompanyId { get; set; }

    [AggregateObject("CompanyId")]  // Auto-join loading
    public Company Company { get; set; }
}
```

#### Mapping Attributes
Located in `DataAccessAbstractions/Attributes/`:
- `[Field]` - Column mapping
- `[PrimaryKey]` - Primary key
- `[ReliableId]` - Unique RID identifier
- `[ForeignKey]` - Foreign key relationships
- `[AggregateField]` / `[AggregateList]` / `[AggregateObject]` - Join loading
- `[NoUpdate]` / `[ReadOnly]` - Persistence control
- Validation attributes - Business rules

#### Query Builder
```csharp
// Short alias
var query = new q("SELECT * FROM Users WHERE Name = {0}", "John");

// Fluent builder (QueryBuilder)
var qb = new QueryBuilder()
    .Append("SELECT * FROM Users")
    .Where("Age > {0}", 18);
```

### Data Flow Module

**Location:** `Figlotech.DataFlow/`

ETL-style transformation pipelines using pipe-and-filter architecture:
```csharp
var pipeline = new CsvDataSource("input.csv")
    .Pipe(new Selector(...))
    .Pipe(new OtherTransform(...));

await foreach(var row in pipeline) {
    // Process transformed data
}
```

**Core interfaces:**
- `IDataTransform` - Pipeline stage
- `IDataSelector` - Column selector/transformer

## Key Utilities

### ReflectionTool
**Location:** `Figlotech.Core/Helpers/ReflectionTool.cs`

Cached reflection operations to avoid repeated costly reflection:
- Member access (fields & properties)
- Attribute scanning
- Type relationship checking

### WorkQueuer
**Location:** `Figlotech.Core/WorkQueuer.cs`

Background job processing with sophisticated features:
```csharp
var queuer = new WorkQueuer("MyQueue", maxConcurrentJobs: 4);
await queuer.Enqueue("JobName", async (cancelToken) => {
    // Async work here
});
```

Features: priority-based execution, concurrent workers, cancellation, telemetry.

### RID (Reliable ID)
**Location:** `Figlotech.Core/RID.cs`

Globally unique identifier generation:
- Machine fingerprinting
- Base36 encoding
- Distributed ID generation

### Caching Solutions
- `TimedCache<TKey, T>` - Time-based expiration
- `SelfInitializedCache` - Lazy initialization with factory
- `BasicRamCache` - Simple in-memory cache
- `ResultCache` - Memoization
- `FileAssistedCache` - Persistent cache backed by filesystem

### AsyncEnumerableWeaver
**Location:** `Figlotech.Core/ParallelFlow/AsyncEnumerableWeaver.cs`

Merges multiple sorted async enumerables into a single ordered stream (merge-sort pattern):
```csharp
await foreach(var item in AsyncEnumerableWeaver.Weave(comparer, batch1, batch2, batch3)) {
    // Items yielded in sorted order
}
```

### Extension Methods
**Location:** `Figlotech.Core/Extensions/`

Extensive extension methods for:
- Object - JSON serialization, DTO mapping, memberwise copying
- String - Regex, encoding, parsing
- IEnumerable - LINQ extensions, parallel processing
- DateTime - Date manipulation
- Stream - Stream processing
- IAsyncEnumerable - Async enumeration
- And many more (ADO, decimals, doubles, arrays, etc.)

## Configuration

### Global Settings
```csharp
FiTechCoreExtensions.EnableDebug
FiTechCoreExtensions.EnableBenchMarkers
FiTechCoreExtensions.ApiLogger
```

### Warning Configurations
All projects treat these warnings as errors:
- `CS4014` - Unawaited async calls
- `VSTHRD103` - Threading violations
- `CA2000` - Dispose objects
- `CA2213` - Disposable fields
- `CA1001` - Types that own disposable fields

## Development Patterns

### Async-First Design
Most I/O operations return `Task`, `ValueTask`, or `IAsyncEnumerable<T>`.

### Interface-Driven Abstraction
Heavy use of interfaces (IFileSystem, IDataAccessor, IDataObject) enables swappable implementations.

### Attribute-Driven Configuration
Database mapping, validation rules, and persistence behavior defined via attributes on classes/properties.

### Reflection Caching
ReflectionTool caches member info to avoid repeated costly reflection operations.

### Transaction-Scoped Operations
Database operations use explicit transaction objects for control flow.

## Versioning

Version format: `1.0.<git-rev-count>.<manual-rev>`
- `git-rev-count`: Number of commits in branch (from `git rev-list --count`)
- `manual-rev`: Incremented counter in `rev` file

## CI/CD

Jenkins pipeline (Jenkinsfile) builds and publishes to GitHub Packages with .NET 6.0.

## Important Files

- `fitech.version` - Auto-generated git revision count
- `rev` - Manual revision counter
- `figlotech.sln` - Main solution file
- `nuget.config` - NuGet feed configuration
