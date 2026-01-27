# Figlotech.BDados DataAccessAbstractions Attributes

## Purpose
DataAccessAbstractions attributes provide metadata for BDados data objects. They guide how tables/columns are created, how data is queried, and how related objects are aggregated. Most attributes are passive markers interpreted by BDados reflection and structure checking.

## Attribute Groups

### Schema + Persistence
- `FieldAttribute`: Declares a persistent column.
  - `Type`: RDBMS column type.
  - `Options`: Extra RDBMS-specific options used by the structure checker.
  - `Size`: Character length or size (maps to `CHARACTER_MAXIMUM_LENGTH`).
  - `Precision`: Numeric precision (maps to `NUMERIC_PRECISION`).
  - `DefaultValue`: Default value (maps to `COLUMN_DEFAULT`).
  - `AllowNull`: Nullability (maps to `IS_NULLABLE`).
  - `PrimaryKey`, `Unique`, `Index`, `Unsigned`: Column constraints.
  - `Table`, `Name`: Override table/column names (maps to `TABLE_NAME`, `COLUMN_NAME`).
  - `Charset`, `Collation`: Character set settings.
  - `Comment`: Column comment.
  - `GenerationExpression`: Computed/generated expression.
- `PrimaryKeyAttribute`: Marks a primary key field (legacy note: BDados treats `Id` as PK by convention).
- `ForeignKeyAttribute`: Describes a foreign key relationship.
  - `Table`, `Column`: Local table/column.
  - `RefTable`, `RefColumn`: Referenced table/column.
  - `RefType`: Referenced CLR type (sets `RefTable` from type name).
  - `ConstraintName`: Optional explicit constraint name; default uses `fk_{Table}_{Column}_{RefTable}_{RefColumn}`.
- `OldNameAttribute`: Provides previous column name for migrations/renames (`Name`).
- `ViewOnlyAttribute`: Prevents table creation for a data object (view-only intent).

### Persistence Policies
- `NoUpdateAttribute`: Skips updates for a field on persistence.
- `ReadOnlyAttribute`: Implements `IPersistencePolicyAttribute`, always returns `false` to skip persistence.

### Timestamps + Identity
- `CreationTimeStampAttribute`: Marks a creation timestamp field.
- `UpdateTimeStampAttribute`: Marks a last-update timestamp field.
- `ReliableIdAttribute`: Marks a reliable identifier (RID) used for data sync scenarios beyond auto-increment IDs.

### Query Behavior + Display
- `QueryComparisonAttribute`: Declares preferred comparison behavior (contains, exact, ignore-case, etc.).
- `DisplayAttribute`: UI metadata for label, order, format, and custom formatter type.

### Aggregation (Object Graph Loading)
- `AbstractAggregationAttribute`: Base type with `Flags` that can be split for loader behavior.
- `AggregateObjectAttribute`: Loads a related object into a field using a key.
- `AggregateFieldAttribute`: Loads a field from a related object.
- `AggregateListAttribute`: Loads a related list using a remote key field.
- `AggregateFarFieldAttribute`: Loads a field from an indirectly related object via an intermediate relation.

### Validation
- `ValidationAttribute`: Base class for custom validators.
- `MaxLengthAttribute`, `MinLengthAttribute`, `MaskAttribute`: Sample validators returning `ValidationErrors`.

## Design Notes
- Attributes are declarative; BDados reflection and structure checks interpret them.
- For schema-related attributes, prefer explicit metadata over inferred defaults when compatibility matters.

## IRdbmsDataAccessor

### Role
`IRdbmsDataAccessor` is the primary RDBMS gateway. It owns transaction lifecycle helpers, query execution, CRUD for `IDataObject` types, aggregation loading, and schema checks. The default implementation is `RdbmsDataAccessor`.

### Core Usage Model
Most usage is ORM-like: you define data objects annotated with BDados attributes and let the accessor handle persistence and object graph loading. For advanced scenarios, you can execute raw SQL via `Execute`/`Query`, and for precise updates use `UpdateAndMutateAsync`/`UpdateAndMutateIfSuccessAsync` to keep in-memory objects synchronized with database updates.

### Key Responsibilities
- Transaction handling via `Access`/`AccessAsync` and explicit `BDadosTransaction` creation.
- Query execution through `IQueryBuilder` and `IQueryGenerator`.
- CRUD: `LoadAll`, `Fetch`, `SaveItem`, `SaveList`, `Delete` and async variants.
- Aggregated loading of object graphs via `AggregateLoad` methods.
- Structure checks and metadata loading (`GetInfoSchemaColumns`).
- Change events for save success/failure and object mutation.

### ORM-Like Mapping (Attributes + IDataObject)
Define `IDataObject` types and annotate them with the attributes described above (fields, keys, aggregates, validation). BDados uses those attributes to map tables/columns, enforce constraints, and load related objects.

Typical pattern:
- Call `EnsureDatabaseExistsAsync` and `CheckStructure` once during setup.
- Use `LoadAll`, `Fetch`, `LoadFirstOrDefault`, `LoadByRid` for reading.
- Use `SaveItem`/`SaveList` for persistence.
- Use `AggregateLoad*` to populate related objects/lists driven by aggregation attributes.

### Transactions
Use transaction helpers for consistency and rollback.

- `Access`/`AccessAsync`: wraps a function in a transaction and handles commit/rollback.
- `CreateNewTransaction`/`CreateNewTransactionAsync`: creates a `BDadosTransaction` you can pass to other methods.
- `CreateNonDbLevelTransaction`: creates a transactional scope without opening a DB-level transaction (advanced).
- `AccessAsyncCoroutinely`: provides a streaming transaction scope using a `ChannelWriter<T>`.

```csharp
await dataAccessor.AccessAsync(async tsn => {
    var item = await dataAccessor.LoadFirstOrDefaultAsync<MyDataObject>(tsn, x => x.Id == 1);
    item.Name = "Updated";
    await dataAccessor.SaveItemAsync(tsn, item);
}, CancellationToken.None);
```

### CRUD + Query Methods
All core CRUD methods exist in sync/async and transaction/non-transaction forms.

- `LoadAll<T>` / `LoadAllAsync<T>`: materialize full lists. Accept `IQueryBuilder`, `LoadAllArgs<T>`, ordering and paging.
- `Fetch<T>` / `FetchAsync<T>`: enumerable/streaming-friendly reads.
- `LoadFirstOrDefault*`, `LoadByRid`, `LoadById`: convenience reads.
- `SaveItem*`, `SaveList*`: persistence with optional `recoverIds` for ID backfill.
- `Delete*`: delete by instance, by predicate, or by RID-not-in.

### Aggregation (Object Graph Loading)
`AggregateLoad*` loads objects along with related fields/lists defined by aggregation attributes.

```csharp
var orders = dataAccessor.AggregateLoad<Order>(
    LoadAll.From<Order>().Where(o => o.CustomerRid == customerRid)
);
```

Streaming aggregation:
```csharp
await dataAccessor.AccessAsync(async tsn => {
    await foreach (var item in dataAccessor.AggregateLoadAsyncCoroutinely(
        tsn,
        LoadAll.From<Order>().Where(o => o.Status == "Open")
    )) {
        // consume item
    }
}, ct);
```

### Raw SQL (Execute / Query)
Use `Execute` for non-query SQL and `Query`/`QueryAsync` for mapping results to objects.

- `Execute(IQueryBuilder)` / `Execute(BDadosTransaction, IQueryBuilder)`
- `Query<T>(IQueryBuilder)` / `QueryAsync<T>(...)`
- `QueryCoroutinely<T>` for streaming large query results.
- `QueryToJsonAsync<T>` to serialize results directly to a writer.

```csharp
var totals = dataAccessor.Query<TotalsRow>(
    Qb.Fmt("SELECT Customer, SUM(Total) Total FROM Orders GROUP BY Customer")
);

var affected = dataAccessor.Execute(
    Qb.Fmt("UPDATE Orders SET Status = {0} WHERE Status = {1}", "Closed", "Open")
);
```

### Granular Updates with Rollback-Safe Mutation
`UpdateAsync`, `UpdateAndMutateAsync`, and `UpdateAndMutateIfSuccessAsync` allow fine-grained column updates without saving the whole object.

- `UpdateAsync`: updates columns in DB only.
- `UpdateAndMutateAsync`: updates DB and mutates the in-memory object immediately (use when you want the object to reflect the new values even if additional work happens in-memory).
- `UpdateAndMutateIfSuccessAsync`: defers mutation until the transaction commits successfully, keeping rollback semantics consistent.

```csharp
await dataAccessor.AccessAsync(async tsn => {
    await dataAccessor.UpdateAndMutateAsync(
        tsn,
        item,
        (x => x.Status, "Approved"),
        (x => x.UpdatedAt, DateTime.UtcNow)
    );
}, ct);
```

### Existence Checks
Use `ExistsByRIDAsync` / `ExistsByIdAsync` to verify existence without loading an object.

### Schema / Metadata
- `EnsureDatabaseExistsAsync` creates the database when missing.
- `GetInfoSchemaColumns` returns schema metadata as `FieldAttribute` instances.

### Data Sync / Replication
`SendLocalUpdates`, `ReceiveRemoteUpdates*`, and `LoadUpdatedItemsSince` are used for synchronization pipelines and incremental replication using timestamp/rid semantics.

### Events
The accessor emits notifications for save lifecycle and in-memory changes:
- `OnSuccessfulSave`
- `OnFailedSave`
- `OnDataObjectAltered`

### Usage Examples

Create/ensure structure and load data:
```csharp
await dataAccessor.EnsureDatabaseExistsAsync();
await dataAccessor.CheckStructure(typeof(MyDataObject).Assembly);

var items = dataAccessor.LoadAll<MyDataObject>(
    LoadAll.Where<MyDataObject>(x => x.IsActive)
        .OrderBy(x => x.Name)
        .Limit(100)
);
```

Transaction-scoped work:
```csharp
dataAccessor.Access(tsn => {
    var item = dataAccessor.LoadByRid<MyDataObject>(tsn, rid);
    item.Name = "Updated";
    dataAccessor.SaveItem(tsn, item);
});
```

Aggregate load of related objects:
```csharp
var orders = dataAccessor.AggregateLoad<Order>(
    LoadAll.Where<Order>(o => o.CustomerRid == customerRid)
);
```

Raw query via `IQueryBuilder`:
```csharp
var results = dataAccessor.Query<MyRow>(
    Qb.Fmt("select * from {0} where status = {1}", "Orders", "Open")
);
```
