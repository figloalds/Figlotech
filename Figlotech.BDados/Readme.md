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

### Key Responsibilities
- Transaction handling via `Access`/`AccessAsync` and explicit `BDadosTransaction` creation.
- Query execution through `IQueryBuilder` and `IQueryGenerator`.
- CRUD: `LoadAll`, `Fetch`, `SaveItem`, `SaveList`, `Delete` and async variants.
- Aggregated loading of object graphs via `AggregateLoad` methods.
- Structure checks and metadata loading (`GetInfoSchemaColumns`).
- Change events for save success/failure and object mutation.

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
