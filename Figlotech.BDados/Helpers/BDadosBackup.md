# BDados binary backups

`BDadosBackup` accepts an `IRdbmsDataAccessor` and an explicit list of model types.
Only public instance fields/properties marked `[Field]` are exported, including inherited
members. Properties must be readable and writable; readonly fields and indexers are rejected.
Models need a public parameterless constructor. Database operations additionally require
`IDataObject` and reject `[ViewOnly]` models and duplicate table names.

```csharp
using Figlotech.BDados.Helpers;

var schema = BDadosBackup.CreateSchema<Customer>();
byte[] schemaBytes = schema.Serialize();
byte[] rowBytes = BDadosBackup.Export(customer, schema);

var storedSchema = BDadosBackupSchema.Deserialize(schemaBytes);
Customer copy = BDadosBackup.Import<Customer>(storedSchema, rowBytes);

var tables = new[] { typeof(Customer), typeof(Order) };
await new BDadosBackup(sourceAccessor).BackupAsync(outputStream, tables, cancellationToken);
await new BDadosBackup(targetAccessor).RestoreAsync(inputStream, tables, cancellationToken);
```

## Timestamp incrementals

```csharp
await new BDadosBackup(sourceAccessor).BackupIncrementalAsync(
    outputStream, tables, since, cancellationToken);

// Incrementals use the same self-contained archive format and restoration method.
await new BDadosBackup(targetAccessor).RestoreAsync(inputStream, tables, cancellationToken);
```

`since` is a `DateTime`. The source database selects rows with
`CreatedAt > since OR UpdatedAt > since` using parameterized queries. Equality does not
qualify; a null UpdatedAt still allows a row to qualify through CreatedAt. Rows satisfying
both conditions are exported once, and the accessor's default query limit is not applied.

Each selected table's schema is written once in the header, including tables with no matching
rows. Incrementals retain the full backup's length framing, integrity checks, transaction,
cancellation and non-seeking stream support. No full table is loaded into memory to filter it.

Models must expose both persisted timestamps as `[Field]` members of type DateTime or
DateTime?. `[CreationTimeStamp]` and `[UpdateTimeStamp]` take precedence over the conventional
CreatedAt/UpdatedAt names, allowing custom timestamp column names. Missing, ambiguous or
incompatible timestamp mappings fail before any bytes are written. Full backups do not
require these timestamp mappings.

The cutoff is passed unchanged, including its Kind and precision; use the source database's
time convention. This method exports current rows created or updated since a cutoff; it does
not track deletions, record the cutoff in the archive, or manage backup chains/watermarks.
Keep that bookkeeping alongside the archive. Restore uses the existing save behavior described
below, including its identity and timestamp policies.

The schema stores ordered column names, wire types, CLR nullability, enum identity, and
mapping metadata (database type, size, precision, nullability, keys, uniqueness, unsignedness,
old names and foreign references). Table/column names follow BDados' CLR type/member name
convention. New schemas order columns by ordinal name; imported rows always follow the stored
schema order, even when the destination model has changed. Aggregate/navigation properties
without `[Field]` are excluded. Export still requires an exact schema; import is deliberately
more permissive.

Supported values are Boolean, all signed/unsigned integer primitives, Single, Double,
Decimal, Char, String, byte[], Guid, DateTime, DateTimeOffset, TimeSpan, DateOnly, TimeOnly,
and enums using their underlying integer representation. Nullable value types are supported.
Null and empty strings/arrays remain distinct. DateTime ticks and Kind, DateTimeOffset ticks
and offset, and decimal bits are preserved. Unsupported types fail explicitly; no fallback
object serialization or assembly loading occurs.

## Version 1 format

All integers and numeric values use little-endian encoding. All byte lengths exclude their
own prefix. Text and schemas use strict UTF-8 without a BOM. The archive is:

1. Eight ASCII bytes `FGBACKUP`.
2. Int32 protocol version (`1`).
3. Int32 header byte length, followed by that many header bytes.
4. 32-byte SHA-256 of the header bytes.
5. Table data sections, in header order.
6. Eight ASCII bytes `FGBEND01`.
7. 32-byte SHA-256 of **all preceding archive bytes**, including the completion marker.

The header starts with an Int32 table count. Each table entry contains an Int32 UTF-8 table
name length and name bytes, then an Int32 schema length and JSON schema bytes. Thus every
schema is available and validated before restore writes any rows.

Each table data section contains repeated Int32 row lengths and raw row bytes. Int32 `-1`
terminates the table, followed by an Int64 actual row count. Empty tables have the same
terminator followed by a zero count. Counts do not need to be known before streaming starts.

A raw row contains, for each column in schema order, an Int32 value length and the value
bytes. A length of `-1` means null and has no payload. Strings and byte arrays contain their
raw bytes, with no extra internal length prefix. There are no member names, type tags, or
schema bytes in a row. Boolean is one byte (0/1); Char is a UInt16 UTF-16 code unit; Guid is
the 16-byte .NET `Guid.ToByteArray()` layout; Decimal is the 16-byte `BinaryWriter` layout.
DateTime is Int64 ticks plus one byte Kind; DateTimeOffset is Int64 ticks plus Int16 offset
minutes; TimeSpan/TimeOnly are Int64 ticks; DateOnly is an Int32 day number.

The hashes detect corruption, including reordered/removed rows. They are not authentication
or encryption. The reader consumes exactly one archive, so another archive or enclosing
protocol may follow it on the same stream.

## Streaming and persistence

Only the header and one row at a time are buffered. The default limits are 16 MiB for the
header and 64 MiB per row; constructor arguments can override these. Readers validate lengths
before allocating payload buffers and handle short reads. The supplied streams need only
support asynchronous reading or writing. They remain open; flushing the destination is the
caller's responsibility. Failed exports leave incomplete archives that cannot be restored.

Each operation uses one accessor transaction with Serializable isolation by default; an
optional isolation argument allows provider-specific choices. Backup fetches all rows
without the accessor's default query limit. Restore binds reflected members once per table,
then deserializes and saves each row through `SaveItemAsync`. Length/count/checksum failures,
truncation, cancellation, and failed saves abort restoration through the accessor transaction.
Providers must support transactions for database rollback guarantees.

Create destination tables before restoring. Supply backup types in foreign-key dependency
order when constraints require it; that order is preserved in the archive. Registration order
on restore is irrelevant. Create/update the destination DDL using its current models and
provider before restoring. The backup's database type, size, precision and other DDL metadata
are descriptive; they are not imposed on the destination engine. Cyclic foreign-key handling
is not performed.

## Restoring an older model version

Restore and standalone `Import` decode using the **source schema's wire types**, then map
and convert into the current model. The existing version 1 archive format is unchanged.

- Current table/column names match case-insensitively. `[OldName("PreviousName")]` on the
  destination class or member maps renamed tables and columns, consistent with the structure
  checker. Current names take priority if an archive contains both current and historical
  column names. Ambiguous names/aliases fail rather than depending on reflection order.
- Removed columns are consumed using their byte lengths without assigning them. Added columns
  use the current model's explicitly specified `[Field(DefaultValue = ...)]`, taking precedence
  over constructor/property initializers. When no default is specified, the initializer is
  retained. This includes newly required fields, inherited attributes and public fields.
  Defaults are read and converted once per table from the destination class; they are not
  serialized into the backup schema. Explicit null defaults apply to reference/nullable types.
  Defaults never replace a value already supplied by the archive, including nulls and values
  mapped through `[OldName]`. Invalid or unconvertible defaults fail before database writes,
  naming the affected member. No DDL or SQL-expression evaluation is attempted: defaults must
  be literal values convertible to the CLR member type, not expressions such as CURRENT_TIMESTAMP.
- Nullable/non-nullable changes are accepted. Old non-null values are converted normally;
  old nulls assigned to non-nullable value types retain the constructor/initializer default.
  Old nulls assigned to reference/nullable types remain null.
- Enum type and namespace renames are accepted. Numeric values, including undefined values
  and flags, are retained with checked conversion to the destination underlying integer type.
  Enum member renumbering is a semantic migration and cannot be inferred from these archives.
- Numeric width and signedness changes are accepted per value when the destination can
  represent it exactly. This includes unsigned-to-signed changes common across engines.
  Overflow, fractional-to-integer rounding and floating-point precision loss fail explicitly.
  Numeric 0/1 and Boolean values can be converted between those representations.
- Strings can be parsed into supported scalar types (including GUIDs and enum names); scalar
  values can be formatted as strings. Parsing/formatting uses invariant culture. Dates use
  round-trip formatting, and DateTimeOffset strings require the round-trip `O` format with
  an explicit offset. Binary arrays are not guessed to be strings or GUIDs.
- Newly added tables in the destination do not need to exist in the backup. Unknown archive
  tables fail by default to catch incomplete registration. To deliberately omit removed
  tables, pass `ignoreMissingTables: true`; their bytes, row counts and checksums are still read
  and checked, without saving those rows.

```csharp
[OldName("CustomerV1")]
public class Customer : BaseDataObject<long> {
    [Field, PrimaryKey] public override long Id { get; set; }
    [Field, OldName("Name")] public string DisplayName { get; set; }
    [Field(DefaultValue = 10)] public int NewlyRequiredValue { get; set; }
}

await new BDadosBackup(targetAccessor).RestoreAsync(
    inputStream, currentModelTypes, cancellationToken, ignoreMissingTables: true);
```

`OldNameAttribute` records one historical name. Several consecutive renames cannot be
inferred if the oldest backup name is absent from the destination's name/alias. Unrelated
semantic changes (such as a changed unit of measure or a replacement key scheme) also need
an explicit application migration rather than automatic conversion.

## Engine migration considerations

Restore follows normal accessor save policies: it does not clear existing rows, force
auto-increment IDs, or bypass `[NoUpdate]` and other persistence policies. Use reliable IDs
or application-generated keys where relationships must survive provider identity generation.
Legacy objects are marked `IsReceivedFromSync` to preserve their update timestamp; generic
`IDataObject` saves still apply the accessor's normal timestamp behavior. The standalone
`Export`/`Import` codec preserves all supported mapped values exactly when their types and
nullability have not changed.

In particular, the MySQL and PostgreSQL insert generators omit database-generated primary
keys unless the model implements `IApplicationGeneratedId<TId>`. Do not assume numeric IDs
will remain equal after migration; preserve relations through reliable/application-generated
keys or handle remapping explicitly. For explicitly inserted sequence-backed keys, reseed the
destination sequence as part of the DDL migration before normal inserts resume.

Temporal SQL column types, time zones, constraints, collations, timestamp precision and
provider parameter support remain destination concerns. The codec does not change a
DateTime's Kind or invent a time zone. PostgreSQL's existing parameter adapter also assumes
Int32-backed enums and formats UInt64 values as strings; using other enum backings or unsigned
numeric columns there can require an adapter/model migration, even though the binary codec
can represent those values. These restore compatibility changes do not alter global provider
save behavior.
