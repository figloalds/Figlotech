# BDados binary backups

`BDadosBackup` accepts an `IRdbmsDataAccessor` and an explicit list of model types.
Only public instance fields/properties marked `[Field]` are exported, including inherited
members. Readonly fields and properties without setters are omitted, including inherited members.
Remaining properties must be readable; indexers are rejected. On restore, archived columns whose
current members are readonly are skipped like removed columns.
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

// Publish a completed, flushed archive through a sibling temporary file.
await new BDadosBackup(sourceAccessor).BackupFileAsync("backup.fgb", tables, cancellationToken);

// Prefer an explicit recovery policy and inspect the report after commit.
var restorer = new BDadosBackup(targetAccessor, new BDadosBackupOptions { RequireEmptyTables = true });
restorer.Options.IgnoredTables.Add("IntentionallyRemovedTable");
BDadosBackupRestoreReport report = await restorer.RestoreWithReportAsync(inputStream, tables, cancellationToken);
```

Restore saves batches of 1,000 rows per table by default. Configure the options after
construction or supply an options instance to the constructor:

```csharp
var bkp = new BDadosBackup(targetAccessor);
bkp.Options.RestoreChunkSize = 500;
await bkp.RestoreAsync(inputStream, tables, cancellationToken);

var configured = new BDadosBackup(targetAccessor,
    new BDadosBackupOptions { RestoreChunkSize = 2000 });
```

Each backup instance creates its own default options when none are supplied. A supplied
options instance is retained. `RestoreChunkSize` must be positive and is read once when
restore starts; changes during restoration apply to the next operation. A final partial
batch is saved at each table boundary; empty or skipped tables do not issue saves.
All batches remain in the same restore transaction. `RestoreChunkBytes` additionally limits
each batch to 16 MiB of encoded rows by default; a single larger row can occupy its own batch.
Decoded objects can be larger than their encoded representation. Duplicate keys are tracked
across batches per table, with limits of `MaxTrackedKeys = 1_000_000` and
`MaxTrackedKeyBytes = 64 MiB` (an estimate including key payloads and entry overhead).
Exceeding either limit fails and rolls back; increase the configured limits for larger tables.

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
Keep that bookkeeping alongside the archive. This is a changed-row export, not a complete
point-in-time recovery chain. Strict timestamp cutoffs can miss equal timestamps and
late-committing changes. A reliable chain needs commit-aware watermarks/change tracking and
deletion records outside this API; do not advance a watermark to the backup completion time.
Restore preserves the archived identities and timestamps as described below.

The schema stores ordered column names, wire types, CLR nullability, enum identity, and
mapping metadata (database type, size, precision, nullability, keys, uniqueness, unsignedness,
old names and foreign references). Table/column names follow BDados' CLR type/member name
convention. New schemas order columns by ordinal name; imported rows always follow the stored
schema order, even when the destination model has changed. Aggregate/navigation properties
without `[Field]` are excluded. Export still requires an exact schema; import is deliberately
more permissive.

Supported values are Boolean, all signed/unsigned integer primitives, Single, Double,
Decimal, Char, String, byte[], Guid, DateTime, DateTimeOffset, TimeSpan, DateOnly, TimeOnly,
enums using their underlying integer representation, and custom `IBinarySerializable` values.
Nullable value types are supported.
Null and empty strings/arrays remain distinct. DateTime ticks and Kind, DateTimeOffset ticks
and offset, and decimal bits are preserved. Unsupported types fail explicitly; no fallback
object serialization or assembly loading occurs.

## Custom binary values

Implement `Figlotech.Core.Interfaces.IBinarySerializable` on a concrete struct or a class
with a public parameterless constructor. `ToBytes()` returns the payload without framing;
`FromBytes(byte[])` replaces the new instance's state and should reject invalid payloads.
Null values bypass both methods. `ToBytes()` must not return null.

```csharp
using System;
using System.Buffers.Binary;
using Figlotech.Core.Interfaces;

public struct TimeField : IBinarySerializable {
    public long Ticks { get; set; }
    public static bool IsFixedLength => true;
    public static int Length => 8;

    public byte[] ToBytes() {
        var bytes = new byte[Length];
        BinaryPrimitives.WriteInt64LittleEndian(bytes, Ticks);
        return bytes;
    }

    public void FromBytes(byte[] bytes) {
        ArgumentNullException.ThrowIfNull(bytes);
        if (bytes.Length != Length) throw new ArgumentException("Expected eight bytes.", nameof(bytes));
        Ticks = BinaryPrimitives.ReadInt64LittleEndian(bytes);
    }
}
```

The interface's static virtual properties default to `IsFixedLength = false` and `Length = 0`;
variable-length types only need the two instance methods. Static interface members require
C# 11 or later (Core now uses C# 11). The static metadata is cached per CLR type and stored in
the schema, so row processing does not repeatedly discover it. Fixed length must be non-negative
and every emitted payload must have that exact size; zero-byte payloads are allowed.

Custom columns store `BinaryType` (the CLR full name) and `FixedLength` (null for variable-length).
Restoration requires matching custom type identity (or a retained `[BackupAlias]` containing
its old full CLR name) and length metadata on the caller's model;
custom values are not automatically converted to strings, byte arrays or other custom types.
Keep the custom payload representation stable across versions. Aliases do not convert binary
layouts: changing a payload requires a version tag inside the payload and a `FromBytes` reader
that understands every historical version. Equal payload sizes cannot prove semantic compatibility.
Removed columns can still be
skipped using only the archive metadata, without constructing or resolving the old custom type.

## Archive and schema versions

The outer archive protocol remains version 1. New schemas have `Version = 2` and use compact
fixed-size field framing. Version 1 schemas/archives remain readable, and explicitly exporting
against an existing version 1 schema retains its original length-prefixed layout. Older readers
reject version 2 schemas before restoring any rows.

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

A raw row contains values in schema order. With a version 2 schema:

- A column with `FixedLength` writes exactly that many payload bytes, without a length prefix.
  Nullable fixed-size columns first write one marker byte: `0` for null (no payload), `1` for
  a non-null payload. Other marker values are invalid.
- Variable-length columns (String, byte[], and variable-length custom values) write an Int32
  byte length and the raw payload. Length `-1` means null and has no payload.

All built-in fixed-size columns record their byte size once in the schema: Boolean/Byte/SByte
use 1; Int16/UInt16/Char use 2; Int32/UInt32/Single/DateOnly use 4;
Int64/UInt64/Double/TimeSpan/TimeOnly use 8; DateTime uses 9; DateTimeOffset uses 10;
Decimal/Guid use 16. Enums use the size of their underlying integer type. Schema validation
rejects incorrect sizes. Version 1 schemas always use an Int32 length prefix for every field.

There are no member names, type tags, or schema bytes in a row. Boolean is one byte (0/1);
Char is a UInt16 UTF-16 code unit; Guid is
the 16-byte .NET `Guid.ToByteArray()` layout; Decimal is the 16-byte `BinaryWriter` layout.
DateTime is Int64 ticks plus one byte Kind; DateTimeOffset is Int64 ticks plus Int16 offset
minutes; TimeSpan/TimeOnly are Int64 ticks; DateOnly is an Int32 day number.

The hashes detect corruption, including reordered/removed rows. They are not authentication
or encryption. The reader consumes exactly one archive, so another archive or enclosing
protocol may follow it on the same stream.

## Streaming and persistence

Backup buffers the header and one encoded row at a time. Restore additionally buffers up to
`Options.RestoreChunkSize` decoded rows. The default limits are 16 MiB for the
header and 64 MiB per row; constructor arguments can override these. Readers validate lengths
before allocating payload buffers and handle short reads. The supplied streams need only
support asynchronous reading or writing. They remain open; flushing the destination is the
caller's responsibility. Failed exports leave incomplete archives that cannot be restored.
`BackupFileAsync` supplies an optional file workflow: write a unique sibling temporary file,
flush it to disk, close it, then move it to the requested path. It does not overwrite an existing
file unless `overwrite: true`; failed writes leave the previous file intact. Filesystem rename
and crash-durability guarantees depend on the destination filesystem/storage service.

Each operation uses one accessor transaction with Serializable isolation by default; an
optional isolation argument allows provider-specific choices. Backup fetches all rows
without the accessor's default query limit. Restore binds reflected members once per table,
then deserializes and saves batches through `SaveListAsync<T>` using the concrete destination
model type. An internal `BDadosTransaction.RestorationMode` routes legacy and generic objects
through the existing generic save methods, preserving keys, CreatedAt and UpdatedAt. A small
shared SQL binding supplies exactly the mapped/defaulted columns and bypasses ordinary
identity generation, RID reconciliation, NoUpdate policies and application save notifications.
The backup transaction likewise bypasses OnAfterLoad hooks. Ordinary accessor operations
outside these modes retain their existing behavior.

Restore reads each saved row back and checks supplied values, detecting database rounding,
truncation and immediate trigger changes. It also rejects key collisions caused by changed
database equality/collations instead of silently merging different keys. This currently uses
an existence query, a write, and a verification query per row; restoration prioritizes
correctness over the legacy bulk-save throughput. Transaction callbacks, rollback object lists,
command history and per-row benchmark history do not retain transferred objects. The bounded
identity index retains keys only. Length/count/checksum failures,
truncation, cancellation, and failed saves abort restoration through the accessor transaction.
Providers must support transactions for database rollback guarantees. Cancellation does not
cancel rollback or MySQL session cleanup. If MySQL session cleanup itself fails, the plugin
clears the affected connection pool and closes the unsafe connection. Only providers explicitly implementing the backup
dialect/identifier contract can restore; SQLite, PostgreSQL and MySQL are supported. MySQL
source and destination tables must use InnoDB.

Create destination tables before restoring. Supply backup types in foreign-key dependency
order when constraints require it; that order is preserved in the archive. Registration order
on restore is irrelevant. Create/update the destination DDL using its current models and
provider before restoring. The backup's database type, size, precision and other DDL metadata
are descriptive; they are not imposed on the destination engine. Restore never creates or
clears tables. By default it merges by the preserved primary key and leaves unrelated rows
in place; set `RequireEmptyTables = true` for recovery into empty tables. Restores should run
against an isolated destination with application writers stopped.

Each model must have one persisted primary key supplied by the archive. Missing keys,
duplicate keys (even across batches), and custom binary primary keys are rejected. Keys are
never synthesized from constructors or application generators. `RestoreWithReportAsync`
returns source/destination tables, skipped tables, ignored/defaulted columns, rows read and
rows restored/merged only after successful completion. The archive format is unchanged.

SQLite uses transaction-scoped `PRAGMA defer_foreign_keys = ON` and runs
`pragma_foreign_key_check` before committing, even when connection-level enforcement was off.
MySQL saves and restores both foreign_key_checks and sql_mode, enables strict conversion and
NO_AUTO_VALUE_ON_ZERO during restoration, and validates actual affected foreign keys before
commit. Validation includes composite keys. MySQL and SQLite database triggers still run;
triggers with external effects or changes to other rows need an explicit recovery policy.

### PostgreSQL foreign-key checks during restore

Restore executes `SET LOCAL session_replication_role = 'replica'` on the same connection
and transaction as the saves. PostgreSQL restores the previous session setting on commit
or rollback; restore does not issue an enable command inside a failed transaction. This
preserves the original database error instead of masking it with SQLSTATE `25P02`
(transaction already aborted).

The restore login needs superuser privileges, or on PostgreSQL 15 and later an administrator
can grant the narrower parameter privilege (replace `restore_user` with the actual role):

```sql
GRANT SET ON PARAMETER session_replication_role TO restore_user;
```

Database/table ownership alone is insufficient. A missing privilege produces SQLSTATE
`42501`; inspect the inner exception of `BDadosException` for the original PostgreSQL error.
The replica setting also suppresses ordinary triggers/rules, not just foreign keys, and
restoring the setting does not retroactively validate the imported relationships. BDadosBackup
therefore explicitly checks actual foreign-key constraints involving restored tables before
commit, including composite MATCH FULL constraints and references to omitted tables. Restored
tables are locked against concurrent writes. Ascending SERIAL/IDENTITY sequences are advanced
without moving them backward, using transactional ALTER SEQUENCE RESTART; sequence privileges
are also required. Descending or exhausted sequences fail explicitly. Restore supports
GENERATED ALWAYS identities through OVERRIDING SYSTEM VALUE.

References: [PostgreSQL SET](https://www.postgresql.org/docs/current/sql-set.html),
[session_replication_role](https://www.postgresql.org/docs/current/runtime-config-client.html#GUC-SESSION-REPLICATION-ROLE),
[PostgreSQL 15 GRANT](https://www.postgresql.org/docs/15/sql-grant.html).

## Restoring an older model version

Restore and standalone `Import` decode using the **source schema's wire types**, then map
and convert into the current model. The existing version 1 archive format is unchanged.

- Current table/column names match case-insensitively. `[OldName("PreviousName")]` on the
  destination class or member maps renamed tables and columns, consistent with the structure
  checker. Current names take priority if an archive contains both current and historical
  column names. Ambiguous names/aliases fail rather than depending on reflection order.
  Add repeatable `[BackupAlias("OlderName")]` attributes to retain all historical names without
  changing the DDL checker's OldName behavior. Two historical source columns targeting one
  destination member fail when no current-name match disambiguates them.
- Removed columns are consumed using their byte lengths without assigning them. Added columns
  use the current model's explicitly specified `[Field(DefaultValue = ...)]`, taking precedence
  over constructor/property initializers. Standalone Import retains initializers when no
  attribute default is specified. Database restoration instead omits those absent columns
  from INSERT/UPDATE: database defaults (including SQL expressions) apply on insert, and
  existing values remain unchanged on merge. A CLR initializer is not a database default.
  Newly required columns need an explicit Field default or an actual destination SQL default.
  Defaults are read and converted once per table from the destination class; they are not
  serialized into the backup schema. Explicit null defaults apply to reference/nullable types.
  Defaults never replace a value already supplied by the archive, including nulls and values
  mapped through `[OldName]`. Invalid or unconvertible defaults fail before database writes,
  naming the affected member. No DDL or SQL-expression evaluation is attempted: defaults must
  be literal values convertible to the CLR member type, not expressions such as CURRENT_TIMESTAMP.
- Old non-null values can be restored into nullable or non-nullable members. An archived
  NULL remains distinct from an absent column: NULL-to-non-nullable value types fails, and
  reference/nullable values remain NULL subject to the database's NOT NULL constraint.
  A default never replaces a supplied NULL; an explicit data migration is required.
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
  tables, prefer `Options.IgnoredTables`. The broader `ignoreMissingTables: true` remains available;
  their bytes, row counts and checksums are still read
  and checked, without saving those rows.

```csharp
[OldName("CustomerV2"), BackupAlias("CustomerV1")]
public class Customer : BaseDataObject<long> {
    [Field, PrimaryKey] public override long Id { get; set; }
    [Field, OldName("Name")] public string DisplayName { get; set; }
    [Field(DefaultValue = 10)] public int NewlyRequiredValue { get; set; }
}

await new BDadosBackup(targetAccessor).RestoreAsync(inputStream, currentModelTypes, cancellationToken);
```

`OldNameAttribute` records one historical name. Keep permanent BackupAlias entries for earlier
names, and do not reuse a historical table/column name for unrelated data. Unrelated
semantic changes (such as a changed unit of measure or a replacement key scheme) also need
an explicit application migration rather than automatic conversion.

## Engine migration considerations

Restore preserves numeric IDs without requiring IApplicationGeneratedId. SQLite and InnoDB
advance counters on explicit inserts; PostgreSQL sequences are synchronized before commit.
Application save methods retain their ordinary identity policies outside RestorationMode.

Temporal SQL column types, time zones, constraints, collations, timestamp precision and
provider parameter support remain destination concerns. The codec does not change a
DateTime's Kind or invent a time zone. PostgreSQL's existing parameter adapter also assumes
Int32-backed enums and formats UInt64 values as strings; using other enum backings or unsigned
numeric columns there can require an adapter/model migration, even though the binary codec
can represent those values. These restore compatibility changes do not alter global provider
save behavior. Stored DateTime ticks are compared during readback; SQL engines generally do
not retain DateTime.Kind. DateTimeOffset offsets must survive exactly. Compatible DDL also
means no incompatible precision/length reductions, new uniqueness collisions, invalid foreign
keys, generated-column conflicts, or changed enum/unit meanings. Readback verifies each row's
supplied fields immediately; it does not validate arbitrary application semantics or subsequent
cross-row trigger effects. Defaults omitted from the archive are supplied by the destination.

## Compatibility and verification

Unknown tables now fail by default, supplied NULLs are no longer replaced with CLR defaults,
and constructor-only defaults do not override database defaults. Rebuild callers to pick up
the new optional-argument default for ignoreMissingTables; previously compiled calls can
still explicitly pass the old value. Prefer explicit options for recovery procedures.

Run `dotnet test Figlotech.BDados.Tests/Figlotech.BDados.Tests.csproj --filter FullyQualifiedName~BDadosBackup`.
The suite includes SQLite integration, independent v1 framing tests, and a checked-in v2
archive produced by commit `51600ff5f06476e165b1c9bcf8ca6d504b481389` before these fixes
(`Fixtures/BDadosBackup/v2-row.fgb`, SHA-256
`6A5CAF2E12BD2A4F9ECD7AF0E340BCC3EF36E0B2A51391D193B70DE51AF74B91`). Do not regenerate historical
fixtures with the current writer. Add released archives to the corpus when formats evolve.

Set FIGLOTECH_TEST_POSTGRES or FIGLOTECH_TEST_MYSQL to test-server connection strings to run
provider integration tests. Those tests create and remove uniquely named PostgreSQL schemas
or MySQL databases; use isolated servers with the necessary creation/restore privileges.
Without those settings, the corresponding tests are explicitly skipped. Periodic recovery
drills against representative production-sized backups remain an operational requirement.
